package postgres

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// batchCalls are the two batch statements, as one call each.
func batchCalls(s *Store, ref store.LeaseRef) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"AckMany": func(ctx context.Context) error { return s.AckMany(ctx, []store.LeaseRef{ref})[0] },
		"ExtendLeaseMany": func(ctx context.Context) error {
			return s.ExtendLeaseMany(ctx, []store.Extension{{LeaseRef: ref, Duration: 30 * time.Second}})[0]
		},
	}
}

// silentServer accepts connections and never answers, as a stopped
// container's address does to a dial with no deadline of its own.
func silentServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var mu sync.Mutex
	var held []net.Conn
	t.Cleanup(func() {
		_ = lis.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range held {
			_ = c.Close()
		}
	})
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c)
			mu.Unlock()
		}
	}()
	return lis.Addr().String()
}

// heldPool is a one-connection pool over the test database whose only
// connection is taken for the whole test.
func heldPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.MaxConns = 1
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	c, err := p.Acquire(ctx)
	require.NoError(t, err)
	t.Cleanup(c.Release)
	return p
}

// TestABatchWhoseDialHangsAnswersUnavailableWithinTheBound: a batch's flight
// is shared, so a dial to a database that has gone must free it at the
// acquire bound, not at the 5 s connect timeout that equals the worker's own
// attempt deadline (docs/semantics.md §11.3).
func TestABatchWhoseDialHangsAnswersUnavailableWithinTheBound(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://hopper@" + silentServer(t) + "/hopper?sslmode=disable&connect_timeout=5")
	require.NoError(t, err)
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	ref := store.LeaseRef{ID: uuid.New(), LeaseID: uuid.New()}

	for name, call := range batchCalls(New(p), ref) {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			start := time.Now()
			err := call(ctx)
			took := time.Since(start)
			require.ErrorIs(t, err, store.ErrUnavailable)
			require.Less(t, took, 2*time.Second, "a hung dial frees the batch at the bound, not after %s", took)
		})
	}
}

// TestABatchWithNoConnectionFreeAnswersUnavailableWithinTheBound: a pool
// that can't hand out a connection within the bound gets the same answer,
// and nothing is applied, because nothing was sent.
func TestABatchWithNoConnectionFreeAnswersUnavailableWithinTheBound(t *testing.T) {
	ctx := ctxT(t)
	shared := newStore(t)
	ref := leasedRef(t, ctx, shared, "q")
	s := New(heldPool(t, ctx))

	for name, call := range batchCalls(s, ref) {
		t.Run(name, func(t *testing.T) {
			callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			start := time.Now()
			err := call(callCtx)
			require.ErrorIs(t, err, store.ErrUnavailable)
			require.Less(t, time.Since(start), 2*time.Second)
			require.Equal(t, store.StateLeased, stateOf(t, ctx, shared, ref.ID), "nothing was sent, so nothing was applied")
		})
	}
}

// TestACallerDeadlineInsideTheBoundStaysTheCallers: a caller that stops
// waiting before the bound gets its own deadline back, not UNAVAILABLE
// (errors.go: cancellation and deadlines belong to the caller).
func TestACallerDeadlineInsideTheBoundStaysTheCallers(t *testing.T) {
	ctx := ctxT(t)
	shared := newStore(t)
	ref := leasedRef(t, ctx, shared, "q")
	s := New(heldPool(t, ctx))

	for name, call := range batchCalls(s, ref) {
		t.Run(name, func(t *testing.T) {
			callCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer cancel()
			err := call(callCtx)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NotErrorIs(t, err, store.ErrUnavailable)
		})
	}
}

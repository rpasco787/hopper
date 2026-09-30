package integration

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/client"
	"github.com/rpasco787/hopper/internal/broker"
	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/internal/store/postgres"
	workersdk "github.com/rpasco787/hopper/worker"
)

// outageDB is a database of its own on the shared server, so a test can make
// it unreachable without touching the database every other test uses.
type outageDB struct {
	name string
	pool *pgxpool.Pool // the broker's pool; its sessions are named hopper-outage
}

func outageContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func newOutageDB(t *testing.T) *outageDB {
	t.Helper()
	ctx, cancel := outageContext(t)
	defer cancel()
	db := &outageDB{name: "hopper_outage"}
	_, err := pool.Exec(ctx, "drop database if exists "+db.name+" with (force)")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "create database "+db.name)
	require.NoError(t, err)
	cfg := pool.Config()
	cfg.ConnConfig.Database = db.name
	cfg.ConnConfig.RuntimeParams["application_name"] = "hopper-outage"
	db.pool, err = pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		restoreCtx, restoreCancel := outageContext(t)
		if _, err := pool.Exec(restoreCtx, "alter database "+db.name+" allow_connections true"); err != nil {
			t.Errorf("restore %s during cleanup: %v", db.name, err)
		}
		restoreCancel()
		db.pool.Close()
		dropCtx, dropCancel := outageContext(t)
		defer dropCancel()
		if _, err := pool.Exec(dropCtx, "drop database if exists "+db.name+" with (force)"); err != nil {
			t.Errorf("drop %s during cleanup: %v", db.name, err)
		}
	})
	require.NoError(t, postgres.Migrate(ctx, db.pool))
	return db
}

// refuse makes the database unreachable the way a restart does: new
// connections are refused and every open one is terminated.
func (db *outageDB) refuse(t *testing.T) {
	t.Helper()
	ctx, cancel := outageContext(t)
	defer cancel()
	_, err := pool.Exec(ctx, "alter database "+db.name+" allow_connections false")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "select pg_terminate_backend(pid, 5000) from pg_stat_activity where datname = $1", db.name)
	require.NoError(t, err)
}

func (db *outageDB) restore(t *testing.T) {
	t.Helper()
	ctx, cancel := outageContext(t)
	defer cancel()
	_, err := pool.Exec(ctx, "alter database "+db.name+" allow_connections true")
	require.NoError(t, err)
}

// gatedHandler runs until released, and records how often it started and
// whether its context was ever cancelled.
type gatedHandler struct {
	runs      atomic.Int32
	started   chan struct{}
	release   chan struct{}
	cancelled chan error
}

func newGatedHandler() *gatedHandler {
	return &gatedHandler{started: make(chan struct{}, 1), release: make(chan struct{}), cancelled: make(chan error, 1)}
}

func (h *gatedHandler) handle(ctx context.Context, _ workersdk.Job) error {
	h.runs.Add(1)
	h.started <- struct{}{}
	select {
	case <-h.release:
		return nil
	case <-ctx.Done():
		h.cancelled <- context.Cause(ctx)
		return context.Cause(ctx)
	}
}

// completedOnce waits for the job to complete and checks it did so on its
// first lease, with the handler started once and never cancelled.
func completedOnce(t *testing.T, st store.Store, id uuid.UUID, h *gatedHandler) {
	t.Helper()
	require.Eventually(t, func() bool {
		ctx, cancel := outageContext(t)
		defer cancel()
		row, err := st.GetJob(ctx, id)
		return err == nil && row.State == store.StateCompleted
	}, 20*time.Second, 50*time.Millisecond, "the job never completed")
	ctx, cancel := outageContext(t)
	defer cancel()
	row, err := st.GetJob(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int32(1), row.Attempts, "the job was redelivered")
	require.Equal(t, int32(1), h.runs.Load(), "the handler started more than once")
	select {
	case cause := <-h.cancelled:
		t.Fatalf("the handler was cancelled: %v", cause)
	default:
	}
}

// An Ack that meets a database outage is retried until the database is back,
// so the job completes on its first lease. Before the broker reported the
// outage as UNAVAILABLE, the SDK treated the INTERNAL answer as final, gave
// the delivery up, and the job ran again after its lease expired.
func TestAckRidesOutADatabaseOutage(t *testing.T) {
	ctx := ctxT(t)
	db := newOutageDB(t)
	st := postgres.New(db.pool)
	brokerLog := &lockedBuffer{}
	addr := startBrokerWithConfig(t, st, broker.Config{
		LeaseDuration: 30 * time.Second, PollInterval: 5 * time.Millisecond,
		Logger: slog.New(slog.NewJSONHandler(brokerLog, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	res, err := producerAt(t, addr).Enqueue(ctx, client.Job{Queue: "outage", Payload: json.RawMessage(`{}`)})
	require.NoError(t, err)
	h := newGatedHandler()
	w := workersdk.New(addr, workersdk.Options{
		Concurrency: 1, ShutdownTimeout: 5 * time.Second, Logger: slog.New(slog.DiscardHandler),
	})
	w.Handle("outage", h.handle)
	r := startSDKWorker(t, w)
	select {
	case <-h.started:
	case <-ctx.Done():
		t.Fatal("the handler never started")
	}

	db.refuse(t)
	close(h.release) // the handler returns, and its Ack meets the outage
	require.Eventually(t, func() bool {
		return strings.Contains(brokerLog.String(), `"msg":"rpc unavailable","rpc":"Ack"`)
	}, 10*time.Second, 20*time.Millisecond, "no Ack met the outage as UNAVAILABLE; broker log:\n%s", brokerLog.String())
	time.Sleep(time.Second) // several retries meet it
	db.restore(t)

	completedOnce(t, st, res.ID, h)
	require.NoError(t, r.stop(t))
}

// renewals reports every ExtendLease result, so a test can place an outage
// between two renewals and see the one that met it.
type renewals struct {
	store.Store
	results chan error
}

func (s *renewals) ExtendLease(ctx context.Context, id, leaseID uuid.UUID, d time.Duration) error {
	err := s.Store.ExtendLease(ctx, id, leaseID, d)
	select {
	case s.results <- err:
	default:
	}
	return err
}

// A renewal that meets a database outage is retried at the next interval, so
// a handler whose lease outlives the outage keeps running. Before, the SDK
// read INTERNAL as a rejected heartbeat and cancelled the handler.
func TestHeartbeatRidesOutADatabaseOutage(t *testing.T) {
	ctx := ctxT(t)
	db := newOutageDB(t)
	st := &renewals{Store: postgres.New(db.pool), results: make(chan error, 64)}
	// A 6s lease renews every 2s, counted from when the previous renewal
	// returned, and the SDK stops trusting it 5.5s after the last successful
	// renewal was sent. An outage starting just after a renewal and lasting
	// 2.5s fails the next renewal only, and the one after it, about 4s after
	// the last success, keeps the lease. That assumes the failing renewal
	// returns at once, as it does when the connection is refused; one that
	// took two seconds to fail would spend the lease (worker/window_test.go).
	addr := startBrokerWithConfig(t, st, broker.Config{LeaseDuration: 6 * time.Second, PollInterval: 5 * time.Millisecond})
	res, err := producerAt(t, addr).Enqueue(ctx, client.Job{Queue: "outage", Payload: json.RawMessage(`{}`)})
	require.NoError(t, err)
	h := newGatedHandler()
	w := workersdk.New(addr, workersdk.Options{
		Concurrency: 1, ShutdownTimeout: 5 * time.Second, Logger: slog.New(slog.DiscardHandler),
	})
	w.Handle("outage", h.handle)
	r := startSDKWorker(t, w)
	select {
	case <-h.started:
	case <-ctx.Done():
		t.Fatal("the handler never started")
	}
	// Discard the acquisition's result, then start the outage right after
	// the first periodic renewal succeeds.
	for len(st.results) > 0 {
		<-st.results
	}
	select {
	case err := <-st.results:
		require.NoError(t, err, "the first renewal")
	case <-time.After(10 * time.Second):
		t.Fatal("no renewal")
	}
	db.refuse(t)
	time.Sleep(2500 * time.Millisecond)
	db.restore(t)

	// The outage must have met a renewal, as UNAVAILABLE at the store, and a
	// later renewal must have succeeded.
	var metOutage bool
	deadline := time.After(10 * time.Second)
	for renewed := false; !renewed; {
		select {
		case err := <-st.results:
			switch {
			case errors.Is(err, store.ErrUnavailable):
				metOutage = true
			case err == nil:
				renewed = metOutage
			default:
				t.Fatalf("a renewal failed with something other than unavailability: %v", err)
			}
		case cause := <-h.cancelled:
			t.Fatalf("the handler was cancelled: %v", cause)
		case <-deadline:
			t.Fatalf("no renewal succeeded after the outage (one met it: %v)", metOutage)
		}
	}
	close(h.release)
	completedOnce(t, st, res.ID, h)
	require.NoError(t, r.stop(t))
}

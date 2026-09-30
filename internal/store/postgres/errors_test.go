package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func TestUnavailableClassifiesConnectionFailures(t *testing.T) {
	// A real failed connection attempt: nothing listens on port 1.
	_, connectErr := pgconn.Connect(context.Background(), "postgres://hopper@127.0.0.1:1/hopper?connect_timeout=2")
	require.Error(t, connectErr)
	cfg, err := pgconn.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.AfterConnect = func(context.Context, *pgconn.PgConn) error { return errors.New("after connect failed") }
	_, afterConnectErr := pgconn.ConnectConfig(context.Background(), cfg)
	require.Error(t, afterConnectErr)
	timeoutCfg, err := pgconn.ParseConfig(dsn)
	require.NoError(t, err)
	timeoutCfg.ConnectTimeout = 10 * time.Millisecond
	timeoutCfg.DialFunc = func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	_, connectTimeoutErr := pgconn.ConnectConfig(context.Background(), timeoutCfg)
	require.Error(t, connectTimeoutErr)

	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"connection refused", connectErr, true},
		{"connection setup failed", afterConnectErr, true},
		{"connection timeout with live caller", connectTimeoutErr, true},
		{"session terminated by an administrator", &pgconn.PgError{Code: "57P01"}, true},
		{"server crashing", &pgconn.PgError{Code: "57P02"}, true},
		{"server starting up", &pgconn.PgError{Code: "57P03"}, true},
		{"too many connections", &pgconn.PgError{Code: "53300"}, true},
		{"connection failure", &pgconn.PgError{Code: "08006"}, true},
		{"network error", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, true},
		{"unexpected EOF", fmt.Errorf("failed to receive message: %w", io.ErrUnexpectedEOF), true},
		{"closed connection", fmt.Errorf("conn: %w", pgconn.ErrConnClosed), true},
		{"unique violation", &pgconn.PgError{Code: "23505"}, false},
		{"untranslatable character", &pgconn.PgError{Code: "22P05"}, false},
		{"no rows", pgx.ErrNoRows, false},
		{"caller cancelled", context.Canceled, false},
		{"caller's deadline", fmt.Errorf("timeout: %w", context.DeadlineExceeded), false},
		{"anything else", errors.New("boom"), false},
		{"nil", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, unavailable(tc.err))
			if tc.err != nil {
				require.Equal(t, tc.want, errors.Is(wrap("op", tc.err), store.ErrUnavailable))
				require.ErrorIs(t, wrap("op", tc.err), tc.err, "wrap keeps the driver's error")
			}
		})
	}
}

// Every error the store returns from a database call must go through wrap,
// which is what marks unavailability. A direct fmt.Errorf("store: …: %w")
// would report an outage as INTERNAL, and the SDK would give the delivery up.
func TestEveryStoreErrorGoesThroughWrap(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	direct := regexp.MustCompile(`fmt\.Errorf\("store: [^"]*%w`)
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") || f == "migrate.go" || f == "errors.go" {
			continue
		}
		src, err := os.ReadFile(f)
		require.NoError(t, err)
		for i, line := range strings.Split(string(src), "\n") {
			if direct.MatchString(line) {
				t.Errorf("%s:%d returns a store error without wrap: %s", f, i+1, strings.TrimSpace(line))
			}
		}
	}
}

// A connection PostgreSQL terminated under the store, as a restart does, is
// unavailability: the call did not take effect, and once the pool has
// replaced the connection a retry succeeds.
func TestADroppedConnectionIsUnavailable(t *testing.T) {
	ctx := ctxT(t)
	newStore(t) // truncates
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.MaxConns = 1 // so the next call uses the connection terminated below
	cfg.ConnConfig.RuntimeParams["application_name"] = "hopper-dropped"
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	s := New(p)

	job, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "dropped"})
	require.NoError(t, err)
	leased, err := s.Lease(ctx, []string{"dropped"}, 1, time.Minute, "w")
	require.NoError(t, err)
	require.Len(t, leased, 1)

	// Use the connection again well inside pgxpool's one-second idle ping, so
	// the Ack meets the terminated session rather than a fresh one.
	var terminated bool
	require.NoError(t, pool.QueryRow(ctx, `select pg_terminate_backend(pid, 5000) from pg_stat_activity
	                                       where application_name = 'hopper-dropped'`).Scan(&terminated))
	require.True(t, terminated)
	err = s.Ack(ctx, job.ID, *leased[0].LeaseID)
	require.ErrorIs(t, err, store.ErrUnavailable)
	require.Equal(t, store.StateLeased, readJob(t, job.ID).State, "the failed Ack did not take effect")

	require.NoError(t, s.Ack(ctx, job.ID, *leased[0].LeaseID), "the pool replaced the connection")
	require.Equal(t, store.StateCompleted, readJob(t, job.ID).State)
}

// The database refusing a statement is not unavailability: retrying cannot
// change the answer.
func TestARejectedStatementIsNotUnavailable(t *testing.T) {
	s := newStore(t)
	_, _, err := s.Enqueue(ctxT(t), store.EnqueueParams{Queue: "q", Payload: []byte(`{"x":"\u0000"}`)})
	require.Error(t, err)
	require.NotErrorIs(t, err, store.ErrUnavailable)
}

// Package integration runs the broker against a real Postgres.
//
// It lives outside internal/broker on purpose. That package's tests use a fake
// store so they need no Docker and can inject error paths that are hard to
// provoke for real; this package needs Docker and proves the two halves fit
// together. Keeping them apart makes `go test ./internal/broker` Docker-free;
// `internal/store/postgres` already requires Docker.
package integration

import (
	"context"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/broker"
	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/internal/store/postgres"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	ctr, err := tcpg.Run(ctx, "postgres:16-alpine",
		tcpg.WithDatabase("hopper"),
		tcpg.WithUsername("hopper"),
		tcpg.WithPassword("hopper"),
		tcpg.BasicWaitStrategies(),
	)
	if err != nil {
		log.Fatalf("start postgres: %v", err)
	}

	code := 1
	defer func() {
		if pool != nil {
			pool.Close()
		}
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			log.Printf("terminate container: %v", err)
		}
		os.Exit(code)
	}()

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("connection string: %v", err)
		return
	}
	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		log.Printf("open pool: %v", err)
		return
	}
	if err := postgres.Migrate(ctx, pool); err != nil {
		log.Printf("migrate: %v", err)
		return
	}

	code = m.Run()
}

// newClient serves a real broker over the real store on a loopback port and
// returns a client. Truncating first means each test starts from an empty
// table, so these tests must not call t.Parallel() — the same rule as
// internal/store/postgres, for the same reason.
//
// No sweep loops run: a test that needs the reaper or promoter calls the store
// directly, so it decides exactly when they act.
func newClient(t *testing.T) hopperv1.HopperServiceClient {
	t.Helper()
	return newClientFor(t, postgres.New(pool))
}

// newClientFor is newClient over a caller-supplied store, for tests that need a
// fast retry policy or a store wrapper that injects failures.
func newClientFor(t *testing.T, st store.Store) hopperv1.HopperServiceClient {
	t.Helper()

	conn, err := grpc.NewClient(startBroker(t, st),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return hopperv1.NewHopperServiceClient(conn)
}

// startBroker truncates the jobs table, serves a broker over st on a loopback
// port and returns the address. Shutdown runs at cleanup and must succeed, so
// every test also proves the broker drains with whatever streams it left open.
func startBroker(t *testing.T, st store.Store) string {
	t.Helper()

	_, err := pool.Exec(context.Background(), "truncate jobs, cron_schedules")
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := broker.NewServer(st, broker.Config{
		LeaseDuration: 30 * time.Second,
		PollInterval:  10 * time.Millisecond,
	})
	go func() { _ = srv.Serve(lis) }()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, srv.Shutdown(ctx))
	})
	return lis.Addr().String()
}

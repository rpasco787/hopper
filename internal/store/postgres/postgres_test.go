package postgres

import (
	"context"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// One Postgres container serves every test in this package: startup costs
// seconds, truncating between tests costs milliseconds.
//
// The consequence is that tests in this package must NOT call t.Parallel().
// They share one database and newStore truncates it, so a parallel test would
// delete another test's rows out from under it. If a future test really needs
// isolation, give it its own schema rather than its own container.
var pool *pgxpool.Pool

// dsn is the connection string for the shared container, kept so that a test
// can open a second pool against a different database on the same server.
var dsn string

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

	// code is set by the deferred func so that every cleanup below still runs
	// if m.Run panics or a helper calls log.Fatal.
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

	dsn, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("connection string: %v", err)
		return
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Printf("parse config: %v", err)
		return
	}
	// The lease concurrency test runs 50 goroutines at once. With pgx's small
	// default the goroutines would queue on the pool and the test would prove
	// nothing about Postgres row locking, so give each one its own connection.
	cfg.MaxConns = 60

	pool, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Printf("open pool: %v", err)
		return
	}

	if err := Migrate(ctx, pool); err != nil {
		log.Printf("migrate: %v", err)
		return
	}

	code = m.Run()
}

// newStore returns a Store over the shared pool with an empty jobs table.
func newStore(t *testing.T) *Store {
	t.Helper()
	_, err := pool.Exec(context.Background(), "truncate jobs, cron_schedules")
	require.NoError(t, err)
	return New(pool)
}

// ctxT returns a context that is cancelled when the test ends, so a hung query
// fails the test instead of blocking the package.
func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestMigrateIsIdempotent(t *testing.T) {
	ctx := ctxT(t)

	// TestMain already migrated. A second run must be a no-op, not an error,
	// because every process calls Migrate on startup.
	require.NoError(t, Migrate(ctx, pool))

	var n int
	require.NoError(t, pool.QueryRow(ctx,
		"select count(*) from schema_migrations").Scan(&n))
	require.Equal(t, 1, n, "second Migrate must not re-record the migration")
}

func TestMigrateCreatedJobsTable(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	require.NotNil(t, s)

	// The partial indexes are load-bearing for Lease, the promoter and the
	// reaper, so assert they exist rather than just the table.
	rows, err := pool.Query(ctx,
		`select indexname from pg_indexes where tablename = 'jobs' order by indexname`)
	require.NoError(t, err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		got = append(got, name)
	}
	require.NoError(t, rows.Err())

	require.Equal(t, []string{
		"jobs_dead_idx",
		"jobs_idem_uidx",
		"jobs_lease_idx",
		"jobs_pkey",
		"jobs_promote_idx",
		"jobs_reap_idx",
	}, got)
}

// TestMigrateIsSafeUnderConcurrentStartup pins the claim in Migrate's doc
// comment: a whole worker fleet can boot at once and every process can call
// Migrate without one of them dying on a duplicate key or an "already exists"
// error. Without the advisory lock this test fails.
//
// It needs a database nobody has migrated yet, so it creates one rather than
// using the shared one TestMain already migrated.
func TestMigrateIsSafeUnderConcurrentStartup(t *testing.T) {
	ctx := ctxT(t)

	// create database cannot run inside a transaction block.
	_, err := pool.Exec(ctx, "create database migrate_race")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(), "drop database if exists migrate_race (force)")
		require.NoError(t, err)
	})

	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.ConnConfig.Database = "migrate_race"
	cfg.MaxConns = 20

	racePool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer racePool.Close()

	const migrators = 10
	errs := make([]error, migrators)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range migrators {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = Migrate(ctx, racePool)
		}(i)
	}
	close(start) // release them all at once
	wg.Wait()

	for i, err := range errs {
		require.NoErrorf(t, err, "migrator %d failed", i)
	}

	var n int
	require.NoError(t, racePool.QueryRow(ctx,
		"select count(*) from schema_migrations").Scan(&n))
	require.Equal(t, 1, n, "the migration must be recorded exactly once")
}

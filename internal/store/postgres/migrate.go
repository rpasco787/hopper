package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrateLockKey is an arbitrary but fixed application-chosen id for the
// advisory lock that serialises migration runs. Any process that migrates this
// database must use this same number; nothing else may use it.
const migrateLockKey int64 = 7_243_015_119_421_077

// Migrate applies every embedded migrations/*.sql file, in filename order, that
// is not already recorded in schema_migrations. It is safe to call on every
// process start, and safe to call from several processes at once.
//
// Each file is applied in its own transaction together with its
// schema_migrations row, so a migration that fails part way leaves no trace and
// is retried on the next start. Postgres has transactional DDL, which is what
// makes that possible; on MySQL this design would not work.
//
// Concurrency: a whole worker fleet may boot at once and every process calls
// Migrate. Without coordination two processes could both see a migration as
// unapplied and both try to run it, and the loser would die on a duplicate key
// or a "relation already exists" error. Each transaction therefore takes a
// transaction-scoped advisory lock first: the second process blocks until the
// first commits, then reads schema_migrations and correctly skips the file.
// The lock is released automatically on commit or rollback, which is why it is
// pg_advisory_xact_lock and not pg_advisory_lock; a session-level lock taken on
// one pooled connection could not be released reliably on another.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if err := ensureMigrationsTable(ctx, pool); err != nil {
		return err
	}

	names, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("store: list migrations: %w", err)
	}
	// Glob order is unspecified. Filenames are zero-padded (001_, 002_) so a
	// plain lexical sort is also numeric order.
	sort.Strings(names)

	for _, name := range names {
		if err := applyOne(ctx, pool, name); err != nil {
			return err
		}
	}
	return nil
}

// ensureMigrationsTable creates the bookkeeping table. It takes the same
// advisory lock as applyOne because "create table if not exists" is itself
// racy: two concurrent sessions can both pass the existence check and one then
// fails on a duplicate catalogue entry.
func ensureMigrationsTable(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: migrate begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once Commit has succeeded

	if _, err := tx.Exec(ctx, `select pg_advisory_xact_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("store: migrate lock: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		create table if not exists schema_migrations (
			version    text primary key,
			applied_at timestamptz not null default now()
		)`); err != nil {
		return fmt.Errorf("store: create schema_migrations: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: create schema_migrations commit: %w", err)
	}
	return nil
}

// applyOne runs a single migration file if it has not been recorded yet. The
// check and the apply share one transaction and one advisory lock, so the
// decision cannot go stale between them.
func applyOne(ctx context.Context, pool *pgxpool.Pool, name string) error {
	sqlBytes, err := migrationFS.ReadFile(name)
	if err != nil {
		return fmt.Errorf("store: read %s: %w", name, err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: migrate %s: begin: %w", name, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once Commit has succeeded

	if _, err := tx.Exec(ctx, `select pg_advisory_xact_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("store: migrate %s: lock: %w", name, err)
	}

	var applied bool
	if err := tx.QueryRow(ctx,
		`select exists (select 1 from schema_migrations where version = $1)`, name,
	).Scan(&applied); err != nil {
		return fmt.Errorf("store: migrate %s: check: %w", name, err)
	}
	if applied {
		return nil // deferred Rollback releases the lock
	}

	if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
		return fmt.Errorf("store: migrate %s: apply: %w", name, err)
	}
	if _, err := tx.Exec(ctx,
		`insert into schema_migrations (version) values ($1)`, name); err != nil {
		return fmt.Errorf("store: migrate %s: record: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: migrate %s: commit: %w", name, err)
	}
	return nil
}

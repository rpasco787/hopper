// Package postgres implements store.Store on PostgreSQL using pgx.
//
// This is the only package in hopper that contains SQL. Brokers, workers and
// the SDK program against the store.Store interface, so swapping the backend
// means adding a package here, not touching callers. Behaviour is specified in
// docs/semantics.md; that document is authoritative when it and this code
// disagree.
package postgres

import (
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rpasco787/hopper/internal/store"
)

// Store is the Postgres-backed store.Store. It is safe for concurrent use: it
// holds no mutable state of its own, and pgxpool.Pool is itself concurrent.
type Store struct {
	pool  *pgxpool.Pool
	retry store.RetryPolicy
}

// Compile-time proof that *Store satisfies the interface. This is what turns a
// signature drift in store.Store into a build failure here rather than a
// runtime surprise at the call site.
var _ store.Store = (*Store)(nil)

// jobColumns is the one place the jobs column list is spelled out. Every
// RETURNING and SELECT that produces a store.Job uses it, so a column added to
// the table is added here once and store.Job's db tags are the only other
// thing that has to agree. pgx.RowToStructByName matches by name, not
// position, so order here does not matter; it mirrors the schema for
// readability.
const jobColumns = `id, queue, payload, state, priority, run_at, attempts, max_attempts,
	lease_id, leased_by, leased_at, lease_expires, idempotency_key, last_error,
	created_at, updated_at, finished_at`

// qualify prefixes every column in list with alias, turning "id, queue" into
// "j.id, j.queue". UPDATE ... FROM needs it because the RETURNING clause sees
// both the target table and the CTE, and bare names would be ambiguous.
func qualify(alias, list string) string {
	cols := strings.Split(list, ",")
	for i, c := range cols {
		cols[i] = alias + "." + strings.TrimSpace(c)
	}
	return strings.Join(cols, ", ")
}

// New returns a Store over an existing pool, using the default retry policy.
// The caller owns the pool and is responsible for closing it.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, retry: store.DefaultRetryPolicy()}
}

// WithRetryPolicy returns a copy of s that uses p for retry backoff. It copies
// rather than mutating so that handing a Store to one component with a shorter
// backoff cannot change the policy another component already holds. Tests use
// it to avoid waiting out a real exponential delay.
func (s *Store) WithRetryPolicy(p store.RetryPolicy) *Store {
	clone := *s
	clone.retry = p
	return &clone
}

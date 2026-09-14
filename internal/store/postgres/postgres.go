// Package postgres implements store.Store on PostgreSQL using pgx.
//
// This is the only package in hopper that contains SQL. Brokers, workers and
// the SDK program against the store.Store interface, so swapping the backend
// means adding a package here, not touching callers. Behaviour is specified in
// docs/semantics.md; that document is authoritative when it and this code
// disagree.
package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
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

// --- temporary stubs: each is replaced by its own file in Tasks 7 to 11 ---
// They exist only so the interface assertion above compiles while the store is
// being built out. Panicking is deliberate: a stub that returned a zero value
// could be mistaken for working code.

func (s *Store) Enqueue(ctx context.Context, p store.EnqueueParams) (store.Job, error) {
	panic("store/postgres: Enqueue not implemented (Task 7)")
}

func (s *Store) Lease(ctx context.Context, queues []string, n int, leaseDur time.Duration, workerID string) ([]store.Job, error) {
	panic("store/postgres: Lease not implemented (Task 8)")
}

func (s *Store) Ack(ctx context.Context, id, leaseID uuid.UUID) error {
	panic("store/postgres: Ack not implemented (Task 10)")
}

func (s *Store) Nack(ctx context.Context, id, leaseID uuid.UUID, errMsg string) error {
	panic("store/postgres: Nack not implemented (Task 11)")
}

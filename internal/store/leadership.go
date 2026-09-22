package store

import (
	"context"
	"errors"
)

// ErrNotLeader means another process holds the leadership lock. It is an
// ordinary outcome of contention, not a failure: a broker that loses the
// election retries on its next interval.
var ErrNotLeader = errors.New("store: leadership held elsewhere")

// Leadership is exclusive possession of the singleton scheduler role, held
// for as long as the underlying database session lives. It is mutual
// exclusion, not a fencing token: a process that stalls can still believe it
// leads after the database has given the role away. Safety under that race
// comes from the scheduler's idempotency keys and its guarded advance
// (docs/semantics.md §13), never from this interface.
type Leadership interface {
	// Check reports whether leadership is still held. A definitive loss and
	// an ambiguous failure both return an error, because stepping down is
	// always safe and continuing without the lock is not.
	Check(ctx context.Context) error

	// Release relinquishes leadership. It always gives up the role, even when
	// it returns an error: a session that might still hold the lock is
	// destroyed rather than reused. Calling it twice is safe.
	Release(ctx context.Context) error
}

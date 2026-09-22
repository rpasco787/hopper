package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rpasco787/hopper/internal/store"
)

// schedulerLockKey is the advisory-lock id for the singleton scheduler role.
// Advisory locks are per database, so every hopper process pointed at this
// database contends on this number, and nothing else may use it. The other
// allocation is migrateLockKey in migrate.go; keep both listed here so a
// third is not chosen by accident.
const schedulerLockKey int64 = 7_243_015_119_421_078

// discard removes conn from the pool and closes it. Use it whenever the
// session may still hold the advisory lock: returning such a connection to
// the pool would hand an unrelated caller a session that silently holds the
// scheduler role, and the lock would not be released until that connection
// was recycled for reasons of its own. Closing the session is what makes
// PostgreSQL drop the lock.
func discard(conn *pgxpool.Conn) {
	raw := conn.Hijack()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = raw.Close(ctx)
}

// TryLeadership attempts to become the single active scheduler by taking a
// session-scoped advisory lock on a connection held out of the pool for the
// whole term (docs/plans decision 1). migrate.go uses the transaction-scoped
// form deliberately because that lock must not survive a commit; leadership
// needs the opposite property, a lock that outlives every transaction, which
// is why it is pinned to one session instead.
func (s *Store) TryLeadership(ctx context.Context) (store.Leadership, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: leadership: acquire connection: %w", err)
	}
	var won bool
	if err := conn.QueryRow(ctx, `select pg_try_advisory_lock($1)`, schedulerLockKey).Scan(&won); err != nil {
		// The server may have taken the lock even though we failed to read
		// the answer, so this session cannot go back in the pool.
		discard(conn)
		return nil, fmt.Errorf("store: leadership: %w", err)
	}
	if !won {
		conn.Release() // no lock was taken; the session is clean
		return nil, store.ErrNotLeader
	}
	return &leadership{conn: conn}, nil
}

// leadership is a held advisory lock, pinned to one *pgxpool.Conn for its
// whole term.
type leadership struct {
	conn *pgxpool.Conn

	mu       sync.Mutex
	released bool
}

// Check is a liveness probe, not a re-query of pg_locks: a session advisory
// lock is released only by an explicit unlock or the session ending, and
// nothing in this process unlocks it except Release, so a live connection
// proves the lock is still held (docs/plans decision 2). An ambiguous
// failure — a context deadline, a network timeout — is treated the same as a
// definitive loss: stepping down is always safe, continuing as a leader that
// no longer holds the lock is not.
func (l *leadership) Check(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return store.ErrNotLeader
	}
	var one int
	if err := l.conn.QueryRow(ctx, `select 1`).Scan(&one); err != nil {
		return fmt.Errorf("store: leadership check: %w", err)
	}
	return nil
}

// Release relinquishes leadership. It always gives up the role, even when it
// returns an error: the connection is destroyed rather than returned to the
// pool whenever the unlock could not be confirmed, because a returned
// connection that still held the lock would hand an unrelated caller a
// session that silently holds the scheduler role. Calling it twice is safe.
func (l *leadership) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	l.released = true

	var unlocked bool
	err := l.conn.QueryRow(ctx, `select pg_advisory_unlock($1)`, schedulerLockKey).Scan(&unlocked)
	switch {
	case err != nil:
		discard(l.conn)
		return fmt.Errorf("store: leadership release: %w", err)
	case !unlocked:
		// This session did not hold the lock, which should be impossible;
		// destroy it rather than trust its state any further.
		discard(l.conn)
		return fmt.Errorf("store: leadership release: session did not hold the lock")
	}
	l.conn.Release()
	return nil
}

package postgres

import (
	"context"

	"github.com/rpasco787/hopper/internal/store"
)

// availableChannel is the NOTIFY channel announcements travel on. The payload
// is a queue name.
const availableChannel = "hopper_available"

var _ store.Wakeups = (*Store)(nil)

// Announce sends one notification per queue, in one statement. PostgreSQL
// delivers a notification only when its transaction commits, to every session
// listening on the channel, including this process's own listener.
//
// Every NOTIFY briefly takes a lock that serializes committing notifiers
// across the whole server, so callers should announce in batches, not once
// per job; the broker sends one Announce at a time and coalesces whatever
// arrives meanwhile (internal/broker, wakeups).
func (s *Store) Announce(ctx context.Context, queues []string) error {
	if len(queues) == 0 {
		return nil
	}
	if _, err := s.pool.Exec(ctx,
		`select pg_notify($1, q) from unnest($2::text[]) as q`, availableChannel, queues); err != nil {
		return wrap("announce", err)
	}
	return nil
}

// Listen holds one connection from the pool, LISTENs on it, and reports every
// notification until ctx ends or the session fails.
//
// The connection never goes back to the pool. A session that has run LISTEN
// keeps receiving notifications after it is released, and a later borrower
// would find them queued on its connection; and a WaitForNotification that
// ctx interrupted leaves the connection in no state to reuse. So it is
// closed, as leadership closes a session that may still hold its lock.
func (s *Store) Listen(ctx context.Context, wake func(queue string)) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return wrap("listen: acquire connection", err)
	}
	defer discard(conn)
	if _, err := conn.Exec(ctx, "listen "+availableChannel); err != nil {
		return wrap("listen", err)
	}
	wake("")
	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return wrap("listen", err)
		}
		wake(n.Payload)
	}
}

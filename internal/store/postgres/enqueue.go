package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rpasco787/hopper/internal/store"
)

// defaultMaxAttempts is the dead-letter threshold when EnqueueParams leaves
// MaxAttempts at zero (docs/semantics.md §6).
const defaultMaxAttempts int32 = 5

// Enqueue inserts one job and returns the stored row.
//
// The initial state is decided in SQL, not Go: the row is 'available' when
// run_at <= now() by the database's clock and 'scheduled' otherwise. The
// promoter and the lease query both trust the state column and both run on
// the database's clock, so letting the application clock choose the state
// would let a skewed app server enqueue an 'available' job whose run_at is in
// the database's future, breaking the "due-ness is a state" invariant in
// docs/semantics.md §4. A zero RunAt is passed as NULL and coalesced to now()
// for the same reason.
//
// Dedup (docs/semantics.md §8). The conflict target repeats jobs_idem_uidx's
// WHERE clause because it is a partial index: without the predicate Postgres
// cannot infer which index to arbitrate on and the statement is rejected. A
// row with a null key is not in that index, so ON CONFLICT never fires for it.
//
// The lookup has to be a second statement. Folding it in as a CTE —
//
//	with ins as (insert ... on conflict do nothing returning ...)
//	select ... from ins
//	union all select ... from jobs where queue = $2 and idempotency_key = $7
//
// reads better and is wrong: every part of one statement sees one snapshot,
// taken before the concurrent inserter committed, so the fallback branch would
// find no row and Enqueue would return nothing. A separate statement takes a
// fresh snapshot, and by then the row we deferred to is certainly committed:
// ON CONFLICT DO NOTHING does not race an in-flight conflicting insert, it
// waits for that transaction to commit or abort and then takes the do-nothing
// path. That guarantee is a READ COMMITTED one, which is the pool's isolation
// level; under REPEATABLE READ the same wait can surface as a serialization
// failure instead.
//
// The loop covers one narrow case: the row we deferred to is deleted by
// retention or Purge in the microseconds between the insert and the lookup. The
// key is free again, so the insert can now succeed. Three passes is already
// paranoid; the bound is there so a pathological purge loop cannot spin here
// forever.
func (s *Store) Enqueue(ctx context.Context, p store.EnqueueParams) (store.Job, bool, error) {
	if p.Queue == "" {
		return store.Job{}, false, errors.New("store: enqueue: queue is required")
	}

	payload := p.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	maxAttempts := p.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = defaultMaxAttempts
	}
	// nil, not the zero time.Time, so the SQL can coalesce it to now().
	var runAt *time.Time
	if !p.RunAt.IsZero() {
		runAt = &p.RunAt
	}

	for range 3 {
		// UUIDv7 is time-ordered, so consecutive inserts land at the right edge
		// of the primary-key B-tree instead of splitting random pages the way
		// v4 does. Generated inside the loop so a second pass is a genuinely
		// new row rather than a retry of an id the first pass already used.
		id, err := uuid.NewV7()
		if err != nil {
			return store.Job{}, false, fmt.Errorf("store: enqueue: generate id: %w", err)
		}

		// The ::timestamptz cast on $6 is needed because Postgres cannot infer
		// the parameter's type from a bare comparison against now() inside CASE.
		rows, err := s.pool.Query(ctx, `
			insert into jobs (id, queue, payload, priority, max_attempts, run_at, idempotency_key, state)
			values (
				$1, $2, $3, $4, $5,
				coalesce($6::timestamptz, now()),
				$7,
				case when coalesce($6::timestamptz, now()) <= now() then 'available' else 'scheduled' end
			)
			on conflict (queue, idempotency_key) where idempotency_key is not null do nothing
			returning `+jobColumns,
			id, p.Queue, payload, p.Priority, maxAttempts, runAt, p.IdempotencyKey,
		)
		if err != nil {
			return store.Job{}, false, fmt.Errorf("store: enqueue: %w", err)
		}
		job, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[store.Job])
		switch {
		case err == nil:
			return job, true, nil
		case !errors.Is(err, pgx.ErrNoRows):
			return store.Job{}, false, fmt.Errorf("store: enqueue: %w", err)
		}

		// Zero rows means DO NOTHING fired, so a row with this (queue, key) is
		// committed. New statement, new snapshot, so we can see it.
		rows, err = s.pool.Query(ctx,
			`select `+jobColumns+` from jobs where queue = $1 and idempotency_key = $2`,
			p.Queue, p.IdempotencyKey)
		if err != nil {
			return store.Job{}, false, fmt.Errorf("store: enqueue: lookup: %w", err)
		}
		existing, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[store.Job])
		switch {
		case err == nil:
			return existing, false, nil
		case !errors.Is(err, pgx.ErrNoRows):
			return store.Job{}, false, fmt.Errorf("store: enqueue: lookup: %w", err)
		}
		// Deleted under us. Go round again.
	}
	return store.Job{}, false, fmt.Errorf(
		"store: enqueue: queue %q: the conflicting row for this idempotency key was deleted three times mid-insert",
		p.Queue)
}

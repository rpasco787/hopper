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
// A duplicate idempotency key currently surfaces as the raw unique-violation
// error from Postgres. Session 3 replaces that with the ON CONFLICT dedup
// described in docs/semantics.md §8.
func (s *Store) Enqueue(ctx context.Context, p store.EnqueueParams) (store.Job, error) {
	if p.Queue == "" {
		return store.Job{}, errors.New("store: enqueue: queue is required")
	}

	// UUIDv7 is time-ordered, so consecutive inserts land at the right edge of
	// the primary-key B-tree instead of splitting random pages the way v4 does.
	id, err := uuid.NewV7()
	if err != nil {
		return store.Job{}, fmt.Errorf("store: enqueue: generate id: %w", err)
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

	// The ::timestamptz cast on $6 is needed because Postgres cannot infer the
	// parameter's type from a bare comparison against now() inside CASE.
	rows, err := s.pool.Query(ctx, `
		insert into jobs (id, queue, payload, priority, max_attempts, run_at, idempotency_key, state)
		values (
			$1, $2, $3, $4, $5,
			coalesce($6::timestamptz, now()),
			$7,
			case when coalesce($6::timestamptz, now()) <= now() then 'available' else 'scheduled' end
		)
		returning `+jobColumns,
		id, p.Queue, payload, p.Priority, maxAttempts, runAt, p.IdempotencyKey,
	)
	if err != nil {
		return store.Job{}, fmt.Errorf("store: enqueue: %w", err)
	}
	job, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[store.Job])
	if err != nil {
		return store.Job{}, fmt.Errorf("store: enqueue: %w", err)
	}
	return job, nil
}

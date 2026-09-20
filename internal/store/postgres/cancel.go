package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rpasco787/hopper/internal/store"
)

// Cancel is the transition table in docs/semantics.md §10.
//
// One guarded UPDATE, like Ack: the WHERE names the three non-terminal states
// and the SET is the swap, so a job that reached a terminal state first is
// left alone and we report which state that was.
//
// This deliberately does not go through classifyMiss. That function answers
// "what happened to my lease?" and is keyed on lease_id; Cancel has no lease.
// It is a client-side operation — whoever enqueued a job may cancel it, and no
// token is required — so its miss has a different vocabulary: not found, or
// already terminal.
//
// lease_id, leased_by and lease_expires are left exactly as they are. That is
// what lets the holding worker's next Ack, Nack or Heartbeat land on
// classifyMiss and come back ErrCancelled rather than ErrLeaseLost, which is
// the difference between "stop, this was called off" and "stop, someone else
// has it now".
func (s *Store) Cancel(ctx context.Context, id uuid.UUID) (store.Job, error) {
	rows, err := s.pool.Query(ctx, `
		update jobs
		set state = 'cancelled', finished_at = now(), updated_at = now()
		where id = $1 and state in ('scheduled', 'available', 'leased')
		returning `+jobColumns,
		id)
	if err != nil {
		return store.Job{}, fmt.Errorf("store: cancel: %w", err)
	}
	job, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[store.Job])
	switch {
	case err == nil:
		return job, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return store.Job{}, fmt.Errorf("store: cancel: %w", err)
	}

	var state store.State
	err = s.pool.QueryRow(ctx, `select state from jobs where id = $1`, id).Scan(&state)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return store.Job{}, store.ErrNotFound
	case err != nil:
		return store.Job{}, fmt.Errorf("store: cancel: classify: %w", err)
	}
	if !store.Terminal(state) {
		return store.Job{}, fmt.Errorf("store: cancel: job %s changed state concurrently (now %s); retry", id, state)
	}
	return store.Job{}, &store.TerminalStateError{State: state}
}

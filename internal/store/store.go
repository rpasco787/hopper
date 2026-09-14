// Package store defines the job record and the Store interface that the rest
// of hopper programs against. All SQL lives in a backend package
// (internal/store/postgres); brokers and workers never touch the database
// directly. Behaviour is specified in docs/semantics.md; that file is
// authoritative when it and the code disagree.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// State is a job's lifecycle state. Transitions are shown in
// docs/semantics.md §2. A row is in exactly one state, and every transition is
// a single UPDATE guarded by a WHERE state = ... clause.
type State string

const (
	// StateScheduled: not yet due. Either a delayed enqueue or a retry waiting
	// out its backoff; run_at says when. Not leasable.
	StateScheduled State = "scheduled"
	// StateAvailable: due and waiting for a worker. The only leasable state.
	StateAvailable State = "available"
	// StateLeased: held by exactly one worker until lease_expires.
	StateLeased State = "leased"
	// StateCompleted: acked by the worker that held the lease. Terminal.
	StateCompleted State = "completed"
	// StateCancelled: cancelled by a client before completion. Terminal.
	StateCancelled State = "cancelled"
	// StateDead: exhausted max_attempts. Kept for inspection; only Redrive
	// moves it. Terminal until redriven.
	StateDead State = "dead"
)

// Errors returned by Ack and Nack (and, from Session 2, ExtendLease) when the
// guarded UPDATE touches zero rows. The mapping from row state to error is the
// table in docs/semantics.md §5. Callers compare with errors.Is.
var (
	// ErrNotFound: no job with that id exists.
	ErrNotFound error = errors.New("Not found")
	// ErrLeaseLost: the presented lease_id no longer holds the job. The lease
	// expired and the reaper took the job back, possibly re-leasing it to
	// another worker. The caller must stop working on it.
	ErrLeaseLost error = errors.New("Lease lost")
	// ErrCancelled: the job was cancelled while leased. The SDK cancels the
	// handler's context on this error; side effects already performed are not
	// undone.
	ErrCancelled error = errors.New("Cancelled")
)

// Terminal reports whether s is a state that no automatic process ever leaves.
// Dead is terminal in this sense even though Redrive can move it, because
// Redrive is an operator action, not part of the job lifecycle.
func Terminal(s State) bool {
	switch s {
	case StateCompleted, StateCancelled, StateDead:
		return true
	}
	return false
}

// Job is a row of the jobs table. Field order and db tags match the schema so
// pgx.RowToStructByName can scan a RETURNING clause directly; adding a column
// means one migration and one field here. Column semantics are in
// docs/semantics.md §3. Nullable columns are pointers; nil means SQL NULL.
type Job struct {
	// ID is a client-generated UUIDv7 so the primary-key index appends in
	// time order rather than fragmenting.
	ID uuid.UUID `db:"id"`
	// Queue is the handler name. Workers subscribe to a set of queues.
	Queue string `db:"queue"`
	// Payload is opaque to hopper. It must be valid JSON; hopper never reads it.
	Payload json.RawMessage `db:"payload"`
	State   State           `db:"state"`
	// Priority: higher leases first within a queue. Best-effort; can starve.
	Priority int16 `db:"priority"`
	// RunAt is the earliest time the job may be leased. Set by Enqueue
	// (delay) and by retry backoff.
	RunAt time.Time `db:"run_at"`
	// Attempts is the number of leases ever granted, incremented at lease
	// time so a worker that crashes still consumes an attempt. It is 1 on the
	// first delivery.
	Attempts int32 `db:"attempts"`
	// MaxAttempts is the dead-letter threshold. A delivery that fails when
	// Attempts >= MaxAttempts sends the job to StateDead.
	MaxAttempts int32 `db:"max_attempts"`
	// LeaseID identifies one delivery. Generated per row at lease time,
	// overwritten by the next lease, never cleared. Every Ack/Nack must
	// present it; keeping it after completion is what makes re-ack idempotent.
	LeaseID *uuid.UUID `db:"lease_id"`
	// LeasedBy and LeasedAt record which worker holds or held the lease and
	// since when. Observability only; never used for authorization.
	LeasedBy *string    `db:"leased_by"`
	LeasedAt *time.Time `db:"leased_at"`
	// LeaseExpires is the visibility timeout. Past this instant the reaper
	// may take the job back.
	LeaseExpires *time.Time `db:"lease_expires"`
	// IdempotencyKey is optional and unique per (queue, key) among rows that
	// exist. Protection lasts exactly as long as the row does.
	IdempotencyKey *string `db:"idempotency_key"`
	// LastError is the message from the most recent Nack, or "lease expired"
	// if the reaper took the job.
	LastError *string   `db:"last_error"`
	CreatedAt time.Time `db:"created_at"`
	// UpdatedAt is set explicitly in every UPDATE; there is no trigger.
	UpdatedAt time.Time `db:"updated_at"`
	// FinishedAt is set when the job enters completed, cancelled, or dead.
	FinishedAt *time.Time `db:"finished_at"`
}

// EnqueueParams is the caller-supplied part of a job. Zero values take the
// defaults noted on each field; everything else on Job is set by the store.
type EnqueueParams struct {
	// Queue is required.
	Queue string
	// Payload must be valid JSON. Empty is stored as {}.
	Payload json.RawMessage
	// Priority defaults to 0.
	Priority int16
	// RunAt defaults to now. A future value enqueues as StateScheduled.
	RunAt time.Time
	// MaxAttempts defaults to 5.
	MaxAttempts int32
	// IdempotencyKey is optional. A duplicate (Queue, key) returns the
	// existing job rather than inserting a second row.
	IdempotencyKey *string
}

// Store is the persistence contract. Every method takes a context for
// cancellation and timeouts. Implementations must make each state transition
// a single guarded UPDATE so a lost race updates zero rows and is reported,
// never silently overwritten. Session 1 covers these four methods; Session 2
// adds ReapExpiredLeases, ExtendLease, ListDead, Redrive, and Purge.
type Store interface {
	// Enqueue inserts one job and returns the stored row, including the
	// generated ID and the state the store chose from RunAt.
	Enqueue(ctx context.Context, p EnqueueParams) (Job, error)

	// Lease atomically takes up to n available jobs from the given queues,
	// ordered by priority desc then run_at, marks them leased by workerID
	// for leaseDur, increments Attempts, and returns them. Concurrent calls
	// never return the same job (FOR UPDATE SKIP LOCKED). An empty result is
	// not an error.
	Lease(ctx context.Context, queues []string, n int, leaseDur time.Duration, workerID string) ([]Job, error)

	// Ack marks a leased job completed. leaseID must be the token returned
	// by the Lease that delivered it; a stale token gets ErrLeaseLost. Acking
	// an already-completed job with the same leaseID returns nil, so the SDK
	// can safely retry an Ack whose response was lost.
	Ack(ctx context.Context, id, leaseID uuid.UUID) error

	// Nack reports a failed attempt and records errMsg in LastError. The
	// same leaseID guard as Ack applies. The job is rescheduled with
	// exponential backoff, or moved to StateDead when
	// Attempts >= MaxAttempts (docs/semantics.md §6). Attempts is not
	// changed; it was counted at lease time.
	Nack(ctx context.Context, id, leaseID uuid.UUID, errMsg string) error
}

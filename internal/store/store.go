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
	"fmt"
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

// Errors returned by Ack, Nack and ExtendLease when the
// guarded UPDATE touches zero rows. The mapping from row state to error is the
// table in docs/semantics.md §5. Callers compare with errors.Is.
var (
	// ErrNotFound: no job with that id exists.
	ErrNotFound error = errors.New("store: job not found")
	// ErrLeaseLost: the presented lease_id no longer holds the job. The lease
	// expired and the reaper took the job back, possibly re-leasing it to
	// another worker. The caller must stop working on it.
	ErrLeaseLost error = errors.New("store: lease lost")
	// ErrCancelled: the job was cancelled while leased. The SDK cancels the
	// handler's context on this error; side effects already performed are not
	// undone.
	ErrCancelled error = errors.New("store: job cancelled")
	// ErrTerminal: the job is already in a terminal state, so the requested
	// transition is impossible. Cancel returns it wrapped in a
	// *TerminalStateError carrying that state; errors.Is is the cheap check and
	// errors.As is how a caller reads which state blocked it.
	ErrTerminal error = errors.New("store: job already terminal")
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

// QueueDepth is how many jobs one queue holds in one state at one instant.
type QueueDepth struct {
	Queue string `db:"queue"`
	State State  `db:"state"`
	Count int64  `db:"count"`
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

// DeadCursor is a keyset-pagination position for ListDead: the (FinishedAt,
// ID) of the last job on the previous page. Keyset rather than OFFSET so a
// page stays correct while rows are being redriven or purged underneath it.
type DeadCursor struct {
	FinishedAt time.Time
	ID         uuid.UUID
}

// CronSchedule is a row of the cron_schedules table. One row produces at most
// one job per tick; the scheduler is described in docs/semantics.md §13.
type CronSchedule struct {
	// Name is the primary key and the first half of every tick's idempotency
	// key, so renaming a schedule starts a new dedup lineage.
	Name  string `db:"name"`
	Queue string `db:"queue"`
	// Payload is copied verbatim into every job this schedule enqueues.
	Payload  json.RawMessage `db:"payload"`
	CronExpr string          `db:"cron_expr"`
	// Timezone is an IANA name resolved with time.LoadLocation. cmd/broker
	// embeds time/tzdata because the distroless image has no zoneinfo.
	Timezone    string `db:"timezone"`
	Enabled     bool   `db:"enabled"`
	Priority    int16  `db:"priority"`
	MaxAttempts int32  `db:"max_attempts"`
	// NextRunAt is the pending tick: the instant this schedule is next due,
	// and the second half of that tick's idempotency key. It advances only
	// through AdvanceSchedule, guarded on its current value.
	NextRunAt time.Time `db:"next_run_at"`
	// LastEnqueuedAt is observability only; it records when a tick was last
	// fired, never which tick.
	LastEnqueuedAt *time.Time `db:"last_enqueued_at"`
	CreatedAt      time.Time  `db:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at"`
}

// CronScheduleSpec is the caller-supplied part of a schedule. NextRunAt is
// used only when the row is inserted or its expression or timezone changed;
// see UpsertSchedule.
type CronScheduleSpec struct {
	Name, Queue, CronExpr, Timezone string
	Payload                         json.RawMessage
	Priority                        int16
	MaxAttempts                     int32
	Enabled                         bool
	NextRunAt                       time.Time
}

// TerminalStateError reports which terminal state blocked a transition. It
// unwraps to ErrTerminal, so errors.Is(err, ErrTerminal) works and the gRPC
// layer can still pull the state out with errors.As to choose a status code.
type TerminalStateError struct {
	State State
}

func (e *TerminalStateError) Error() string {
	return fmt.Sprintf("store: job already %s", e.State)
}

func (e *TerminalStateError) Unwrap() error { return ErrTerminal }

// QueueDepthSource is implemented by stores that can report queue depth. It is
// separate from Store because only the broker's metrics need it.
type QueueDepthSource interface {
	// QueueDepths counts jobs per queue in the states that describe work still
	// owed or evidence still kept — scheduled, available, leased and dead — in
	// one statement, so the counts are one consistent snapshot. Completed and
	// cancelled rows are deliberately not counted: they are most of the table,
	// bounded by retention rather than load, and nothing scales on them. A
	// queue with no rows in any counted state is absent; a caller that needs
	// its zero must know the queue (docs/semantics.md §14).
	QueueDepths(ctx context.Context) ([]QueueDepth, error)
}

// Store is the persistence contract. Every method takes a context for
// cancellation and timeouts. Implementations must make each state transition
// a single guarded UPDATE so a lost race updates zero rows and is reported,
// never silently overwritten. internal/broker serves this interface over
// gRPC; maintenance loops also consume it. This package knows neither caller.
type Store interface {
	// Enqueue inserts one job and returns the stored row, including the
	// generated ID and the state the store chose from RunAt. inserted reports
	// whether a row was actually written: false means an existing job with the
	// same (Queue, IdempotencyKey) was returned instead, and this call's
	// Payload, Priority and RunAt were ignored (docs/semantics.md §8). A call
	// with no IdempotencyKey always inserts.
	Enqueue(ctx context.Context, p EnqueueParams) (job Job, inserted bool, err error)

	// Lease atomically takes up to n available jobs from the given queues,
	// ordered by priority desc then run_at, marks them leased by workerID
	// for leaseDur, increments Attempts, and returns them. Concurrent calls
	// never return the same job (FOR UPDATE SKIP LOCKED). An empty result is
	// not an error.
	Lease(ctx context.Context, queues []string, n int, leaseDur time.Duration, workerID string) ([]Job, error)

	// PromoteDue moves up to limit scheduled jobs whose RunAt has passed to
	// StateAvailable, longest-overdue first, and returns how many it moved.
	// It is the only thing that makes a delayed or retried job leasable:
	// Lease has no run_at predicate (docs/semantics.md §4, "Due-ness is a
	// state, not a filter"). limit <= 0 is a no-op. Safe to run concurrently
	// from several brokers.
	PromoteDue(ctx context.Context, limit int) (int, error)

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

	// Cancel moves a scheduled, available or leased job to StateCancelled and
	// returns the cancelled row. A leased job is cancelled in the store
	// immediately; the worker holding it finds out at its next heartbeat or
	// ack, which gets ErrCancelled, and side effects already performed are not
	// undone. A job that is already completed, cancelled or dead is left
	// untouched and the error is a *TerminalStateError wrapping ErrTerminal.
	// An unknown id gets ErrNotFound. (docs/semantics.md §10.)
	Cancel(ctx context.Context, id uuid.UUID) (Job, error)

	// GetJob returns one job by id, or ErrNotFound. It is a read with no
	// guard and no side effects: the row it returns may have changed state by
	// the time the caller reads it, so it is for inspection and for reporting,
	// never for deciding a transition. Transitions are guarded UPDATEs.
	GetJob(ctx context.Context, id uuid.UUID) (Job, error)

	// ListJobs pages a queue by ascending ID, optionally filtered to states
	// (empty means all). after is the last ID on the previous page.
	// UUIDv7 order approximates application creation time, not DB commit or
	// created_at order. This is a live inspection traversal, not a snapshot;
	// concurrent inserts or state changes behind the cursor may be missed.
	//
	// This is an inspection API, not a hot path: it has no dedicated index and
	// walks the primary key filtering on queue and state. If it ever becomes
	// hot, add an index on (queue, id) and say so here.
	ListJobs(ctx context.Context, queue string, states []State, limit int, after *uuid.UUID) ([]Job, error)

	// ExtendLease pushes a live lease's expiry to now + dur (a heartbeat).
	// The same leaseID guard as Ack applies: a stale token gets ErrLeaseLost,
	// a cancelled job gets ErrCancelled. There is no idempotent-success case;
	// a heartbeat that misses always means the caller no longer holds the job.
	ExtendLease(ctx context.Context, id, leaseID uuid.UUID, dur time.Duration) error

	// ReapExpiredLeases takes back up to limit jobs whose lease has expired
	// and applies the retry policy exactly as Nack does: StateScheduled with
	// backoff, or StateDead when Attempts >= MaxAttempts, with LastError set
	// to "lease expired". It returns how many rows it moved. Calling it with
	// limit <= 0 is a no-op. Safe to run concurrently from several brokers.
	ReapExpiredLeases(ctx context.Context, limit int) (int, error)

	// ListDead returns up to limit dead jobs on queue, ordered by
	// (FinishedAt, ID) ascending, starting after the cursor when it is
	// non-nil. Build the next cursor from the last job returned; an empty
	// result means the end. queue is required; limit <= 0 returns nothing.
	ListDead(ctx context.Context, queue string, limit int, after *DeadCursor) ([]Job, error)

	// Redrive moves dead jobs back to StateAvailable with Attempts reset to
	// 0, LastError and FinishedAt cleared and RunAt = now, giving each a
	// fresh MaxAttempts. IDs that are not dead are ignored. Returns how many
	// rows moved.
	Redrive(ctx context.Context, ids []uuid.UUID) (int, error)

	// Purge deletes dead jobs, releasing their idempotency keys. IDs that are
	// not dead are ignored so an operator cannot delete a live job by mistake.
	// Returns how many rows were deleted.
	Purge(ctx context.Context, ids []uuid.UUID) (int, error)

	// DeleteFinished removes up to limit completed or cancelled jobs that
	// finished before cutoff, oldest first, and returns how many it deleted.
	// Dead jobs are never touched: they are evidence, kept until Purge
	// (docs/semantics.md §7, §10). Deleting a row releases its idempotency
	// key, so this is also where dedup protection ends (§8). limit <= 0 is a
	// no-op. Safe to run concurrently from several brokers.
	DeleteFinished(ctx context.Context, cutoff time.Time, limit int) (int, error)

	// DueSchedules returns up to limit enabled schedules whose NextRunAt has
	// passed, soonest first, together with the database clock that judged
	// them. Callers compute the following occurrence from that instant rather
	// than from time.Now, so a process whose clock trails the database's
	// cannot manufacture a backfill (docs/semantics.md §13). The returned
	// time is zero when nothing is due, in which case there is nothing to
	// compute. This is a plain read: exclusion between replicas comes from
	// leadership, not from row locks.
	DueSchedules(ctx context.Context, limit int) ([]CronSchedule, time.Time, error)

	// AdvanceSchedule moves a schedule's pending tick from to to and stamps
	// LastEnqueuedAt, reporting whether it matched. The from guard is what
	// makes a lost leader harmless: a process that fired a tick another
	// leader has already advanced past matches no row, reports false, and
	// cannot rewind the schedule. A false return is a normal outcome during
	// failover, not an error.
	AdvanceSchedule(ctx context.Context, name string, from, to time.Time) (bool, error)

	// UpsertSchedule inserts or updates one schedule and returns the stored
	// row. An update keeps the existing NextRunAt unless CronExpr or Timezone
	// changed, so redeploying an unchanged schedule neither skips its pending
	// tick nor re-fires a past one.
	UpsertSchedule(ctx context.Context, spec CronScheduleSpec) (CronSchedule, error)

	// ListSchedules returns every schedule ordered by name. Inspection only.
	ListSchedules(ctx context.Context) ([]CronSchedule, error)

	// Now returns the database's current time. Reconciliation computes a
	// newly inserted schedule's first tick from this rather than from
	// time.Now, for the reason DueSchedules returns its own clock: every
	// instant that decides when a schedule fires is judged by one clock, so a
	// broker whose clock is skewed cannot write a tick the database would not
	// have chosen (docs/semantics.md §13). The tick loop has no use for it —
	// DueSchedules already carries the clock that judged its rows.
	Now(ctx context.Context) (time.Time, error)

	// DeleteSchedulesExcept deletes every schedule whose name is not in keep
	// and returns how many it removed: the prune half of reconciling a
	// declarative cron file. An empty keep is rejected rather than treated as
	// "delete everything", because the difference between "the file lists
	// nothing" and "no file was loaded" is one a caller must state.
	DeleteSchedulesExcept(ctx context.Context, keep []string) (int, error)

	// TryLeadership attempts to become the single active scheduler, returning
	// ErrNotLeader when another process holds the role. Leadership occupies
	// one connection for its whole term (see the implementation), so a pool
	// sized for N concurrent queries serves N-1 while a process leads.
	TryLeadership(ctx context.Context) (Leadership, error)
}

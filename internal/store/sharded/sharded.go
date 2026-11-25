// Package sharded splits hopper's jobs across several PostgreSQL databases
// behind the one store.Store interface (docs/design/sharding.md).
//
// It holds no SQL. Every statement runs in one shard's own store, so the
// per-database contract in docs/semantics.md holds on each shard unchanged.
// This package decides only which shard a call goes to.
package sharded

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/rpasco787/hopper/internal/store"
)

// Shard is what each shard must be: a whole store that can batch
// confirmations and carry wakeups. *postgres.Store is one.
type Shard interface {
	store.Store
	store.BatchConfirmer
	store.Wakeups
}

// Store is a store.Store over N shards. It is safe for concurrent use.
type Store struct {
	shards []Shard
	health *health
	next   atomic.Uint64 // the rotation for unkeyed enqueues, leases and announcements
	logger *slog.Logger
}

var (
	_ store.Store          = (*Store)(nil)
	_ store.BatchConfirmer = (*Store)(nil)
	_ store.BatchEnqueuer  = (*Store)(nil)
	_ store.Announcer      = (*Store)(nil)
	_ store.ShardRouter    = (*Store)(nil)
)

// New returns a Store over shards, which must be listed in shard order: shard
// i must stamp i into the IDs it generates (postgres.Store.WithShard).
func New(shards []Shard, logger *slog.Logger) (*Store, error) {
	if len(shards) < 2 || len(shards) > store.MaxShards {
		return nil, fmt.Errorf("sharded: need 2 to %d shards, got %d", store.MaxShards, len(shards))
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Store{shards: slices.Clone(shards), health: newHealth(len(shards)), logger: logger}, nil
}

func (s *Store) Shards() int                       { return len(s.shards) }
func (s *Store) ShardOf(id uuid.UUID) (int, error) { return store.ShardOf(id, len(s.shards)) }

// Listeners returns every shard's listener. Every broker listens on all of
// them, so an announcement made on any one shard reaches every broker.
func (s *Store) Listeners() []store.Listener {
	out := make([]store.Listener, len(s.shards))
	for i, sh := range s.shards {
		out[i] = sh
	}
	return out
}

// rotate returns the next shard to start from.
func (s *Store) rotate() int { return int((s.next.Add(1) - 1) % uint64(len(s.shards))) }

// saw notes what a call to shard i says about that shard's health and
// returns its error, naming the shard. errors.Is and errors.As see through it.
func (s *Store) saw(i int, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, store.ErrUnavailable) {
		s.health.down(i, time.Now())
	}
	return fmt.Errorf("sharded: shard %d: %w", i, err)
}

// owner is the shard that holds id.
func (s *Store) owner(id uuid.UUID) (int, Shard, error) {
	i, err := s.ShardOf(id)
	if err != nil {
		return 0, nil, err
	}
	return i, s.shards[i], nil
}

func (s *Store) Ack(ctx context.Context, id, leaseID uuid.UUID) error {
	i, sh, err := s.owner(id)
	if err != nil {
		return err
	}
	return s.saw(i, sh.Ack(ctx, id, leaseID))
}

func (s *Store) Nack(ctx context.Context, id, leaseID uuid.UUID, errMsg string) error {
	i, sh, err := s.owner(id)
	if err != nil {
		return err
	}
	return s.saw(i, sh.Nack(ctx, id, leaseID, errMsg))
}

func (s *Store) ExtendLease(ctx context.Context, id, leaseID uuid.UUID, dur time.Duration) error {
	i, sh, err := s.owner(id)
	if err != nil {
		return err
	}
	return s.saw(i, sh.ExtendLease(ctx, id, leaseID, dur))
}

func (s *Store) Cancel(ctx context.Context, id uuid.UUID) (store.Job, error) {
	i, sh, err := s.owner(id)
	if err != nil {
		return store.Job{}, err
	}
	job, err := sh.Cancel(ctx, id)
	return job, s.saw(i, err)
}

func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (store.Job, error) {
	i, sh, err := s.owner(id)
	if err != nil {
		return store.Job{}, err
	}
	job, err := sh.GetJob(ctx, id)
	return job, s.saw(i, err)
}

// Cron lives on shard 0: the schedules, the clock that judges them and the
// leader lock (docs/semantics.md §13). Tick jobs are keyed, so Enqueue sends
// each to its key's shard, and a re-fired tick deduplicates there.

func (s *Store) DueSchedules(ctx context.Context, limit int) ([]store.CronSchedule, time.Time, error) {
	due, at, err := s.shards[0].DueSchedules(ctx, limit)
	return due, at, s.saw(0, err)
}

func (s *Store) AdvanceSchedule(ctx context.Context, name string, from, to time.Time) (bool, error) {
	ok, err := s.shards[0].AdvanceSchedule(ctx, name, from, to)
	return ok, s.saw(0, err)
}

func (s *Store) UpsertSchedule(ctx context.Context, spec store.CronScheduleSpec) (store.CronSchedule, error) {
	cs, err := s.shards[0].UpsertSchedule(ctx, spec)
	return cs, s.saw(0, err)
}

func (s *Store) ListSchedules(ctx context.Context) ([]store.CronSchedule, error) {
	got, err := s.shards[0].ListSchedules(ctx)
	return got, s.saw(0, err)
}

func (s *Store) Now(ctx context.Context) (time.Time, error) {
	now, err := s.shards[0].Now(ctx)
	return now, s.saw(0, err)
}

func (s *Store) DeleteSchedulesExcept(ctx context.Context, keep []string) (int, error) {
	n, err := s.shards[0].DeleteSchedulesExcept(ctx, keep)
	return n, s.saw(0, err)
}

// TryLeadership takes the leader lock on shard 0. It returns ErrNotLeader
// unwrapped, so the scheduler's log lines read as they do unsharded.
func (s *Store) TryLeadership(ctx context.Context, ttl time.Duration) (store.Leadership, error) {
	lead, err := s.shards[0].TryLeadership(ctx, ttl)
	if errors.Is(err, store.ErrNotLeader) {
		return nil, err
	}
	return lead, s.saw(0, err)
}

// The methods below are stubs, so this task's tests compile. Task 8
// replaces every one.

func (s *Store) ListJobs(ctx context.Context, queue string, states []store.State, limit int, after *uuid.UUID) ([]store.Job, error) {
	panic("Task 8")
}

func (s *Store) ListDead(ctx context.Context, queue string, limit int, after *store.DeadCursor) ([]store.Job, error) {
	panic("Task 8")
}

func (s *Store) Redrive(ctx context.Context, ids []uuid.UUID) (int, error) {
	panic("Task 8")
}

func (s *Store) Purge(ctx context.Context, ids []uuid.UUID) (int, error) {
	panic("Task 8")
}

func (s *Store) PromoteDue(ctx context.Context, limit int) (int, error) {
	panic("Task 8")
}

func (s *Store) ReapExpiredLeases(ctx context.Context, limit int) (int, error) {
	panic("Task 8")
}

func (s *Store) DeleteFinished(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	panic("Task 8")
}

func (s *Store) AckMany(ctx context.Context, refs []store.LeaseRef) []error {
	panic("Task 8")
}

func (s *Store) ExtendLeaseMany(ctx context.Context, exts []store.Extension) []error {
	panic("Task 8")
}

func (s *Store) Announce(ctx context.Context, queues []string) error {
	panic("Task 8")
}

package postgres

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func leasedRef(t *testing.T, ctx context.Context, s *Store, queue string) store.LeaseRef {
	t.Helper()
	j := leaseOne(t, ctx, s, queue, "w")
	return store.LeaseRef{ID: j.ID, LeaseID: *j.LeaseID}
}

func stateOf(t *testing.T, ctx context.Context, s *Store, id uuid.UUID) store.State {
	t.Helper()
	j, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	return j.State
}

// TestAckManyAppliesHeldLeasesAndMarksTheRest: a held lease completes. Every
// other row of the docs/semantics.md §5 table comes back ErrNotApplied, for
// the one-row Ack to classify, and is left exactly as it was.
func TestAckManyAppliesHeldLeasesAndMarksTheRest(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	held := leasedRef(t, ctx, s, "held")
	done := leasedRef(t, ctx, s, "done")
	require.NoError(t, s.Ack(ctx, done.ID, done.LeaseID))
	stolen := leasedRef(t, ctx, s, "stolen")
	stolen.LeaseID = uuid.New() // leased, but not under this token
	nacked := leasedRef(t, ctx, s, "nacked")
	require.NoError(t, s.Nack(ctx, nacked.ID, nacked.LeaseID, "boom")) // scheduled
	cancelled := leasedRef(t, ctx, s, "cancelled")
	_, err := s.Cancel(ctx, cancelled.ID)
	require.NoError(t, err)
	missing := store.LeaseRef{ID: uuid.New(), LeaseID: uuid.New()}

	errs := s.AckMany(ctx, []store.LeaseRef{held, done, stolen, nacked, cancelled, missing})

	require.Len(t, errs, 6)
	require.NoError(t, errs[0], "a held lease completes")
	for i, name := range []string{"done", "stolen", "nacked", "cancelled", "missing"} {
		require.ErrorIs(t, errs[i+1], store.ErrNotApplied, name)
	}
	require.Equal(t, store.StateCompleted, stateOf(t, ctx, s, held.ID))
	require.Equal(t, store.StateLeased, stateOf(t, ctx, s, stolen.ID), "a pair it doesn't apply is left alone")
	require.Equal(t, store.StateScheduled, stateOf(t, ctx, s, nacked.ID))
	require.Equal(t, store.StateCancelled, stateOf(t, ctx, s, cancelled.ID))
}

func TestAckManyGivesRepeatsTheSameAnswer(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	ref := leasedRef(t, ctx, s, "q")
	stale := store.LeaseRef{ID: ref.ID, LeaseID: uuid.New()}

	errs := s.AckMany(ctx, []store.LeaseRef{ref, ref, stale})

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.ErrorIs(t, errs[2], store.ErrNotApplied, "the same job under another token is not ours")
}

// TestAckManyNeverWaitsForALockedRow: the batch skips a row another
// transaction holds and returns at once, with the free row committed, while
// the lock is still held. Waiting would run into the 2 s deadline.
func TestAckManyNeverWaitsForALockedRow(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	locked := leasedRef(t, ctx, s, "locked")
	free := leasedRef(t, ctx, s, "free")

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background()) //nolint:errcheck
	_, err = tx.Exec(ctx, "select 1 from jobs where id = $1 for update", locked.ID)
	require.NoError(t, err)

	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	errs := s.AckMany(callCtx, []store.LeaseRef{locked, free})

	require.ErrorIs(t, errs[0], store.ErrNotApplied)
	require.NoError(t, errs[1])
	require.Equal(t, store.StateCompleted, stateOf(t, ctx, s, free.ID))
	require.Equal(t, store.StateLeased, stateOf(t, ctx, s, locked.ID))
}

func TestAckManyFailsEverySlotWhenTheStatementFails(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	ref := leasedRef(t, ctx, s, "q")
	gone, cancel := context.WithCancel(ctx)
	cancel()

	for _, err := range s.AckMany(gone, []store.LeaseRef{ref, ref}) {
		require.ErrorIs(t, err, context.Canceled)
	}
	require.Equal(t, store.StateLeased, stateOf(t, ctx, s, ref.ID))
}

type completions struct {
	store.NopObserver
	n atomic.Int32
}

func (c *completions) Completed(string, time.Time, time.Time) { c.n.Add(1) }

func TestAckManyReportsOnlyTheCompletionsItMade(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	obs := &completions{}
	so := s.WithObserver(obs)
	fresh := leasedRef(t, ctx, s, "fresh")
	retried := leasedRef(t, ctx, s, "retried")
	require.NoError(t, s.Ack(ctx, retried.ID, retried.LeaseID))

	errs := so.AckMany(ctx, []store.LeaseRef{fresh, retried})
	require.NoError(t, errs[0])
	require.ErrorIs(t, errs[1], store.ErrNotApplied)
	require.EqualValues(t, 1, obs.n.Load(), "a retried ack is not a second completion")
}

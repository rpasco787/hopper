package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func TestCancelScheduledAndAvailable(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	avail, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q"})
	require.NoError(t, err)
	sched, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", RunAt: time.Now().Add(time.Hour)})
	require.NoError(t, err)

	before := time.Now()
	for _, j := range []store.Job{avail, sched} {
		got, err := s.Cancel(ctx, j.ID)
		require.NoError(t, err)
		require.Equal(t, store.StateCancelled, got.State)
		require.NotNil(t, got.FinishedAt)
		require.WithinDuration(t, before, *got.FinishedAt, 2*time.Second)
		require.Equal(t, int32(0), got.Attempts, "a cancelled job never ran")
		require.False(t, got.UpdatedAt.Before(j.UpdatedAt))

		stored := readJob(t, j.ID)
		require.Equal(t, store.StateCancelled, stored.State)
		require.Equal(t, got.FinishedAt.UnixMicro(), stored.FinishedAt.UnixMicro())
	}

	n, err := s.PromoteDue(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 0, n, "a cancelled job is not scheduled any more")
	got, err := s.Lease(ctx, []string{"q"}, 10, time.Second, "w")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestCancelLeasedJobIsLearnedByTheWorker(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	j := leaseOne(t, ctx, s, "q", "w")

	got, err := s.Cancel(ctx, j.ID)
	require.NoError(t, err)
	require.Equal(t, store.StateCancelled, got.State)
	require.Equal(t, *j.LeaseID, *got.LeaseID,
		"the lease token stays so the holder's next call can be told why, not just that it lost the lease")
	require.Equal(t, "w", *got.LeasedBy)

	require.ErrorIs(t, s.ExtendLease(ctx, j.ID, *j.LeaseID, time.Minute), store.ErrCancelled)
	require.ErrorIs(t, s.Ack(ctx, j.ID, *j.LeaseID), store.ErrCancelled)
	require.ErrorIs(t, s.Nack(ctx, j.ID, *j.LeaseID, "too late"), store.ErrCancelled)

	expireLease(t, j.ID)
	n, err := s.ReapExpiredLeases(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Equal(t, store.StateCancelled, readJob(t, j.ID).State)
}

func TestCancelIsRejectedOnTerminalStates(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	done := leaseOne(t, ctx, s, "q", "w")
	require.NoError(t, s.Ack(ctx, done.ID, *done.LeaseID))
	dead := deadJob(t, ctx, s, "q2")

	twice, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q3"})
	require.NoError(t, err)
	_, err = s.Cancel(ctx, twice.ID)
	require.NoError(t, err)

	for _, tc := range []struct {
		name  string
		id    uuid.UUID
		state store.State
	}{
		{"completed", done.ID, store.StateCompleted},
		{"dead", dead.ID, store.StateDead},
		{"already cancelled", twice.ID, store.StateCancelled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Cancel(ctx, tc.id)
			require.ErrorIs(t, err, store.ErrTerminal)
			var tse *store.TerminalStateError
			require.ErrorAs(t, err, &tse)
			require.Equal(t, tc.state, tse.State, "the error must name the state that blocked the cancel")
			require.Equal(t, tc.state, readJob(t, tc.id).State, "a rejected cancel must not touch the row")
		})
	}
}

func TestCancelUnknownIDIsNotFound(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	_, err := s.Cancel(ctx, uuid.New())
	require.ErrorIs(t, err, store.ErrNotFound)
	require.NotErrorIs(t, err, store.ErrTerminal, "a missing job is not a terminal job")
}

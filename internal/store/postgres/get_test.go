package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func TestGetJobReturnsEveryColumn(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	// A leased job is the row with the most non-null columns, so it is the one
	// that catches a column missing from jobColumns or a db tag that drifted.
	j := leaseOne(t, ctx, s, "q", "w")

	got, err := s.GetJob(ctx, j.ID)
	require.NoError(t, err)

	require.Equal(t, j.ID, got.ID)
	require.Equal(t, "q", got.Queue)
	require.Equal(t, store.StateLeased, got.State)
	require.Equal(t, int32(1), got.Attempts)
	require.NotNil(t, got.LeaseID)
	require.Equal(t, *j.LeaseID, *got.LeaseID)
	require.NotNil(t, got.LeasedBy)
	require.Equal(t, "w", *got.LeasedBy)
	require.NotNil(t, got.LeasedAt)
	require.NotNil(t, got.LeaseExpires)
	require.Nil(t, got.FinishedAt)
	require.WithinDuration(t, j.RunAt, got.RunAt, time.Millisecond)
}

func TestGetJobSeesTerminalRows(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	dead := deadJob(t, ctx, s, "q")

	got, err := s.GetJob(ctx, dead.ID)
	require.NoError(t, err)
	require.Equal(t, store.StateDead, got.State)
	require.NotNil(t, got.FinishedAt, "a dead job has finished")
	require.NotNil(t, got.LastError)
}

func TestGetJobUnknownIDIsNotFound(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	_, err := s.GetJob(ctx, uuid.New())
	require.ErrorIs(t, err, store.ErrNotFound)
}

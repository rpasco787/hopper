package postgres

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func TestEnqueueDefaults(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	before := time.Now()
	j, err := s.Enqueue(ctx, store.EnqueueParams{
		Queue:   "email",
		Payload: json.RawMessage(`{"to":"a@b.c"}`),
	})
	require.NoError(t, err)

	require.NotEqual(t, uuid.Nil, j.ID)
	require.Equal(t, uuid.Version(7), j.ID.Version(), "ids must be UUIDv7 so the pkey appends in time order")

	require.Equal(t, "email", j.Queue)
	require.JSONEq(t, `{"to":"a@b.c"}`, string(j.Payload))
	require.Equal(t, store.StateAvailable, j.State, "a due job is leasable immediately")
	require.Equal(t, int16(0), j.Priority)
	require.Equal(t, int32(0), j.Attempts)
	require.Equal(t, int32(5), j.MaxAttempts)
	require.Nil(t, j.LeaseID)
	require.Nil(t, j.LeasedBy)
	require.Nil(t, j.LeasedAt)
	require.Nil(t, j.LeaseExpires)
	require.Nil(t, j.IdempotencyKey)
	require.Nil(t, j.LastError)
	require.Nil(t, j.FinishedAt)
	require.WithinDuration(t, before, j.RunAt, 2*time.Second, "zero RunAt defaults to now")
	require.WithinDuration(t, before, j.CreatedAt, 2*time.Second)
	require.WithinDuration(t, before, j.UpdatedAt, 2*time.Second)
}

func TestEnqueueFutureRunAtIsScheduled(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	runAt := time.Now().Add(time.Hour)
	j, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "email", RunAt: runAt})
	require.NoError(t, err)

	require.Equal(t, store.StateScheduled, j.State, "a future job is not leasable; the promoter moves it later")
	// The DB stores microseconds and returns its own zone, so compare instants
	// with a tolerance rather than Equal.
	require.WithinDuration(t, runAt, j.RunAt, time.Millisecond)
}

func TestEnqueueOptionalFieldsRoundTrip(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	key := "order-42"
	j, err := s.Enqueue(ctx, store.EnqueueParams{
		Queue:          "orders",
		Payload:        nil,
		Priority:       7,
		MaxAttempts:    2,
		IdempotencyKey: &key,
	})
	require.NoError(t, err)

	require.JSONEq(t, `{}`, string(j.Payload), "empty payload is stored as {}")
	require.Equal(t, int16(7), j.Priority)
	require.Equal(t, int32(2), j.MaxAttempts)
	require.NotNil(t, j.IdempotencyKey)
	require.Equal(t, key, *j.IdempotencyKey)

	// And the row really is what RETURNING said it is.
	got := readJob(t, j.ID)
	require.Equal(t, j.ID, got.ID)
	require.Equal(t, key, *got.IdempotencyKey)
	require.JSONEq(t, `{}`, string(got.Payload))
}

func TestEnqueueEmptyQueueRejected(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	_, err := s.Enqueue(ctx, store.EnqueueParams{Queue: ""})
	require.Error(t, err)

	var n int
	require.NoError(t, pool.QueryRow(ctx, "select count(*) from jobs").Scan(&n))
	require.Equal(t, 0, n, "validation must fail before anything is written")
}

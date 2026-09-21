package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

// TestStateRoundTrips is exhaustive on purpose. A state added to store.State
// without a proto value would otherwise convert to UNSPECIFIED and a job would
// silently appear stateless on the wire.
func TestStateRoundTrips(t *testing.T) {
	all := []store.State{
		store.StateScheduled, store.StateAvailable, store.StateLeased,
		store.StateCompleted, store.StateCancelled, store.StateDead,
	}
	seen := make(map[hopperv1.JobState]bool, len(all))

	for _, s := range all {
		p, err := stateToProto(s)
		require.NoErrorf(t, err, "state %q", s)
		require.NotEqual(t, hopperv1.JobState_JOB_STATE_UNSPECIFIED, p)
		require.Falsef(t, seen[p], "two states map to %v", p)
		seen[p] = true

		back, err := stateFromProto(p)
		require.NoError(t, err)
		require.Equal(t, s, back)
	}

	// The enum has exactly one value that is not a state: UNSPECIFIED.
	require.Len(t, hopperv1.JobState_name, len(all)+1)

	_, err := stateToProto(store.State("wat"))
	require.Error(t, err)
	_, err = stateFromProto(hopperv1.JobState_JOB_STATE_UNSPECIFIED)
	require.Error(t, err)
}

func TestJobToProtoCarriesNullsAsAbsent(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	j := store.Job{
		ID:          uuid.New(),
		Queue:       "q",
		Payload:     json.RawMessage(`{"a":1}`),
		State:       store.StateAvailable,
		Priority:    7,
		RunAt:       now,
		Attempts:    0,
		MaxAttempts: 5,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	p, err := jobToProto(j)
	require.NoError(t, err)
	require.Equal(t, j.ID.String(), p.GetId())
	require.JSONEq(t, `{"a":1}`, string(p.GetPayload()))
	require.Equal(t, hopperv1.JobState_JOB_STATE_AVAILABLE, p.GetState())
	require.Equal(t, int32(7), p.GetPriority())
	require.Equal(t, now.UnixMicro(), p.GetRunAt().AsTime().UnixMicro())

	// Every nullable column is absent, not zero.
	require.Nil(t, p.LeaseId)
	require.Nil(t, p.LeasedBy)
	require.Nil(t, p.LeasedAt)
	require.Nil(t, p.LeaseExpires)
	require.Nil(t, p.IdempotencyKey)
	require.Nil(t, p.LastError)
	require.Nil(t, p.FinishedAt)
}

func TestJobToProtoCarriesEveryNonNullField(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	leaseID := uuid.New()
	by, key, lastErr := "worker-1", "k", "boom"
	j := store.Job{
		ID:             uuid.New(),
		Queue:          "q",
		Payload:        json.RawMessage(`{}`),
		State:          store.StateLeased,
		Priority:       -3,
		RunAt:          now,
		Attempts:       2,
		MaxAttempts:    5,
		LeaseID:        &leaseID,
		LeasedBy:       &by,
		LeasedAt:       &now,
		LeaseExpires:   &now,
		IdempotencyKey: &key,
		LastError:      &lastErr,
		CreatedAt:      now,
		UpdatedAt:      now,
		FinishedAt:     &now,
	}

	p, err := jobToProto(j)
	require.NoError(t, err)
	require.Equal(t, leaseID.String(), p.GetLeaseId())
	require.Equal(t, "worker-1", p.GetLeasedBy())
	require.Equal(t, "k", p.GetIdempotencyKey())
	require.Equal(t, "boom", p.GetLastError())
	require.Equal(t, int32(-3), p.GetPriority(), "negative priorities survive the int16->int32 widening")
	require.Equal(t, now.UnixMicro(), p.GetLeasedAt().AsTime().UnixMicro())
	require.Equal(t, now.UnixMicro(), p.GetLeaseExpires().AsTime().UnixMicro())
	require.Equal(t, now.UnixMicro(), p.GetFinishedAt().AsTime().UnixMicro())
}

func TestSpecToParams(t *testing.T) {
	at := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	key := "k"

	got, err := specToParams(&hopperv1.JobSpec{
		Queue:          "q",
		Payload:        []byte(`{"n":1}`),
		Priority:       3,
		RunAt:          timestamppb.New(at),
		MaxAttempts:    9,
		IdempotencyKey: &key,
	})
	require.NoError(t, err)
	require.Equal(t, "q", got.Queue)
	require.JSONEq(t, `{"n":1}`, string(got.Payload))
	require.Equal(t, int16(3), got.Priority)
	require.Equal(t, int32(9), got.MaxAttempts)
	require.Equal(t, at.UnixMicro(), got.RunAt.UnixMicro())
	require.Equal(t, "k", *got.IdempotencyKey)
}

// TestSpecToParamsRejections pins what is a client error rather than a 500
// from Postgres three layers down.
func TestSpecToParamsRejections(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec *hopperv1.JobSpec
		want string
	}{
		{"nil spec", nil, "job is required"},
		{"no queue", &hopperv1.JobSpec{}, "queue is required"},
		{"bad json", &hopperv1.JobSpec{Queue: "q", Payload: []byte(`{`)}, "payload must be valid JSON"},
		{"priority too high", &hopperv1.JobSpec{Queue: "q", Priority: 1 << 20}, "priority"},
		{"priority too low", &hopperv1.JobSpec{Queue: "q", Priority: -(1 << 20)}, "priority"},
		{"negative max attempts", &hopperv1.JobSpec{Queue: "q", MaxAttempts: -1}, "max_attempts"},
		{"invalid nanos", &hopperv1.JobSpec{Queue: "q", RunAt: &timestamppb.Timestamp{Nanos: -1}}, "run_at"},
		{"invalid seconds", &hopperv1.JobSpec{Queue: "q", RunAt: &timestamppb.Timestamp{Seconds: 253402300800}}, "run_at"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := specToParams(tc.spec)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
			require.Equal(t, codes.InvalidArgument, status.Code(err),
				"validation failures must already be gRPC statuses")
		})
	}
}

// An empty payload is legal: the store stores it as {}.
func TestSpecToParamsAllowsEmptyPayload(t *testing.T) {
	got, err := specToParams(&hopperv1.JobSpec{Queue: "q"})
	require.NoError(t, err)
	require.Empty(t, got.Payload)
	require.True(t, got.RunAt.IsZero(), "an absent run_at stays zero so the store coalesces it to now()")
}

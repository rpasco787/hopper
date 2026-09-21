package broker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

// TestIDValidation covers every RPC that takes a uuid, in one place, because
// the failure mode it guards against is "one handler forgot to call parseID
// and passed a zero uuid to the store".
func TestIDValidation(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	client, _ := newTestService(t, &fakeStore{})

	good := uuid.New().String()
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"cancel empty", func() error {
			_, err := client.Cancel(ctx, &hopperv1.CancelRequest{})
			return err
		}},
		{"cancel malformed", func() error {
			_, err := client.Cancel(ctx, &hopperv1.CancelRequest{Id: "not-a-uuid"})
			return err
		}},
		{"get job empty", func() error {
			_, err := client.GetJob(ctx, &hopperv1.GetJobRequest{})
			return err
		}},
		{"ack no lease id", func() error {
			_, err := client.Ack(ctx, &hopperv1.AckRequest{Id: good})
			return err
		}},
		{"ack malformed lease id", func() error {
			_, err := client.Ack(ctx, &hopperv1.AckRequest{Id: good, LeaseId: "nope"})
			return err
		}},
		{"nack no lease id", func() error {
			_, err := client.Nack(ctx, &hopperv1.NackRequest{Id: good})
			return err
		}},
		{"heartbeat no lease id", func() error {
			_, err := client.Heartbeat(ctx, &hopperv1.HeartbeatRequest{Id: good})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

// TestLeaseKeyedErrorsReachTheClient is docs/semantics.md §5 end to end: the
// three sentinel errors a worker can get back from Ack, Nack or Heartbeat, and
// the code each becomes. Injecting them is the whole reason this package tests
// against a fake.
func TestLeaseKeyedErrorsReachTheClient(t *testing.T) {
	t.Parallel()

	id, leaseID := uuid.New(), uuid.New()
	for _, tc := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{"lease lost", store.ErrLeaseLost, codes.Aborted},
		{"cancelled", store.ErrCancelled, codes.FailedPrecondition},
		{"not found", store.ErrNotFound, codes.NotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxT(t)
			client, _ := newTestService(t, &fakeStore{
				ackFn:    func(context.Context, uuid.UUID, uuid.UUID) error { return tc.err },
				nackFn:   func(context.Context, uuid.UUID, uuid.UUID, string) error { return tc.err },
				extendFn: func(context.Context, uuid.UUID, uuid.UUID, time.Duration) error { return tc.err },
			})

			_, err := client.Ack(ctx, &hopperv1.AckRequest{Id: id.String(), LeaseId: leaseID.String()})
			require.Equal(t, tc.want, status.Code(err), "Ack")

			_, err = client.Nack(ctx, &hopperv1.NackRequest{Id: id.String(), LeaseId: leaseID.String(), Error: "boom"})
			require.Equal(t, tc.want, status.Code(err), "Nack")

			_, err = client.Heartbeat(ctx, &hopperv1.HeartbeatRequest{Id: id.String(), LeaseId: leaseID.String()})
			require.Equal(t, tc.want, status.Code(err), "Heartbeat")
		})
	}
}

func TestAckNackHeartbeatPassTheirArguments(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)

	id, leaseID := uuid.New(), uuid.New()
	var gotID, gotLease uuid.UUID
	var gotMsg string
	var gotDur time.Duration

	client, _ := newTestService(t, &fakeStore{
		ackFn: func(_ context.Context, i, l uuid.UUID) error {
			gotID, gotLease = i, l
			return nil
		},
		nackFn: func(_ context.Context, i, l uuid.UUID, m string) error {
			gotMsg = m
			return nil
		},
		extendFn: func(_ context.Context, i, l uuid.UUID, d time.Duration) error {
			gotDur = d
			return nil
		},
	})

	_, err := client.Ack(ctx, &hopperv1.AckRequest{Id: id.String(), LeaseId: leaseID.String()})
	require.NoError(t, err)
	require.Equal(t, id, gotID)
	require.Equal(t, leaseID, gotLease)

	_, err = client.Nack(ctx, &hopperv1.NackRequest{Id: id.String(), LeaseId: leaseID.String(), Error: "handler panicked"})
	require.NoError(t, err)
	require.Equal(t, "handler panicked", gotMsg)

	// An absent duration takes the broker default, so a worker that does not
	// care never has to name one.
	_, err = client.Heartbeat(ctx, &hopperv1.HeartbeatRequest{Id: id.String(), LeaseId: leaseID.String()})
	require.NoError(t, err)
	require.Equal(t, DefaultLeaseDuration, gotDur)

	_, err = client.Heartbeat(ctx, &hopperv1.HeartbeatRequest{
		Id: id.String(), LeaseId: leaseID.String(),
		LeaseDuration: durationpb.New(90 * time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, 90*time.Second, gotDur)
}

func TestHeartbeatRejectsNegativeDuration(t *testing.T) {
	t.Parallel()
	client, _ := newTestService(t, &fakeStore{})
	_, err := client.Heartbeat(ctxT(t), &hopperv1.HeartbeatRequest{
		Id: uuid.New().String(), LeaseId: uuid.New().String(),
		LeaseDuration: durationpb.New(-time.Second),
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCancelReturnsTheCancelledJob(t *testing.T) {
	t.Parallel()

	job := sampleJob("q")
	job.State = store.StateCancelled
	client, _ := newTestService(t, &fakeStore{
		cancelFn: func(context.Context, uuid.UUID) (store.Job, error) { return job, nil },
	})

	resp, err := client.Cancel(ctxT(t), &hopperv1.CancelRequest{Id: job.ID.String()})
	require.NoError(t, err)
	require.Equal(t, hopperv1.JobState_JOB_STATE_CANCELLED, resp.GetJob().GetState())
	require.False(t, resp.GetAlreadyCancelled())
}

// TestCancelIsIdempotent is the decision this session makes on top of Session
// 3's open question. The store is right to say ErrTerminal — the second caller
// cancelled nothing — and the API is right to call it success, because the job
// is in the state the caller asked for and a retried RPC must not look like a
// failure.
func TestCancelIsIdempotent(t *testing.T) {
	t.Parallel()

	job := sampleJob("q")
	job.State = store.StateCancelled
	client, _ := newTestService(t, &fakeStore{
		cancelFn: func(context.Context, uuid.UUID) (store.Job, error) {
			return store.Job{}, &store.TerminalStateError{State: store.StateCancelled}
		},
		getJobFn: func(context.Context, uuid.UUID) (store.Job, error) { return job, nil },
	})

	resp, err := client.Cancel(ctxT(t), &hopperv1.CancelRequest{Id: job.ID.String()})
	require.NoError(t, err, "cancelling an already-cancelled job is a success")
	require.True(t, resp.GetAlreadyCancelled())
	require.Equal(t, hopperv1.JobState_JOB_STATE_CANCELLED, resp.GetJob().GetState())
}

// The other two terminal states are genuinely different from what the caller
// asked for, so they still fail.
func TestCancelOnCompletedOrDeadFails(t *testing.T) {
	t.Parallel()

	for _, st := range []store.State{store.StateCompleted, store.StateDead} {
		t.Run(string(st), func(t *testing.T) {
			client, _ := newTestService(t, &fakeStore{
				cancelFn: func(context.Context, uuid.UUID) (store.Job, error) {
					return store.Job{}, &store.TerminalStateError{State: st}
				},
			})
			_, err := client.Cancel(ctxT(t), &hopperv1.CancelRequest{Id: uuid.New().String()})
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Contains(t, err.Error(), string(st), "the client is told which state blocked it")
		})
	}
}

func TestGetJob(t *testing.T) {
	t.Parallel()

	job := sampleJob("q")
	client, _ := newTestService(t, &fakeStore{
		getJobFn: func(context.Context, uuid.UUID) (store.Job, error) { return job, nil },
	})

	resp, err := client.GetJob(ctxT(t), &hopperv1.GetJobRequest{Id: job.ID.String()})
	require.NoError(t, err)
	require.Equal(t, job.ID.String(), resp.GetJob().GetId())
}

func TestGetJobNotFound(t *testing.T) {
	t.Parallel()
	client, _ := newTestService(t, &fakeStore{
		getJobFn: func(context.Context, uuid.UUID) (store.Job, error) {
			return store.Job{}, store.ErrNotFound
		},
	})
	_, err := client.GetJob(ctxT(t), &hopperv1.GetJobRequest{Id: uuid.New().String()})
	require.Equal(t, codes.NotFound, status.Code(err))
}

// invalidDurations are the wire values leaseDuration must refuse: protobuf-
// invalid (sign mismatch, out-of-range nanos or seconds), negative, and
// protobuf-valid but past what a Go time.Duration can hold, where AsDuration
// would silently saturate instead of failing.
var invalidDurations = map[string]*durationpb.Duration{
	"negative seconds":    {Seconds: -1},
	"sign mismatch":       {Seconds: 1, Nanos: -1},
	"nanos out of range":  {Nanos: 1_000_000_000},
	"past protobuf range": {Seconds: 315_576_000_001},
	"past Go duration":    {Seconds: 10_000_000_000},
	"negative past Go":    {Seconds: -10_000_000_000},
}

func TestLeaseDurationValidation(t *testing.T) {
	t.Parallel()
	svc := NewService(&fakeStore{}, Config{})

	for name, bad := range invalidDurations {
		t.Run(name, func(t *testing.T) {
			_, err := svc.leaseDuration(bad)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
	for _, unset := range []*durationpb.Duration{nil, {}} {
		got, err := svc.leaseDuration(unset)
		require.NoError(t, err)
		require.Equal(t, DefaultLeaseDuration, got)
	}
}

// TestHeartbeatValidDurationsAreExact and its invalid twin below are the
// promise in the plan: nothing invalid reaches ExtendLease, and nothing valid
// is rounded, clamped or replaced by the default on the way there.
func TestHeartbeatValidDurationsAreExact(t *testing.T) {
	t.Parallel()

	var got atomic.Int64
	client, _ := newTestService(t, &fakeStore{
		extendFn: func(_ context.Context, _, _ uuid.UUID, d time.Duration) error {
			got.Store(int64(d))
			return nil
		},
	})

	for _, want := range []time.Duration{time.Nanosecond, 1500*time.Millisecond + 1, 90 * time.Second, 72 * time.Hour} {
		_, err := client.Heartbeat(ctxT(t), &hopperv1.HeartbeatRequest{
			Id: uuid.New().String(), LeaseId: uuid.New().String(),
			LeaseDuration: durationpb.New(want),
		})
		require.NoError(t, err)
		require.Equal(t, want, time.Duration(got.Load()))
	}
}

func TestHeartbeatInvalidDurationsNeverReachTheStore(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client, _ := newTestService(t, &fakeStore{
		extendFn: func(context.Context, uuid.UUID, uuid.UUID, time.Duration) error {
			calls.Add(1)
			return nil
		},
	})

	for name, bad := range invalidDurations {
		t.Run(name, func(t *testing.T) {
			_, err := client.Heartbeat(ctxT(t), &hopperv1.HeartbeatRequest{
				Id: uuid.New().String(), LeaseId: uuid.New().String(),
				LeaseDuration: bad,
			})
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
	require.Zero(t, calls.Load())
}

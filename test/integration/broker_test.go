package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/internal/store/postgres"
)

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// leaseOne opens a lease stream with credit for exactly one job and returns the
// job it delivers. leaseDur may be zero for the broker default.
func leaseOne(t *testing.T, ctx context.Context, client hopperv1.HopperServiceClient, queue, worker string, leaseDur time.Duration) *hopperv1.Job {
	t.Helper()

	sub := &hopperv1.Subscription{Queues: []string{queue}, WorkerId: worker, Credit: 1}
	if leaseDur > 0 {
		sub.LeaseDuration = durationpb.New(leaseDur)
	}
	stream, err := client.LeaseJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: sub},
	}))
	got, err := stream.Recv()
	require.NoError(t, err)
	return got.GetJob()
}

func enqueue(t *testing.T, ctx context.Context, client hopperv1.HopperServiceClient, queue string) *hopperv1.Job {
	t.Helper()
	resp, err := client.Enqueue(ctx, &hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: queue}})
	require.NoError(t, err)
	return resp.GetJob()
}

func jobCount(t *testing.T, ctx context.Context) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx, "select count(*) from jobs").Scan(&n))
	return n
}

// TestEnqueueLeaseAck is the session's "done when", as a test: a job goes in
// over gRPC, comes out of the lease stream, is acked, and the store agrees.
func TestEnqueueLeaseAck(t *testing.T) {
	ctx := ctxT(t)
	client := newClient(t)

	enq, err := client.Enqueue(ctx, &hopperv1.EnqueueRequest{
		Job: &hopperv1.JobSpec{Queue: "demo", Payload: []byte(`{"n":1}`)},
	})
	require.NoError(t, err)
	require.False(t, enq.GetDeduplicated())
	require.Equal(t, hopperv1.JobState_JOB_STATE_AVAILABLE, enq.GetJob().GetState())

	stream, err := client.LeaseJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"demo"}, WorkerId: "worker-1", Credit: 1,
		}},
	}))

	got, err := stream.Recv()
	require.NoError(t, err)
	job := got.GetJob()
	require.Equal(t, enq.GetJob().GetId(), job.GetId())
	require.Equal(t, hopperv1.JobState_JOB_STATE_LEASED, job.GetState())
	require.Equal(t, int32(1), job.GetAttempts(), "attempts is counted at lease time")
	require.NotEmpty(t, job.GetLeaseId())
	require.Equal(t, "worker-1", job.GetLeasedBy())
	require.JSONEq(t, `{"n":1}`, string(job.GetPayload()))

	_, err = client.Ack(ctx, &hopperv1.AckRequest{Id: job.GetId(), LeaseId: job.GetLeaseId()})
	require.NoError(t, err)

	after, err := client.GetJob(ctx, &hopperv1.GetJobRequest{Id: job.GetId()})
	require.NoError(t, err)
	require.Equal(t, hopperv1.JobState_JOB_STATE_COMPLETED, after.GetJob().GetState())
	require.NotNil(t, after.GetJob().GetFinishedAt())
}

// TestIdempotentEnqueueOverGRPC is docs/semantics.md §8 on the wire.
func TestIdempotentEnqueueOverGRPC(t *testing.T) {
	ctx := ctxT(t)
	client := newClient(t)

	key := "order-1"
	spec := &hopperv1.JobSpec{Queue: "orders", IdempotencyKey: &key}

	first, err := client.Enqueue(ctx, &hopperv1.EnqueueRequest{Job: spec})
	require.NoError(t, err)
	require.False(t, first.GetDeduplicated())

	second, err := client.Enqueue(ctx, &hopperv1.EnqueueRequest{Job: spec})
	require.NoError(t, err)
	require.True(t, second.GetDeduplicated())
	require.Equal(t, first.GetJob().GetId(), second.GetJob().GetId())
}

// TestStaleAckOverGRPC is the §5 miss table reaching a real client: the wire
// code for "you no longer hold this job" is Aborted.
func TestStaleAckOverGRPC(t *testing.T) {
	ctx := ctxT(t)
	client := newClient(t)

	enqueue(t, ctx, client, "q")
	job := leaseOne(t, ctx, client, "q", "w", 0)

	// A lease id that never existed is exactly what a worker whose lease was
	// reaped and re-granted would present.
	_, err := client.Ack(ctx, &hopperv1.AckRequest{
		Id:      job.GetId(),
		LeaseId: "00000000-0000-0000-0000-000000000001",
	})
	require.Equal(t, codes.Aborted, status.Code(err))
}

// TestDelayedJobArrivesAfterThePromoter proves the RPC stream notices a
// store promotion; TestBrokerLifecycle separately proves process wiring.
// The broker under test here does not run sweeps — newClient does not start
// them — so the promotion comes from a direct store call.
func TestDelayedJobArrivesAfterThePromoter(t *testing.T) {
	ctx := ctxT(t)
	client := newClient(t)

	enq, err := client.Enqueue(ctx, &hopperv1.EnqueueRequest{
		Job: &hopperv1.JobSpec{
			Queue: "later",
			RunAt: timestamppb.New(time.Now().Add(200 * time.Millisecond)),
		},
	})
	require.NoError(t, err)
	require.Equal(t, hopperv1.JobState_JOB_STATE_SCHEDULED, enq.GetJob().GetState())

	stream, err := client.LeaseJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"later"}, WorkerId: "w", Credit: 1,
		}},
	}))

	require.Eventually(t, func() bool {
		var due bool
		err := pool.QueryRow(ctx, "select run_at <= now() from jobs where id::text = $1", enq.GetJob().GetId()).Scan(&due)
		return err == nil && due
	}, 3*time.Second, 10*time.Millisecond)
	n, err := postgres.New(pool).PromoteDue(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	got, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, enq.GetJob().GetId(), got.GetJob().GetId())
}

// TestNackReschedulesOverGRPC: a failed attempt goes back to scheduled with
// backoff and keeps the handler's message for whoever reads the row.
func TestNackReschedulesOverGRPC(t *testing.T) {
	ctx := ctxT(t)
	client := newClient(t)

	enqueue(t, ctx, client, "q")
	job := leaseOne(t, ctx, client, "q", "w", 0)

	_, err := client.Nack(ctx, &hopperv1.NackRequest{Id: job.GetId(), LeaseId: job.GetLeaseId(), Error: "boom"})
	require.NoError(t, err)

	after, err := client.GetJob(ctx, &hopperv1.GetJobRequest{Id: job.GetId()})
	require.NoError(t, err)
	require.Equal(t, hopperv1.JobState_JOB_STATE_SCHEDULED, after.GetJob().GetState())
	require.Equal(t, "boom", after.GetJob().GetLastError())
	require.Equal(t, int32(1), after.GetJob().GetAttempts(), "Nack does not change attempts; they were counted at lease")
	require.True(t, after.GetJob().GetRunAt().AsTime().After(time.Now()), "the retry waits out a backoff")
}

// TestHeartbeatExtendsTheLease: the lease the heartbeat sets is measured from
// now, not added to the old expiry.
func TestHeartbeatExtendsTheLease(t *testing.T) {
	ctx := ctxT(t)
	client := newClient(t)

	enqueue(t, ctx, client, "q")
	job := leaseOne(t, ctx, client, "q", "w", 5*time.Second)
	before := job.GetLeaseExpires().AsTime()

	_, err := client.Heartbeat(ctx, &hopperv1.HeartbeatRequest{
		Id: job.GetId(), LeaseId: job.GetLeaseId(), LeaseDuration: durationpb.New(time.Minute),
	})
	require.NoError(t, err)

	after, err := client.GetJob(ctx, &hopperv1.GetJobRequest{Id: job.GetId()})
	require.NoError(t, err)
	require.Equal(t, hopperv1.JobState_JOB_STATE_LEASED, after.GetJob().GetState())
	require.WithinDuration(t, before.Add(55*time.Second), after.GetJob().GetLeaseExpires().AsTime(), 5*time.Second)
}

// TestRepeatedAckSucceeds: an Ack whose response was lost is retried by the
// SDK, and the retry must not look like a failure (docs/semantics.md §5).
func TestRepeatedAckSucceeds(t *testing.T) {
	ctx := ctxT(t)
	client := newClient(t)

	enqueue(t, ctx, client, "q")
	job := leaseOne(t, ctx, client, "q", "w", 0)

	req := &hopperv1.AckRequest{Id: job.GetId(), LeaseId: job.GetLeaseId()}
	_, err := client.Ack(ctx, req)
	require.NoError(t, err)
	_, err = client.Ack(ctx, req)
	require.NoError(t, err, "the second Ack of the same lease is a no-op success")
}

// TestCancelledLeasedJobTellsTheWorkerToStop: a worker holding a job that is
// cancelled learns it from FailedPrecondition on its next call, which is
// distinct from Aborted ("someone else has it").
func TestCancelledLeasedJobTellsTheWorkerToStop(t *testing.T) {
	ctx := ctxT(t)
	client := newClient(t)

	enqueue(t, ctx, client, "q")
	job := leaseOne(t, ctx, client, "q", "w", 0)

	cancelled, err := client.Cancel(ctx, &hopperv1.CancelRequest{Id: job.GetId()})
	require.NoError(t, err)
	require.False(t, cancelled.GetAlreadyCancelled())
	require.Equal(t, hopperv1.JobState_JOB_STATE_CANCELLED, cancelled.GetJob().GetState())

	_, err = client.Heartbeat(ctx, &hopperv1.HeartbeatRequest{Id: job.GetId(), LeaseId: job.GetLeaseId()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "Heartbeat")
	_, err = client.Ack(ctx, &hopperv1.AckRequest{Id: job.GetId(), LeaseId: job.GetLeaseId()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "Ack")
	_, err = client.Nack(ctx, &hopperv1.NackRequest{Id: job.GetId(), LeaseId: job.GetLeaseId(), Error: "x"})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "Nack")

	again, err := client.Cancel(ctx, &hopperv1.CancelRequest{Id: job.GetId()})
	require.NoError(t, err, "a retried Cancel is not a failure")
	require.True(t, again.GetAlreadyCancelled())
}

// TestCancelOnCompletedJobFails: the presentation decision covers only the
// already-cancelled state. A completed job is not in the state the caller asked
// for, and the real store's error survives the trip.
func TestCancelOnCompletedJobFails(t *testing.T) {
	ctx := ctxT(t)
	client := newClient(t)

	enqueue(t, ctx, client, "q")
	job := leaseOne(t, ctx, client, "q", "w", 0)
	_, err := client.Ack(ctx, &hopperv1.AckRequest{Id: job.GetId(), LeaseId: job.GetLeaseId()})
	require.NoError(t, err)

	_, err = client.Cancel(ctx, &hopperv1.CancelRequest{Id: job.GetId()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "completed")
}

// TestStaleTokenAfterReapAndRelease: the two ways a lease is lost for real. The
// store's Ack ignores lease_expires, so an expired lease is only lost once the
// reaper has actually taken the job back; and after it is re-granted, the old
// token still must not act on the new delivery.
func TestStaleTokenAfterReapAndRelease(t *testing.T) {
	ctx := ctxT(t)
	// A millisecond of backoff so the reaped job is promotable almost at once.
	st := postgres.New(pool).WithRetryPolicy(store.RetryPolicy{Base: time.Millisecond, Cap: time.Millisecond})
	client := newClientFor(t, st)

	enq := enqueue(t, ctx, client, "q")
	first := leaseOne(t, ctx, client, "q", "w1", time.Millisecond)

	require.Eventually(t, func() bool {
		var expired bool
		err := pool.QueryRow(ctx, "select lease_expires < now() from jobs where id::text = $1", enq.GetId()).Scan(&expired)
		return err == nil && expired
	}, 3*time.Second, 5*time.Millisecond)
	reaped, err := st.ReapExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)

	_, err = client.Ack(ctx, &hopperv1.AckRequest{Id: first.GetId(), LeaseId: first.GetLeaseId()})
	require.Equal(t, codes.Aborted, status.Code(err), "after the reaper took it back")

	require.Eventually(t, func() bool {
		n, err := st.PromoteDue(ctx, 10)
		return err == nil && n == 1
	}, 3*time.Second, 10*time.Millisecond)
	second := leaseOne(t, ctx, client, "q", "w2", 0)
	require.Equal(t, first.GetId(), second.GetId())
	require.NotEqual(t, first.GetLeaseId(), second.GetLeaseId(), "each delivery has its own token")

	_, err = client.Ack(ctx, &hopperv1.AckRequest{Id: first.GetId(), LeaseId: first.GetLeaseId()})
	require.Equal(t, codes.Aborted, status.Code(err), "the old token against the new delivery")
	_, err = client.Ack(ctx, &hopperv1.AckRequest{Id: second.GetId(), LeaseId: second.GetLeaseId()})
	require.NoError(t, err, "the current holder still can")
}

// TestBatchDatabaseRejectionKeepsPrefix pins the deliberately narrow
// prevalidation promise. The broker checks JSON syntax, and \u0000 is valid
// JSON syntax; PostgreSQL's jsonb refuses it. The first item has committed by
// then, the client learns only a sanitized Internal, and stable keys are what
// make the retry safe.
func TestBatchDatabaseRejectionKeepsPrefix(t *testing.T) {
	ctx := ctxT(t)
	client := newClient(t)

	k1, k2 := "batch-1", "batch-2"
	good := &hopperv1.JobSpec{Queue: "b", IdempotencyKey: &k1, Payload: []byte(`{"ok":true}`)}
	bad := &hopperv1.JobSpec{Queue: "b", IdempotencyKey: &k2, Payload: []byte(`{"x":"\u0000"}`)}

	_, err := client.EnqueueBatch(ctx, &hopperv1.EnqueueBatchRequest{Jobs: []*hopperv1.JobSpec{good, bad}})
	require.Equal(t, codes.Internal, status.Code(err))
	require.NotContains(t, err.Error(), "SQLSTATE")
	require.NotContains(t, err.Error(), "jsonb")
	require.Equal(t, 1, jobCount(t, ctx), "the first item committed before the database rejected the second")

	again, err := client.Enqueue(ctx, &hopperv1.EnqueueRequest{Job: good})
	require.NoError(t, err)
	require.True(t, again.GetDeduplicated(), "the committed prefix is what a keyed retry finds")

	_, err = client.EnqueueBatch(ctx, &hopperv1.EnqueueBatchRequest{Jobs: []*hopperv1.JobSpec{good, bad}})
	require.Equal(t, codes.Internal, status.Code(err), "retrying the invalid item fails again")
	require.Equal(t, 1, jobCount(t, ctx))
}

// flakyStore fails one chosen Enqueue call, once, the way a dropped connection
// would, and otherwise delegates to the real store.
type flakyStore struct {
	store.Store
	mu     sync.Mutex
	calls  int
	failOn int // 1-based Enqueue call to fail
}

func (f *flakyStore) Enqueue(ctx context.Context, p store.EnqueueParams) (store.Job, bool, error) {
	f.mu.Lock()
	f.calls++
	fail := f.calls == f.failOn
	f.mu.Unlock()
	if fail {
		return store.Job{}, false, errors.New("connection reset by peer")
	}
	return f.Store.Enqueue(ctx, p)
}

// TestBatchRetryAfterTransientErrorDoesNotDuplicate: the retry story the proto
// comment promises. A batch that fails part way is retried whole with the same
// keys; the committed prefix deduplicates and nothing is inserted twice.
func TestBatchRetryAfterTransientErrorDoesNotDuplicate(t *testing.T) {
	ctx := ctxT(t)
	client := newClientFor(t, &flakyStore{Store: postgres.New(pool), failOn: 2})

	specs := make([]*hopperv1.JobSpec, 3)
	for i, k := range []string{"a", "b", "c"} {
		key := "retry-" + k
		specs[i] = &hopperv1.JobSpec{Queue: "b", IdempotencyKey: &key}
	}

	_, err := client.EnqueueBatch(ctx, &hopperv1.EnqueueBatchRequest{Jobs: specs})
	require.Equal(t, codes.Internal, status.Code(err))
	require.Equal(t, 1, jobCount(t, ctx), "only the item before the failure was written")

	resp, err := client.EnqueueBatch(ctx, &hopperv1.EnqueueBatchRequest{Jobs: specs})
	require.NoError(t, err)
	require.Len(t, resp.GetResults(), 3)
	require.True(t, resp.GetResults()[0].GetDeduplicated(), "the committed item is found, not re-inserted")
	require.False(t, resp.GetResults()[1].GetDeduplicated())
	require.False(t, resp.GetResults()[2].GetDeduplicated())
	require.Equal(t, 3, jobCount(t, ctx), "one row per key")
}

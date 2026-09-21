package broker

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

func TestEnqueuePassesParamsThroughAndReturnsTheJob(t *testing.T) {
	t.Parallel()

	at := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	key := "order-1"
	var got store.EnqueueParams
	job := sampleJob("orders")

	client, _ := newTestService(t, &fakeStore{
		enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			got = p
			return job, true, nil
		},
	})

	resp, err := client.Enqueue(ctxT(t), &hopperv1.EnqueueRequest{
		Job: &hopperv1.JobSpec{
			Queue:          "orders",
			Payload:        []byte(`{"n":1}`),
			Priority:       5,
			RunAt:          timestamppb.New(at),
			MaxAttempts:    9,
			IdempotencyKey: &key,
		},
	})
	require.NoError(t, err)

	require.Equal(t, "orders", got.Queue)
	require.Equal(t, int16(5), got.Priority)
	require.Equal(t, int32(9), got.MaxAttempts)
	require.Equal(t, at.UnixMicro(), got.RunAt.UnixMicro())
	require.Equal(t, "order-1", *got.IdempotencyKey)

	require.Equal(t, job.ID.String(), resp.GetJob().GetId())
	require.False(t, resp.GetDeduplicated())
}

// TestEnqueueReportsDeduplication is the wire half of docs/semantics.md §8 and
// the hook Session 7's dedup counter reads. The store's `inserted` is inverted
// here on purpose: the store reports what it did, the API reports what the
// caller cares about.
func TestEnqueueReportsDeduplication(t *testing.T) {
	t.Parallel()

	existing := sampleJob("orders")
	client, _ := newTestService(t, &fakeStore{
		enqueueFn: func(context.Context, store.EnqueueParams) (store.Job, bool, error) {
			return existing, false, nil
		},
	})

	key := "dup"
	resp, err := client.Enqueue(ctxT(t), &hopperv1.EnqueueRequest{
		Job: &hopperv1.JobSpec{Queue: "orders", IdempotencyKey: &key},
	})
	require.NoError(t, err)
	require.True(t, resp.GetDeduplicated(), "a duplicate is a success, not an error")
	require.Equal(t, existing.ID.String(), resp.GetJob().GetId())
}

func TestEnqueueValidationIsInvalidArgument(t *testing.T) {
	t.Parallel()

	// No enqueueFn: reaching the store at all panics, which is the assertion.
	client, _ := newTestService(t, &fakeStore{})

	for _, tc := range []struct {
		name string
		req  *hopperv1.EnqueueRequest
	}{
		{"no job", &hopperv1.EnqueueRequest{}},
		{"no queue", &hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{}}},
		{"bad json", &hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: "q", Payload: []byte(`{`)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.Enqueue(ctxT(t), tc.req)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestEnqueueStoreFailureIsInternalAndSaysNothingElse(t *testing.T) {
	t.Parallel()

	client, _ := newTestService(t, &fakeStore{
		enqueueFn: func(context.Context, store.EnqueueParams) (store.Job, bool, error) {
			return store.Job{}, false, errors.New(`ERROR: relation "jobs" does not exist (SQLSTATE 42P01)`)
		},
	})

	_, err := client.Enqueue(ctxT(t), &hopperv1.EnqueueRequest{
		Job: &hopperv1.JobSpec{Queue: "q"},
	})
	require.Equal(t, codes.Internal, status.Code(err))
	require.NotContains(t, err.Error(), "SQLSTATE")
}

// TestEnqueueBatchValidatesEverythingFirst is the promise in the proto comment:
// a batch with one bad spec inserts nothing at all, rather than inserting the
// good ones up to the bad one.
func TestEnqueueBatchValidatesEverythingFirst(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client, _ := newTestService(t, &fakeStore{
		enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			calls.Add(1)
			return sampleJob(p.Queue), true, nil
		},
	})

	_, err := client.EnqueueBatch(ctxT(t), &hopperv1.EnqueueBatchRequest{
		Jobs: []*hopperv1.JobSpec{
			{Queue: "good"},
			{Queue: ""}, // rejected
		},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "jobs[1]", "the message names the offending item")
	require.Zero(t, calls.Load(), "validation runs over the whole batch before any insert")
}

// TestEnqueueBatchValidatesTimestampsBeforeAnyInsert pins the last item of the
// batch being the invalid one: a check that ran lazily, at conversion time in
// the insert loop, would already have written the first two.
func TestEnqueueBatchValidatesTimestampsBeforeAnyInsert(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client, _ := newTestService(t, &fakeStore{
		enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			calls.Add(1)
			return sampleJob(p.Queue), true, nil
		},
	})

	_, err := client.EnqueueBatch(ctxT(t), &hopperv1.EnqueueBatchRequest{
		Jobs: []*hopperv1.JobSpec{
			{Queue: "a"},
			{Queue: "b"},
			{Queue: "c", RunAt: &timestamppb.Timestamp{Seconds: 253402300800}}, // year 10000
		},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "jobs[2]")
	require.Zero(t, calls.Load())
}

// TestEnqueueBatchPartialFailure is the honest half of the non-atomic contract:
// once the store fails on item 2, items 0 and 1 are committed, item 3 is never
// attempted, and the client sees only a sanitized Internal.
func TestEnqueueBatchPartialFailure(t *testing.T) {
	t.Parallel()

	var queues []string
	client, _ := newTestService(t, &fakeStore{
		enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			queues = append(queues, p.Queue) // handler is sequential; no lock needed
			if len(queues) == 3 {
				return store.Job{}, false, errors.New(`ERROR: invalid input syntax (SQLSTATE 22P02)`)
			}
			return sampleJob(p.Queue), true, nil
		},
	})

	_, err := client.EnqueueBatch(ctxT(t), &hopperv1.EnqueueBatchRequest{
		Jobs: []*hopperv1.JobSpec{{Queue: "a"}, {Queue: "b"}, {Queue: "c"}, {Queue: "d"}},
	})
	require.Equal(t, codes.Internal, status.Code(err))
	require.NotContains(t, err.Error(), "SQLSTATE")
	require.Equal(t, []string{"a", "b", "c"}, queues, "the earlier items were written and the later one never tried")
}

// TestEnqueueBatchStopsWhenTheCallerGoesAway uses a fake that honours its
// context, as the real store does. A client deadline part way through must stop
// the loop rather than keep inserting on behalf of a caller that has left.
func TestEnqueueBatchStopsWhenTheCallerGoesAway(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client, _ := newTestService(t, &fakeStore{
		enqueueFn: func(ctx context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			if calls.Add(1) == 1 {
				return sampleJob(p.Queue), true, nil
			}
			<-ctx.Done()
			return store.Job{}, false, ctx.Err()
		},
	})

	ctx, cancel := context.WithTimeout(ctxT(t), 200*time.Millisecond)
	defer cancel()
	_, err := client.EnqueueBatch(ctx, &hopperv1.EnqueueBatchRequest{
		Jobs: []*hopperv1.JobSpec{{Queue: "a"}, {Queue: "b"}, {Queue: "c"}},
	})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))

	require.Eventually(t, func() bool { return calls.Load() == 2 }, 2*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { return calls.Load() > 2 }, 100*time.Millisecond, 10*time.Millisecond,
		"the third item must not be inserted after the caller's deadline")
}

// TestBatchResponseTooLarge: the response can exceed the transport ceiling
// after every store call has already succeeded. The client gets
// ResourceExhausted and must not read that as "nothing was written" — which is
// why the wire docs promise no rollback.
func TestBatchResponseTooLarge(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	// A valid JSON string of ~1.3 MiB. Five of them exceed the 4 MiB ceiling,
	// while every request spec stays a few bytes.
	fat := []byte(`"` + strings.Repeat("x", 1300<<10) + `"`)
	client, _ := newTestService(t, &fakeStore{
		enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			calls.Add(1)
			j := sampleJob(p.Queue)
			j.Payload = fat
			return j, false, nil // an existing, deduplicated row
		},
	})

	specs := make([]*hopperv1.JobSpec, 5)
	for i := range specs {
		key := "k" + string(rune('a'+i))
		specs[i] = &hopperv1.JobSpec{Queue: "q", IdempotencyKey: &key}
	}
	_, err := client.EnqueueBatch(ctxT(t), &hopperv1.EnqueueBatchRequest{Jobs: specs})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, int32(5), calls.Load(), "every store call completed before the response failed")
}

func TestEnqueueBatchReturnsResultsInRequestOrder(t *testing.T) {
	t.Parallel()

	client, _ := newTestService(t, &fakeStore{
		enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			j := sampleJob(p.Queue)
			// Dedup the second one, so the flags are not uniformly false.
			return j, p.Queue != "b", nil
		},
	})

	resp, err := client.EnqueueBatch(ctxT(t), &hopperv1.EnqueueBatchRequest{
		Jobs: []*hopperv1.JobSpec{{Queue: "a"}, {Queue: "b"}, {Queue: "c"}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetResults(), 3)
	require.Equal(t, "a", resp.GetResults()[0].GetJob().GetQueue())
	require.Equal(t, "b", resp.GetResults()[1].GetJob().GetQueue())
	require.Equal(t, "c", resp.GetResults()[2].GetJob().GetQueue())
	require.False(t, resp.GetResults()[0].GetDeduplicated())
	require.True(t, resp.GetResults()[1].GetDeduplicated())
}

func TestEnqueueBatchRejectsEmpty(t *testing.T) {
	t.Parallel()
	client, _ := newTestService(t, &fakeStore{})
	_, err := client.EnqueueBatch(ctxT(t), &hopperv1.EnqueueBatchRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

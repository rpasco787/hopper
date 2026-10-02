package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// fakeBroker stands in for a broker. Every job it accepts is stored as
// completed, leased 10ms and finished 30ms after it was created.
type fakeBroker struct {
	mu         sync.Mutex
	delay      time.Duration // per call
	failCall   int           // 1-based call that fails with UNAVAILABLE; 0 never
	calls      int
	single     int   // Enqueue calls
	batchSizes []int // EnqueueBatch sizes
	keys       []string
	jobs       []*hopperv1.Job
	// pending is how many drain checks still see an unfinished job.
	pending int
	// foreign jobs sit in the queue but were not enqueued by this run.
	foreign int
	// state overrides the stored state of the job at each index.
	state map[int]hopperv1.JobState
}

func (f *fakeBroker) accept(specs []*hopperv1.JobSpec) ([]*hopperv1.Job, error) {
	time.Sleep(f.delay)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls == f.failCall {
		return nil, status.Error(codes.Unavailable, "injected")
	}
	out := make([]*hopperv1.Job, len(specs))
	created := time.Now()
	for i, s := range specs {
		if s.IdempotencyKey != nil {
			f.keys = append(f.keys, *s.IdempotencyKey)
		}
		j := &hopperv1.Job{
			Id: uuid.NewString(), Queue: s.GetQueue(), Payload: s.GetPayload(),
			State: hopperv1.JobState_JOB_STATE_COMPLETED, Attempts: 1,
			CreatedAt:  timestamppb.New(created),
			LeasedAt:   timestamppb.New(created.Add(10 * time.Millisecond)),
			FinishedAt: timestamppb.New(created.Add(30 * time.Millisecond)),
		}
		if st, ok := f.state[len(f.jobs)]; ok {
			j.State = st
		}
		f.jobs = append(f.jobs, j)
		out[i] = j
	}
	return out, nil
}

func (f *fakeBroker) Enqueue(_ context.Context, req *hopperv1.EnqueueRequest, _ ...grpc.CallOption) (*hopperv1.EnqueueResponse, error) {
	jobs, err := f.accept([]*hopperv1.JobSpec{req.GetJob()})
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.single++
	f.mu.Unlock()
	return &hopperv1.EnqueueResponse{Job: jobs[0]}, nil
}

func (f *fakeBroker) EnqueueBatch(_ context.Context, req *hopperv1.EnqueueBatchRequest, _ ...grpc.CallOption) (*hopperv1.EnqueueBatchResponse, error) {
	jobs, err := f.accept(req.GetJobs())
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.batchSizes = append(f.batchSizes, len(req.GetJobs()))
	f.mu.Unlock()
	resp := &hopperv1.EnqueueBatchResponse{}
	for _, j := range jobs {
		resp.Results = append(resp.Results, &hopperv1.EnqueueResponse{Job: j})
	}
	return resp, nil
}

// ListJobs answers drain checks from pending, and pages everything else by
// index, with foreign jobs first.
func (f *fakeBroker) ListJobs(_ context.Context, req *hopperv1.ListJobsRequest, _ ...grpc.CallOption) (*hopperv1.ListJobsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(req.GetStates()) > 0 {
		if f.pending > 0 {
			f.pending--
			return &hopperv1.ListJobsResponse{Jobs: []*hopperv1.Job{{Id: uuid.NewString()}}}, nil
		}
		return &hopperv1.ListJobsResponse{}, nil
	}
	all := make([]*hopperv1.Job, 0, f.foreign+len(f.jobs))
	for range f.foreign {
		all = append(all, &hopperv1.Job{Id: uuid.NewString(), State: hopperv1.JobState_JOB_STATE_COMPLETED})
	}
	all = append(all, f.jobs...)
	from := 0
	if tok := req.GetPageToken(); tok != "" {
		from, _ = strconv.Atoi(tok)
	}
	to := min(from+int(req.GetLimit()), len(all))
	resp := &hopperv1.ListJobsResponse{Jobs: all[from:to]}
	if to-from == int(req.GetLimit()) {
		resp.NextPageToken = strconv.Itoa(to)
	}
	return resp, nil
}

func dialFake(f *fakeBroker) dialer {
	return func(string) (api, io.Closer, error) { return f, io.NopCloser(nil), nil }
}

func run(t *testing.T, f *fakeBroker, args ...string) (*Report, string, error) {
	t.Helper()
	drainPoll = time.Millisecond
	var stdout, stderr bytes.Buffer
	err := runWithContext(context.Background(), append(args, "-json", "-broker", "fake:1"), &stdout, &stderr, dialFake(f))
	var rep Report
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &rep), "run: %v; stdout: %s", err, stdout.String())
	return &rep, stderr.String(), err
}

func TestConfigDefaultsAndValidation(t *testing.T) {
	t.Setenv("HOPPER_BROKER", "")
	c, err := parseConfig([]string{"-n", "1", "-broker", "a:1, b:2"})
	require.NoError(t, err)
	require.Equal(t, []string{"a:1", "b:2"}, c.brokers)
	require.Equal(t, "sleep", c.queue)
	require.Equal(t, 4, c.producers)
	require.Equal(t, 1, c.batch)
	require.True(t, c.wait)
	require.Regexp(t, `^lg-\d+$`, c.run)

	t.Setenv("HOPPER_BROKER", "env:9")
	c, err = parseConfig([]string{"-n", "1"})
	require.NoError(t, err)
	require.Equal(t, []string{"env:9"}, c.brokers, "HOPPER_BROKER applies when -broker is absent")

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{}, "set -n, -duration or both"},
		{[]string{"-n", "1", "-broker", "a,,b"}, "empty address"},
		{[]string{"-n", "1", "-batch", "1001"}, "batch must be between 1 and 1000"},
		{[]string{"-n", "1", "-producers", "0"}, "producers must be between"},
		{[]string{"-n", "1", "-rate", "-1"}, "rate must not be negative"},
		{[]string{"-n", "1", "-payload-bytes", "2000000"}, "payload-bytes must be between"},
		{[]string{"-n", "1", "-queue", ""}, "queue is required"},
		{[]string{"-n", "1", "-rpc-timeout", "0s"}, "must be positive"},
	} {
		_, err := parseConfig(tc.args)
		require.ErrorContains(t, err, tc.want, "%v", tc.args)
	}
}

func TestPayloadIsPaddedJSON(t *testing.T) {
	require.Equal(t, `{"ms":0}`, string(payload(0, 0)))
	p := payload(5, 300)
	require.GreaterOrEqual(t, len(p), 300)
	var body struct {
		Ms  int64  `json:"ms"`
		Pad string `json:"pad"`
	}
	require.NoError(t, json.Unmarshal(p, &body))
	require.Equal(t, int64(5), body.Ms)
	require.NotEmpty(t, body.Pad)
}

func TestProduceEnqueuesExactlyNInBatches(t *testing.T) {
	f := &fakeBroker{}
	rep, _, err := run(t, f, "-n", "1001", "-batch", "100", "-producers", "3")
	require.NoError(t, err)
	require.Equal(t, int64(1001), rep.Jobs)
	require.Equal(t, int64(11), rep.Calls)
	require.Zero(t, f.single)
	total := 0
	for _, n := range f.batchSizes {
		require.LessOrEqual(t, n, 100)
		total += n
	}
	require.Equal(t, 1001, total)
	require.Equal(t, int64(1001), rep.Finished)
	require.False(t, rep.Failed())
}

func TestSingleJobsUseEnqueueAndKeysAreUnique(t *testing.T) {
	f := &fakeBroker{}
	rep, _, err := run(t, f, "-n", "10", "-producers", "2", "-keys", "-run", "r1")
	require.NoError(t, err)
	require.Equal(t, 10, f.single)
	require.Empty(t, f.batchSizes)
	require.Len(t, f.keys, 10)
	seen := map[string]bool{}
	for _, k := range f.keys {
		require.Regexp(t, `^r1-\d+-\d+-0$`, k)
		require.False(t, seen[k], "key %s used twice", k)
		seen[k] = true
	}
	require.Equal(t, int64(10), rep.Jobs)
}

// Four producers at 200 jobs/s are each due every 20ms, staggered by 5ms, so
// exactly 25 calls each fall inside half a second.
func TestProduceOffersTheRate(t *testing.T) {
	f := &fakeBroker{}
	rep, _, err := run(t, f, "-rate", "200", "-duration", "500ms", "-producers", "4", "-wait=false")
	require.NoError(t, err)
	require.Equal(t, int64(100), rep.Calls)
	require.InDelta(t, 0.5, rep.ProduceSeconds, 0.2)
	require.False(t, rep.Waited)
}

// A broker slower than the schedule charges each call for the time it spent
// waiting to be sent, not only for its own round trip.
func TestLatencyCountsFromTheSchedule(t *testing.T) {
	f := &fakeBroker{delay: 50 * time.Millisecond}
	rep, _, err := run(t, f, "-rate", "100", "-n", "5", "-producers", "1", "-wait=false")
	require.NoError(t, err)
	// The fifth call was due at 40ms and answered at about 250ms.
	require.GreaterOrEqual(t, rep.Enqueue.Max, 190.0)
	require.GreaterOrEqual(t, rep.MaxLagMs, 150.0)
}

func TestTheFirstErrorStopsTheRun(t *testing.T) {
	f := &fakeBroker{failCall: 3, delay: time.Millisecond}
	rep, summary, err := run(t, f, "-n", "1000", "-producers", "2")
	require.ErrorContains(t, err, "failed")
	require.Equal(t, int64(1), rep.Errors)
	require.Contains(t, rep.FirstError, "Unavailable")
	require.Less(t, rep.Calls, int64(1000), "the other producer stopped too")
	require.False(t, rep.Waited, "a failed run does not wait for its jobs")
	require.Contains(t, summary, "errors=1")
}

func TestAwaitWaitsForTheDrainAndReadsOnlyItsJobs(t *testing.T) {
	f := &fakeBroker{pending: 3, foreign: 7}
	rep, _, err := run(t, f, "-n", "2500", "-batch", "500", "-producers", "2", "-payload-bytes", "4000")
	require.NoError(t, err)
	require.Zero(t, f.pending, "every drain check was used up before reading back")
	require.Equal(t, int64(2500), rep.Finished)
	require.Zero(t, rep.Unfinished)
	require.Equal(t, 2500, rep.Pickup.N)
	require.Equal(t, 10.0, rep.Pickup.P50)
	require.Equal(t, 30.0, rep.EndToEnd.P99)
	require.Positive(t, rep.Throughput)
}

func TestAwaitCountsDeadAndMissingJobsAsUnfinished(t *testing.T) {
	f := &fakeBroker{state: map[int]hopperv1.JobState{1: hopperv1.JobState_JOB_STATE_DEAD}}
	// The third job disappears from the queue before loadgen reads it back.
	dial := func(string) (api, io.Closer, error) { return &losingBroker{fakeBroker: f}, io.NopCloser(nil), nil }
	drainPoll = time.Millisecond
	var stdout, stderr bytes.Buffer
	err := runWithContext(context.Background(), []string{"-n", "3", "-producers", "1", "-json", "-broker", "x"}, &stdout, &stderr, dial)
	require.ErrorContains(t, err, "2 unfinished")
	var rep Report
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &rep))
	require.Equal(t, int64(1), rep.Finished)
	require.Equal(t, int64(2), rep.Unfinished)
}

// losingBroker drops the last job from every listing.
type losingBroker struct{ *fakeBroker }

func (l *losingBroker) ListJobs(ctx context.Context, req *hopperv1.ListJobsRequest, opts ...grpc.CallOption) (*hopperv1.ListJobsResponse, error) {
	resp, err := l.fakeBroker.ListJobs(ctx, req, opts...)
	if err == nil && len(req.GetStates()) == 0 && len(resp.Jobs) > 0 {
		resp.Jobs = resp.Jobs[:len(resp.Jobs)-1]
	}
	return resp, err
}

func TestPageLimitStaysUnderTheMessageCeiling(t *testing.T) {
	require.Equal(t, int32(1000), pageLimit(0))
	require.GreaterOrEqual(t, pageLimit(maxPayloadBytes), int32(1))
	for _, p := range []int{0, 1000, 4096, 65536, maxPayloadBytes} {
		require.LessOrEqual(t, int(pageLimit(p))*(p+600), 3<<20, "payload %d", p)
	}
}

// An interrupted run cannot be quoted as a completed offered load.
func TestCanceledRunFailsWithoutWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	err := runWithContext(ctx, []string{"-n", "5", "-wait=false", "-json"}, &stdout, &stderr, dialFake(&fakeBroker{}))
	require.ErrorContains(t, err, "context canceled")
	var rep Report
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &rep))
	require.True(t, rep.Failed())
	require.Positive(t, rep.Errors)
}

// A successful RPC must confirm exactly the submitted jobs, each with an ID.
type responseBroker struct {
	*fakeBroker
	singleResponse *hopperv1.EnqueueResponse
	batchResponse  *hopperv1.EnqueueBatchResponse
}

func (b *responseBroker) Enqueue(context.Context, *hopperv1.EnqueueRequest, ...grpc.CallOption) (*hopperv1.EnqueueResponse, error) {
	return b.singleResponse, nil
}
func (b *responseBroker) EnqueueBatch(context.Context, *hopperv1.EnqueueBatchRequest, ...grpc.CallOption) (*hopperv1.EnqueueBatchResponse, error) {
	return b.batchResponse, nil
}

func TestCallRejectsIncompleteOrDuplicateConfirmations(t *testing.T) {
	job := func(id string) *hopperv1.EnqueueResponse {
		return &hopperv1.EnqueueResponse{Job: &hopperv1.Job{Id: id}}
	}
	for _, tc := range []struct {
		name   string
		batch  int
		broker *responseBroker
	}{
		{"nil single", 1, &responseBroker{}},
		{"missing single id", 1, &responseBroker{singleResponse: job("")}},
		{"nil batch", 2, &responseBroker{}},
		{"short batch", 2, &responseBroker{batchResponse: &hopperv1.EnqueueBatchResponse{Results: []*hopperv1.EnqueueResponse{job("a")}}}},
		{"empty batch id", 2, &responseBroker{batchResponse: &hopperv1.EnqueueBatchResponse{Results: []*hopperv1.EnqueueResponse{job("a"), job("")}}}},
		{"duplicate batch id", 2, &responseBroker{batchResponse: &hopperv1.EnqueueBatchResponse{Results: []*hopperv1.EnqueueResponse{job("a"), job("a")}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := call(context.Background(), config{batch: tc.batch, rpcTimeout: time.Second}, tc.broker, payload(0, 0), 0, 0, tc.batch)
			require.Error(t, err)
		})
	}
}

func TestProduceRejectsAnIDConfirmedBySeparateCalls(t *testing.T) {
	b := &responseBroker{singleResponse: &hopperv1.EnqueueResponse{Job: &hopperv1.Job{Id: "same"}}}
	rep := &Report{}
	produce(context.Background(), config{n: 2, producers: 1, batch: 1, rpcTimeout: time.Second}, []api{b}, rep)
	require.True(t, rep.Failed())
	require.ErrorContains(t, fmt.Errorf("%s", rep.FirstError), "duplicate")
}

// Duplicate own-job rows must not stand in for a missing confirmed job.
type readbackBroker struct {
	*fakeBroker
	pages [][]*hopperv1.Job
}

func (b *readbackBroker) ListJobs(_ context.Context, req *hopperv1.ListJobsRequest, _ ...grpc.CallOption) (*hopperv1.ListJobsResponse, error) {
	if len(req.GetStates()) != 0 {
		return &hopperv1.ListJobsResponse{}, nil
	}
	page := 0
	if req.GetPageToken() != "" {
		page, _ = strconv.Atoi(req.GetPageToken())
	}
	resp := &hopperv1.ListJobsResponse{Jobs: b.pages[page]}
	if page+1 < len(b.pages) {
		resp.NextPageToken = strconv.Itoa(page + 1)
	}
	return resp, nil
}

func completedJob(id string) *hopperv1.Job {
	return &hopperv1.Job{Id: id, State: hopperv1.JobState_JOB_STATE_COMPLETED,
		CreatedAt:  timestamppb.New(time.Unix(100, 0)),
		LeasedAt:   timestamppb.New(time.Unix(100, 10_000_000)),
		FinishedAt: timestamppb.New(time.Unix(100, 30_000_000))}
}

func TestAwaitRejectsDuplicateOwnJobRowsAcrossPages(t *testing.T) {
	b := &readbackBroker{pages: [][]*hopperv1.Job{{completedJob("a")}, {completedJob("a")}}}
	err := await(context.Background(), config{drainTimeout: time.Second, rpcTimeout: time.Second}, b,
		map[string]struct{}{"a": {}, "b": {}}, &Report{})
	require.ErrorContains(t, err, "duplicate")
}

func TestAwaitRejectsUnusableCompletedTimestamps(t *testing.T) {
	for _, field := range []string{"created", "leased", "finished"} {
		for _, invalid := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/invalid=%v", field, invalid), func(t *testing.T) {
				j := completedJob("a")
				var ts *timestamppb.Timestamp
				if invalid {
					ts = &timestamppb.Timestamp{Seconds: 253402300800}
				}
				switch field {
				case "created":
					j.CreatedAt = ts
				case "leased":
					j.LeasedAt = ts
				case "finished":
					j.FinishedAt = ts
				}
				b := &readbackBroker{pages: [][]*hopperv1.Job{{j}}}
				err := await(context.Background(), config{drainTimeout: time.Second, rpcTimeout: time.Second}, b,
					map[string]struct{}{"a": {}}, &Report{})
				require.ErrorContains(t, err, "timestamp")
			})
		}
	}
}

func TestConfigRejectsRatesThatCannotBePaced(t *testing.T) {
	for _, rate := range []string{"NaN", "+Inf", "-Inf", "1e30", "1e-30"} {
		t.Run(rate, func(t *testing.T) {
			_, err := parseConfig([]string{"-n", "1", "-rate", rate})
			require.ErrorContains(t, err, "rate")
		})
	}
}

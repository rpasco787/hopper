package broker

import (
	"context"
	"errors"
	"io"
	"sync"
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

// leaseRecorder is a fake Lease that hands out n jobs per call, up to total,
// and records the arguments it was called with.
type leaseRecorder struct {
	mu        sync.Mutex
	calls     []int // the n requested on each call
	queues    []string
	dur       time.Duration
	workerID  string
	remaining int
}

func (r *leaseRecorder) fn(_ context.Context, queues []string, n int, d time.Duration, worker string) ([]store.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, n)
	r.queues, r.dur, r.workerID = queues, d, worker

	count := min(n, r.remaining)
	r.remaining -= count
	jobs := make([]store.Job, count)
	for i := range jobs {
		jobs[i] = sampleJob(queues[0])
	}
	return jobs, nil
}

func (r *leaseRecorder) maxRequested() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	maxN := 0
	for _, n := range r.calls {
		maxN = max(maxN, n)
	}
	return maxN
}

func TestLeaseStreamRequiresSubscriptionFirst(t *testing.T) {
	t.Parallel()
	client, _ := newTestService(t, &fakeStore{})

	stream, err := client.LeaseJobs(ctxT(t))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Credit{Credit: 5},
	}))
	_, err = stream.Recv()
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestLeaseStreamValidatesTheSubscription(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		sub  *hopperv1.Subscription
	}{
		{"no queues", &hopperv1.Subscription{WorkerId: "w", Credit: 1}},
		{"empty queue name", &hopperv1.Subscription{Queues: []string{""}, WorkerId: "w", Credit: 1}},
		{"no worker id", &hopperv1.Subscription{Queues: []string{"q"}, Credit: 1}},
		{"negative lease duration", &hopperv1.Subscription{
			Queues: []string{"q"}, WorkerId: "w", Credit: 1,
			LeaseDuration: durationpb.New(-time.Second),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := newTestService(t, &fakeStore{})
			stream, err := client.LeaseJobs(ctxT(t))
			require.NoError(t, err)
			require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
				Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: tc.sub},
			}))
			_, err = stream.Recv()
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

// TestLeaseStreamNeverExceedsCredit is the whole reason the stream is
// bidirectional. The worker declares capacity for 3; the store has 100 jobs
// ready; the broker must hand over exactly 3 and then stop.
func TestLeaseStreamNeverExceedsCredit(t *testing.T) {
	t.Parallel()

	rec := &leaseRecorder{remaining: 100}
	client, _ := newTestService(t, &fakeStore{leaseFn: rec.fn})

	streamCtx, cancelStream := context.WithCancel(ctxT(t))
	defer cancelStream()
	stream, err := client.LeaseJobs(streamCtx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"q"}, WorkerId: "w", Credit: 3,
		}},
	}))

	for i := range 3 {
		resp, err := stream.Recv()
		require.NoErrorf(t, err, "job %d", i)
		require.NotEmpty(t, resp.GetJob().GetId())
	}

	// Credit is spent. Nothing more may arrive, however many jobs are ready.
	recvCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	var extra atomic.Bool
	go func() {
		defer close(done)
		if _, err := stream.Recv(); err == nil {
			extra.Store(true)
		}
	}()
	select {
	case <-done:
	case <-recvCtx.Done():
	}
	cancelStream() // recvCtx alone does not cancel this RPC's Recv
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("extra Recv did not exit after cancellation")
	}
	require.False(t, extra.Load(), "the broker handed out more jobs than the worker had credit for")
	rec.mu.Lock()
	totalRequested := 0
	for _, n := range rec.calls {
		totalRequested += n
	}
	rec.mu.Unlock()
	require.Equal(t, 3, totalRequested)
	require.LessOrEqual(t, rec.maxRequested(), 3, "and it never asked the store for more than the credit")
}

// TestLeaseStreamResumesOnCredit: after the worker returns credit, delivery
// continues on the same stream without reconnecting.
func TestLeaseStreamResumesOnCredit(t *testing.T) {
	t.Parallel()

	rec := &leaseRecorder{remaining: 100}
	client, _ := newTestService(t, &fakeStore{leaseFn: rec.fn})

	stream, err := client.LeaseJobs(ctxT(t))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"q"}, WorkerId: "w", Credit: 1,
		}},
	}))
	_, err = stream.Recv()
	require.NoError(t, err)

	for range 4 {
		require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
			Msg: &hopperv1.LeaseJobsRequest_Credit{Credit: 1},
		}))
		_, err := stream.Recv()
		require.NoError(t, err)
	}
}

func TestLeaseStreamPassesSubscriptionToTheStore(t *testing.T) {
	t.Parallel()

	rec := &leaseRecorder{remaining: 1}
	client, _ := newTestService(t, &fakeStore{leaseFn: rec.fn})

	stream, err := client.LeaseJobs(ctxT(t))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues:        []string{"a", "b"},
			WorkerId:      "worker-7",
			Credit:        1,
			LeaseDuration: durationpb.New(90 * time.Second),
		}},
	}))
	_, err = stream.Recv()
	require.NoError(t, err)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Equal(t, []string{"a", "b"}, rec.queues)
	require.Equal(t, "worker-7", rec.workerID)
	require.Equal(t, 90*time.Second, rec.dur)
}

// TestLeaseStreamCapsTheBatch: a worker with 1000 credit requests at most
// MaxLeaseBatch rows per store call, not per stream lifetime or outstanding
// delivery set (docs/semantics.md §11).
func TestLeaseStreamCapsTheBatch(t *testing.T) {
	t.Parallel()

	rec := &leaseRecorder{remaining: 1000}
	svc := NewService(&fakeStore{leaseFn: rec.fn}, Config{
		MaxLeaseBatch: 4,
		PollInterval:  5 * time.Millisecond,
	})
	client := newTestClient(t, svc)

	stream, err := client.LeaseJobs(ctxT(t))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"q"}, WorkerId: "w", Credit: 100,
		}},
	}))
	for range 10 {
		_, err := stream.Recv()
		require.NoError(t, err)
	}
	require.LessOrEqual(t, rec.maxRequested(), 4)
}

// TestLeaseStreamPollsAnEmptyQueue: an empty queue is not an error and not a
// hot loop. The stream stays open and keeps asking.
func TestLeaseStreamPollsAnEmptyQueue(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	ready := make(chan struct{})
	var once sync.Once

	client, _ := newTestService(t, &fakeStore{
		leaseFn: func(_ context.Context, queues []string, n int, _ time.Duration, _ string) ([]store.Job, error) {
			if calls.Add(1) < 3 {
				return nil, nil // empty queue
			}
			once.Do(func() { close(ready) })
			return []store.Job{sampleJob(queues[0])}, nil
		},
	})

	stream, err := client.LeaseJobs(ctxT(t))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"q"}, WorkerId: "w", Credit: 1,
		}},
	}))

	resp, err := stream.Recv()
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetJob().GetId())
	<-ready
	require.GreaterOrEqual(t, calls.Load(), int64(3), "the broker polled rather than giving up")
}

func TestLeaseStreamStoreErrorEndsTheStream(t *testing.T) {
	t.Parallel()

	client, _ := newTestService(t, &fakeStore{
		leaseFn: func(context.Context, []string, int, time.Duration, string) ([]store.Job, error) {
			return nil, errors.New("connection reset by peer")
		},
	})

	stream, err := client.LeaseJobs(ctxT(t))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"q"}, WorkerId: "w", Credit: 1,
		}},
	}))
	_, err = stream.Recv()
	require.Equal(t, codes.Internal, status.Code(err))
}

// TestLeaseStreamEndsCleanlyOnHalfClose: a worker that is shutting down closes
// its send side, and the stream ends with io.EOF, not an error.
func TestLeaseStreamEndsCleanlyOnHalfClose(t *testing.T) {
	t.Parallel()

	client, _ := newTestService(t, &fakeStore{
		leaseFn: func(context.Context, []string, int, time.Duration, string) ([]store.Job, error) {
			return nil, nil
		},
	})

	stream, err := client.LeaseJobs(ctxT(t))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"q"}, WorkerId: "w", Credit: 1,
		}},
	}))
	require.NoError(t, stream.CloseSend())

	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF, "a half-close is a clean end, not a failure")
}

// TestLeaseStreamStopsOnDrain is what makes GracefulStop terminate. Without
// it the server would wait forever for a stream that has no reason to end.
func TestLeaseStreamStopsOnDrain(t *testing.T) {
	t.Parallel()

	client, svc := newTestService(t, &fakeStore{
		leaseFn: func(context.Context, []string, int, time.Duration, string) ([]store.Job, error) {
			return nil, nil
		},
	})

	stream, err := client.LeaseJobs(ctxT(t))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"q"}, WorkerId: "w", Credit: 1,
		}},
	}))

	svc.Drain()
	_, err = stream.Recv()
	require.Equal(t, codes.Unavailable, status.Code(err),
		"a draining broker tells the worker to reconnect elsewhere")
}

// invalidSubscriptionDurations reuse the table Heartbeat is tested with: the
// same leaseDuration rules apply to a Subscription, and nothing invalid may
// reach the store's Lease.
func TestLeaseStreamInvalidDurationsNeverReachLease(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	for name, bad := range invalidDurations {
		t.Run(name, func(t *testing.T) {
			client, _ := newTestService(t, &fakeStore{
				leaseFn: func(context.Context, []string, int, time.Duration, string) ([]store.Job, error) {
					calls.Add(1)
					return nil, nil
				},
			})
			stream, err := client.LeaseJobs(ctxT(t))
			require.NoError(t, err)
			require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
				Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
					Queues: []string{"q"}, WorkerId: "w", Credit: 1, LeaseDuration: bad,
				}},
			}))
			_, err = stream.Recv()
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
	require.Zero(t, calls.Load())
}

func TestDrainConcurrent(t *testing.T) {
	t.Parallel()

	svc := NewService(&fakeStore{}, Config{})
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			svc.Drain()
		}()
	}
	wg.Wait()
	select {
	case <-svc.shutdown:
	default:
		t.Fatal("not drained")
	}
}

// TestLeaseStreamBusyTermination: a stream whose queue is never empty and whose
// worker has effectively unlimited credit must still stop on drain and on
// half-close. Termination that only happened while idle would leave
// GracefulStop waiting on exactly the busiest workers.
func TestLeaseStreamBusyTermination(t *testing.T) {
	t.Parallel()

	for _, halfClose := range []bool{false, true} {
		name := "drain"
		if halfClose {
			name = "half_close"
		}
		t.Run(name, func(t *testing.T) {
			st := &fakeStore{leaseFn: func(ctx context.Context, _ []string, n int, _ time.Duration, _ string) ([]store.Job, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				out := make([]store.Job, n)
				for i := range out {
					out[i] = sampleJob("q")
				}
				return out, nil // never an empty queue
			}}
			client, svc := newTestService(t, st)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			stream, err := client.LeaseJobs(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(subscribeMsg(^uint32(0))))
			_, err = stream.Recv() // proves leasing started before termination
			require.NoError(t, err)

			if halfClose {
				require.NoError(t, stream.CloseSend())
			} else {
				svc.Drain()
			}
			for err == nil {
				_, err = stream.Recv()
			}
			if halfClose {
				require.ErrorIs(t, err, io.EOF)
			} else {
				require.Equal(t, codes.Unavailable, status.Code(err))
			}
		})
	}
}

// TestLeaseStreamDrainBeforeSubscribe: the handler is parked in its very first
// receive, with no subscription ever sent. Drain must still end it; otherwise
// a client that connects and says nothing pins GracefulStop forever.
func TestLeaseStreamDrainBeforeSubscribe(t *testing.T) {
	t.Parallel()

	tr := newStreamTracker()
	svc := NewService(&fakeStore{}, Config{})
	client := newTestClient(t, svc, tr.option())

	stream, err := client.LeaseJobs(ctxT(t))
	require.NoError(t, err)
	// Header blocks until the server answers, so a goroutine sitting in it also
	// has to be released when the stream ends.
	headerDone := make(chan struct{})
	go func() {
		defer close(headerDone)
		_, _ = stream.Header()
	}()
	tr.waitEntered(t) // the RPC is at the server, in its initial receive

	svc.Drain()
	_, err = stream.Recv()
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, codes.Unavailable, status.Code(tr.waitExited(t)))
	waitFor(t, headerDone, "the Header goroutine to exit")
}

// TestLeaseStreamDrainCancelsQuery: drain arrives while the handler is inside
// store.Lease. The query's context must be cancelled, not waited out.
func TestLeaseStreamDrainCancelsQuery(t *testing.T) {
	t.Parallel()

	entered, exited := make(chan struct{}), make(chan struct{})
	var once sync.Once
	client, svc := newTestService(t, &fakeStore{
		leaseFn: func(ctx context.Context, _ []string, _ int, _ time.Duration, _ string) ([]store.Job, error) {
			once.Do(func() { close(entered) })
			<-ctx.Done()
			close(exited)
			return nil, ctx.Err()
		},
	})

	stream, err := client.LeaseJobs(ctxT(t))
	require.NoError(t, err)
	require.NoError(t, stream.Send(subscribeMsg(1)))
	waitFor(t, entered, "Lease to be entered")

	svc.Drain()
	_, err = stream.Recv()
	require.Equal(t, codes.Unavailable, status.Code(err))
	waitFor(t, exited, "the Lease query to see its context cancelled")
}

// TestLeaseStreamBlockedSendTerminates: the handler is stuck in a transport
// Send that will not complete. Drain and half-close must still end it, and it
// must not wait for the Send: gRPC only cancels the transport after the
// handler returns. Once the test simulates that, the abandoned Send and the
// receive pump must both drain out.
func TestLeaseStreamBlockedSendTerminates(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		terminate func(*Service, *fakeLeaseStream)
		check     func(*testing.T, error)
	}{
		{
			name:      "drain",
			terminate: func(svc *Service, _ *fakeLeaseStream) { svc.Drain() },
			check: func(t *testing.T, err error) {
				require.Equal(t, codes.Unavailable, status.Code(err))
			},
		},
		{
			// More than the reader could buffer if it blocked on each credit
			// message, so the EOF behind them is only seen if it never does.
			name: "half close behind many credits",
			terminate: func(_ *Service, s *fakeLeaseStream) {
				for range 100 {
					s.in <- creditMsg(1)
				}
				close(s.in)
			},
			check: func(t *testing.T, err error) { require.NoError(t, err) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sendEntered := make(chan struct{})
			var once sync.Once
			stream := newFakeLeaseStream(t, func(ctx context.Context, _ *hopperv1.LeaseJobsResponse) error {
				once.Do(func() { close(sendEntered) })
				<-ctx.Done() // a transport that never makes progress
				return status.FromContextError(ctx.Err()).Err()
			})
			svc := NewService(&fakeStore{
				leaseFn: func(_ context.Context, _ []string, _ int, _ time.Duration, _ string) ([]store.Job, error) {
					return []store.Job{sampleJob("q")}, nil
				},
			}, Config{PollInterval: time.Millisecond})

			handlerDone := make(chan error, 1)
			go func() { handlerDone <- svc.LeaseJobs(stream) }()

			stream.in <- subscribeMsg(1)
			waitFor(t, sendEntered, "Send to block")

			tc.terminate(svc, stream)
			select {
			case err := <-handlerDone:
				tc.check(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("the handler did not return while its Send was blocked")
			}

			// gRPC cancels the transport once the handler has returned.
			stream.cancel()
			require.Eventually(t, stream.idle, 5*time.Second, 5*time.Millisecond,
				"the abandoned Send and the receive pump must both exit")
		})
	}
}

// TestLeaseStreamCreditValidation covers the credit protocol's edges.
func TestLeaseStreamCreditValidation(t *testing.T) {
	t.Parallel()

	t.Run("zero credit waits, then positive credit resumes", func(t *testing.T) {
		t.Parallel()

		var calls atomic.Int32
		client, _ := newTestService(t, &fakeStore{
			leaseFn: func(_ context.Context, queues []string, _ int, _ time.Duration, _ string) ([]store.Job, error) {
				calls.Add(1)
				return []store.Job{sampleJob(queues[0])}, nil
			},
		})
		stream, err := client.LeaseJobs(ctxT(t))
		require.NoError(t, err)
		require.NoError(t, stream.Send(subscribeMsg(0)))
		require.Never(t, func() bool { return calls.Load() > 0 }, 100*time.Millisecond, 10*time.Millisecond,
			"a worker with no capacity must not cause a lease query")

		// A zero credit message adds nothing.
		require.NoError(t, stream.Send(creditMsg(0)))
		require.Never(t, func() bool { return calls.Load() > 0 }, 50*time.Millisecond, 10*time.Millisecond)

		require.NoError(t, stream.Send(creditMsg(1)))
		resp, err := stream.Recv()
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetJob().GetId())
	})

	for _, tc := range []struct {
		name string
		msg  *hopperv1.LeaseJobsRequest
	}{
		{"duplicate subscription", subscribeMsg(0)},
		{"empty message", &hopperv1.LeaseJobsRequest{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newTestService(t, &fakeStore{})
			stream, err := client.LeaseJobs(ctxT(t))
			require.NoError(t, err)
			require.NoError(t, stream.Send(subscribeMsg(0)))
			require.NoError(t, stream.Send(tc.msg))
			_, err = stream.Recv()
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}

	// The outstanding total is a uint32 on the wire's terms. Adding past it must
	// fail loudly; wrapping to a small number would silently starve the worker.
	// Lease is gated so the credit has not been spent when the addition arrives.
	t.Run("overflow is ResourceExhausted", func(t *testing.T) {
		t.Parallel()

		entered := make(chan struct{})
		var once sync.Once
		client, _ := newTestService(t, &fakeStore{
			leaseFn: func(ctx context.Context, _ []string, _ int, _ time.Duration, _ string) ([]store.Job, error) {
				once.Do(func() { close(entered) })
				<-ctx.Done()
				return nil, ctx.Err()
			},
		})
		stream, err := client.LeaseJobs(ctxT(t))
		require.NoError(t, err)
		require.NoError(t, stream.Send(subscribeMsg(^uint32(0))))
		waitFor(t, entered, "Lease to be gated")

		require.NoError(t, stream.Send(creditMsg(1)))
		_, err = stream.Recv()
		require.Equal(t, codes.ResourceExhausted, status.Code(err))
	})
}

// TestLeaseStreamClientCancellation: a client that goes away must free the
// handler wherever it is parked, and the server sees Canceled, not a fault.
func TestLeaseStreamClientCancellation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// setup returns the store, the subscription to send (nil sends nothing),
		// the poll interval, and a function that blocks until the handler is
		// parked at the point under test.
		setup func(tr *streamTracker, opened <-chan struct{}) (*fakeStore, *hopperv1.LeaseJobsRequest, time.Duration, func(*testing.T))
	}{
		{"initial receive", func(tr *streamTracker, _ <-chan struct{}) (*fakeStore, *hopperv1.LeaseJobsRequest, time.Duration, func(*testing.T)) {
			return &fakeStore{}, nil, 0, tr.waitEntered
		}},
		{"zero credit wait", func(_ *streamTracker, opened <-chan struct{}) (*fakeStore, *hopperv1.LeaseJobsRequest, time.Duration, func(*testing.T)) {
			return &fakeStore{}, subscribeMsg(0), 0, func(t *testing.T) { waitFor(t, opened, "the stream to open") }
		}},
		{"poll wait", func(_ *streamTracker, _ <-chan struct{}) (*fakeStore, *hopperv1.LeaseJobsRequest, time.Duration, func(*testing.T)) {
			polled := make(chan struct{})
			var once sync.Once
			st := &fakeStore{
				leaseFn: func(context.Context, []string, int, time.Duration, string) ([]store.Job, error) {
					once.Do(func() { close(polled) })
					return nil, nil
				},
			}
			// An hour: only cancellation can end this wait inside the test.
			return st, subscribeMsg(1), time.Hour, func(t *testing.T) { waitFor(t, polled, "the first poll") }
		}},
		{"blocked query", func(_ *streamTracker, _ <-chan struct{}) (*fakeStore, *hopperv1.LeaseJobsRequest, time.Duration, func(*testing.T)) {
			entered := make(chan struct{})
			var once sync.Once
			st := &fakeStore{
				leaseFn: func(ctx context.Context, _ []string, _ int, _ time.Duration, _ string) ([]store.Job, error) {
					once.Do(func() { close(entered) })
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}
			return st, subscribeMsg(1), 0, func(t *testing.T) { waitFor(t, entered, "Lease to block") }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tr := newStreamTracker()
			logger, opened := newSignalLogger("lease stream opened")
			st, sub, poll, parked := tc.setup(tr, opened)
			svc := NewService(st, Config{PollInterval: poll, Logger: logger})
			client := newTestClient(t, svc, tr.option())

			ctx, cancel := context.WithCancel(ctxT(t))
			defer cancel()
			stream, err := client.LeaseJobs(ctx)
			require.NoError(t, err)
			if sub != nil {
				require.NoError(t, stream.Send(sub))
			}
			parked(t)

			cancel()
			_, err = stream.Recv()
			require.Equal(t, codes.Canceled, status.Code(err))
			require.Equal(t, codes.Canceled, status.Code(tr.waitExited(t)),
				"the handler returns, and reports the cancellation as such")
		})
	}
}

// TestLeaseStreamSendFailureLeavesLeases: a Send that fails part way through a
// batch ends the stream and does nothing else. The jobs in the batch stay
// leased; the reaper, not the stream, gives them back (docs/semantics.md §11).
// An Ack or Nack here would be a guess about whether the worker got the job.
func TestLeaseStreamSendFailureLeavesLeases(t *testing.T) {
	t.Parallel()

	var leaseCalls, settled, sends atomic.Int32
	sendErr := status.Error(codes.Unavailable, "transport is closing")
	stream := newFakeLeaseStream(t, func(context.Context, *hopperv1.LeaseJobsResponse) error {
		if sends.Add(1) == 3 {
			return sendErr
		}
		return nil
	})
	svc := NewService(&fakeStore{
		leaseFn: func(_ context.Context, queues []string, n int, _ time.Duration, _ string) ([]store.Job, error) {
			leaseCalls.Add(1)
			jobs := make([]store.Job, n)
			for i := range jobs {
				jobs[i] = sampleJob(queues[0])
			}
			return jobs, nil
		},
		ackFn: func(context.Context, uuid.UUID, uuid.UUID) error {
			settled.Add(1)
			return nil
		},
		nackFn: func(context.Context, uuid.UUID, uuid.UUID, string) error {
			settled.Add(1)
			return nil
		},
	}, Config{PollInterval: time.Millisecond})

	stream.in <- subscribeMsg(5)
	err := svc.LeaseJobs(stream)

	require.Equal(t, sendErr, err)
	require.Equal(t, int32(3), sends.Load(), "the batch stops at the failed Send")
	require.Equal(t, int32(1), leaseCalls.Load(), "no further lease query starts")
	require.Zero(t, settled.Load(), "nothing is acked or nacked on the worker's behalf")

	stream.cancel()
	require.Eventually(t, stream.idle, 5*time.Second, 5*time.Millisecond)
}

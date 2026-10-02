package broker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

type fakeWakeups struct {
	announceFn func(context.Context, []string) error
	listenFn   func(context.Context, func(string)) error
}

func (f *fakeWakeups) Announce(ctx context.Context, queues []string) error {
	return f.announceFn(ctx, queues)
}
func (f *fakeWakeups) Listen(ctx context.Context, wake func(string)) error {
	return f.listenFn(ctx, wake)
}

func signalled(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestWakeSignalsOnlyTheStreamsThatServeTheQueue(t *testing.T) {
	t.Parallel()
	w := newWakeups()
	a, b := make(chan struct{}, 1), make(chan struct{}, 1)
	w.subscribe(a, []string{"x", "y"})
	unsubscribeB := w.subscribe(b, []string{"z"})

	w.wake("y")
	require.True(t, signalled(a))
	require.False(t, signalled(b))

	w.wake("")
	require.True(t, signalled(a), "the empty queue wakes every stream")
	require.True(t, signalled(b))

	unsubscribeB()
	w.wake("z")
	require.False(t, signalled(b))
}

// idleStream opens a lease stream on queue q against a broker whose poll
// interval is an hour, so anything it delivers within the test came from a
// wakeup. The store's first lease finds nothing; once ready is set, it finds
// one job.
func idleStream(t *testing.T, st *fakeStore, ready *atomic.Bool) (hopperv1.HopperServiceClient, <-chan *hopperv1.Job, *atomic.Int64) {
	t.Helper()
	var leases atomic.Int64
	firstLease := make(chan struct{})
	st.leaseFn = func(_ context.Context, queues []string, n int, _ time.Duration, _ string) ([]store.Job, error) {
		if leases.Add(1) == 1 {
			defer close(firstLease)
		}
		if !ready.Load() {
			return nil, nil
		}
		ready.Store(false)
		return []store.Job{sampleJob(queues[0])}, nil
	}
	svc := NewService(st, Config{PollInterval: time.Hour})
	client := newTestClient(t, svc)
	stream, err := client.LeaseJobs(ctxT(t))
	require.NoError(t, err)
	require.NoError(t, stream.Send(subscribeMsg(1)))
	waitFor(t, firstLease, "the stream's first lease")

	delivered := make(chan *hopperv1.Job, 1)
	go func() {
		if resp, err := stream.Recv(); err == nil {
			delivered <- resp.GetJob()
		}
	}()
	return client, delivered, &leases
}

func TestAnEnqueueWakesAnIdleStream(t *testing.T) {
	t.Parallel()
	for _, rpc := range []string{"Enqueue", "EnqueueBatch"} {
		t.Run(rpc, func(t *testing.T) {
			t.Parallel()
			var ready atomic.Bool
			st := &fakeStore{enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
				ready.Store(true)
				return sampleJob(p.Queue), true, nil
			}}
			client, delivered, _ := idleStream(t, st, &ready)

			spec := &hopperv1.JobSpec{Queue: "q"}
			var err error
			if rpc == "Enqueue" {
				_, err = client.Enqueue(ctxT(t), &hopperv1.EnqueueRequest{Job: spec})
			} else {
				_, err = client.EnqueueBatch(ctxT(t), &hopperv1.EnqueueBatchRequest{Jobs: []*hopperv1.JobSpec{spec}})
			}
			require.NoError(t, err)
			select {
			case job := <-delivered:
				require.Equal(t, "q", job.GetQueue())
			case <-time.After(3 * time.Second):
				t.Fatal("the idle stream was not woken; it would have waited out its hour-long poll")
			}
		})
	}
}

// A delayed job is not leasable until the promoter moves it, and a
// deduplicated call added nothing, so neither wakes a stream.
func TestNoWakeupForADelayedOrDuplicateJob(t *testing.T) {
	t.Parallel()
	var ready atomic.Bool
	var calls atomic.Int64
	st := &fakeStore{enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
		job := sampleJob(p.Queue)
		if calls.Add(1) == 1 {
			job.State = store.StateScheduled
			return job, true, nil
		}
		return job, false, nil // deduplicated
	}}
	client, _, leases := idleStream(t, st, &ready)
	for range 2 {
		_, err := client.Enqueue(ctxT(t), &hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: "q"}})
		require.NoError(t, err)
	}
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int64(1), leases.Load(), "the stream leased again without a wakeup")
}

// While one announcement is in flight, everything enqueued meanwhile is sent
// as one more, not one per job.
func TestAnnouncementsCoalesceWhileOneIsInFlight(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	started := make(chan []string, 10)
	var calls atomic.Int64
	wakes := &fakeWakeups{
		announceFn: func(_ context.Context, queues []string) error {
			started <- queues
			if calls.Add(1) == 1 {
				<-release // the first announcement is slow
			}
			return nil
		},
		listenFn: func(ctx context.Context, _ func(string)) error { <-ctx.Done(); return ctx.Err() },
	}
	svc := NewService(&fakeStore{}, Config{Wakeups: wakes})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = svc.RunWakeups(ctx) }()

	svc.wakes.announce("a")
	var first []string
	select {
	case first = <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("no announcement")
	}
	require.Equal(t, []string{"a"}, first)
	for range 100 {
		svc.wakes.announce("b")
		svc.wakes.announce("a")
	}
	close(release)
	select {
	case second := <-started:
		require.Equal(t, []string{"a", "b"}, second)
	case <-time.After(3 * time.Second):
		t.Fatal("the pending announcements were never sent")
	}
	select {
	case extra := <-started:
		t.Fatalf("a third announcement %v: they did not coalesce", extra)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestTheListenerWakesStreamsAndReconnects(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	wakes := &fakeWakeups{
		announceFn: func(context.Context, []string) error { return nil },
		listenFn: func(ctx context.Context, wake func(string)) error {
			if calls.Add(1) == 1 {
				wake("")
				return errors.New("connection lost")
			}
			wake("q")
			<-ctx.Done()
			return ctx.Err()
		},
	}
	svc := NewService(&fakeStore{}, Config{Wakeups: wakes})
	ch := make(chan struct{}, 1)
	svc.wakes.subscribe(ch, []string{"q"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.RunWakeups(ctx) }()

	for i := range 2 { // once for the first session's "", once for the second's "q"
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatalf("wakeup %d never arrived", i+1)
		}
	}
	require.Equal(t, int64(2), calls.Load(), "the listener reconnected once")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestRunWakeupsWithoutAStoreWaitsForItsContext(t *testing.T) {
	t.Parallel()
	svc := NewService(&fakeStore{}, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var err error
	wg.Go(func() { err = svc.RunWakeups(ctx) })
	cancel()
	wg.Wait()
	require.ErrorIs(t, err, context.Canceled)
}

// Local-only services have no announcer to drain pending queues, so enqueue
// must wake their streams without retaining queue names for their lifetime.
func TestLocalWakeupsDoNotRetainAnnouncementsWithoutAStore(t *testing.T) {
	t.Parallel()
	svc := NewService(&fakeStore{}, Config{})
	ch := make(chan struct{}, 1)
	defer svc.wakes.subscribe(ch, []string{"q"})()

	svc.announceIfLeasable(sampleJob("q"), true)
	require.True(t, signalled(ch), "local streams still wake without a notification store")
	require.Empty(t, svc.wakes.take(), "local-only enqueues must not accumulate pending queues")
	require.False(t, signalled(svc.wakes.kick), "there is no announcer to kick")
}

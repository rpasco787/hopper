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

// One enqueue adds one job, so it should cost one idle stream a lease, not
// every idle stream on the queue: the others would lease, find nothing, and
// park again. The measured cost of waking them all was about seven leases per
// job, most of them empty, on a broker with four streams.
func TestAnEnqueueWakesOneIdleStreamNotEveryOne(t *testing.T) {
	t.Parallel()
	const streams = 4
	var leases atomic.Int64
	var ready atomic.Bool
	st := &fakeStore{
		leaseFn: func(_ context.Context, queues []string, _ int, _ time.Duration, _ string) ([]store.Job, error) {
			leases.Add(1)
			if ready.CompareAndSwap(true, false) {
				return []store.Job{sampleJob(queues[0])}, nil
			}
			return nil, nil
		},
		enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			ready.Store(true)
			return sampleJob(p.Queue), true, nil
		},
	}
	svc := NewService(st, Config{PollInterval: time.Hour})
	client := newTestClient(t, svc)

	delivered := make(chan struct{}, streams)
	for range streams {
		stream, err := client.LeaseJobs(ctxT(t))
		require.NoError(t, err)
		require.NoError(t, stream.Send(subscribeMsg(1)))
		go func() {
			if _, err := stream.Recv(); err == nil {
				delivered <- struct{}{}
			}
		}()
	}
	require.Eventually(t, func() bool { return leases.Load() == streams }, 5*time.Second, 5*time.Millisecond,
		"every stream makes its first lease")

	_, err := client.Enqueue(ctxT(t), &hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: "q"}})
	require.NoError(t, err)
	waitFor(t, delivered, "the job to reach a stream")
	require.Never(t, func() bool { return leases.Load() > streams+1 }, 300*time.Millisecond, 5*time.Millisecond,
		"only the stream that was woken may lease again; the poll interval is an hour")
	require.Len(t, delivered, 0, "one job reaches one stream")
}

// signalledSet drains every channel and returns the indexes that held a signal.
func signalledSet(chans []chan struct{}) map[int]bool {
	got := make(map[int]bool)
	for i, ch := range chans {
		if signalled(ch) {
			got[i] = true
		}
	}
	return got
}

func subscribeAll(w *wakeups, n int, queues ...string) []chan struct{} {
	chans := make([]chan struct{}, n)
	for i := range chans {
		chans[i] = make(chan struct{}, 1)
		w.subscribe(chans[i], queues)
	}
	return chans
}

// A wake picks one parked stream and takes it out of the pool, so a burst of
// enqueues spreads across the idle streams instead of landing on one.
func TestWakeChoosesOneParkedStreamPerCall(t *testing.T) {
	t.Parallel()
	w := newWakeups()
	chans := subscribeAll(w, 3, "x")
	for _, ch := range chans {
		w.setParked(ch, true)
	}

	woken := make(map[int]bool)
	for call := 1; call <= 3; call++ {
		w.wake("x")
		got := signalledSet(chans)
		require.Len(t, got, 1, "call %d must signal exactly one stream", call)
		for i := range got {
			require.False(t, woken[i], "call %d signalled stream %d again", call, i)
			woken[i] = true
		}
	}
	require.Len(t, woken, 3)
}

// A stream that has not parked may be between an empty lease and its wait. It
// has to see the signal when it gets there, or the enqueue would cost it a
// poll. Streams that are all busy are therefore all signalled.
func TestWakeSignalsEveryServingStreamWhenNoneIsParked(t *testing.T) {
	t.Parallel()
	w := newWakeups()
	chans := subscribeAll(w, 3, "x")
	other := make(chan struct{}, 1)
	w.subscribe(other, []string{"z"})

	w.wake("x")
	require.Len(t, signalledSet(chans), 3)
	require.False(t, signalled(other))
}

func TestWakeSkipsStreamsThatAreNotParkedOrNotOnTheQueue(t *testing.T) {
	t.Parallel()
	w := newWakeups()
	chans := subscribeAll(w, 3, "x")
	elsewhere := make(chan struct{}, 1)
	w.subscribe(elsewhere, []string{"z"})
	w.setParked(elsewhere, true)

	w.setParked(chans[0], true)
	w.setParked(chans[1], true)
	w.setParked(chans[1], false) // woke by its timer, or by credit
	w.setParked(chans[2], false)

	w.wake("x")
	require.Equal(t, map[int]bool{0: true}, signalledSet(chans), "only the parked stream on the queue")
	require.False(t, signalled(elsewhere))
}

func TestParkingAnUnsubscribedStreamDoesNothing(t *testing.T) {
	t.Parallel()
	w := newWakeups()
	ch := make(chan struct{}, 1)
	unsubscribe := w.subscribe(ch, []string{"x"})
	unsubscribe()

	w.setParked(ch, true)
	w.wake("x")
	require.False(t, signalled(ch))
}

// The listener sends the empty queue when it reconnects, because announcements
// made while it was down are lost. Every stream must hear it, parked or not.
func TestTheEmptyQueueStillWakesEveryStream(t *testing.T) {
	t.Parallel()
	w := newWakeups()
	chans := subscribeAll(w, 3, "x")
	w.setParked(chans[0], true)

	w.wake("")
	require.Len(t, signalledSet(chans), 3)
}

// Waking one stream per enqueue must not strand a job: a burst of enqueues
// reaches as many idle streams, each on its own wake, with no poll to fall
// back on.
func TestABurstOfEnqueuesReachesDistinctIdleStreams(t *testing.T) {
	t.Parallel()
	const streams = 4
	var leases, available atomic.Int64
	st := &fakeStore{
		leaseFn: func(_ context.Context, queues []string, _ int, _ time.Duration, _ string) ([]store.Job, error) {
			leases.Add(1)
			for {
				n := available.Load()
				if n == 0 {
					return nil, nil
				}
				if available.CompareAndSwap(n, n-1) {
					return []store.Job{sampleJob(queues[0])}, nil
				}
			}
		},
		enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			available.Add(1)
			return sampleJob(p.Queue), true, nil
		},
	}
	svc := NewService(st, Config{PollInterval: time.Hour})
	client := newTestClient(t, svc)

	delivered := make(chan struct{}, streams)
	for range streams {
		stream, err := client.LeaseJobs(ctxT(t))
		require.NoError(t, err)
		require.NoError(t, stream.Send(subscribeMsg(1)))
		go func() {
			if _, err := stream.Recv(); err == nil {
				delivered <- struct{}{}
			}
		}()
	}
	require.Eventually(t, func() bool { return leases.Load() == streams }, 5*time.Second, 5*time.Millisecond,
		"every stream makes its first lease")

	for range streams {
		_, err := client.Enqueue(ctxT(t), &hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: "q"}})
		require.NoError(t, err)
	}
	for i := range streams {
		select {
		case <-delivered:
		case <-time.After(3 * time.Second):
			t.Fatalf("only %d of %d jobs reached a stream; the rest would wait out an hour-long poll", i, streams)
		}
	}
}

package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

type observerEvent struct {
	queue    string
	status   HandlerStatus
	duration time.Duration
}

type recordingObserver struct {
	mu       sync.Mutex
	started  []string
	finished []observerEvent
}

func (o *recordingObserver) HandlerStarted(queue string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.started = append(o.started, queue)
}

func (o *recordingObserver) HandlerFinished(queue string, status HandlerStatus, d time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.finished = append(o.finished, observerEvent{queue: queue, status: status, duration: d})
}

func (o *recordingObserver) snapshot() ([]string, []observerEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.started...), append([]observerEvent(nil), o.finished...)
}

func TestObserverSeesEachHandlerOnce(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	observer := &recordingObserver{}
	w := New(f.addr, Options{
		Concurrency:     3,
		ShutdownTimeout: 3 * time.Second,
		Logger:          quietLogger(),
		Observer:        observer,
	})
	const handlerDelay = 20 * time.Millisecond
	w.Handle("ok", func(context.Context, Job) error {
		time.Sleep(handlerDelay)
		return nil
	})
	w.Handle("error", func(context.Context, Job) error {
		time.Sleep(handlerDelay)
		return errors.New("failed")
	})
	w.Handle("panic", func(context.Context, Job) error {
		time.Sleep(handlerDelay)
		panic("failed")
	})
	r := runWorker(t, w)
	stream := f.awaitStream(t, ctx)

	for _, queue := range []string{"ok", "error", "panic"} {
		require.NoError(t, stream.deliver(ctx, newFixture(queue).proto()))
	}
	awaitCalls(t, ctx, f, "Ack", 1)
	awaitCalls(t, ctx, f, "Nack", 2)
	require.NoError(t, r.stopAndWait(t))

	started, finished := observer.snapshot()
	require.ElementsMatch(t, []string{"ok", "error", "panic"}, started)
	require.Len(t, finished, 3)
	statuses := make(map[string]HandlerStatus, len(finished))
	for _, event := range finished {
		statuses[event.queue] = event.status
		require.GreaterOrEqual(t, event.duration, handlerDelay)
	}
	require.Equal(t, map[string]HandlerStatus{
		"ok": HandlerOK, "error": HandlerError, "panic": HandlerPanic,
	}, statuses)
}

func TestObserverNeverSeesUnstartedJobs(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	observer := &recordingObserver{}
	known := newFixture("known")
	gate := newGate(t)
	acquiring := make(chan struct{}, 1)
	f.set(func() {
		f.onHeartbeat = func(hctx context.Context, req *hopperv1.HeartbeatRequest) error {
			if req.GetId() != known.ID.String() {
				return nil
			}
			select {
			case acquiring <- struct{}{}:
			default:
			}
			return gate.wait(hctx)
		}
	})
	r, stream := runHandler(t, ctx, f, Options{Observer: observer, ShutdownTimeout: 5 * time.Second}, "known",
		func(context.Context, Job) error { t.Error("unstarted handler ran"); return nil })

	require.NoError(t, stream.deliver(ctx, newFixture("unknown").proto()))
	awaitCalls(t, ctx, f, "Nack", 1)
	require.NoError(t, stream.deliver(ctx, known.proto()))
	select {
	case <-acquiring:
	case <-ctx.Done():
		t.Fatal("acquisition never started")
	}
	r.stop()
	require.NoError(t, stream.awaitEOF(ctx))
	gate.release()
	awaitCalls(t, ctx, f, "Nack", 2)
	require.NoError(t, r.wait(t))

	started, finished := observer.snapshot()
	require.Empty(t, started)
	require.Empty(t, finished)
}

type countingObserver struct {
	current atomic.Int64
	min     atomic.Int64
	max     atomic.Int64
}

func (o *countingObserver) HandlerStarted(string) {
	n := o.current.Add(1)
	for old := o.max.Load(); n > old && !o.max.CompareAndSwap(old, n); old = o.max.Load() {
	}
}

func (o *countingObserver) HandlerFinished(string, HandlerStatus, time.Duration) {
	n := o.current.Add(-1)
	for old := o.min.Load(); n < old && !o.min.CompareAndSwap(old, n); old = o.min.Load() {
	}
}

func TestInflightNeverGoesNegative(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	observer := &countingObserver{}
	r, stream := runHandler(t, ctx, f, Options{Concurrency: 16, Observer: observer}, "q",
		func(context.Context, Job) error { time.Sleep(time.Millisecond); return nil })

	for range 200 {
		require.NoError(t, stream.deliver(ctx, newFixture("q").proto()))
	}
	awaitCalls(t, ctx, f, "Ack", 200)
	require.NoError(t, r.stopAndWait(t))
	require.Zero(t, observer.current.Load())
	require.GreaterOrEqual(t, observer.min.Load(), int64(0))
	require.LessOrEqual(t, observer.max.Load(), int64(16))
}

func TestPanicNackMessageIsUnchanged(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	r, stream := runHandler(t, ctx, f, Options{Observer: &recordingObserver{}}, "q",
		func(context.Context, Job) error { panic("payload made no sense") })

	require.NoError(t, stream.deliver(ctx, newFixture("q").proto()))
	awaitCalls(t, ctx, f, "Nack", 1)
	require.NoError(t, r.stopAndWait(t))
	require.Len(t, f.nackRequests(), 1)
	require.True(t, strings.HasPrefix(f.nackRequests()[0].GetError(), "worker: handler panicked: "))
}

func TestNilObserverIsFine(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	r, stream := runHandler(t, ctx, f, Options{Observer: nil}, "q", func(context.Context, Job) error { return nil })

	require.NoError(t, stream.deliver(ctx, newFixture("q").proto()))
	awaitCalls(t, ctx, f, "Ack", 1)
	require.NoError(t, r.stopAndWait(t))
}

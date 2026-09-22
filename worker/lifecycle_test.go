package worker

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func quietLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func newTestLifecycle(t *testing.T, ctx context.Context, timeout time.Duration) *lifecycle {
	t.Helper()
	lc := newLifecycle(ctx, timeout, quietLogger())
	t.Cleanup(lc.stop)
	return lc
}

func TestDrainBeginsOnceAndFixesTheDeadline(t *testing.T) {
	lc := newTestLifecycle(t, context.Background(), time.Minute)
	require.False(t, lc.isDraining())

	_, ok := lc.shutdownDeadline()
	require.False(t, ok, "there is no deadline before drain begins")

	lc.beginDrain("first")
	first, ok := lc.shutdownDeadline()
	require.True(t, ok)
	require.True(t, lc.isDraining())

	lc.beginDrain("second")
	again, _ := lc.shutdownDeadline()
	require.Equal(t, first, again, "a later caller must not extend a shutdown already running out of time")
}

func TestCallerCancellationBeginsDrain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	lc := newTestLifecycle(t, ctx, time.Minute)
	require.False(t, lc.isDraining())

	cancel()
	require.NoError(t, lc.await(), "nothing was in flight, so the drain completes immediately")
	require.True(t, lc.isDraining())
}

func TestAnAlreadyCancelledContextDrainsBeforeNewLifecycleReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	lc := newTestLifecycle(t, ctx, time.Minute)
	require.True(t, lc.isDraining(), "whether a doomed worker opens a stream must not depend on scheduling")
}

func TestForceContextKeepsValuesButNotCancellation(t *testing.T) {
	type key struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "trace-1"))
	lc := newTestLifecycle(t, ctx, time.Minute)

	require.Equal(t, "trace-1", lc.forceCtx.Value(key{}))
	cancel()
	require.NoError(t, lc.forceCtx.Err(), "cancelling the caller drains; it does not kill handlers")

	lc.force(ErrShuttingDown)
	require.ErrorIs(t, context.Cause(lc.forceCtx), ErrShuttingDown)
}

func TestAwaitWaitsForActiveDeliveries(t *testing.T) {
	lc := newTestLifecycle(t, context.Background(), time.Minute)
	wk := lc.begin()
	require.Equal(t, counts{deliveries: 1, acquiring: 1}, lc.counts())

	done := make(chan error, 1)
	go func() { done <- lc.await() }()

	select {
	case err := <-done:
		t.Fatalf("await returned %v with a delivery still active", err)
	case <-time.After(50 * time.Millisecond):
	}

	wk.end()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(barrier):
		t.Fatal("await did not return after the last delivery finished")
	}
	require.Equal(t, counts{}, lc.counts())
}

func TestAwaitReportsAbandonmentWithSeparateCounts(t *testing.T) {
	lc := newTestLifecycle(t, context.Background(), 50*time.Millisecond)

	acquiring := lc.begin()
	running := lc.begin()
	require.True(t, running.acceptHandler())
	confirming := lc.begin()
	require.True(t, confirming.acceptHandler())
	confirming.confirming()
	require.Equal(t, counts{deliveries: 3, acquiring: 1, handlers: 1, confirming: 1}, lc.counts())

	start := time.Now()
	err := lc.await()
	elapsed := time.Since(start)

	require.ErrorIs(t, err, ErrAbandoned)
	require.ErrorContains(t, err, "3 active deliveries")
	require.ErrorContains(t, err, "1 running handlers")
	require.ErrorContains(t, err, "1 acquiring a lease")
	require.ErrorContains(t, err, "1 awaiting confirmation")
	require.Less(t, elapsed, barrier, "abandonment happens at the deadline, not later")
	require.ErrorIs(t, context.Cause(lc.forceCtx), ErrShuttingDown, "the deadline cancels everything the SDK owns")

	// The counters survive so the abandoned work can still be logged, and the
	// deliveries can still finish without corrupting them.
	acquiring.end()
	running.end()
	confirming.end()
	require.Equal(t, counts{}, lc.counts())
}

func TestAcceptHandlerLosesToDrain(t *testing.T) {
	lc := newTestLifecycle(t, context.Background(), time.Minute)

	before := lc.begin()
	require.True(t, before.acceptHandler(), "acceptance before drain wins and that handler is in flight")

	lc.beginDrain("cancelled")

	after := lc.begin()
	require.False(t, after.acceptHandler(), "once drain wins, user code never starts")
	require.Equal(t, counts{deliveries: 2, acquiring: 1, handlers: 1}, lc.counts())

	// The refused delivery is still active work: it holds its token until its
	// unstarted nack finishes.
	after.confirming()
	require.Equal(t, counts{deliveries: 2, handlers: 1, confirming: 1}, lc.counts())
	after.end()
	before.end()
	require.Equal(t, counts{}, lc.counts())
}

func TestWorkEndsExactlyOnce(t *testing.T) {
	lc := newTestLifecycle(t, context.Background(), time.Minute)
	first := lc.begin()
	second := lc.begin()

	first.end()
	first.end()
	first.end()
	require.Equal(t, counts{deliveries: 1, acquiring: 1}, lc.counts())

	require.False(t, first.acceptHandler(), "a finished delivery cannot start a handler")
	second.end()
	require.Equal(t, counts{}, lc.counts())
}

func TestPhaseTransitionsMoveExactlyOneCount(t *testing.T) {
	lc := newTestLifecycle(t, context.Background(), time.Minute)
	wk := lc.begin()
	require.Equal(t, counts{deliveries: 1, acquiring: 1}, lc.counts())

	require.True(t, wk.acceptHandler())
	require.Equal(t, counts{deliveries: 1, handlers: 1}, lc.counts())

	wk.confirming()
	require.Equal(t, counts{deliveries: 1, confirming: 1}, lc.counts())
	wk.confirming()
	require.Equal(t, counts{deliveries: 1, confirming: 1}, lc.counts(), "repeating a phase changes nothing")

	wk.end()
	require.Equal(t, counts{}, lc.counts())
}

func TestDrainSignalIsClosedOnce(t *testing.T) {
	lc := newTestLifecycle(t, context.Background(), time.Minute)
	go lc.beginDrain("a")
	go lc.beginDrain("b")
	lc.beginDrain("c")
	select {
	case <-lc.drainSignal:
	case <-time.After(barrier):
		t.Fatal("drain signal never closed")
	}
}

// TestAbandonmentSurvivesADeliveryUnwindingAtTheDeadline pins the one ordering
// that used to lose a dropped job.
//
// An abandoned delivery decrements its own counters on the way out, and that
// single decrement both empties the live counts and closes drained. So by the
// time await looks, a worker that has just dropped a job is indistinguishable
// from one that shut down perfectly — and await reported success. The verdict
// has to come from the snapshot taken when the deadline fired.
func TestAbandonmentSurvivesADeliveryUnwindingAtTheDeadline(t *testing.T) {
	lc := newTestLifecycle(t, context.Background(), 20*time.Millisecond)
	w := lc.begin()
	require.True(t, w.acceptHandler())
	w.confirming()
	lc.beginDrain("test")

	// The budget runs out with the confirmation still in flight...
	<-lc.forceCtx.Done()
	// ... and only then does the delivery unwind.
	w.end()

	err := lc.await()
	require.ErrorIs(t, err, ErrAbandoned, "a job was dropped; a clean exit would lie about it")
	require.ErrorContains(t, err, "1 awaiting confirmation")
}

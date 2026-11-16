package broker

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// gate is a batch function the test steps through: each batch reports its
// items on started and waits for its results on finish, or for its context
// to end.
type gate struct {
	started chan []int
	finish  chan []error
}

func newGate() *gate { return &gate{started: make(chan []int, 16), finish: make(chan []error)} }

func (g *gate) run(ctx context.Context, items []int) []error {
	g.started <- items
	select {
	case errs := <-g.finish:
		return errs
	case <-ctx.Done():
		return slices.Repeat([]error{ctx.Err()}, len(items))
	}
}

func oks(n int) []error { return make([]error, n) }

func (b *batcher[T]) queued() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue)
}

func call(ctx context.Context, b *batcher[int], item int) <-chan error {
	out := make(chan error, 1)
	go func() { out <- b.do(ctx, item) }()
	return out
}

func waitQueued(t *testing.T, b *batcher[int], n int) {
	t.Helper()
	require.Eventually(t, func() bool { return b.queued() == n }, time.Second, time.Millisecond)
}

// waitIdle is b.wait bounded by d. (Not waitFor: stream_test.go has one.)
func waitIdle(t *testing.T, b *batcher[int], d time.Duration) error {
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	return b.wait(ctx)
}

func TestALoneCallRunsAtOnce(t *testing.T) {
	g := newGate()
	b := newBatcher(64, 2, g.run)
	done := call(t.Context(), b, 7)
	require.Equal(t, []int{7}, <-g.started)
	g.finish <- oks(1)
	require.NoError(t, <-done)
}

func TestCallsThatArriveDuringABatchShareTheNextOne(t *testing.T) {
	g := newGate()
	b := newBatcher(64, 1, g.run)
	first := call(t.Context(), b, 0)
	require.Equal(t, []int{0}, <-g.started)
	var rest []<-chan error
	for i := 1; i <= 5; i++ {
		rest = append(rest, call(t.Context(), b, i))
	}
	waitQueued(t, b, 5)
	g.finish <- oks(1)
	require.NoError(t, <-first)
	require.ElementsMatch(t, []int{1, 2, 3, 4, 5}, <-g.started, "one statement for everything that queued")
	g.finish <- oks(5)
	for _, c := range rest {
		require.NoError(t, <-c)
	}
}

func TestEachCallerGetsItsOwnResult(t *testing.T) {
	g := newGate()
	b := newBatcher(64, 1, g.run)
	first := call(t.Context(), b, 0)
	<-g.started
	a := call(t.Context(), b, 1)
	waitQueued(t, b, 1)
	c := call(t.Context(), b, 2)
	waitQueued(t, b, 2)
	g.finish <- oks(1)
	<-first
	require.Equal(t, []int{1, 2}, <-g.started)
	lost := errors.New("lost")
	g.finish <- []error{nil, lost}
	require.NoError(t, <-a)
	require.ErrorIs(t, <-c, lost)
}

func TestABatchHoldsAtMostMax(t *testing.T) {
	g := newGate()
	b := newBatcher(2, 1, g.run)
	first := call(t.Context(), b, 0)
	<-g.started
	var rest []<-chan error
	for i := 1; i <= 5; i++ {
		rest = append(rest, call(t.Context(), b, i))
		waitQueued(t, b, i)
	}
	g.finish <- oks(1)
	<-first
	var sizes []int
	for total := 0; total < 5; {
		items := <-g.started
		sizes = append(sizes, len(items))
		total += len(items)
		g.finish <- oks(len(items))
	}
	require.Equal(t, []int{2, 2, 1}, sizes)
	for _, c := range rest {
		require.NoError(t, <-c)
	}
}

func TestNoMoreThanFlightsBatchesRunAtOnce(t *testing.T) {
	g := newGate()
	b := newBatcher(64, 2, g.run)
	one := call(t.Context(), b, 1)
	<-g.started
	two := call(t.Context(), b, 2)
	<-g.started
	three := call(t.Context(), b, 3)
	waitQueued(t, b, 1)
	select {
	case items := <-g.started:
		t.Fatalf("a third batch started while two ran: %v", items)
	case <-time.After(50 * time.Millisecond):
	}
	g.finish <- oks(1)
	require.Equal(t, []int{3}, <-g.started, "a finished flight takes what queued")
	g.finish <- oks(1)
	g.finish <- oks(1)
	for _, c := range []<-chan error{one, two, three} {
		require.NoError(t, <-c)
	}
}

func TestACallerThatStopsWaitingIsLeftOut(t *testing.T) {
	g := newGate()
	b := newBatcher(64, 1, g.run)
	first := call(t.Context(), b, 0)
	<-g.started
	ctx, cancel := context.WithCancel(t.Context())
	gone := call(ctx, b, 1)
	waitQueued(t, b, 1)
	stays := call(t.Context(), b, 2)
	waitQueued(t, b, 2)
	cancel()
	require.ErrorIs(t, <-gone, context.Canceled, "it stops waiting at once")
	g.finish <- oks(1)
	<-first
	require.Equal(t, []int{2}, <-g.started, "no statement runs for a caller who left")
	g.finish <- oks(1)
	require.NoError(t, <-stays)
}

// TestAStatementRunsWhileAnyCallerWaits: one of two callers leaving doesn't
// cut the statement short; the second leaving cancels it.
func TestAStatementRunsWhileAnyCallerWaits(t *testing.T) {
	g := newGate()
	b := newBatcher(64, 1, g.run)
	first := call(t.Context(), b, 0)
	<-g.started
	one, leaveOne := context.WithCancel(t.Context())
	two, leaveTwo := context.WithCancel(t.Context())
	a := call(one, b, 1)
	waitQueued(t, b, 1)
	c := call(two, b, 2)
	waitQueued(t, b, 2)
	g.finish <- oks(1)
	require.NoError(t, <-first)
	require.Equal(t, []int{1, 2}, <-g.started)

	leaveOne()
	require.ErrorIs(t, <-a, context.Canceled)
	require.ErrorIs(t, waitIdle(t, b, 50*time.Millisecond), context.DeadlineExceeded,
		"the statement runs on for the caller still waiting")

	leaveTwo()
	require.ErrorIs(t, <-c, context.Canceled)
	require.NoError(t, waitIdle(t, b, time.Second), "the statement was cancelled once nobody waited")
}

// TestALoneCallersDeadlineCancelsItsStatement: a caller's deadline ends a
// statement it waits for alone. A copied deadline would end it too, but only
// cancellation reaches it when the caller is cancelled rather than timed out.
// Here the statement sees Canceled from the batcher, not its own cap.
func TestALoneCallersDeadlineCancelsItsStatement(t *testing.T) {
	ended := make(chan error, 1)
	b := newBatcher(64, 1, func(ctx context.Context, items []int) []error {
		<-ctx.Done()
		ended <- ctx.Err()
		return slices.Repeat([]error{ctx.Err()}, len(items))
	})
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, b.do(ctx, 1), context.DeadlineExceeded)
	select {
	case err := <-ended:
		require.ErrorIs(t, err, context.Canceled, "cancelled because its caller left, not timed out at the cap")
	case <-time.After(time.Second):
		t.Fatal("the statement ran on after its only caller left")
	}
}

func TestABatchIsCappedByItsOwnBound(t *testing.T) {
	var deadline time.Time
	b := newBatcher(64, 1, func(ctx context.Context, items []int) []error {
		deadline, _ = ctx.Deadline()
		return oks(len(items))
	})
	b.timeout = time.Minute
	require.NoError(t, b.do(t.Context(), 1)) // a caller with no deadline of its own
	require.WithinDuration(t, time.Now().Add(time.Minute), deadline, 5*time.Second)
}

func TestWaitReturnsOnceNoFlightRuns(t *testing.T) {
	g := newGate()
	b := newBatcher(64, 1, g.run)
	require.NoError(t, waitIdle(t, b, time.Second), "idle from the start")
	done := call(t.Context(), b, 1)
	<-g.started
	require.ErrorIs(t, waitIdle(t, b, 50*time.Millisecond), context.DeadlineExceeded)
	g.finish <- oks(1)
	require.NoError(t, <-done)
	require.NoError(t, waitIdle(t, b, time.Second))
}

func TestAShortResultFailsTheBatchInsteadOfHanging(t *testing.T) {
	b := newBatcher(64, 1, func(context.Context, []int) []error { return nil })
	require.ErrorContains(t, b.do(t.Context(), 1), "results for 1 items")
}

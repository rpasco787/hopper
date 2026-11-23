package worker

// Tests for the start rule (docs/design/credit-anchored-freshness.md).

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// TestFreshDeadlineCountsFromTheCreditSend: a delivery read off the stream
// less than G/10 after its credit was sent starts on deadlineFrom(sent, G),
// which is never later than receipt would give. Anything else takes the check.
func TestFreshDeadlineCountsFromTheCreditSend(t *testing.T) {
	grant := 30 * time.Second
	sent := time.Now()
	for _, tc := range []struct {
		name  string
		age   time.Duration
		fresh bool
	}{
		{"moments after", time.Millisecond, true},
		{"just under a tenth", grant/10 - time.Nanosecond, true},
		{"a tenth", grant / 10, false},
		{"half the grant", grant / 2, false},
		{"before the send", -time.Millisecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			received := sent.Add(tc.age)
			deadline, ok := freshDeadline(sent, received, grant)
			require.Equal(t, tc.fresh, ok)
			if !ok {
				return
			}
			require.True(t, deadline.Equal(deadlineFrom(sent, grant)), "measured from the credit's send")
			require.False(t, deadline.After(deadlineFrom(received, grant)), "never later than receipt would give")
			require.True(t, deadline.After(received), "and still ahead of the delivery")
		})
	}
	_, ok := freshDeadline(time.Time{}, sent, grant)
	require.False(t, ok, "a unit never sent proves nothing")
}

// TestAFreshDeliveryStartsWithoutTheOwnershipCheck: in a drain the broker
// leases moments after the credit arrives, so the handler starts at once.
func TestAFreshDeliveryStartsWithoutTheOwnershipCheck(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	r, s := runHandler(t, ctx, f, Options{}, "q", func(context.Context, Job) error { return nil })

	require.NoError(t, s.deliver(ctx, newFixture("q").proto())) // G = 30 s: fresh for 3 s
	awaitCalls(t, ctx, f, "Ack", 1)
	require.NoError(t, r.stopAndWait(t))

	beats, _ := f.counts("Heartbeat")
	require.Zero(t, beats, "a delivery leased moments after its credit needs no check")
}

// TestAStaleDeliveryStillTakesTheCheck: a delivery that arrives G/10 or more
// after its credit was sent proves nothing about now, so the SDK asks the
// broker before its handler starts.
func TestAStaleDeliveryStillTakesTheCheck(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	beatsAtStart := make(chan int, 1)
	r, s := runHandler(t, ctx, f, Options{}, "q", func(context.Context, Job) error {
		n, _ := f.counts("Heartbeat")
		beatsAtStart <- n
		return nil
	})

	fx := newFixture("q")
	fx.Grant = time.Second             // the threshold is 100 ms
	time.Sleep(150 * time.Millisecond) // the subscription's credit is now older than that
	require.NoError(t, s.deliver(ctx, fx.proto()))

	select {
	case n := <-beatsAtStart:
		require.Equal(t, 1, n, "the ownership check ran before the handler")
	case <-ctx.Done():
		t.Fatal("the handler never started")
	}
	awaitCalls(t, ctx, f, "Ack", 1)
	require.NoError(t, r.stopAndWait(t))
}

// TestACancelledJobThatStartedStopsAtItsFirstRenewal: a job cancelled before
// a fresh delivery starts is learned at the first renewal, G/3 in, which
// docs/semantics.md §10 allows. Nothing is confirmed for it.
func TestACancelledJobThatStartedStopsAtItsFirstRenewal(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	f.set(func() {
		f.onHeartbeat = func(context.Context, *hopperv1.HeartbeatRequest) error {
			return status.Error(codes.FailedPrecondition, "job cancelled")
		}
	})
	type stop struct {
		cause error
		after time.Duration
	}
	stopped := make(chan stop, 1)
	r, s := runHandler(t, ctx, f, Options{}, "q", func(hctx context.Context, _ Job) error {
		start := time.Now()
		<-hctx.Done()
		stopped <- stop{context.Cause(hctx), time.Since(start)}
		return hctx.Err()
	})

	fx := newFixture("q")
	fx.Grant = 3 * time.Second // fresh for 300 ms; the first renewal is 1 s after start
	require.NoError(t, s.deliver(ctx, fx.proto()))

	var got stop
	select {
	case got = <-stopped:
	case <-ctx.Done():
		t.Fatal("the handler never started, or was never stopped")
	}
	require.ErrorIs(t, got.cause, ErrJobCancelled)
	require.GreaterOrEqual(t, got.after, 800*time.Millisecond, "learned at the renewal, about G/3 in")
	require.Less(t, got.after, 2*time.Second)
	require.NoError(t, r.stopAndWait(t))

	beats, _ := f.counts("Heartbeat")
	acks, _ := f.counts("Ack")
	nacks, _ := f.counts("Nack")
	require.Equal(t, 1, beats, "one call, the renewal that was refused")
	require.Zero(t, acks, "a cancelled job is not confirmed")
	require.Zero(t, nacks, "a cancelled job is not confirmed")
}

// TestACancelledJobThatFinishesFirstIsRefusedAtItsAck: a short handler on a
// cancelled job finishes before any renewal; its Ack is refused and not
// retried, which §10 allows.
func TestACancelledJobThatFinishesFirstIsRefusedAtItsAck(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	f.set(func() {
		f.onAck = func(context.Context, *hopperv1.AckRequest) error {
			return status.Error(codes.FailedPrecondition, "job cancelled")
		}
	})
	ran := make(chan struct{}, 1)
	r, s := runHandler(t, ctx, f, Options{}, "q", func(context.Context, Job) error {
		ran <- struct{}{}
		return nil
	})

	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	select {
	case <-ran:
	case <-ctx.Done():
		t.Fatal("the handler never started")
	}
	awaitCalls(t, ctx, f, "Ack", 1)
	require.NoError(t, s.awaitCredit(ctx, 1), "the delivery ended and freed its slot")
	require.NoError(t, r.stopAndWait(t))

	acks, _ := f.counts("Ack")
	nacks, _ := f.counts("Nack")
	beats, _ := f.counts("Heartbeat")
	require.Equal(t, 1, acks, "FAILED_PRECONDITION is a refusal, not retried")
	require.Zero(t, nacks)
	require.Zero(t, beats, "nothing asked the broker before the handler ran")
}

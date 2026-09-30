package worker

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// These tests pin the outage window docs/semantics.md §11.3 documents: how
// long a delivery survives its broker answering UNAVAILABLE. The rule is the
// local deadline — the send time of the last successful ownership check or
// renewal, plus the grant, less the safety margin. A failed call never moves
// it. With a 3s grant, renewals are 1s apart and the deadline is 2.7s after
// the last success.

// failRenewal answers the first Heartbeat, the ownership check, and fails the
// second, the first renewal, with UNAVAILABLE after taking `slow` to do so.
// Every later Heartbeat succeeds at once. The returned value holds when the
// check was answered, in Unix nanoseconds.
func failRenewal(f *fake, slow time.Duration) *atomic.Int64 {
	var calls atomic.Int32
	var checked atomic.Int64
	f.set(func() {
		f.onHeartbeat = func(ctx context.Context, _ *hopperv1.HeartbeatRequest) error {
			switch calls.Add(1) {
			case 1:
				checked.Store(time.Now().UnixNano())
				return nil
			case 2:
				select {
				case <-time.After(slow):
				case <-ctx.Done():
				}
				return status.Error(codes.Unavailable, "database unavailable")
			default:
				return nil
			}
		}
	})
	return &checked
}

// A renewal that fails at once costs one interval: the next one, a second
// later, is still well before the deadline and keeps the lease.
func TestAFastFailedRenewalCostsOneInterval(t *testing.T) {
	f := startFake(t)
	failRenewal(f, 0)
	x := newHB(t, f, 3*time.Second)
	require.NoError(t, x.h.acquire(x.d.acquisitionBudget()))
	x.h.start()

	select {
	case err := <-x.lost:
		t.Fatalf("a renewal that failed at once cost the lease: %v", err)
	case <-time.After(3500 * time.Millisecond): // past the deadline the check set
	}
	require.True(t, x.h.lease.live())
	started, _ := f.counts("Heartbeat")
	require.GreaterOrEqual(t, started, 3, "the check, the failed renewal, and the one that kept the lease")
}

// Renewals are an interval apart counted from when the previous one returned,
// so a failure that takes 0.9s to arrive pushes the next renewal to 2.9s,
// past the 2.7s deadline. The failure lasted less than a third of the grant,
// and the lease is lost anyway.
func TestASlowFailedRenewalCanSpendTheLease(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	checked := failRenewal(f, 900*time.Millisecond)
	x := newHB(t, f, 3*time.Second)
	require.NoError(t, x.h.acquire(x.d.acquisitionBudget()))
	deadline, _ := x.h.lease.state()
	x.h.start()

	require.ErrorIs(t, x.awaitLost(t, ctx), ErrLeaseExpired)
	require.WithinDuration(t, deadline, time.Now(), 300*time.Millisecond, "lost at the deadline the check set")
	require.WithinDuration(t, time.Unix(0, checked.Load()).Add(2700*time.Millisecond), deadline, 100*time.Millisecond)
	time.Sleep(500 * time.Millisecond)
	started, _ := f.counts("Heartbeat")
	require.Equal(t, 2, started, "the renewal that would have kept the lease was due after it ran out")
}

// confirmAfterFailedRenewal runs one delivery with a 3s grant whose ownership
// check succeeds, whose renewals then fail with UNAVAILABLE, and whose handler
// returns 1.5s in, after the first renewal failed. Acks fail with UNAVAILABLE
// until `ackOutage` after the check. It returns the worker's log messages, how
// many Acks succeeded, and when the check was answered.
func confirmAfterFailedRenewal(t *testing.T, ackOutage time.Duration) (msgs []string, acked int32, checked time.Time) {
	t.Helper()
	ctx := waitCtx(t)
	f := startFake(t)
	var beats, acks atomic.Int32
	var mu sync.Mutex
	f.set(func() {
		f.onHeartbeat = func(context.Context, *hopperv1.HeartbeatRequest) error {
			if beats.Add(1) == 1 {
				mu.Lock()
				checked = time.Now()
				mu.Unlock()
				return nil
			}
			return status.Error(codes.Unavailable, "database unavailable")
		}
		f.onAck = func(context.Context, *hopperv1.AckRequest) error {
			mu.Lock()
			up := time.Since(checked) >= ackOutage
			mu.Unlock()
			if !up {
				return status.Error(codes.Unavailable, "database unavailable")
			}
			acks.Add(1)
			return nil
		}
	})
	sink := &logSink{}
	r, s := runHandler(t, ctx, f, Options{Logger: slog.New(sink), ShutdownTimeout: 10 * time.Second}, "q",
		func(hctx context.Context, _ Job) error {
			select {
			case <-time.After(1500 * time.Millisecond):
				return nil
			case <-hctx.Done():
				return context.Cause(hctx)
			}
		})
	fx := newFixture("q")
	fx.Grant = 3 * time.Second
	require.NoError(t, s.deliver(ctx, fx.proto()))
	require.NoError(t, s.awaitCredit(ctx, 1), "the delivery ends")
	require.NoError(t, r.stopAndWait(t))
	mu.Lock()
	defer mu.Unlock()
	return sink.messages(), acks.Load(), checked
}

// The Ack has only what is left of the lease, and a failed renewal left the
// deadline where the check put it: 2.7s after the check, 1.2s after the
// handler returned, not two thirds of the grant. Acks keep failing until
// 2.9s, so the delivery is given up, though the handler returned only 1.4s
// before the outage ended.
func TestAConfirmationHasOnlyWhatIsLeftOfTheLease(t *testing.T) {
	msgs, acked, checked := confirmAfterFailedRenewal(t, 2900*time.Millisecond)
	require.Zero(t, acked)
	require.Contains(t, msgs, "job left unconfirmed; its lease ran out and it will be redelivered")
	require.WithinDuration(t, checked.Add(2700*time.Millisecond), time.Now(), time.Second,
		"given up at the deadline the check set")
}

// The same delivery, with an outage that ends at 1.7s, before the deadline:
// a retry lands after it and the job is confirmed.
func TestALateConfirmationStillLandsBeforeTheDeadline(t *testing.T) {
	msgs, acked, _ := confirmAfterFailedRenewal(t, 1700*time.Millisecond)
	require.Equal(t, int32(1), acked)
	require.Contains(t, msgs, "job confirmed")
	require.False(t, slices.Contains(msgs, "job left unconfirmed; its lease ran out and it will be redelivered"))
}

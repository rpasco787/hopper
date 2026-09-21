package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

func TestReconnectPreservesActiveDeliveriesAndAdvertisesOnlyFreeCapacity(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sp := newSpy(t, true)

	breakFirst := make(chan struct{})
	var once sync.Once
	f.set(func() {
		f.onLease = func(ctx context.Context, s *fakeStream) error {
			first := false
			once.Do(func() { first = true })
			if first {
				select {
				case <-breakFirst:
					return status.Error(codes.Unavailable, "broker draining")
				case <-ctx.Done():
					return status.FromContextError(ctx.Err()).Err()
				}
			}
			return s.idle(ctx)
		}
	})

	w := newTestWorker(t, f.addr, Options{Concurrency: 3}, sp.run, "q")
	r := runWorker(t, w)
	s1 := f.awaitStream(t, ctx)
	require.NoError(t, s1.deliver(ctx, newFixture("q").proto()))
	require.NoError(t, s1.deliver(ctx, newFixture("q").proto()))
	sp.await(t, ctx)
	sp.await(t, ctx)

	close(breakFirst)
	s2 := f.awaitStream(t, ctx)
	require.Equal(t, uint32(1), s2.sub.GetCredit(),
		"the replacement stream advertises only capacity not held by old deliveries")
	require.NoError(t, s2.deliver(ctx, newFixture("q").proto()))
	sp.await(t, ctx)
	require.Equal(t, int64(3), sp.dispatched.Load())
	require.Equal(t, uint64(0), s2.creditTotal())

	sp.hold.release()
	require.NoError(t, s2.awaitCredit(ctx, 3), "all three tokens become useful on the replacement stream")
	require.NoError(t, r.stopAndWait(t))
}

func TestImmediateCleanEOFBacksOffBeforeReconnect(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sp := newSpy(t, false)

	var mu sync.Mutex
	var opened []time.Time
	f.set(func() {
		f.onLease = func(context.Context, *fakeStream) error {
			mu.Lock()
			opened = append(opened, time.Now())
			mu.Unlock()
			return nil
		}
	})

	w := newTestWorker(t, f.addr, Options{Concurrency: 1}, sp.run, "q")
	r := runWorker(t, w)
	f.awaitStream(t, ctx)
	f.awaitStream(t, ctx)

	mu.Lock()
	require.GreaterOrEqual(t, opened[1].Sub(opened[0]), 50*time.Millisecond,
		"an empty clean session must not create a tight reconnect loop")
	mu.Unlock()
	require.NoError(t, r.stopAndWait(t))
}

func TestPermanentStreamCodesStopWithoutReconnect(t *testing.T) {
	for _, code := range []codes.Code{
		codes.InvalidArgument,
		codes.Unimplemented,
		codes.PermissionDenied,
		codes.Unauthenticated,
	} {
		t.Run(code.String(), func(t *testing.T) {
			f := startFake(t)
			sp := newSpy(t, false)
			f.set(func() {
				f.onSubscribe = func(context.Context, *hopperv1.Subscription) error {
					return status.Error(code, "permanent")
				}
			})
			w := newTestWorker(t, f.addr, Options{Concurrency: 1}, sp.run, "q")
			r := runWorker(t, w)
			err := r.wait(t)
			require.Equal(t, code, status.Code(err))
			require.Len(t, f.subscriptions(), 1)
		})
	}
}

func TestDrainDoesNotOpenAnotherStream(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sp := newSpy(t, false)
	w := newTestWorker(t, f.addr, Options{Concurrency: 1}, sp.run, "q")
	r := runWorker(t, w)
	s := f.awaitStream(t, ctx)

	r.stop()
	require.NoError(t, s.awaitEOF(ctx))
	require.NoError(t, r.wait(t))
	require.Len(t, f.subscriptions(), 1)
}

func TestRepeatedFailuresReclaimUnspentCredit(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sp := newSpy(t, false)
	var sessions atomic.Int64
	f.set(func() {
		f.onLease = func(ctx context.Context, s *fakeStream) error {
			if sessions.Add(1) <= 2 {
				return status.Error(codes.Unavailable, "restart")
			}
			return s.idle(ctx)
		}
	})

	w := newTestWorker(t, f.addr, Options{Concurrency: 2}, sp.run, "q")
	r := runWorker(t, w)
	s1 := f.awaitStream(t, ctx)
	s2 := f.awaitStream(t, ctx)
	s3 := f.awaitStream(t, ctx)
	require.Equal(t, uint32(2), s1.sub.GetCredit())
	require.Equal(t, uint32(2), s2.sub.GetCredit())
	require.Equal(t, uint32(2), s3.sub.GetCredit(),
		"each failed stream returns its unspent reservation exactly once")
	require.NoError(t, r.stopAndWait(t))
}

func TestTransientSubscriptionFailureReconnects(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sp := newSpy(t, false)
	var attempts atomic.Int64
	f.set(func() {
		f.onSubscribe = func(context.Context, *hopperv1.Subscription) error {
			if attempts.Add(1) == 1 {
				return status.Error(codes.Unavailable, "broker draining")
			}
			return nil
		}
	})

	w := newTestWorker(t, f.addr, Options{Concurrency: 2}, sp.run, "q")
	r := runWorker(t, w)
	s := f.awaitStream(t, ctx)
	require.Equal(t, uint32(2), s.sub.GetCredit())
	require.Len(t, f.subscriptions(), 2)
	require.NoError(t, r.stopAndWait(t))
}

func TestSendFailureAfterDeliveryDoesNotDoubleRefundCapacity(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	ctl := &streamControl{}
	sp := newSpy(t, false)
	creditForwarded := make(chan struct{})
	returnFailure := make(chan struct{})
	var sends atomic.Int64
	ctl.set(func() {
		ctl.onSend = func(ctx context.Context, _ any, forward func() error) error {
			if err := forward(); err != nil {
				return err
			}
			if sends.Add(1) != 2 {
				return nil
			}
			close(creditForwarded)
			select {
			case <-returnFailure:
				return status.Error(codes.Unavailable, "write outcome was uncertain")
			case <-ctx.Done():
				return status.FromContextError(ctx.Err()).Err()
			}
		}
	})

	w := newTestWorker(t, f.addr, Options{
		Concurrency: 2, DialOptions: []grpc.DialOption{ctl.dialOption()},
	}, sp.run, "q")
	r := runWorker(t, w)
	s1 := f.awaitStream(t, ctx)
	require.NoError(t, s1.deliver(ctx, newFixture("q").proto()))
	sp.await(t, ctx)
	select {
	case <-creditForwarded:
	case <-ctx.Done():
		t.Fatal("the returned token was not sent")
	}
	require.NoError(t, s1.deliver(ctx, newFixture("q").proto()),
		"the broker can answer an uncertain Send before it returns")
	sp.await(t, ctx)
	close(returnFailure)

	s2 := f.awaitStream(t, ctx)
	require.Equal(t, uint32(2), s2.sub.GetCredit(),
		"the received delivery spent the uncertain reservation before reconciliation")
	require.NoError(t, r.stopAndWait(t))
}

func TestReconnectCountsAConfirmationAsActiveDelivery(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	breakFirst := make(chan struct{})
	ackEntered := make(chan struct{})
	releaseAck := make(chan struct{})
	var leaseCalls atomic.Int64
	f.set(func() {
		f.onLease = func(ctx context.Context, s *fakeStream) error {
			if leaseCalls.Add(1) == 1 {
				select {
				case <-breakFirst:
					return status.Error(codes.Unavailable, "restart")
				case <-ctx.Done():
					return status.FromContextError(ctx.Err()).Err()
				}
			}
			return s.idle(ctx)
		}
		f.onAck = func(ctx context.Context, _ *hopperv1.AckRequest) error {
			select {
			case <-ackEntered:
			default:
				close(ackEntered)
			}
			select {
			case <-releaseAck:
				return nil
			case <-ctx.Done():
				return status.FromContextError(ctx.Err()).Err()
			}
		}
	})
	t.Cleanup(func() {
		if !channelClosed(releaseAck) {
			close(releaseAck)
		}
	})

	w := New(f.addr, Options{Concurrency: 2, ShutdownTimeout: 2 * time.Second, Logger: quietLogger()})
	w.Handle("q", func(context.Context, Job) error { return nil })
	r := runWorker(t, w)
	s1 := f.awaitStream(t, ctx)
	require.NoError(t, s1.deliver(ctx, newFixture("q").proto()))
	select {
	case <-ackEntered:
	case <-ctx.Done():
		t.Fatal("the delivery never reached confirmation")
	}
	close(breakFirst)
	s2 := f.awaitStream(t, ctx)
	require.Equal(t, uint32(1), s2.sub.GetCredit(),
		"a blocked confirmation retains the delivery token across reconnect")
	close(releaseAck)
	require.NoError(t, s2.awaitCredit(ctx, 2))
	require.NoError(t, r.stopAndWait(t))
}

func channelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

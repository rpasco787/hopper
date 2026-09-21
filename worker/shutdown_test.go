package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

func TestShutdownDrainsRunningHandlerThroughHeartbeatAndAck(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	renewed := make(chan struct{}, 1)
	var beats atomic.Int64
	f.set(func() {
		f.onHeartbeat = func(context.Context, *hopperv1.HeartbeatRequest) error {
			if beats.Add(1) >= 2 {
				select {
				case renewed <- struct{}{}:
				default:
				}
			}
			return nil
		}
	})

	handlerStarted := make(chan context.Context, 1)
	finishHandler := newGate(t)
	w := New(f.addr, Options{
		Concurrency: 1, ShutdownTimeout: 2 * time.Second, Logger: quietLogger(),
	})
	w.Handle("q", func(hctx context.Context, _ Job) error {
		handlerStarted <- hctx
		<-finishHandler.ch
		return nil
	})
	r := runWorker(t, w)
	s := f.awaitStream(t, ctx)
	fx := newFixture("q")
	fx.Grant = 600 * time.Millisecond
	require.NoError(t, s.deliver(ctx, fx.proto()))

	var handlerCtx context.Context
	select {
	case handlerCtx = <-handlerStarted:
	case <-ctx.Done():
		t.Fatal("the handler never started")
	}
	r.stop()
	require.NoError(t, s.awaitEOF(ctx), "normal cancellation half-closes intake")
	select {
	case <-handlerCtx.Done():
		t.Fatalf("graceful drain cancelled the handler: %v", context.Cause(handlerCtx))
	default:
	}
	select {
	case <-renewed:
	case <-ctx.Done():
		t.Fatal("the running handler did not retain heartbeat renewal during drain")
	}

	finishHandler.release()
	awaitCalls(t, ctx, f, "Ack", 1)
	require.NoError(t, r.wait(t))
	require.Len(t, f.ackRequests(), 1)
	require.Empty(t, f.nackRequests())
}

func TestPermanentConsumeFailureIsJoinedWithHandlerAbandonment(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	failStream := make(chan struct{})
	f.set(func() {
		f.onLease = func(ctx context.Context, _ *fakeStream) error {
			select {
			case <-failStream:
				return status.Error(codes.InvalidArgument, "permanent stream failure")
			case <-ctx.Done():
				return status.FromContextError(ctx.Err()).Err()
			}
		}
	})

	handlerStarted := make(chan struct{}, 1)
	handlerDone := make(chan struct{})
	releaseHandler := make(chan struct{})
	t.Cleanup(func() {
		if !channelClosed(releaseHandler) {
			close(releaseHandler)
		}
	})
	const budget = 300 * time.Millisecond
	w := New(f.addr, Options{Concurrency: 1, ShutdownTimeout: budget, Logger: quietLogger()})
	w.Handle("q", func(context.Context, Job) error {
		defer close(handlerDone)
		handlerStarted <- struct{}{}
		<-releaseHandler // deliberately ignores cancellation
		return nil
	})
	r := runWorker(t, w)
	s := f.awaitStream(t, ctx)
	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	select {
	case <-handlerStarted:
	case <-ctx.Done():
		t.Fatal("the handler never started")
	}

	started := time.Now()
	close(failStream)
	err := r.wait(t)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "the permanent consume status is preserved")
	require.ErrorIs(t, err, ErrAbandoned)
	require.ErrorContains(t, err, "1 running handlers")
	require.GreaterOrEqual(t, time.Since(started), budget)
	require.Less(t, time.Since(started), 2*budget)

	close(releaseHandler)
	select {
	case <-handlerDone:
	case <-ctx.Done():
		t.Fatal("the released handler did not return")
	}
	require.Empty(t, f.ackRequests())
	require.Empty(t, f.nackRequests())
}

func TestForcedShutdownCountsBlockedAcquisition(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	acquiring := make(chan struct{}, 1)
	f.set(func() {
		f.onHeartbeat = func(hctx context.Context, _ *hopperv1.HeartbeatRequest) error {
			select {
			case acquiring <- struct{}{}:
			default:
			}
			<-hctx.Done()
			return status.FromContextError(hctx.Err()).Err()
		}
	})

	const budget = 250 * time.Millisecond
	ran := make(chan struct{}, 1)
	w := New(f.addr, Options{Concurrency: 1, ShutdownTimeout: budget, Logger: quietLogger()})
	w.Handle("q", func(context.Context, Job) error { ran <- struct{}{}; return nil })
	r := runWorker(t, w)
	s := f.awaitStream(t, ctx)
	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	select {
	case <-acquiring:
	case <-ctx.Done():
		t.Fatal("acquisition never started")
	}

	started := time.Now()
	r.stop()
	err := r.wait(t)
	require.ErrorIs(t, err, ErrAbandoned)
	require.ErrorContains(t, err, "1 acquiring a lease")
	require.Less(t, time.Since(started), 2*budget)
	require.Empty(t, f.ackRequests())
	require.Empty(t, f.nackRequests())
	require.Empty(t, ran)
}

func TestForcedShutdownCountsBlockedUnstartedNack(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	acquiring := make(chan struct{}, 1)
	releaseAcquisition := make(chan struct{})
	nacking := make(chan struct{}, 1)
	f.set(func() {
		f.onHeartbeat = func(hctx context.Context, _ *hopperv1.HeartbeatRequest) error {
			select {
			case acquiring <- struct{}{}:
			default:
			}
			select {
			case <-releaseAcquisition:
				return nil
			case <-hctx.Done():
				return status.FromContextError(hctx.Err()).Err()
			}
		}
		f.onNack = func(nctx context.Context, _ *hopperv1.NackRequest) error {
			select {
			case nacking <- struct{}{}:
			default:
			}
			<-nctx.Done()
			return status.FromContextError(nctx.Err()).Err()
		}
	})

	const budget = 300 * time.Millisecond
	ran := make(chan struct{}, 1)
	w := New(f.addr, Options{Concurrency: 1, ShutdownTimeout: budget, Logger: quietLogger()})
	w.Handle("q", func(context.Context, Job) error { ran <- struct{}{}; return nil })
	r := runWorker(t, w)
	s := f.awaitStream(t, ctx)
	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	select {
	case <-acquiring:
	case <-ctx.Done():
		t.Fatal("acquisition never started")
	}

	r.stop()
	require.NoError(t, s.awaitEOF(ctx))
	close(releaseAcquisition)
	select {
	case <-nacking:
	case <-ctx.Done():
		t.Fatal("the unstarted Nack never began")
	}
	err := r.wait(t)
	require.ErrorIs(t, err, ErrAbandoned)
	require.ErrorContains(t, err, "1 awaiting confirmation")
	require.Len(t, f.nackRequests(), 1)
	require.Empty(t, ran)
}

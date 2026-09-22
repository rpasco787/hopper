package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// hb is a heartbeat under test, with the delivery it owns and a channel
// carrying whatever it reports to the handler.
type hb struct {
	h    *heartbeat
	d    *delivery
	lost chan error
	f    *fake
}

func newHB(t *testing.T, f *fake, grant time.Duration) *hb {
	t.Helper()

	fx := newFixture("q")
	fx.Grant = grant
	d, err := parseDelivery(fx.proto())
	require.NoError(t, err)

	parent, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	lost := make(chan error, 8)
	h := newHeartbeat(parent, f.dial(t), quietLogger(), d, func(err error) { lost <- err })
	t.Cleanup(h.stop)
	return &hb{h: h, d: d, lost: lost, f: f}
}

func (x *hb) awaitLost(t *testing.T, ctx context.Context) error {
	t.Helper()
	select {
	case err := <-x.lost:
		return err
	case <-ctx.Done():
		t.Fatalf("the handler was never told the lease went away: %v", ctx.Err())
		return nil
	}
}

func TestAcquisitionSpendsOneHeartbeatAndEstablishesADeadline(t *testing.T) {
	f := startFake(t)
	x := newHB(t, f, 30*time.Second)

	sent := time.Now()
	require.NoError(t, x.h.acquire(x.d.acquisitionBudget()))

	started, finished := f.counts("Heartbeat")
	require.Equal(t, 1, started, "acquisition is exactly one extra RPC per valid delivery")
	require.Equal(t, 1, finished)

	deadline, cause := x.h.lease.state()
	require.NoError(t, cause)
	require.WithinDuration(t, sent.Add(30*time.Second-leaseSafetyMargin), deadline, time.Second)
	require.True(t, deadline.After(time.Now()), "a handler may only start against a future deadline")

	require.Equal(t, 30*time.Second, f.heartbeatRequests()[0].GetLeaseDuration().AsDuration(),
		"validation pins the duration carried by the delivery instead of consulting a new broker default")
}

func TestRenewalUsesTheBrokerSGrantNotTheRequest(t *testing.T) {
	f := startFake(t)
	// The broker granted 400ms although nothing was requested. Renewal has to
	// pace off the grant: it is the only evidence of what the broker chose.
	x := newHB(t, f, 400*time.Millisecond)
	require.NoError(t, x.h.acquire(x.d.acquisitionBudget()))

	deadline, _ := x.h.lease.state()
	require.WithinDuration(t, time.Now().Add(400*time.Millisecond-40*time.Millisecond), deadline, 100*time.Millisecond,
		"the margin scales down for a short grant")
}

func TestPeriodicRenewalKeepsAHandlerAliveAcrossSeveralGrants(t *testing.T) {
	f := startFake(t)
	x := newHB(t, f, 200*time.Millisecond)
	require.NoError(t, x.h.acquire(x.d.acquisitionBudget()))
	x.h.start()

	// Four grants' worth of handler. Renewal targets a third of the grant, so
	// the lease should never come close to running out.
	select {
	case err := <-x.lost:
		t.Fatalf("the lease was lost while renewal was healthy: %v", err)
	case <-time.After(800 * time.Millisecond):
	}
	require.True(t, x.h.lease.live())

	started, _ := f.counts("Heartbeat")
	require.Greater(t, started, 4, "one acquisition plus a beat roughly every 66ms")

	x.h.stop()
	require.True(t, x.h.lease.live(), "a normal stop is not a rejection; the Ack that follows is still valid")
	require.Empty(t, x.lost, "and the handler is never told anything went wrong")
}

func TestEveryRenewalPinsTheOriginalBrokerGrant(t *testing.T) {
	f := startFake(t)
	x := newHB(t, f, 150*time.Millisecond)
	require.NoError(t, x.h.acquire(x.d.acquisitionBudget()))
	x.h.start()
	awaitCalls(t, waitCtx(t), f, "Heartbeat", 3)
	x.h.stop()

	requests := f.heartbeatRequests()
	require.GreaterOrEqual(t, len(requests), 3)
	for i, req := range requests {
		require.Equalf(t, 150*time.Millisecond, req.GetLeaseDuration().AsDuration(),
			"heartbeat %d must not select a different broker's default", i)
	}
}

func TestShortGrantsGetNoMinimumInterval(t *testing.T) {
	f := startFake(t)
	x := newHB(t, f, 60*time.Millisecond)
	require.NoError(t, x.h.acquire(x.d.acquisitionBudget()))
	before, _ := f.counts("Heartbeat")
	x.h.start()

	time.Sleep(90 * time.Millisecond)
	after, _ := f.counts("Heartbeat")
	require.Greater(t, after, before,
		"a 60ms grant heartbeats every 20ms; a fixed floor would guarantee it expires")
}

func TestTerminalHeartbeatMappings(t *testing.T) {
	tests := []struct {
		name string
		code codes.Code
		want error
	}{
		{"stale token", codes.Aborted, ErrLeaseLost},
		{"row is gone", codes.NotFound, ErrLeaseLost},
		{"job cancelled", codes.FailedPrecondition, ErrJobCancelled},
	}
	for _, tc := range tests {
		t.Run("acquisition: "+tc.name, func(t *testing.T) {
			f := startFake(t)
			f.set(func() {
				f.onHeartbeat = func(context.Context, *hopperv1.HeartbeatRequest) error {
					return status.Error(tc.code, tc.name)
				}
			})
			x := newHB(t, f, 30*time.Second)
			err := x.h.acquire(x.d.acquisitionBudget())
			require.ErrorIs(t, err, tc.want)
			require.False(t, x.h.lease.live(), "a refused acquisition never starts a handler")
		})

		t.Run("renewal: "+tc.name, func(t *testing.T) {
			ctx := waitCtx(t)
			f := startFake(t)
			x := newHB(t, f, 150*time.Millisecond)
			require.NoError(t, x.h.acquire(x.d.acquisitionBudget()))

			f.set(func() {
				f.onHeartbeat = func(context.Context, *hopperv1.HeartbeatRequest) error {
					return status.Error(tc.code, tc.name)
				}
			})
			x.h.start()
			require.ErrorIs(t, x.awaitLost(t, ctx), tc.want)
			require.False(t, x.h.lease.live())
		})
	}
}

func TestAGenericRejectionStopsWorkWithItsDiagnostic(t *testing.T) {
	f := startFake(t)
	f.set(func() {
		f.onHeartbeat = func(context.Context, *hopperv1.HeartbeatRequest) error {
			return status.Error(codes.Internal, "internal error")
		}
	})
	x := newHB(t, f, 30*time.Second)

	err := x.h.acquire(x.d.acquisitionBudget())
	require.ErrorContains(t, err, "heartbeat rejected")
	require.Equal(t, codes.Internal, status.Code(err), "the underlying status survives for the log")
	require.NotErrorIs(t, err, ErrLeaseLost, "INTERNAL is not evidence the lease went anywhere")
	require.False(t, x.h.lease.live(), "but it is not retried blindly either")

	started, _ := f.counts("Heartbeat")
	require.Equal(t, 1, started)
}

func TestTransientFailuresAreRetriedWithinTheBudget(t *testing.T) {
	f := startFake(t)
	var calls int
	f.set(func() {
		f.onHeartbeat = func(context.Context, *hopperv1.HeartbeatRequest) error {
			f.mu.Lock()
			calls++
			n := calls
			f.mu.Unlock()
			if n < 3 {
				return status.Error(codes.Unavailable, "broker is shutting down")
			}
			return nil
		}
	})
	x := newHB(t, f, 30*time.Second)

	require.NoError(t, x.h.acquire(x.d.acquisitionBudget()))
	started, _ := f.counts("Heartbeat")
	require.Equal(t, 3, started, "two transient failures, then ownership is proven")
	require.True(t, x.h.lease.live())
}

func TestAcquisitionGivesUpWhenItsBudgetRunsOut(t *testing.T) {
	f := startFake(t)
	g := newGate(t)
	f.set(func() {
		f.onHeartbeat = func(ctx context.Context, _ *hopperv1.HeartbeatRequest) error {
			return g.wait(ctx)
		}
	})
	// A 300ms grant leaves a 270ms budget: the whole attempt is bounded by it.
	x := newHB(t, f, 300*time.Millisecond)

	start := time.Now()
	err := x.h.acquire(x.d.acquisitionBudget())
	elapsed := time.Since(start)

	require.ErrorIs(t, err, ErrLeaseExpired)
	require.False(t, x.h.lease.live(), "no handler runs after an unsuccessful acquisition")
	require.Less(t, elapsed, time.Second, "the budget bounds the waiting, not the RPC timeout")
}

func TestExpiryIsIndependentOfHeartbeatProgress(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	x := newHB(t, f, 300*time.Millisecond)
	require.NoError(t, x.h.acquire(x.d.acquisitionBudget()))
	deadline, _ := x.h.lease.state()

	// From here every heartbeat stalls until its own RPC deadline. If expiry
	// depended on an RPC returning, the handler would run on past a lease the
	// reaper is already free to take back.
	g := newGate(t)
	f.set(func() {
		f.onHeartbeat = func(ctx context.Context, _ *hopperv1.HeartbeatRequest) error {
			return g.wait(ctx)
		}
	})
	x.h.start()

	require.ErrorIs(t, x.awaitLost(t, ctx), ErrLeaseExpired)
	require.WithinDuration(t, deadline, time.Now(), 150*time.Millisecond,
		"expiry happens at the deadline, not whenever the network gives up")

	started, _ := f.counts("Heartbeat")
	require.Greater(t, started, 1, "renewals were attempted and got nowhere")
}

func TestRenewalCancelsHandlerWhenItObservesAnExpiredLease(t *testing.T) {
	f := startFake(t)
	x := newHB(t, f, 3*time.Millisecond)
	x.h.lease.establish(time.Now().Add(-time.Second))

	x.h.wg.Add(1)
	go x.h.renew()
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, x.awaitLost(t, ctx), ErrLeaseExpired)
}

func TestStopDuringABlockedRenewalIsPromptAndNotARejection(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	// A long grant means a long renewal interval, so the stalled RPC would
	// otherwise sit there for ten seconds.
	x := newHB(t, f, 30*time.Second)
	require.NoError(t, x.h.acquire(x.d.acquisitionBudget()))

	g := newGate(t)
	entered := make(chan struct{}, 1)
	f.set(func() {
		f.onHeartbeat = func(ctx context.Context, _ *hopperv1.HeartbeatRequest) error {
			select {
			case entered <- struct{}{}:
			default:
			}
			return g.wait(ctx)
		}
	})
	// Renew immediately rather than waiting a third of thirty seconds.
	x.h.grant = 30 * time.Millisecond
	x.h.start()

	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("no renewal was attempted")
	}

	start := time.Now()
	x.h.stop()
	require.Less(t, time.Since(start), 2*time.Second, "stopping cancels the RPC in flight")
	require.True(t, x.h.lease.live(), "our own cancellation is not the broker refusing us")
	require.Empty(t, x.lost)
}

func TestLeaseTransitions(t *testing.T) {
	now := time.Now()

	t.Run("a stale timer cannot expire a renewed lease", func(t *testing.T) {
		var l lease
		old := now.Add(100 * time.Millisecond)
		l.establish(old)
		require.True(t, l.extend(now.Add(time.Second), now))

		// The wakeup armed for the old deadline fires. It must look at the
		// deadline as it is now, not the one it was scheduled against.
		require.False(t, l.expireIfPast(old), "the lease was renewed under it")
		require.True(t, l.live())
	})

	t.Run("a response after its usable deadline is discarded", func(t *testing.T) {
		var l lease
		l.establish(now.Add(time.Second))
		require.False(t, l.extend(now.Add(-time.Millisecond), now), "already spent when it arrived")
		deadline, cause := l.state()
		require.NoError(t, cause, "a late response is not a lost lease")
		require.Equal(t, now.Add(time.Second), deadline, "the older, still-live deadline stands")
	})

	t.Run("a renewal cannot revive a lease whose deadline has passed", func(t *testing.T) {
		var l lease
		l.establish(now.Add(-time.Millisecond))
		require.False(t, l.extend(now.Add(time.Second), now))
		require.ErrorIs(t, errFrom(&l), ErrLeaseExpired)
	})

	t.Run("terminal is forever", func(t *testing.T) {
		var l lease
		l.establish(now.Add(time.Second))
		require.True(t, l.lose(ErrLeaseLost))

		require.False(t, l.lose(ErrJobCancelled), "the first cause is the true one")
		require.False(t, l.extend(now.Add(time.Hour), now), "no late response may revive it")
		require.False(t, l.expireIfPast(now.Add(time.Hour)))
		require.ErrorIs(t, errFrom(&l), ErrLeaseLost)

		l.establish(now.Add(time.Hour))
		require.False(t, l.live())
	})

	t.Run("expiry fires exactly once at the deadline", func(t *testing.T) {
		var l lease
		l.establish(now)
		require.False(t, l.expireIfPast(now.Add(-time.Nanosecond)), "not yet")
		require.True(t, l.expireIfPast(now))
		require.False(t, l.expireIfPast(now), "and not again")
	})
}

func errFrom(l *lease) error {
	_, cause := l.state()
	return cause
}

func TestDeadlineFromUsesTheSendInstantAndScalesItsMargin(t *testing.T) {
	sent := time.Now()

	// A long grant loses the fixed margin.
	require.Equal(t, sent.Add(30*time.Second-leaseSafetyMargin), deadlineFrom(sent, 30*time.Second))
	// A short one loses a tenth, so the margin cannot eat the lease.
	require.Equal(t, sent.Add(time.Second-100*time.Millisecond), deadlineFrom(sent, time.Second))
	require.Equal(t, 10*time.Millisecond, safetyMargin(100*time.Millisecond))
	require.Equal(t, leaseSafetyMargin, safetyMargin(time.Hour))
}

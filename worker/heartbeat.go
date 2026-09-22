package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

const (
	// leaseSafetyMargin is scheduling slack subtracted from every grant. It is
	// not a proof against anything: a paused process or a long stop-the-world
	// pause can exceed any fixed margin. Handler idempotency is what covers
	// that; this only stops the SDK from treating the last instant of a lease
	// as usable time.
	leaseSafetyMargin = 500 * time.Millisecond

	// acquisitionRPCTimeout bounds one ownership-validation attempt. The
	// acquisition budget bounds the sequence.
	acquisitionRPCTimeout = 5 * time.Second
)

// safetyMargin scales the margin down for short grants, so a two-second lease
// does not lose a quarter of itself to slack meant for a thirty-second one.
func safetyMargin(grant time.Duration) time.Duration {
	return min(leaseSafetyMargin, grant/10)
}

// deadlineFrom turns a granted duration into a local deadline.
//
// The base is the instant the request was *sent*, never the instant the
// response arrived. The broker set lease_expires from its own clock when it
// handled the call, which is somewhere inside the round trip; measuring from
// the send is the only choice that cannot overestimate what is left.
func deadlineFrom(sent time.Time, granted time.Duration) time.Time {
	return sent.Add(granted - safetyMargin(granted))
}

// lease is the SDK's synchronized view of one delivery's ownership.
//
// It is deliberately conservative. The deadline is what the SDK believes it
// may still safely work until, not a guard the store enforces: the store's
// guard is the (id, lease_id) pair, and the reaper is what actually takes a
// job back. Losing this race locally is cheap; winning it wrongly means two
// handlers running at once.
type lease struct {
	mu       sync.Mutex
	deadline time.Time
	// cause is nil while the lease is live and terminal afterwards. Terminal
	// is forever: no late response may revive a delivery the SDK has already
	// stopped working on.
	cause error
}

// establish records the deadline won by a successful acquisition.
func (l *lease) establish(deadline time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cause == nil {
		l.deadline = deadline
	}
}

// extend moves the deadline forward after a successful renewal. It refuses in
// two cases that look similar and are not: a lease that is already terminal,
// and a response that arrived too late to be worth anything. The caller
// distinguishes them with live().
func (l *lease) extend(deadline, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cause != nil {
		return false
	}
	if !now.Before(l.deadline) {
		// The previous deadline has already passed. A renewal cannot revive a
		// delivery the SDK has stopped trusting, even if the broker would
		// still accept the token.
		l.cause = ErrLeaseExpired
		return false
	}
	if !now.Before(deadline) {
		// A response whose usable deadline is already spent. Discard it and
		// leave the older, still-live deadline in place.
		return false
	}
	l.deadline = deadline
	return true
}

// expireIfPast marks the lease expired, but only if the deadline it is looking
// at is the current one. A timer wakeup armed for a deadline that has since
// been extended must not expire the renewed lease.
func (l *lease) expireIfPast(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cause != nil || now.Before(l.deadline) {
		return false
	}
	l.cause = ErrLeaseExpired
	return true
}

// lose marks the lease terminal for a reason the broker gave us.
func (l *lease) lose(cause error) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cause != nil {
		return false
	}
	l.cause = cause
	return true
}

func (l *lease) state() (time.Time, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.deadline, l.cause
}

func (l *lease) live() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cause == nil
}

// heartbeat owns one delivery's lease: it proves ownership before a handler
// starts, renews while the handler runs, and gives up at a deadline it can
// reach without the network.
type heartbeat struct {
	rpc hopperv1.HopperServiceClient
	log *slog.Logger

	jobID   uuid.UUID
	leaseID uuid.UUID
	// grant is what the broker demonstrably gives this delivery. Renewals are
	// measured against it rather than against Options.LeaseDuration, because a
	// worker that left the duration unset is using the broker's choice and has
	// no other way to learn what that was.
	grant time.Duration
	lease lease

	// ctx bounds every heartbeat RPC. Cancelling it cancels the one in flight,
	// which is what makes stopping prompt. Its cancellation is deliberately not
	// stripped: an RPC that outlives the delivery it belongs to is a leak.
	ctx    context.Context
	cancel context.CancelFunc

	// onLost cancels the handler's context with the cause.
	onLost func(error)

	// stopping separates the SDK's own cancellation from the broker's refusal.
	// Without it, cancelling the in-flight RPC when a handler finishes normally
	// would look exactly like a rejection and suppress the Ack that follows.
	stopping atomic.Bool
	stopOnce sync.Once
	lostOnce sync.Once
	stopped  chan struct{}
	wg       sync.WaitGroup
}

func newHeartbeat(parent context.Context, rpc hopperv1.HopperServiceClient, log *slog.Logger,
	d *delivery, onLost func(error)) *heartbeat {
	ctx, cancel := context.WithCancel(parent)
	return &heartbeat{
		rpc:     rpc,
		log:     log.With("job_id", d.job.ID, "lease_id", d.leaseID, "queue", d.job.Queue),
		jobID:   d.job.ID,
		leaseID: d.leaseID,
		grant:   d.grant,
		ctx:     ctx,
		cancel:  cancel,
		onLost:  onLost,
		stopped: make(chan struct{}),
	}
}

func (h *heartbeat) request() *hopperv1.HeartbeatRequest {
	// The delivery's timestamps are the only demonstrated grant. Pin every
	// validation and renewal to it: asking each broker to choose again would
	// let rolling configuration changes shorten the store lease while this
	// worker continued scheduling against the older, longer duration.
	return &hopperv1.HeartbeatRequest{
		Id: h.jobID.String(), LeaseId: h.leaseID.String(),
		LeaseDuration: durationpb.New(h.grant),
	}
}

func (h *heartbeat) notifyLost(cause error) {
	h.lostOnce.Do(func() { h.onLost(cause) })
}

// acquire proves the lease is still ours before any handler runs.
//
// This is an SDK policy, not something docs/semantics.md requires. It costs one
// unary RPC per delivery and buys the one thing the delivery message cannot
// provide: evidence about *now*. A job that sat in a transport buffer while the
// reaper took it back is rejected here instead of running a second copy of a
// handler whose first copy may still be going.
//
// The budget is fixed at receipt and bounds the waiting. It is not a claim of
// ownership; only a successful response is that.
func (h *heartbeat) acquire(budget time.Time) error {
	err := h.attemptAcquire(budget)
	if err != nil {
		// Every failure is terminal for this delivery, including simply
		// running out of budget. Leaving the lease looking live afterwards
		// would let a later check mistake "never proven" for "still ours".
		h.lease.lose(err)
	}
	return err
}

func (h *heartbeat) attemptAcquire(budget time.Time) error {
	b := newBackoff()
	for attempt := 1; ; attempt++ {
		sent := time.Now()
		if !sent.Before(budget) {
			return fmt.Errorf("worker: job %s: lease acquisition exhausted its budget after %d attempts: %w",
				h.jobID, attempt-1, ErrLeaseExpired)
		}

		ctx, cancel := context.WithDeadline(h.ctx, earlier(sent.Add(acquisitionRPCTimeout), budget))
		_, err := h.rpc.Heartbeat(ctx, h.request())
		cancel()

		if err == nil {
			deadline := deadlineFrom(sent, h.grant)
			if !time.Now().Before(deadline) {
				// The round trip consumed the grant. Whatever the broker
				// thinks, there is no time left here to run anything in.
				return fmt.Errorf("worker: job %s: the lease was spent before acquisition returned: %w",
					h.jobID, ErrLeaseExpired)
			}
			h.lease.establish(deadline)
			return nil
		}
		if h.ctx.Err() != nil {
			return fmt.Errorf("worker: job %s: lease acquisition stopped: %w", h.jobID, context.Cause(h.ctx))
		}

		cause, retry := classifyHeartbeat(err)
		if !retry {
			return cause
		}
		// Transient. Wait inside what is left of the budget; running out of it
		// is the same answer as being refused, just slower.
		if !wait(h.ctx, b.next(), budget) {
			return fmt.Errorf("worker: job %s: lease acquisition exhausted its budget after %d attempts: %w",
				h.jobID, attempt, ErrLeaseExpired)
		}
	}
}

// start begins renewal and expiry monitoring, on separate goroutines. They are
// separate because expiry must not depend on an RPC making progress: a
// heartbeat stuck on a wedged broker is exactly when the handler most needs to
// learn that its lease ran out.
func (h *heartbeat) start() {
	h.wg.Add(2)
	go h.monitor()
	go h.renew()
}

// stop ends renewal and expiry monitoring and joins both goroutines. Cancelling
// the in-flight RPC is what makes it prompt; the stopping flag is what keeps
// that cancellation from being mistaken for a lost lease.
func (h *heartbeat) stop() {
	h.stopOnce.Do(func() {
		h.stopping.Store(true)
		close(h.stopped)
		h.cancel()
	})
	h.wg.Wait()
}

// monitor owns expiry timing. It re-reads the deadline on every wakeup, so a
// timer armed for a deadline that has since moved cannot expire a renewed
// lease; it simply re-arms on the newer one.
func (h *heartbeat) monitor() {
	defer h.wg.Done()
	for {
		deadline, cause := h.lease.state()
		if cause != nil {
			return
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-timer.C:
			if h.lease.expireIfPast(time.Now()) {
				h.log.Warn("lease expired locally; cancelling the handler",
					"deadline", deadline.Format(time.RFC3339Nano))
				h.notifyLost(ErrLeaseExpired)
				return
			}
		case <-h.stopped:
			timer.Stop()
			return
		case <-h.ctx.Done():
			timer.Stop()
			return
		}
		timer.Stop()
	}
}

// renew extends the lease at a third of the grant. There is deliberately no
// minimum interval: a floor larger than a short lease's usable life would
// guarantee expiry for exactly the configurations that can least afford it.
// A grant too short for its own round trip expires, conservatively, on its own.
func (h *heartbeat) renew() {
	defer h.wg.Done()
	interval := max(h.grant/3, time.Nanosecond)

	for {
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-h.stopped:
			timer.Stop()
			return
		case <-h.ctx.Done():
			timer.Stop()
			return
		}
		timer.Stop()

		deadline, cause := h.lease.state()
		if cause != nil {
			h.notifyLost(cause)
			return
		}
		if now := time.Now(); !now.Before(deadline) {
			h.lease.expireIfPast(now)
			_, cause = h.lease.state()
			h.log.Warn("lease expired locally; cancelling the handler",
				"deadline", deadline.Format(time.RFC3339Nano))
			h.notifyLost(cause)
			return
		}

		// Each attempt stops at the earlier of its own interval and the lease
		// deadline: there is no point waiting for an answer that could only
		// arrive after the lease it is about has run out.
		sent := time.Now()
		ctx, cancel := context.WithDeadline(h.ctx, earlier(sent.Add(interval), deadline))
		_, err := h.rpc.Heartbeat(ctx, h.request())
		cancel()

		if err != nil {
			if h.stopping.Load() {
				// Our own doing, because the handler finished. The lease stays
				// live and eligible for the Ack that follows.
				return
			}
			cause, retry := classifyHeartbeat(err)
			if retry {
				now := time.Now()
				if now.Before(deadline) && h.lease.live() {
					continue
				}
				h.lease.expireIfPast(now)
				_, cause = h.lease.state()
				if cause == nil {
					cause = ErrLeaseExpired
					h.lease.lose(cause)
				}
				h.notifyLost(cause)
				return
			}
			if h.lease.lose(cause) {
				h.log.Warn("heartbeat rejected; cancelling the handler", "err", err)
			}
			_, cause = h.lease.state()
			h.notifyLost(cause)
			return
		}

		if !h.lease.extend(deadlineFrom(sent, h.grant), time.Now()) {
			_, cause := h.lease.state()
			if cause == nil {
				// A response too late to use. The older deadline still stands,
				// so try again rather than giving up on a live lease.
				continue
			}
			h.log.Warn("lease expired while applying a renewal; cancelling the handler",
				"err", cause)
			h.notifyLost(cause)
			return
		}
	}
}

// classifyHeartbeat maps a Heartbeat failure onto what the SDK should do.
//
// ABORTED and NOT_FOUND are definitive: the token is stale or the row is gone.
// FAILED_PRECONDITION under this RPC means the job was cancelled. UNAVAILABLE
// and DEADLINE_EXCEEDED describe the network, not the lease, so they are worth
// retrying while the lease is locally live. Everything else — INTERNAL above
// all — stops the work: a generic store failure may be a permanent rejection or
// an ambiguous commit, and retrying blindly is how a worker keeps hold of a job
// it has already lost.
func classifyHeartbeat(err error) (cause error, retry bool) {
	switch status.Code(err) {
	case codes.Aborted, codes.NotFound:
		return fmt.Errorf("%w: %w", ErrLeaseLost, err), false
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: %w", ErrJobCancelled, err), false
	case codes.Unavailable, codes.DeadlineExceeded:
		return err, true
	default:
		return fmt.Errorf("worker: heartbeat rejected: %w", err), false
	}
}

func earlier(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// wait sleeps for d, stopping early at the budget or at cancellation. It
// reports whether there is any point carrying on.
func wait(ctx context.Context, d time.Duration, budget time.Time) bool {
	if left := time.Until(budget); d > left {
		d = left
	}
	if d <= 0 {
		return false
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

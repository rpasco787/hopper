package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// errDeliveryDone releases a handler context once its delivery is over. It is
// never reported: by the time it is used the handler has already returned.
var errDeliveryDone = errors.New("worker: delivery finished")

// execute owns one delivery from dispatch to confirmation. It runs on the
// delivery's own goroutine, holding that delivery's capacity token for the
// whole of it — validation, execution and confirmation alike.
func (w *Worker) execute(lc *lifecycle, wk *work, rpc hopperv1.HopperServiceClient, d *delivery) {
	log := w.log.With(
		"job_id", d.job.ID, "lease_id", d.leaseID, "queue", d.job.Queue, "attempt", d.job.Attempt)

	// The handler's context descends from the force context, so it keeps the
	// caller's values, survives the caller's own cancellation, and still ends
	// at the shutdown deadline. Its cause is how a handler learns which of the
	// four reasons stopped it.
	handlerCtx, cancelHandler := context.WithCancelCause(lc.forceCtx)
	defer cancelHandler(errDeliveryDone)

	hb := newHeartbeat(lc.forceCtx, rpc, w.log, d, cancelHandler)
	// Idempotent, and needed on every path: an acquisition that failed still
	// left a context hanging off the force context, and one per delivery adds up.
	defer hb.stop()

	if err := hb.acquire(d.acquisitionBudget()); err != nil {
		// Nothing was started and nothing is safe to mutate: without proven
		// ownership a Nack would be a stale worker writing to someone else's
		// job. The lease simply expires and the broker redelivers it.
		log.Warn("lease not confirmed; leaving the job to be redelivered", "err", err)
		return
	}

	c := &confirmer{
		rpc: rpc, log: log, forceCtx: lc.forceCtx,
		lease: &hb.lease, jobID: d.job.ID, leaseID: d.leaseID,
	}

	if d.handler == nil {
		nackUnstarted(wk, c, log, fmt.Sprintf("no handler is registered for queue %q", d.job.Queue))
		return
	}
	// Acceptance is the linearization point against shutdown. Winning it means
	// this handler runs and the drain waits for it; losing it means user code
	// never starts and the job goes back now rather than after a lease expiry.
	if !wk.acceptHandler() {
		nackUnstarted(wk, c, log, "the worker was shutting down; this job was never started")
		return
	}

	hb.start()
	log.Info("job started")
	err := invoke(handlerCtx, d)

	// Stop renewal before confirming. This cancels the heartbeat RPC in flight,
	// which is our own doing rather than a rejection, so the lease stays live
	// and the confirmation below runs under the last deadline it won.
	hb.stop()
	var message string
	var loggedErr any
	if err != nil {
		message = sanitizeError(err.Error())
		loggedErr = message
	}
	log.Info("handler finished", "err", loggedErr, "duration_ms", time.Since(d.receivedAt).Milliseconds())
	wk.confirming()

	if lc.forceCtx.Err() != nil {
		// A handler that outlived the shutdown deadline. Its job was already
		// counted as abandoned and its lease will expire; confirming now would
		// be writing after the worker promised it had stopped.
		log.Warn("handler returned after the shutdown deadline; not confirming",
			"cause", context.Cause(lc.forceCtx))
		return
	}

	if err == nil {
		outcome, cerr := c.ack()
		logConfirmation(log, "ack", outcome, cerr)
		return
	}
	outcome, cerr := c.nack(message)
	logConfirmation(log, "nack", outcome, cerr)
}

// nackUnstarted returns a job the SDK knows never started: an unregistered
// queue, or a delivery that lost the race with shutdown.
//
// This is not the same as a job the SDK is merely unsure about. The attempt has
// already been counted at lease time, so this Nack consumes it and applies the
// store's retry policy: on the final attempt the job becomes dead without any
// handler ever having run. That is the honest trade for returning it in
// milliseconds rather than after a whole lease expires.
func nackUnstarted(wk *work, c *confirmer, log *slog.Logger, reason string) {
	wk.confirming()
	log.Info("nacking an unstarted job", "reason", reason)
	outcome, err := c.nack(reason)
	logConfirmation(log, "nack", outcome, err)
}

// invoke runs the handler and turns a panic into an ordinary error, so one bad
// payload fails its own job instead of taking the worker down with it.
//
// The boundary is this goroutine only. A panic in a goroutine the handler
// started itself unwinds to the runtime and kills the process, as it does
// anywhere else in Go; recover cannot reach across goroutines.
func invoke(ctx context.Context, d *delivery) (err error) {
	defer func() {
		if p := recover(); p != nil {
			// The stack is bounded when it becomes a Nack message, which is
			// where an unbounded one would otherwise reach the database.
			err = fmt.Errorf("worker: handler panicked: %v\n%s", p, debug.Stack())
		}
	}()
	return d.handler(ctx, d.job)
}

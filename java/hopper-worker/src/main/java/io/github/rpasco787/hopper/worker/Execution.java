package io.github.rpasco787.hopper.worker;

import io.grpc.Channel;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * The default {@link DeliveryRunner}: one delivery from dispatch to confirmation, on the
 * delivery's own thread, holding that delivery's token for the whole of it (worker/execute.go).
 */
final class Execution implements DeliveryRunner {
    private static final WorkerObserver NO_OBSERVER = new WorkerObserver() {
        @Override
        public void handlerStarted(String queue) {}

        @Override
        public void handlerFinished(String queue, HandlerStatus status, Duration elapsed) {}
    };

    private final EventLog log;
    private final WorkerObserver observer;
    private final boolean checkEveryDelivery;
    private final TimeSource time;

    /** log already carries worker_id; observer may be null. */
    Execution(EventLog log, WorkerObserver observer, boolean checkEveryDelivery, TimeSource time) {
        this.log = log;
        this.observer = observer != null ? observer : NO_OBSERVER;
        this.checkEveryDelivery = checkEveryDelivery;
        this.time = time;
    }

    @Override
    public void run(Lifecycle lc, Work wk, Channel channel, Delivery d) {
        Job job = d.job();
        EventLog dlog = log.with("job_id", job.id(), "lease_id", d.leaseId(), "queue", job.queue(),
                "attempt", job.attempt());

        // The handler's context is stopped with the reason that ends its lease, and at the
        // shutdown deadline (the lifecycle stops every registered context).
        HandlerContext ctx = new HandlerContext(job);
        lc.register(ctx);
        Heartbeat hb = new Heartbeat(lc.forceContext(), channel, log, d, time, ended -> ctx.stop(ended.reason()));
        try {
            execute(lc, wk, channel, d, dlog, ctx, hb);
        } finally {
            // Idempotent, and needed on every path: an acquisition that failed still left a
            // context hanging off the root one, and one per delivery adds up.
            hb.stop();
            lc.unregister(ctx);
        }
    }

    private void execute(Lifecycle lc, Work wk, Channel channel, Delivery d, EventLog dlog, HandlerContext ctx,
                         Heartbeat hb) {
        Job job = d.job();
        OptionalLong fresh = startDeadline(d);
        if (fresh.isPresent()) {
            // The credit rule: the broker leased this row after the credit it was matched to was
            // sent, so the lease runs at least until then plus the grant. A cancellation since is
            // learned at the first renewal or the confirmation (docs/semantics.md §10).
            hb.lease().establish(fresh.getAsLong());
        } else {
            Optional<Lease.Ended> err = hb.acquire(d.acquisitionBudget());
            if (err.isPresent()) {
                // Nothing was started and nothing is safe to mutate: without proven ownership a
                // Nack would be a stale worker writing to someone else's job. The lease simply
                // expires and the broker redelivers it.
                dlog.log(EventLog.Level.WARN, LogMessages.LEASE_NOT_CONFIRMED, "err", err.get().detail());
                return;
            }
        }

        Confirmer c = new Confirmer(channel, dlog, lc, hb.lease(), d, time);
        if (d.handler() == null) {
            nackUnstarted(wk, c, dlog, "no handler is registered for queue " + quote(job.queue()));
            return;
        }
        // Acceptance is the linearization point against shutdown. Winning it means this handler
        // runs and the drain waits for it; losing it means user code never starts and the job
        // goes back now rather than after a lease expiry.
        if (!wk.acceptHandler()) {
            nackUnstarted(wk, c, dlog, "the worker was shutting down; this job was never started");
            return;
        }

        hb.start();
        observer.handlerStarted(job.queue());
        dlog.log(EventLog.Level.INFO, LogMessages.JOB_STARTED);
        long started = time.nanoTime();
        ctx.bind(Thread.currentThread());
        Throwable failure = invoke(d.handler(), ctx);
        ctx.finished(); // no interrupt reaches this thread after this: it confirms next
        long handlerNanos = time.nanoTime() - started;
        observer.handlerFinished(job.queue(), status(failure), Duration.ofNanos(handlerNanos));

        // Stop renewal before confirming. This cancels the heartbeat RPC in flight, which is our
        // own doing rather than a rejection, so the lease stays live and the confirmation runs
        // under the last deadline it won.
        hb.stop();
        String message = failure == null ? null : ErrorText.sanitize(text(failure));
        dlog.log(EventLog.Level.INFO, LogMessages.HANDLER_FINISHED, "err", message,
                "handler_ms", Duration.ofNanos(handlerNanos).toMillis(),
                "duration_ms", Duration.ofNanos(time.nanoTime() - d.receivedAt()).toMillis());
        wk.confirming();

        if (lc.forced()) {
            // A handler that outlived the shutdown deadline. Its job was already counted as
            // abandoned and its lease will expire; confirming now would be writing after the
            // worker promised it had stopped.
            Throwable cause = lc.forceContext().cancellationCause();
            dlog.log(EventLog.Level.WARN, LogMessages.RETURNED_AFTER_DEADLINE,
                    "cause", cause == null ? StopReason.SHUTTING_DOWN.text() : cause.getMessage());
            return;
        }
        if (failure == null) {
            Confirmer.logConfirmation(dlog, "ack", c.ack());
        } else {
            Confirmer.logConfirmation(dlog, "nack", c.nack(message));
        }
    }

    /** The start rule, unless a test has pinned the ownership check. Empty: take the check. */
    private OptionalLong startDeadline(Delivery d) {
        if (checkEveryDelivery) {
            return OptionalLong.empty();
        }
        return StartRule.freshDeadline(d.creditSent(), d.creditSentAt(), d.receivedAt(), d.grantNanos());
    }

    /**
     * Returns a job the SDK knows never started: an unregistered queue, or a delivery that lost
     * the race with shutdown. The attempt was counted at lease time, so this Nack consumes it
     * and applies the store's retry policy; that is the honest trade for returning it in
     * milliseconds rather than after a whole lease.
     */
    private static void nackUnstarted(Work wk, Confirmer c, EventLog dlog, String reason) {
        wk.confirming();
        dlog.log(EventLog.Level.INFO, LogMessages.NACKING_UNSTARTED, "reason", reason);
        Confirmer.logConfirmation(dlog, "nack", c.nack(reason));
    }

    /**
     * Runs the handler and catches whatever it throws (D22): an Exception fails its own job, and
     * so does an Error, so one bad payload cannot take the worker down. The boundary is this
     * thread only, as Go's recover is its goroutine.
     */
    private static Throwable invoke(JobHandler handler, HandlerContext ctx) {
        try {
            handler.handle(ctx);
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    private static HandlerStatus status(Throwable t) {
        if (t == null) {
            return HandlerStatus.OK;
        }
        return t instanceof Error ? HandlerStatus.PANIC : HandlerStatus.ERROR;
    }

    /** An Exception's toString(); an Error's toString() and its stack, which sanitize bounds. */
    private static String text(Throwable t) {
        if (!(t instanceof Error)) {
            return t.toString();
        }
        StringWriter stack = new StringWriter();
        t.printStackTrace(new PrintWriter(stack));
        return stack.toString();
    }

    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}

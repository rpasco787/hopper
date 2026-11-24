package io.github.rpasco787.hopper.worker;

import io.grpc.Context;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Owns shutdown (worker/lifecycle.go). There is exactly one deadline: the first transition to
 * draining records now + timeout, and opening a stream, sending on it, half-closing it, waiting
 * for handlers and retrying confirmations all share that instant. Two sequential budgets would
 * let a worker take twice as long as it was configured to, which is how a pod gets killed
 * mid-handler.
 *
 * <p>It also owns the counts, kept apart because they call for different responses (see
 * {@link AbandonedCounts}). Everything is under one lock.
 */
final class Lifecycle {
    /** A consistent snapshot of what is in flight. */
    record Counts(int deliveries, int acquiring, int handlers, int confirming) {}

    private final Duration timeout;
    private final TimeSource time;
    private final EventLog log;

    /** The root of every RPC; its cancellation is the forced-shutdown signal. */
    private final Context.CancellableContext force = Context.ROOT.withCancellation();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final List<Runnable> drainListeners = new ArrayList<>();
    private final Set<HandlerContext> handlerContexts = new LinkedHashSet<>();
    private boolean draining;
    private long deadline;
    private int active;
    private int acquiring;
    private int handlers;
    private int confirming;
    private boolean intakeFinished;
    private boolean drained;
    private boolean stopped;
    private TimeSource.Timer deadlineTimer;
    // deadlineFired records that the budget ran out, and deadlineSnap what was still in flight
    // at that instant. The verdict has to be this snapshot rather than a later reading: an
    // abandoned delivery decrements the counters on its way out, so a second look can show an
    // empty, apparently clean worker that has in fact just dropped a job.
    private boolean deadlineFired;
    private Counts deadlineSnap;

    Lifecycle(Duration timeout, TimeSource time, EventLog log) {
        this.timeout = timeout;
        this.time = time;
        this.log = log;
    }

    /** The root of every RPC the worker makes. Cancelled at the deadline and when run() ends. */
    Context.CancellableContext forceContext() {
        return force;
    }

    /**
     * Starts the drain; safe from anywhere, any number of times. Only the first call has an
     * effect: a later caller must not be able to extend a shutdown already running out of time.
     */
    void beginDrain(String reason) {
        Counts snap;
        long dl;
        List<Runnable> listeners;
        lock.lock();
        try {
            if (draining) {
                return;
            }
            draining = true;
            dl = time.nanoTime() + timeout.toNanos();
            deadline = dl;
            snap = countsLocked();
            listeners = List.copyOf(drainListeners);
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        Instant wallDeadline = Instant.now().plus(timeout);
        // The drain signal first, as Go closes drainSignal before anything else: the writer
        // half-closes on it, and a slow log line must not delay that.
        listeners.forEach(Runnable::run);
        // Enforcement is armed from the absolute deadline before logging. A slow or blocked
        // handler must not be able to extend the shutdown budget merely by delaying this line.
        TimeSource.Timer t = time.schedule(this::enforceDeadline, dl - time.nanoTime());
        lock.lock();
        try {
            if (stopped) {
                t.cancel();
            } else {
                deadlineTimer = t;
            }
        } finally {
            lock.unlock();
        }
        // test/accounting reads this line to prove the signal landed while a handler was
        // running, so the counts are the synchronized snapshot above, not a second look.
        log.log(EventLog.Level.INFO, LogMessages.SHUTDOWN_STARTED,
                "reason", reason,
                "deadline", DateTimeFormatter.ISO_INSTANT.format(wallDeadline),
                "timeout", GoDuration.format(timeout),
                "active_deliveries", snap.deliveries(),
                "running_handlers", snap.handlers());
    }

    /**
     * What makes the budget real. It cannot wait for the drain to finish, because the drain
     * itself may be blocked on a broker that never answered a half-close; it cancels the
     * transport out from under it. One snapshot serves both the log line and await's verdict.
     */
    private void enforceDeadline() {
        Counts snap;
        List<HandlerContext> running;
        lock.lock();
        try {
            if (stopped || deadlineFired) {
                return;
            }
            deadlineSnap = countsLocked();
            deadlineFired = true;
            snap = deadlineSnap;
            running = List.copyOf(handlerContexts);
        } finally {
            lock.unlock();
        }
        for (HandlerContext c : running) {
            c.stop(StopReason.SHUTTING_DOWN);
        }
        cancelForce();
        // Go logs "counts", snap, which encoding/json renders {} (unexported fields). Java
        // names the four counts.
        log.log(EventLog.Level.WARN, LogMessages.SHUTDOWN_DEADLINE_REACHED,
                "active_deliveries", snap.deliveries(),
                "running_handlers", snap.handlers(),
                "acquiring", snap.acquiring(),
                "confirming", snap.confirming());
    }

    private void cancelForce() {
        force.cancel(new IllegalStateException(StopReason.SHUTTING_DOWN.text()));
        lock.lock();
        try {
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Runs listener once when the drain begins; at once if it already has. */
    void onDrain(Runnable listener) {
        boolean now;
        lock.lock();
        try {
            now = draining;
            if (!now) {
                drainListeners.add(listener);
            }
        } finally {
            lock.unlock();
        }
        if (now) {
            listener.run();
        }
    }

    /** The deadline stops a registered handler with SHUTTING_DOWN; one registered after it, at once. */
    void register(HandlerContext ctx) {
        boolean late;
        lock.lock();
        try {
            late = deadlineFired || stopped;
            if (!late) {
                handlerContexts.add(ctx);
            }
        } finally {
            lock.unlock();
        }
        if (late) {
            ctx.stop(StopReason.SHUTTING_DOWN);
        }
    }

    void unregister(HandlerContext ctx) {
        lock.lock();
        try {
            handlerContexts.remove(ctx);
        } finally {
            lock.unlock();
        }
    }

    boolean isDraining() {
        lock.lock();
        try {
            return draining;
        } finally {
            lock.unlock();
        }
    }

    /** The root context was cancelled: by the deadline, or because run() ended. */
    boolean forced() {
        return force.isCancelled();
    }

    /** The shared deadline, on the TimeSource, once the drain has begun. */
    OptionalLong shutdownDeadline() {
        lock.lock();
        try {
            return draining ? OptionalLong.of(deadline) : OptionalLong.empty();
        } finally {
            lock.unlock();
        }
    }

    /** Whether the drain has completed: intake finished and nothing active. For tests. */
    boolean isDrained() {
        lock.lock();
        try {
            return drained;
        } finally {
            lock.unlock();
        }
    }

    Counts counts() {
        lock.lock();
        try {
            return countsLocked();
        } finally {
            lock.unlock();
        }
    }

    private Counts countsLocked() {
        return new Counts(active, acquiring, handlers, confirming);
    }

    /** Registers a new active delivery, in its acquisition phase. */
    Work begin() {
        lock.lock();
        try {
            active++;
            acquiring++;
        } finally {
            lock.unlock();
        }
        return new Work(this);
    }

    /**
     * No delivery begins after this. The session calls it only after the stream's reader has
     * stopped: before that a half-closing stream may still yield buffered deliveries.
     */
    void finishIntake() {
        lock.lock();
        try {
            if (intakeFinished) {
                return;
            }
            intakeFinished = true;
            if (active == 0) {
                drained = true;
                changed.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Begins the drain (a no-op if it has begun), finishes intake, and blocks until everything
     * has drained or the root context is cancelled. Returns what the deadline abandoned.
     *
     * <p>Either way can win after the budget has run out: abandoning the last delivery zeroes the
     * counters and completes the drain in the same decrement, so "drained" does not prove the
     * drain was clean. Only the snapshot taken when the deadline fired tells a worker that
     * finished from one that gave up. The wait is uninterruptible, bounded by the deadline; an
     * interrupt is remembered and restored before returning.
     */
    Optional<AbandonedCounts> await() {
        beginDrain("run returned");
        finishIntake();
        boolean interrupted = false;
        lock.lock();
        try {
            while (!drained && !force.isCancelled()) {
                try {
                    changed.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (!deadlineFired || deadlineSnap.deliveries() == 0) {
                return Optional.empty();
            }
            Counts s = deadlineSnap;
            return Optional.of(new AbandonedCounts(s.deliveries(), s.handlers(), s.acquiring(), s.confirming()));
        } finally {
            lock.unlock();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Releases the lifecycle: cancels anything still attached to the root context, the last line
     * of defence against an SDK thread outliving run().
     */
    void stop() {
        TimeSource.Timer t;
        lock.lock();
        try {
            if (stopped) {
                return;
            }
            stopped = true;
            t = deadlineTimer;
        } finally {
            lock.unlock();
        }
        if (t != null) {
            t.cancel();
        }
        cancelForce();
    }

    // --- Work's transitions, each under the lifecycle's lock ---

    /** acceptHandler's half: refused once draining. */
    boolean accept() {
        lock.lock();
        try {
            if (draining) {
                return false;
            }
            acquiring--;
            handlers++;
            return true;
        } finally {
            lock.unlock();
        }
    }

    void move(Work.Phase from, Work.Phase to) {
        lock.lock();
        try {
            count(from, -1);
            count(to, +1);
        } finally {
            lock.unlock();
        }
    }

    void finish(Work.Phase from) {
        lock.lock();
        try {
            count(from, -1);
            active--;
            if (draining && intakeFinished && active == 0) {
                drained = true;
                changed.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    private void count(Work.Phase p, int delta) {
        switch (p) {
            case ACQUIRING -> acquiring += delta;
            case RUNNING -> handlers += delta;
            case CONFIRMING -> confirming += delta;
            case NONE -> { }
        }
    }
}

package io.github.rpasco787.hopper.worker;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The production clock: System.nanoTime(), and one daemon scheduler thread named
 * hopper-scheduler. A task that throws is logged at ERROR and the scheduler carries on.
 */
final class SystemTimeSource implements TimeSource, AutoCloseable {
    /** Not a contract message: a timer task should never throw. */
    static final String TASK_FAILED = "worker: a scheduled task threw; this is an SDK bug";

    private final EventLog log;
    private final ScheduledThreadPoolExecutor scheduler;

    SystemTimeSource(EventLog log) {
        this.log = log;
        this.scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "hopper-scheduler");
            t.setDaemon(true);
            return t;
        });
        scheduler.setRemoveOnCancelPolicy(true);
    }

    @Override
    public long nanoTime() {
        return System.nanoTime();
    }

    @Override
    public TimeSource.Timer schedule(Runnable task, long delayNanos) {
        Runnable guarded = () -> {
            try {
                task.run();
            } catch (Throwable t) {
                log.log(EventLog.Level.ERROR, TASK_FAILED, "err", t);
            }
        };
        try {
            ScheduledFuture<?> f = scheduler.schedule(guarded, Math.max(delayNanos, 0), TimeUnit.NANOSECONDS);
            return () -> f.cancel(false);
        } catch (RejectedExecutionException closed) {
            // The worker has finished: nothing it schedules now has anything left to do.
            return () -> {};
        }
    }

    /** Stops the scheduler thread; pending tasks never run. */
    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}

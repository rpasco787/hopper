package io.github.rpasco787.hopper.worker;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The SDK's side of a {@link JobContext} (Decision D21). The thread is interrupted only while
 * the handler runs, under this lock; when the handler returns, {@link #finished()} marks it not
 * running under the same lock and clears the flag, because the thread goes on to confirm, and a
 * stale interrupt would cancel the confirmation's blocking call.
 */
final class HandlerContext implements JobContext {
    private final Job job;
    private final ReentrantLock lock = new ReentrantLock();
    private StopReason reason;
    private Thread thread;
    private boolean running;

    HandlerContext(Job job) {
        this.job = job;
    }

    /**
     * Marks the handler running on thread, which must be the caller. A context already stopped
     * interrupts it at once, as a Go handler handed a cancelled context sees Done at once.
     */
    void bind(Thread thread) {
        lock.lock();
        try {
            this.thread = thread;
            this.running = true;
            if (reason != null) {
                thread.interrupt();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sets the reason if none is set (the first one wins), then, if this call set it and the
     * handler is running, interrupts its thread. Returns whether this call set the reason.
     */
    boolean stop(StopReason why) {
        lock.lock();
        try {
            if (reason != null) {
                return false;
            }
            reason = why;
            if (running) {
                thread.interrupt();
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Called by the bound thread when the handler returns: no interrupt reaches it after this. */
    void finished() {
        lock.lock();
        try {
            if (Thread.currentThread() != thread) {
                throw new IllegalStateException("finished() called from " + Thread.currentThread()
                        + ", not the handler's thread " + thread);
            }
            running = false;
            Thread.interrupted(); // clear a stale flag: the thread confirms next
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Job job() {
        return job;
    }

    @Override
    public boolean isStopped() {
        lock.lock();
        try {
            return reason != null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<StopReason> stopReason() {
        lock.lock();
        try {
            return Optional.ofNullable(reason);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void throwIfStopped() throws JobStoppedException {
        Optional<StopReason> r = stopReason();
        if (r.isPresent()) {
            throw new JobStoppedException(r.get());
        }
    }
}

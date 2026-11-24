package io.github.rpasco787.hopper.worker;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * The worker's fixed pool of concurrency tokens (worker/stream.go capacity). At any moment
 * every token is in exactly one place:
 *
 * <pre>free tokens + credit outstanding on the stream + active deliveries = concurrency</pre>
 *
 * A token leaves the pool when the credit it stands for is sent, and the delivery that credit
 * buys inherits it. It covers the start, the handler and the confirmation, including a handler
 * that ignored its stop: letting a new handler start in its place is exactly how a worker ends
 * up running more jobs than it said it would.
 */
final class Capacity {
    static final String OVER_RELEASED = "worker: released more capacity than exists; this is an SDK bug";

    private final int size;
    private final EventLog log;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private int free;

    Capacity(int size, EventLog log) {
        this.size = size;
        this.log = log;
        this.free = size;
    }

    /** Claims every free token without blocking. */
    int takeAll() {
        lock.lock();
        try {
            int n = free;
            free = 0;
            return n;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Blocks until a token is free or stop is true, re-checking stop on each {@link #wake()}.
     * Returns every free token (at least one), or 0 if stopped; a stop already set wins over a
     * free token, which stays in the pool.
     */
    int takeOneOrWake(BooleanSupplier stop) throws InterruptedException {
        lock.lock();
        try {
            while (true) {
                if (stop.getAsBoolean()) {
                    return 0;
                }
                if (free > 0) {
                    int n = free;
                    free = 0;
                    return n;
                }
                changed.await();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns one token. Releasing more than were taken is an SDK accounting bug, not anything a
     * deployment can cause, so it is logged loudly and dropped: raising the ceiling instead would
     * silently breach the concurrency the caller configured.
     */
    void release() {
        boolean over;
        lock.lock();
        try {
            over = free >= size;
            if (!over) {
                free++;
                changed.signalAll();
            }
        } finally {
            lock.unlock();
        }
        if (over) {
            log.log(EventLog.Level.ERROR, OVER_RELEASED, "concurrency", size);
        }
    }

    /** Wakes every waiter to re-check its stop condition. */
    void wake() {
        lock.lock();
        try {
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Free tokens now; for tests. */
    int free() {
        lock.lock();
        try {
            return free;
        } finally {
            lock.unlock();
        }
    }
}

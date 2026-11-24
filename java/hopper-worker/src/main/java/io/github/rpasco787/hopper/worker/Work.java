package io.github.rpasco787.hopper.worker;

import java.util.concurrent.locks.ReentrantLock;

/**
 * One delivery's claim on the lifecycle (worker/lifecycle.go work). It exists so a delivery that
 * ends early (malformed, refused, abandoned) still decrements exactly the counters it
 * incremented, exactly once. Every phase is active delivery work: a delivery holds its token
 * from the moment it is read off the stream until its confirmation ends.
 */
final class Work {
    enum Phase { NONE, ACQUIRING, RUNNING, CONFIRMING }

    private final Lifecycle lc;
    private final ReentrantLock lock = new ReentrantLock();
    private Phase cur = Phase.ACQUIRING;

    Work(Lifecycle lc) {
        this.lc = lc;
    }

    /**
     * The linearization point between shutdown and user code: one atomic decision under the
     * lifecycle's lock, the one beginDrain takes. Winning it means this handler runs and the
     * drain waits for it; losing it means user code never starts and the job is nacked unstarted.
     */
    boolean acceptHandler() {
        lock.lock();
        try {
            if (cur != Phase.ACQUIRING || !lc.accept()) {
                return false;
            }
            cur = Phase.RUNNING;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Moves into the confirmation phase; handler and confirmation time are counted apart. */
    void confirming() {
        lock.lock();
        try {
            if (cur == Phase.NONE || cur == Phase.CONFIRMING) {
                return;
            }
            lc.move(cur, Phase.CONFIRMING);
            cur = Phase.CONFIRMING;
        } finally {
            lock.unlock();
        }
    }

    /** Finishes the delivery. Idempotent: it is finished once, however many paths lead here. */
    void end() {
        lock.lock();
        try {
            if (cur == Phase.NONE) {
                return;
            }
            lc.finish(cur);
            cur = Phase.NONE;
        } finally {
            lock.unlock();
        }
    }
}

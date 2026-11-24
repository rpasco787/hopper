package io.github.rpasco787.hopper.worker;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The SDK's synchronized view of one delivery's ownership (worker/heartbeat.go lease). The
 * deadline is what the SDK believes it may still safely work until, on its own monotonic clock;
 * the store's (id, lease_id) guard is what actually decides. Ended is terminal, forever: no late
 * response may revive a delivery the SDK has stopped working on. Instants compare only by
 * subtraction. A lease never established has a deadline in the past, as Go's zero time is.
 */
final class Lease {
    record Ended(StopReason reason, String detail) {}

    record State(long deadline, Optional<Ended> ended) {}

    private final ReentrantLock lock = new ReentrantLock();
    private boolean established;
    private long deadline;
    private Ended ended;

    /** Records the deadline won by a start (the start rule or the check). Ignored once ended. */
    void establish(long deadline) {
        lock.lock();
        try {
            if (ended == null) {
                this.deadline = deadline;
                this.established = true;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Moves the deadline forward after a successful renewal. Refuses a lease already ended, a
     * lease whose deadline has passed (which it ends LEASE_EXPIRED: a renewal can't revive what
     * the SDK stopped trusting), and a response whose own deadline is already spent (which leaves
     * the older, live deadline in place). live() tells the cases apart.
     */
    boolean extend(long newDeadline, long now) {
        lock.lock();
        try {
            if (ended != null) {
                return false;
            }
            if (!before(now, deadline)) {
                ended = new Ended(StopReason.LEASE_EXPIRED, StopReason.LEASE_EXPIRED.text());
                return false;
            }
            if (now - newDeadline >= 0) {
                return false;
            }
            deadline = newDeadline;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Ends the lease LEASE_EXPIRED if the current deadline has passed: a stale timer can't. */
    boolean expireIfPast(long now) {
        lock.lock();
        try {
            if (ended != null || before(now, deadline)) {
                return false;
            }
            ended = new Ended(StopReason.LEASE_EXPIRED, StopReason.LEASE_EXPIRED.text());
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Ends the lease for a reason the broker gave. Returns whether this call ended it. */
    boolean lose(StopReason reason, String detail) {
        lock.lock();
        try {
            if (ended != null) {
                return false;
            }
            ended = new Ended(reason, detail);
            return true;
        } finally {
            lock.unlock();
        }
    }

    State state() {
        lock.lock();
        try {
            return new State(deadline, Optional.ofNullable(ended));
        } finally {
            lock.unlock();
        }
    }

    boolean live() {
        lock.lock();
        try {
            return ended == null;
        } finally {
            lock.unlock();
        }
    }

    /** now is strictly before the deadline; never true before a deadline was established. */
    private boolean before(long now, long d) {
        return established && now - d < 0;
    }
}

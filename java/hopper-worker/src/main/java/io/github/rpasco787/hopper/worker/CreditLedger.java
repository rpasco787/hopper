package io.github.rpasco787.hopper.worker;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One lease stream's credit: what the broker may still spend, and, oldest first, when each
 * unit was handed to the transport (the start rule's Stamp and Match). It always holds
 * exactly `outstanding` units. A new stream gets a new ledger.
 */
final class CreditLedger {
    /** The unit a delivery was matched to. sent is false for one reserved on the drain path. */
    record Match(boolean sent, long sentAtNanos) {}

    private static final class Units {
        final boolean sent;
        final long at;
        int left;

        Units(boolean sent, long at, int left) {
            this.sent = sent;
            this.at = at;
            this.left = left;
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Deque<Units> units = new ArrayDeque<>();
    private int outstanding;
    private int delivered;
    private boolean reconciled;

    /**
     * Stamps n units under the lock that counts them, releases it, then runs send, which hands
     * the message carrying them to the transport. The instant is read before the send, and the
     * units are counted before it, so a delivery that answers the send before it returns finds
     * them (the Go SDK's "reserve before sending", stream.go:306-310). The lock is not held
     * across send: a send that blocks must not stop the reader from matching. A send that throws
     * keeps its units, which reconcile returns exactly once. Only the session's one writer, and
     * the subscription before the writer starts, call this.
     */
    void reserveAndSend(int n, TimeSource time, Runnable send) {
        if (n > 0) {
            lock.lock();
            try {
                units.addLast(new Units(true, time.nanoTime(), n));
                outstanding += n;
            } finally {
                lock.unlock();
            }
        }
        send.run();
    }

    /** Units the writer took on the drain path and will never send: they carry no instant. */
    void reserveUnsent(int n) {
        if (n == 0) {
            return;
        }
        lock.lock();
        try {
            units.addLast(new Units(false, 0, n));
            outstanding += n;
        } finally {
            lock.unlock();
        }
    }

    /** Matches one arriving delivery to the oldest unit not yet matched. Empty is an over-delivery. */
    Optional<Match> spend() {
        lock.lock();
        try {
            if (outstanding == 0) {
                return Optional.empty();
            }
            outstanding--;
            delivered++;
            Units head = units.peekFirst();
            Match m = new Match(head.sent, head.at);
            if (--head.left == 0) {
                units.removeFirst();
            }
            return Optional.of(m);
        } finally {
            lock.unlock();
        }
    }

    int deliveredCount() {
        lock.lock();
        try {
            return delivered;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the credit this stream will never spend, exactly once. It runs only after the
     * reader and the writer have both stopped: before that a delivery could still arrive against
     * a send whose outcome is unknown, and refunding early would let the worker run more
     * handlers than it has tokens for.
     */
    void reconcile(Capacity capacity) {
        int n;
        lock.lock();
        try {
            if (reconciled) {
                return;
            }
            reconciled = true;
            n = outstanding;
            outstanding = 0;
            units.clear();
        } finally {
            lock.unlock();
        }
        for (int i = 0; i < n; i++) {
            capacity.release();
        }
    }
}

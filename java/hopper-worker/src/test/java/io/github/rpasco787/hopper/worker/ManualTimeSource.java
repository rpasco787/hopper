package io.github.rpasco787.hopper.worker;

import java.time.Duration;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A clock that moves only when a test moves it (Decision D19). advance runs due timers in
 * (due, then scheduling) order on the calling thread; jump moves the clock and runs nothing,
 * which is a pause. Safe to schedule on from any thread.
 */
final class ManualTimeSource implements TimeSource {
    private final class Entry implements TimeSource.Timer, Comparable<Entry> {
        final long due;
        final long seq;
        final Runnable task;
        boolean cancelled;

        Entry(long due, long seq, Runnable task) {
            this.due = due;
            this.seq = seq;
            this.task = task;
        }

        @Override
        public void cancel() {
            lock.lock();
            try {
                if (!cancelled) {
                    cancelled = true;
                    queue.remove(this);
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public int compareTo(Entry other) {
            long d = due - other.due; // instants compare by subtraction
            if (d != 0) {
                return d < 0 ? -1 : 1;
            }
            return Long.compare(seq, other.seq);
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final PriorityQueue<Entry> queue = new PriorityQueue<>();
    private long now;
    private long seq;

    ManualTimeSource(long start) {
        this.now = start;
    }

    @Override
    public long nanoTime() {
        lock.lock();
        try {
            return now;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public TimeSource.Timer schedule(Runnable task, long delayNanos) {
        lock.lock();
        try {
            Entry e = new Entry(now + Math.max(delayNanos, 0), seq++, task);
            queue.add(e);
            changed.signalAll();
            return e;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Moves the clock forward by d. Each due task runs, in (due, then scheduling) order, with
     * the clock set to its due time first; a task scheduled meanwhile also runs if due within d.
     * The clock ends at start + d. Tasks run on the calling thread, outside any lock.
     */
    void advance(Duration d) {
        long end;
        lock.lock();
        try {
            end = now + d.toNanos();
        } finally {
            lock.unlock();
        }
        while (true) {
            Entry next;
            lock.lock();
            try {
                Entry head = queue.peek();
                if (head == null || head.due - end > 0) {
                    now = end;
                    changed.signalAll();
                    return;
                }
                next = queue.poll();
                next.cancelled = true; // it has fired: a later cancel is a no-op
                if (next.due - now > 0) {
                    now = next.due;
                }
            } finally {
                lock.unlock();
            }
            next.task.run();
        }
    }

    /** Moves the clock forward by d and runs nothing: a pause, during which no timer fires. */
    void jump(Duration d) {
        lock.lock();
        try {
            now += d.toNanos();
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    int pending() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    /** Blocks until at least n timers are pending; AssertionError after timeout. */
    void awaitTimers(int n, Duration timeout) {
        long left = timeout.toNanos();
        lock.lock();
        try {
            while (queue.size() < n) {
                if (left <= 0) {
                    throw new AssertionError("waited " + timeout + " for " + n + " pending timers; have " + queue.size());
                }
                try {
                    left = changed.awaitNanos(left);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted waiting for timers", e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "ManualTimeSource[now=" + nanoTime() + "ns, pending=" + pending() + "]";
    }
}

package io.github.rpasco787.hopper.worker;

import io.grpc.Channel;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A delivery runner the test drives, the port of worker/stream_test.go's spy. With block set,
 * each delivery is held until the test releases them all, deliberately uncooperative: it ends
 * when the test says so, not when the SDK asks, which is what makes abandonment observable.
 * close() releases and joins every delivery it ever held.
 */
final class Spy implements AutoCloseable {
    private final LinkedBlockingQueue<Delivery> deliveries = new LinkedBlockingQueue<>();
    private final CountDownLatch hold;
    private final AtomicInteger dispatched = new AtomicInteger();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final Object joined = new Object();

    Spy(boolean block) {
        this.hold = block ? new CountDownLatch(1) : null;
    }

    void run(Lifecycle lc, Work work, Channel channel, Delivery d) {
        inFlight.incrementAndGet();
        try {
            dispatched.incrementAndGet();
            deliveries.add(d);
            if (hold == null) {
                return;
            }
            boolean interrupted = false;
            while (true) {
                try {
                    hold.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true; // ignored on purpose: an uncooperative delivery
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        } finally {
            if (inFlight.decrementAndGet() == 0) {
                synchronized (joined) {
                    joined.notifyAll();
                }
            }
        }
    }

    /** Lets every held delivery return. */
    void release() {
        if (hold != null) {
            hold.countDown();
        }
    }

    /** The next delivery that reached the runner. */
    Delivery await(Duration timeout) {
        try {
            Delivery d = deliveries.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
            if (d == null) {
                throw new AssertionError("no delivery reached the runner within " + timeout);
            }
            return d;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for a delivery", e);
        }
    }

    /** Deliveries that reached the runner and that no await() has taken yet (Go: len(sp.deliveries)). */
    int count() {
        return deliveries.size();
    }

    /** Every delivery that ever reached the runner. */
    int dispatched() {
        return dispatched.get();
    }

    @Override
    public void close() {
        release();
        long deadline = System.nanoTime() + Harness.BARRIER.toNanos();
        synchronized (joined) {
            while (inFlight.get() > 0) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    throw new AssertionError("a held delivery never returned");
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(joined, left);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted joining the spy", e);
                }
            }
        }
    }
}

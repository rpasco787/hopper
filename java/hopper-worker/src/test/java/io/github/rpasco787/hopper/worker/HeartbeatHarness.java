package io.github.rpasco787.hopper.worker;

import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.Fixture;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A heartbeat under test, with the delivery it owns and a queue of whatever it reported to the
 * handler: the port of worker/heartbeat_test.go's newHB. close() stops it and cancels its parent.
 */
final class HeartbeatHarness implements AutoCloseable {
    final Delivery d;
    final Heartbeat h;
    final LinkedBlockingQueue<Lease.Ended> lost = new LinkedBlockingQueue<>();
    final CapturingLog log = new CapturingLog();
    private final Context.CancellableContext parent = Context.ROOT.withCancellation();

    HeartbeatHarness(FakeBroker fake, Duration grant, TimeSource time, ClientInterceptor... interceptors)
            throws MalformedDeliveryException {
        this.d = Delivery.parse(Fixture.of("q").grant(grant).proto(), time.nanoTime());
        this.h = new Heartbeat(parent, fake.channel(interceptors), log, d, time, lost::add);
    }

    /** What the heartbeat told the handler; fails if nothing within the timeout. */
    Lease.Ended awaitLost(Duration timeout) {
        try {
            Lease.Ended e = lost.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
            if (e == null) {
                throw new AssertionError("the handler was never told the lease went away");
            }
            return e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", e);
        }
    }

    @Override
    public void close() {
        h.stop();
        parent.cancel(null);
    }
}

package io.github.rpasco787.hopper.worker;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import io.grpc.Status;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

/**
 * The start rule's threshold edges and fresh deadline through a real session (Java-only), on the
 * manual clock. The grant is 1 s, so the threshold is 100 ms and the margin 100 ms.
 */
class StartRuleSessionTest {
    private static final Duration BARRIER = Harness.BARRIER;
    private static final Duration GRANT = Duration.ofSeconds(1);

    /** Heartbeats counted when the handler starts, for a delivery read age after the subscription's send. */
    private static int beatsBeforeTheHandler(Duration age, boolean checkEveryDelivery) throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        try (var fake = FakeBroker.start()) {
            var beats = new CompletableFuture<Integer>();
            var b = Harness.builder(fake.channel()).timeSource(time).checkEveryDelivery(checkEveryDelivery);
            try (var run = Harness.runHandler(fake, b, "q", ctx -> beats.complete(fake.started("Heartbeat")))) {
                FakeStream s = fake.awaitStream(BARRIER);
                time.jump(age);
                s.deliver(Fixture.of("q").grant(GRANT).proto(), BARRIER);
                int n = beats.get(10, SECONDS);
                fake.awaitCalls("Ack", 1, BARRIER);
                assertNull(run.stopAndWait());
                return n;
            }
        }
    }

    @Test
    void aDeliveryJustUnderATenthAfterItsCreditStartsWithoutTheCheck() throws Exception {
        assertEquals(0, beatsBeforeTheHandler(Duration.ofMillis(100).minusNanos(1), false),
                "G/10 - 1 ns after the credit's send is fresh");
    }

    @Test
    void aDeliveryAtATenthAfterItsCreditTakesTheCheck() throws Exception {
        assertEquals(1, beatsBeforeTheHandler(Duration.ofMillis(100), false), "G/10 is not fresh: one check first");
    }

    /**
     * Heartbeats answer UNAVAILABLE, so nothing extends the lease. The subscription is stamped at
     * T and the delivery read 50 ms later, still fresh, so the deadline is T + 1 s − 100 ms
     * (deadlineFrom the credit's send), not 50 ms later (from receipt).
     */
    @Test
    void aFreshDeliverysDeadlineIsItsCreditsSendPlusTheGrantLessTheMargin() throws Exception {
        long t = 1_000_000_000L;
        var time = new ManualTimeSource(t);
        try (var fake = FakeBroker.start()) {
            fake.onHeartbeat((req, ctx) -> {
                throw Status.UNAVAILABLE.withDescription("database unavailable").asException();
            });
            var started = new CountDownLatch(1);
            var stopped = new CompletableFuture<StopReason>();
            JobHandler handler = ctx -> {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    stopped.complete(ctx.stopReason().orElseThrow());
                    throw e;
                }
            };
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).timeSource(time), "q", handler)) {
                FakeStream s = fake.awaitStream(BARRIER); // the subscription was stamped at t
                time.jump(Duration.ofMillis(50));
                s.deliver(Fixture.of("q").grant(GRANT).proto(), BARRIER);
                assertTrue(started.await(10, SECONDS));
                long handlerStart = t + Duration.ofMillis(50).toNanos();
                long interval = GRANT.toNanos() / 3;
                long deadline = t + GRANT.toNanos() - Duration.ofMillis(100).toNanos();

                // Two failed renewals, each re-armed a third of the grant after it returned.
                for (int k = 1; k <= 2; k++) {
                    time.awaitTimers(2, BARRIER);
                    time.advance(Duration.ofNanos(handlerStart + k * interval - time.nanoTime()));
                    fake.awaitCalls("Heartbeat", k, BARRIER);
                }
                time.awaitTimers(2, BARRIER);
                time.advance(Duration.ofNanos(deadline - 1 - time.nanoTime()));
                assertFalse(stopped.isDone(), "the handler still runs 1 ns before s + G - margin");
                time.advance(Duration.ofNanos(1));
                assertEquals(StopReason.LEASE_EXPIRED, stopped.get(10, SECONDS));
                assertEquals(deadline, time.nanoTime());
                assertNull(run.stopAndWait());
            }
            assertEquals(0, fake.started("Ack"), "an expired lease is not confirmed");
        }
    }

    @Test
    void checkEveryDeliveryPinsTheCheck() throws Exception {
        assertEquals(1, beatsBeforeTheHandler(Duration.ZERO, true), "with the seam on, a fresh delivery still checks");
    }

    @Test
    void checkEveryDeliveryIsNotPublic() throws Exception {
        Method m = HopperWorker.Builder.class.getDeclaredMethod("checkEveryDelivery", boolean.class);
        assertFalse(Modifier.isPublic(m.getModifiers()));
        assertFalse(Modifier.isProtected(m.getModifiers()));
        assertTrue(Arrays.stream(HopperWorker.Builder.class.getMethods()).noneMatch(x -> x.getName().equals("checkEveryDelivery")),
                "no public method of that name");
        assertThrows(NoSuchMethodException.class, () -> HopperWorker.Builder.class.getMethod("checkEveryDelivery", boolean.class));
    }
}

package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import io.grpc.Status;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Ports of worker/window_test.go. These pin the outage window docs/semantics.md §11.3
 * documents: how long a delivery survives its broker answering UNAVAILABLE. The rule is the
 * local deadline: the send time of the last successful ownership check or renewal, plus the
 * grant, less the safety margin. A failed call never moves it. With a 3s grant, renewals are 1s
 * apart and the deadline is 2.7s after the last success.
 */
class WindowTest {
    private static final Duration BARRIER = Harness.BARRIER;
    private static final Duration GRANT = Duration.ofSeconds(3);

    /**
     * Answers the first Heartbeat, the ownership check, and fails the second, the first renewal,
     * with UNAVAILABLE after taking slow to do so; on the manual clock the hook jumps the clock by
     * slow, which runs no timer. Every later Heartbeat succeeds at once. The returned value holds
     * the manual instant the check was answered.
     */
    static AtomicLong failRenewal(FakeBroker fake, ManualTimeSource time, Duration slow) {
        var calls = new AtomicInteger();
        var checked = new AtomicLong();
        fake.onHeartbeat((req, ctx) -> {
            switch (calls.incrementAndGet()) {
                case 1 -> checked.set(time.nanoTime());
                case 2 -> {
                    time.jump(slow);
                    throw Status.UNAVAILABLE.withDescription("database unavailable").asException();
                }
                default -> { }
            }
        });
        return checked;
    }

    /**
     * A renewal that fails at once costs one interval: the next one, a second later, is still
     * well before the deadline and keeps the lease.
     */
    @Test
    void aFastFailedRenewalCostsOneInterval() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        try (var fake = FakeBroker.start()) {
            failRenewal(fake, time, Duration.ZERO);
            try (var x = new HeartbeatHarness(fake, GRANT, time)) {
                assertEquals(Optional.empty(), x.h.acquire(x.d.acquisitionBudget()));
                x.h.start();

                // To 3.5 s, past the deadline the check set, stopping at each renewal.
                for (Duration step : List.of(Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1),
                        Duration.ofMillis(500))) {
                    time.awaitTimers(2, BARRIER);
                    time.advance(step);
                }
                fake.awaitCalls("Heartbeat", 4, BARRIER);
                time.awaitTimers(2, BARRIER);
                assertTrue(x.lost.isEmpty(), "a renewal that failed at once cost the lease: " + x.lost);
                assertTrue(x.h.lease().live());
                assertEquals(4, fake.started("Heartbeat"), "the check, the failed renewal, and the two that kept the lease");
            }
        }
    }

    /**
     * Renewals are an interval apart counted from when the previous one returned, so a failure
     * that takes 0.9s to arrive pushes the next renewal to 2.9s, past the 2.7s deadline. The
     * failure lasted less than a third of the grant, and the lease is lost anyway.
     */
    @Test
    void aSlowFailedRenewalCanSpendTheLease() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        try (var fake = FakeBroker.start()) {
            AtomicLong checked = failRenewal(fake, time, Duration.ofMillis(900));
            try (var x = new HeartbeatHarness(fake, GRANT, time)) {
                assertEquals(Optional.empty(), x.h.acquire(x.d.acquisitionBudget()));
                long deadline = x.h.lease().state().deadline();
                x.h.start();

                time.awaitTimers(2, BARRIER);
                time.advance(Duration.ofSeconds(1)); // the renewal, which returns 0.9 s later
                time.awaitTimers(2, BARRIER); // the expiry timer and the renewal due at 2.9 s
                time.advance(Duration.ofNanos(deadline - time.nanoTime() - 1));
                assertTrue(x.lost.isEmpty(), "not before the deadline");
                time.advance(Duration.ofNanos(1));
                assertEquals(StopReason.LEASE_EXPIRED, x.awaitLost(BARRIER).reason());
                assertEquals(deadline, time.nanoTime(), "lost at the deadline the check set");
                assertEquals(checked.get() + Duration.ofMillis(2700).toNanos(), deadline);

                time.advance(Duration.ofMillis(500)); // past the renewal that was due at 2.9 s
                x.h.stop(); // joins any renewal still running
                assertEquals(2, fake.started("Heartbeat"),
                        "the renewal that would have kept the lease was due after it ran out");
            }
        }
    }

    /**
     * Runs one delivery with a 3s grant whose first Heartbeat succeeds, whose later Heartbeats
     * fail with UNAVAILABLE, and whose handler returns 1.5s in, after the first of them failed.
     * Acks fail with UNAVAILABLE until ackOutage after that first Heartbeat. Returns the
     * worker's log messages, how many Acks succeeded, and when the first Heartbeat was answered.
     *
     * <p>Ported as Go has it: without checkEveryDelivery (Go's call at window_test.go:126 doesn't
     * set it), on the system clock with Go's tolerances (the outage is wall time in a hook, and
     * the worker retries through backoff timers). So the delivery is fresh, and the first
     * Heartbeat the hook answers is the first renewal at G/3, not an ownership check, though
     * Go's comments still call it "the check" (docs/plans §15).
     */
    private record Outcome(List<String> msgs, int acked, long checkedNanos) {}

    private static Outcome confirmAfterFailedRenewal(Duration ackOutage) throws Exception {
        var log = new CapturingLog();
        var beats = new AtomicInteger();
        var acks = new AtomicInteger();
        var checked = new AtomicLong();
        try (var fake = FakeBroker.start()) {
            fake.onHeartbeat((req, ctx) -> {
                if (beats.incrementAndGet() == 1) {
                    checked.set(System.nanoTime());
                    return;
                }
                throw Status.UNAVAILABLE.withDescription("database unavailable").asException();
            });
            fake.onAck((req, ctx) -> {
                if (System.nanoTime() - checked.get() < ackOutage.toNanos()) {
                    throw Status.UNAVAILABLE.withDescription("database unavailable").asException();
                }
                acks.incrementAndGet();
            });
            JobHandler handler = ctx -> Thread.sleep(1500);
            var w = HopperWorker.builder(fake.channel()).log(log).concurrency(1).shutdownTimeout(Duration.ofSeconds(10))
                    .handle("q", handler).build();
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").grant(GRANT).proto(), BARRIER);
                s.awaitCredit(1, BARRIER); // the delivery ends
                assertNull(run.stopAndWait());
            }
        }
        return new Outcome(log.messages(), acks.get(), checked.get());
    }

    /**
     * The Ack has only what is left of the lease, and a failed renewal left the deadline where
     * the first Heartbeat put it: 2.7s after it, 1.2s after the handler returned, not two thirds
     * of the grant. Acks keep failing until 2.9s, so the delivery is given up, though the handler
     * returned only 1.4s before the outage ended.
     */
    @Test
    @Disabled("Task 17")
    void aConfirmationHasOnlyWhatIsLeftOfTheLease() throws Exception {
        Outcome o = confirmAfterFailedRenewal(Duration.ofMillis(2900));
        long now = System.nanoTime();
        assertEquals(0, o.acked());
        assertTrue(o.msgs().contains(LogMessages.UNCONFIRMED_LEASE_RAN_OUT), o.msgs().toString());
        assertTrue(Math.abs(now - (o.checkedNanos() + Duration.ofMillis(2700).toNanos())) < TimeUnit.SECONDS.toNanos(1),
                "given up at the deadline the first Heartbeat set");
    }

    /** The same delivery, with an outage that ends at 1.7s, before the deadline: a retry lands after it and the job is confirmed. */
    @Test
    @Disabled("Task 17")
    void aLateConfirmationStillLandsBeforeTheDeadline() throws Exception {
        Outcome o = confirmAfterFailedRenewal(Duration.ofMillis(1700));
        assertEquals(1, o.acked());
        assertTrue(o.msgs().contains(LogMessages.JOB_CONFIRMED), o.msgs().toString());
        assertFalse(o.msgs().contains(LogMessages.UNCONFIRMED_LEASE_RAN_OUT));
    }
}

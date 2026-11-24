package io.github.rpasco787.hopper.worker;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.rpasco787.hopper.proto.v1.Job;
import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import io.grpc.Status;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

/**
 * Ports of worker/freshness_test.go, the start rule through a real worker
 * (docs/design/credit-anchored-freshness.md), on the manual clock.
 * freshDeadlineCountsFromTheCreditSend was ported in Task 10 (StartRuleTest).
 */
class FreshnessTest {
    private static final Duration BARRIER = Harness.BARRIER;

    /** In a drain the broker leases moments after the credit arrives, so the handler starts at once. */
    @Test
    void aFreshDeliveryStartsWithoutTheOwnershipCheck() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        try (var fake = FakeBroker.start()) {
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).timeSource(time), "q", ctx -> {})) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER); // G = 30 s: fresh for 3 s
                fake.awaitCalls("Ack", 1, BARRIER);
                assertNull(run.stopAndWait());
            }
            assertEquals(0, fake.started("Heartbeat"), "a delivery leased moments after its credit needs no check");
        }
    }

    /**
     * A delivery that arrives G/10 or more after its credit was sent proves nothing about now, so
     * the SDK asks the broker before its handler starts. Go sleeps 150 ms with a 1 s grant; here
     * the clock jumps 150 ms before the delivery.
     */
    @Test
    void aStaleDeliveryStillTakesTheCheck() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        try (var fake = FakeBroker.start()) {
            var beatsAtStart = new CompletableFuture<Integer>();
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).timeSource(time), "q",
                    ctx -> beatsAtStart.complete(fake.started("Heartbeat")))) {
                FakeStream s = fake.awaitStream(BARRIER);
                time.jump(Duration.ofMillis(150)); // the subscription's credit is now older than 100 ms
                s.deliver(Fixture.of("q").grant(Duration.ofSeconds(1)).proto(), BARRIER);

                assertEquals(1, beatsAtStart.get(10, SECONDS), "the ownership check ran before the handler");
                fake.awaitCalls("Ack", 1, BARRIER);
                assertNull(run.stopAndWait());
            }
        }
    }

    /**
     * Port of freshness_test.go TestACancelledJobThatStartedStopsAtItsFirstRenewal: a job
     * cancelled before a fresh delivery starts is learned at the first renewal, G/3 in, which
     * docs/semantics.md §10 allows. Nothing is confirmed for it. On the manual clock the
     * renewal is exactly G/3 after the handler starts.
     */
    @Test
    void aCancelledJobThatStartedStopsAtItsFirstRenewal() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        var log = new CapturingLog();
        try (var fake = FakeBroker.start()) {
            fake.onHeartbeat((req, ctx) -> {
                throw Status.FAILED_PRECONDITION.withDescription("job cancelled").asException();
            });
            var started = new CompletableFuture<Long>();
            var stopped = new CompletableFuture<StopReason>();
            JobHandler handler = ctx -> {
                started.complete(time.nanoTime());
                try {
                    new CountDownLatch(1).await(); // until interrupted
                } catch (InterruptedException e) {
                    stopped.complete(ctx.stopReason().orElseThrow());
                    throw e;
                }
            };
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).timeSource(time).log(log), "q", handler)) {
                FakeStream s = fake.awaitStream(Harness.BARRIER);
                Job fx = Fixture.of("q").grant(Duration.ofSeconds(3)).proto(); // fresh: age 0 on the manual clock
                s.deliver(fx, Harness.BARRIER);
                long start = started.get(10, SECONDS);
                time.awaitTimers(2, Harness.BARRIER); // the expiry timer and the first renewal
                time.advance(Duration.ofSeconds(1).minusNanos(1));
                assertEquals(0, fake.started("Heartbeat"), "no check, and no renewal before G/3");
                time.advance(Duration.ofNanos(1));
                assertEquals(StopReason.JOB_CANCELLED, stopped.get(10, SECONDS));
                assertEquals(Duration.ofSeconds(1).toNanos(), time.nanoTime() - start, "learned at the renewal, G/3 in");
                log.await(LogMessages.HEARTBEAT_REJECTED, 1, Harness.BARRIER);
                assertEquals(fx.getId(), log.fields(LogMessages.HEARTBEAT_REJECTED).get(0).get("job_id"));
                assertEquals(fx.getLeaseId(), log.fields(LogMessages.HEARTBEAT_REJECTED).get(0).get("lease_id"));
                assertNull(run.stopAndWait());
            }
            assertEquals(1, fake.started("Heartbeat"), "one call, the renewal that was refused");
            assertEquals(0, fake.started("Ack"), "a cancelled job is not confirmed");
            assertEquals(0, fake.started("Nack"), "a cancelled job is not confirmed");
        }
    }

    /**
     * A short handler on a cancelled job finishes before any renewal; its Ack is refused and not
     * retried, which §10 allows.
     */
    @Test
    void aCancelledJobThatFinishesFirstIsRefusedAtItsAck() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        var log = new CapturingLog();
        try (var fake = FakeBroker.start()) {
            fake.onAck((req, ctx) -> {
                throw Status.FAILED_PRECONDITION.withDescription("job cancelled").asException();
            });
            var ran = new CountDownLatch(1);
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).timeSource(time).log(log), "q",
                    ctx -> ran.countDown())) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                assertEquals(true, ran.await(10, SECONDS), "the handler never started");
                fake.awaitCalls("Ack", 1, BARRIER);
                s.awaitCredit(1, BARRIER); // the delivery ended and freed its slot
                log.await(LogMessages.CONFIRMATION_REFUSED, 1, BARRIER);
                assertNull(run.stopAndWait());
            }
            assertEquals(1, fake.started("Ack"), "FAILED_PRECONDITION is a refusal, not retried");
            assertEquals(0, fake.started("Nack"));
            assertEquals(0, fake.started("Heartbeat"), "nothing asked the broker before the handler ran");
            Map<String, Object> refused = log.fields(LogMessages.CONFIRMATION_REFUSED).get(0);
            assertEquals("ack", refused.get("operation"));
            assertEquals("refused", refused.get("outcome"));
        }
    }
}

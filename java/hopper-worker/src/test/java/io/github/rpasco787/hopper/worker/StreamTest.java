package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.proto.v1.Job;
import io.github.rpasco787.hopper.proto.v1.JobState;
import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import io.github.rpasco787.hopper.testing.Gate;
import io.github.rpasco787.hopper.testing.StreamControl;
import io.grpc.Context;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Ports of worker/stream_test.go, with {@link Spy} as the runner. The four ledger tests were
 * ported in Task 11 (CreditLedgerTest), and the parse test in Task 13 (DeliveryTest).
 */
class StreamTest {
    private static final Duration BARRIER = Harness.BARRIER;

    @Test
    void subscriptionAdvertisesSortedQueuesAndAllCredit() {
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            var w = Harness.newTestWorker(Harness.builder(fake.channel())
                    .workerId("worker-7").concurrency(3).leaseDuration(Duration.ofSeconds(7)), spy::run, "zeta", "alpha", "mu");
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                assertEquals(List.of("alpha", "mu", "zeta"), s.sub().getQueuesList());
                assertEquals("worker-7", s.sub().getWorkerId());
                assertEquals(3, s.sub().getCredit(), "a fresh worker advertises its whole capacity");
                assertEquals(com.google.protobuf.Duration.newBuilder().setSeconds(7).build(), s.sub().getLeaseDuration());

                assertNull(run.stopAndWait());
                s.awaitEOF(BARRIER); // a graceful stop half-closes the stream
            }
        }
    }

    @Test
    void zeroLeaseDurationLetsTheBrokerChoose() {
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(1), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                assertFalse(s.sub().hasLeaseDuration(), "an absent duration is how the wire says the broker chooses");
                assertNull(run.stopAndWait());
            }
        }
    }

    @Test
    void capacitySaturatesUntilDeliveriesFinish() {
        try (var fake = FakeBroker.start(); var spy = new Spy(true)) {
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(2), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                spy.await(BARRIER);
                spy.await(BARRIER);

                assertEquals(0, s.creditTotal(), "both tokens are held by deliveries");
                assertEquals(List.of(), s.creditMessages(), "a saturated worker advertises nothing");

                spy.release();
                s.awaitCredit(2, BARRIER); // finished deliveries return their tokens
                assertNull(run.stopAndWait());
            }
        }
    }

    @Test
    void malformedDeliveryReturnsItsTokenAndKeepsTheStream() {
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(1), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                Job broken = Fixture.of("q").proto().toBuilder().setState(JobState.JOB_STATE_AVAILABLE).build();
                s.deliver(broken, BARRIER);

                // The token comes back, which is only observable because the writer
                // re-advertises it. Nothing was executed and nothing was mutated: the
                // identifiers in a malformed message are not trustworthy enough to nack.
                s.awaitCredit(1, BARRIER);
                assertEquals(0, spy.count());
                assertEquals(List.of(), fake.ackRequests());
                assertEquals(List.of(), fake.nackRequests());

                // The stream survives, so a good delivery still runs.
                Fixture good = Fixture.of("q");
                s.deliver(good.proto(), BARRIER);
                assertEquals(good.id(), spy.await(BARRIER).job().id());
                assertNull(run.stopAndWait());
            }
        }
    }

    @Test
    void unknownQueueIsDispatchedWithoutAHandler() {
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(1), spy::run, "known");
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("unknown").proto(), BARRIER);
                Delivery d = spy.await(BARRIER);
                assertEquals("unknown", d.job().queue());
                assertNull(d.handler(), "an unregistered queue is nacked unstarted, not dropped silently");
                assertNull(run.stopAndWait());
            }
        }
    }

    @Test
    void registeredQueueArrivesWithItsHandler() {
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(1), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                Fixture fx = Fixture.of("q").payload("{\"ms\":5}".getBytes(StandardCharsets.UTF_8))
                        .attempt(2).maxAttempts(9).grant(Duration.ofSeconds(12));
                s.deliver(fx.proto(), BARRIER);

                Delivery d = spy.await(BARRIER);
                assertNotNull(d.handler());
                assertEquals(fx.id(), d.job().id());
                assertEquals(fx.leaseId(), d.leaseId());
                assertEquals(2, d.job().attempt());
                assertEquals(9, d.job().maxAttempts());
                assertEquals(Duration.ofSeconds(12).toNanos(), d.grantNanos());
                assertEquals("{\"ms\":5}", d.job().payloadUtf8());
                assertNull(run.stopAndWait());
            }
        }
    }

    @Test
    void overDeliveryEndsTheStreamAndReconnects() {
        try (var fake = FakeBroker.start(); var spy = new Spy(true)) {
            var log = new CapturingLog();
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(1).log(log), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);

                // Spend the only credit on a delivery that holds its token, then send one
                // more anyway. A real broker never does this; if one did, manufacturing a
                // token to run the extra job would breach the limit the caller configured.
                s.deliver(Fixture.of("q").proto(), BARRIER);
                spy.await(BARRIER);
                s.send(Fixture.of("q").proto());

                // Wait for the worker to end the stream before releasing the held delivery.
                // Releasing first would return its token, the writer would advertise fresh
                // credit, and the excess job would become a legitimate delivery.
                s.awaitEnd(BARRIER);
                FakeStream s2 = fake.awaitStream(BARRIER);
                assertEquals(0, s2.sub().getCredit(),
                        "the held delivery keeps its token while the protocol-violating stream is replaced");
                assertEquals(1, spy.dispatched(), "the excess job was never dispatched");
                assertEquals(0, spy.count(), "nor queued for the runner");
                assertTrue(log.fields(LogMessages.STREAM_FAILED_RECONNECTING).stream()
                                .anyMatch(f -> String.valueOf(f.get("err")).contains("arrived with no credit outstanding")),
                        "the stream ended for the over-delivery: " + log.lines());
                assertEquals(1, log.fields(LogMessages.STREAM_CLOSED).get(0).get("delivered"),
                        "the extra job is not counted as delivered");

                spy.release();
                s2.awaitCredit(1, BARRIER);
                assertNull(run.stopAndWait());
            }
        }
    }

    @Test
    void permanentSubscriptionErrorStopsTheWorker() {
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            fake.onSubscribe((sub, ctx) -> {
                throw Status.INVALID_ARGUMENT.withDescription("queues is required").asException();
            });
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(1), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                RuntimeException err = run.await();
                var failed = assertInstanceOf(WorkerFailedException.class, err);
                assertEquals(Status.Code.INVALID_ARGUMENT, failed.permanentFailure().orElseThrow().getCode());
                assertTrue(failed.abandoned().isEmpty(), "nothing was in flight to abandon");
            }
        }
    }

    @Test
    void blockedStreamOpenStopsAtTheDeadline() throws Exception {
        try (var fake = FakeBroker.start(); var gate = new Gate(); var spy = new Spy(false)) {
            var control = new StreamControl();
            var entered = new CountDownLatch(1);
            control.onOpen(forward -> {
                entered.countDown();
                gate.await(Context.current());
            });
            Duration budget = Duration.ofMillis(300);
            var w = Harness.newTestWorker(Harness.builder(fake.channel(control)).concurrency(1).shutdownTimeout(budget),
                    spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                assertTrue(entered.await(BARRIER.toSeconds(), TimeUnit.SECONDS), "the stream open never started");
                long start = System.nanoTime();
                run.stop();
                assertNull(run.await(), "a shutdown during establishment leaves nothing behind");
                assertTrue(System.nanoTime() - start < 2 * budget.toNanos(),
                        "establishment shares the one shutdown deadline; it does not get a budget of its own");
            }
        }
    }

    @Test
    void blockedSubscriptionSendStopsAtTheDeadline() throws Exception {
        try (var fake = FakeBroker.start(); var gate = new Gate(); var spy = new Spy(false)) {
            var control = new StreamControl();
            var entered = new CountDownLatch(1);
            control.onSend((message, forward) -> {
                entered.countDown();
                gate.await(Context.current());
            });
            Duration budget = Duration.ofMillis(300);
            var w = Harness.newTestWorker(Harness.builder(fake.channel(control)).concurrency(1).shutdownTimeout(budget),
                    spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                assertTrue(entered.await(BARRIER.toSeconds(), TimeUnit.SECONDS), "the subscription was never sent");
                long start = System.nanoTime();
                run.stop();
                assertNull(run.await());
                assertTrue(System.nanoTime() - start < 2 * budget.toNanos());
            }
        }
    }

    @Test
    void blockedCreditSendStopsAtTheDeadline() throws Exception {
        try (var fake = FakeBroker.start(); var gate = new Gate(); var spy = new Spy(false)) {
            var control = new StreamControl();
            var sends = new AtomicInteger();
            var entered = new CountDownLatch(1);
            control.onSend((message, forward) -> {
                if (sends.incrementAndGet() == 1) {
                    forward.run(); // let the subscription through
                    return;
                }
                entered.countDown();
                gate.await(Context.current());
            });
            Duration budget = Duration.ofMillis(300);
            var w = Harness.newTestWorker(Harness.builder(fake.channel(control)).concurrency(1).shutdownTimeout(budget),
                    spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                spy.await(BARRIER);
                assertTrue(entered.await(BARRIER.toSeconds(), TimeUnit.SECONDS), "the freed token was never advertised");

                long start = System.nanoTime();
                run.stop();
                assertNull(run.await());
                assertTrue(System.nanoTime() - start < 2 * budget.toNanos());
            }
        }
    }

    @Test
    void forcedShutdownUsesOneBudgetForTransportAndDeliveries() {
        try (var fake = FakeBroker.start(); var spy = new Spy(true)) {
            // A broker that never answers the half-close, combined with a delivery that
            // never finishes. Two sequential budgets would take twice as long, which is
            // how a container gets killed mid-handler.
            fake.onLease((s, ctx) -> new Gate().await(ctx));
            Duration budget = Duration.ofMillis(400);
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(1).shutdownTimeout(budget),
                    spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                spy.await(BARRIER);

                long start = System.nanoTime();
                run.stop();
                RuntimeException err = run.await();
                long elapsed = System.nanoTime() - start;

                var failed = assertInstanceOf(WorkerFailedException.class, err);
                assertTrue(failed.abandoned().isPresent());
                assertTrue(failed.getMessage().contains("1 active deliveries"), failed.getMessage());
                assertTrue(failed.permanentFailure().isEmpty(), "a forced stream end is not a stream failure");
                assertTrue(elapsed >= budget.toNanos(), "the worker waits out its budget before abandoning");
                assertTrue(elapsed < 2 * budget.toNanos(), "there is no second timeout after stream teardown");
            }
        }
    }

    /**
     * Each stream matches deliveries only to its own credit. Credit left unspent on a stream that
     * ended is not carried over, so the first delivery on the next stream is matched to that
     * stream's subscription, sent after the old one ended.
     */
    @Test
    void aReconnectResetsTheCreditAccounting() {
        var time = new ManualTimeSource(1_000_000_000L);
        try (var fake = FakeBroker.start(); var breakFirst = new Gate(); var spy = new Spy(false)) {
            var first = new AtomicBoolean(true);
            fake.onLease((s, ctx) -> {
                if (first.getAndSet(false)) {
                    breakFirst.await(ctx);
                    throw Status.UNAVAILABLE.withDescription("broker draining").asException();
                }
                s.idle(ctx);
            });
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(2).timeSource(time), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                fake.awaitStream(BARRIER); // its credit of 2 is never spent
                long ended = time.nanoTime();
                breakFirst.release();

                time.awaitTimers(1, BARRIER); // the reconnect backoff
                time.advance(Duration.ofMillis(200));
                FakeStream s2 = fake.awaitStream(BARRIER);
                assertEquals(2, s2.sub().getCredit(), "the old stream's unspent credit came back");
                s2.deliver(Fixture.of("q").proto(), BARRIER);
                Delivery d = spy.await(BARRIER);
                assertTrue(d.creditSent());
                assertTrue(d.creditSentAt() - ended > 0,
                        "matched to the new stream's credit, not the old one's (sent " + (d.creditSentAt() - ended)
                                + " ns after the old stream ended)");
                assertNull(run.stopAndWait());
            }
        }
    }
}

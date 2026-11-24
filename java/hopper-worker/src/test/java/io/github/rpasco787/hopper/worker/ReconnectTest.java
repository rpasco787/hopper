package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import io.github.rpasco787.hopper.testing.Gate;
import io.github.rpasco787.hopper.testing.StreamControl;
import io.grpc.Context;
import io.grpc.Status;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Ports of worker/reconnect_test.go, all eight. */
class ReconnectTest {
    private static final Duration BARRIER = Harness.BARRIER;

    /** The first stream waits for breakFirst, then fails UNAVAILABLE; later ones idle. */
    private static void breakFirstStream(FakeBroker fake, Gate breakFirst) {
        var first = new AtomicBoolean(true);
        fake.onLease((s, ctx) -> {
            if (first.getAndSet(false)) {
                breakFirst.await(ctx);
                throw Status.UNAVAILABLE.withDescription("broker draining").asException();
            }
            s.idle(ctx);
        });
    }

    @Test
    void reconnectPreservesActiveDeliveriesAndAdvertisesOnlyFreeCapacity() {
        try (var fake = FakeBroker.start(); var breakFirst = new Gate(); var spy = new Spy(true)) {
            breakFirstStream(fake, breakFirst);
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(3), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                FakeStream s1 = fake.awaitStream(BARRIER);
                s1.deliver(Fixture.of("q").proto(), BARRIER);
                s1.deliver(Fixture.of("q").proto(), BARRIER);
                spy.await(BARRIER);
                spy.await(BARRIER);

                breakFirst.release();
                FakeStream s2 = fake.awaitStream(BARRIER);
                assertEquals(1, s2.sub().getCredit(),
                        "the replacement stream advertises only capacity not held by old deliveries");
                s2.deliver(Fixture.of("q").proto(), BARRIER);
                spy.await(BARRIER);
                assertEquals(3, spy.dispatched());
                assertEquals(0, s2.creditTotal());

                spy.release();
                s2.awaitCredit(3, BARRIER); // all three tokens become useful on the replacement stream
                assertNull(run.stopAndWait());
            }
        }
    }

    /** On the manual clock: no second stream before the backoff's 75 ms floor has passed. */
    @Test
    void immediateCleanEOFBacksOffBeforeReconnect() {
        var time = new ManualTimeSource(1_000_000_000L);
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            fake.onLease((s, ctx) -> {}); // a clean end, at once
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(1).timeSource(time), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                fake.awaitStream(BARRIER);
                time.awaitTimers(1, BARRIER); // the backoff
                time.advance(Duration.ofMillis(75).minusNanos(1));
                assertEquals(1, fake.started("LeaseJobs"), "an empty clean session must not create a tight reconnect loop");

                time.advance(Duration.ofMillis(50)); // past the 125 ms ceiling of the first delay
                fake.awaitStream(BARRIER);
                assertNull(run.stopAndWait());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = Status.Code.class, names = {"INVALID_ARGUMENT", "UNIMPLEMENTED", "PERMISSION_DENIED", "UNAUTHENTICATED"})
    void permanentStreamCodesStopWithoutReconnect(Status.Code code) {
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            fake.onSubscribe((sub, ctx) -> {
                throw code.toStatus().withDescription("permanent").asException();
            });
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(1), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                var failed = assertInstanceOf(WorkerFailedException.class, run.await());
                assertEquals(code, failed.permanentFailure().orElseThrow().getCode());
                assertEquals(1, fake.subscriptions().size());
            }
        }
    }

    @Test
    void drainDoesNotOpenAnotherStream() {
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            var log = new CapturingLog();
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(1).log(log), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                run.stop();
                s.awaitEOF(BARRIER);
                assertNull(run.await());
                assertEquals(1, fake.subscriptions().size());
                assertEquals(1, log.count(LogMessages.NOT_REOPENING));
            }
        }
    }

    @Test
    void repeatedFailuresReclaimUnspentCredit() {
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            var sessions = new AtomicInteger();
            fake.onLease((s, ctx) -> {
                if (sessions.incrementAndGet() <= 2) {
                    throw Status.UNAVAILABLE.withDescription("restart").asException();
                }
                s.idle(ctx);
            });
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(2), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                FakeStream s1 = fake.awaitStream(BARRIER);
                FakeStream s2 = fake.awaitStream(BARRIER);
                FakeStream s3 = fake.awaitStream(BARRIER);
                assertEquals(2, s1.sub().getCredit());
                assertEquals(2, s2.sub().getCredit());
                assertEquals(2, s3.sub().getCredit(), "each failed stream returns its unspent reservation exactly once");
                assertNull(run.stopAndWait());
            }
        }
    }

    @Test
    void transientSubscriptionFailureReconnects() {
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            var attempts = new AtomicInteger();
            fake.onSubscribe((sub, ctx) -> {
                if (attempts.incrementAndGet() == 1) {
                    throw Status.UNAVAILABLE.withDescription("broker draining").asException();
                }
            });
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(2), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                assertEquals(2, s.sub().getCredit());
                assertEquals(2, fake.subscriptions().size());
                assertNull(run.stopAndWait());
            }
        }
    }

    @Test
    void sendFailureAfterDeliveryDoesNotDoubleRefundCapacity() throws Exception {
        try (var fake = FakeBroker.start(); var returnFailure = new Gate(); var spy = new Spy(false)) {
            var control = new StreamControl();
            var creditForwarded = new CountDownLatch(1);
            var sends = new AtomicInteger();
            control.onSend((message, forward) -> {
                forward.run();
                if (sends.incrementAndGet() != 2) {
                    return;
                }
                creditForwarded.countDown();
                returnFailure.await(Context.current());
                throw Status.UNAVAILABLE.withDescription("write outcome was uncertain").asException();
            });
            var w = Harness.newTestWorker(Harness.builder(fake.channel(control)).concurrency(2), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                FakeStream s1 = fake.awaitStream(BARRIER);
                s1.deliver(Fixture.of("q").proto(), BARRIER);
                spy.await(BARRIER);
                assertTrue(creditForwarded.await(BARRIER.toSeconds(), TimeUnit.SECONDS), "the returned token was not sent");
                // the broker can answer an uncertain Send before it returns
                s1.deliver(Fixture.of("q").proto(), BARRIER);
                spy.await(BARRIER);
                returnFailure.release();

                FakeStream s2 = fake.awaitStream(BARRIER);
                assertEquals(2, s2.sub().getCredit(),
                        "the received delivery spent the uncertain reservation before reconciliation");
                assertNull(run.stopAndWait());
            }
        }
    }

    @Test
    @Disabled("Task 17: needs the real runner, which confirms")
    void reconnectCountsAConfirmationAsActiveDelivery() throws Exception {
        try (var fake = FakeBroker.start(); var breakFirst = new Gate(); var releaseAck = new Gate()) {
            breakFirstStream(fake, breakFirst);
            var ackEntered = new CountDownLatch(1);
            fake.onAck((req, ctx) -> {
                ackEntered.countDown();
                releaseAck.await(ctx);
            });
            var w = HopperWorker.builder(fake.channel()).concurrency(2).shutdownTimeout(Duration.ofSeconds(2))
                    .log(Harness.QUIET).handle("q", ctx -> {}).build();
            try (var run = Harness.runWorker(w)) {
                FakeStream s1 = fake.awaitStream(BARRIER);
                s1.deliver(Fixture.of("q").proto(), BARRIER);
                assertTrue(ackEntered.await(BARRIER.toSeconds(), TimeUnit.SECONDS), "the delivery never reached confirmation");
                breakFirst.release();
                FakeStream s2 = fake.awaitStream(BARRIER);
                assertEquals(1, s2.sub().getCredit(), "a blocked confirmation retains the delivery token across reconnect");
                releaseAck.release();
                s2.awaitCredit(2, BARRIER);
                assertNull(run.stopAndWait());
            }
        }
    }
}

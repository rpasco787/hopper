package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.proto.v1.AckRequest;
import io.github.rpasco787.hopper.proto.v1.NackRequest;
import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import io.github.rpasco787.hopper.testing.Gate;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Ports of worker/confirm_test.go, all 14. Four pin the check path with checkEveryDelivery(true),
 * exactly as their Go Options do: aSuccessfulHandlerIsAcked, executionPinsHeartbeatsToTheDeliveredGrant,
 * anUnregisteredQueueIsNackedUnstarted and aDeliveryThatLosesToShutdownIsNackedUnstarted. Three
 * stay on the system clock with Go's tolerances, because they run a whole worker through
 * confirmation retries with backoff: transientConfirmationRetriesKeepTheSameIdentifiers,
 * forcedShutdownEndsConfirmationBackoff and confirmationStopsAtTheLocalLeaseDeadline. The two
 * that time a forced shutdown run on the manual clock.
 */
class ConfirmTest {
    private static final Duration BARRIER = Harness.BARRIER;

    private static void awaitHandler(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(BARRIER.toSeconds(), TimeUnit.SECONDS), "the handler never ran");
    }

    @Test
    void aSuccessfulHandlerIsAcked() throws Exception {
        try (var fake = FakeBroker.start()) {
            var log = new CapturingLog();
            var ran = new LinkedBlockingQueue<Job>();
            Fixture fx = Fixture.of("q").payload("{\"ms\":1}".getBytes(StandardCharsets.UTF_8));
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).log(log).checkEveryDelivery(true), "q",
                    ctx -> ran.add(ctx.job()))) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(fx.proto(), BARRIER);

                Job j = ran.poll(BARRIER.toSeconds(), TimeUnit.SECONDS);
                assertTrue(j != null, "the handler never ran");
                assertEquals(fx.id(), j.id());
                assertEquals("{\"ms\":1}", j.payloadUtf8());

                fake.awaitCalls("Ack", 1, BARRIER);
                assertNull(run.stopAndWait());
            }
            List<AckRequest> acks = fake.ackRequests();
            assertEquals(1, acks.size());
            assertEquals(fx.id().toString(), acks.get(0).getId());
            assertEquals(fx.leaseId().toString(), acks.get(0).getLeaseId());
            assertEquals(List.of(), fake.nackRequests());
            assertTrue(fake.started("Heartbeat") >= 1, "ownership is validated before the handler runs");

            List<String> msgs = log.messages();
            assertTrue(msgs.contains(LogMessages.JOB_STARTED));
            assertTrue(msgs.contains(LogMessages.HANDLER_FINISHED));
            assertTrue(msgs.contains(LogMessages.JOB_CONFIRMED));
            assertTrue(msgs.indexOf(LogMessages.JOB_STARTED) < msgs.indexOf(LogMessages.HANDLER_FINISHED));
            assertTrue(msgs.indexOf(LogMessages.HANDLER_FINISHED) < msgs.indexOf(LogMessages.JOB_CONFIRMED),
                    "completion is logged after the broker accepted it, not when user code returned");
        }
    }

    @Test
    void aFailingHandlerIsNackedWithASanitizedError() throws Exception {
        try (var fake = FakeBroker.start()) {
            var log = new CapturingLog();
            String dirty = "boom \u0000 " + "é".repeat(ErrorText.MAX_ERROR_BYTES) + "\ud800";
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).log(log), "q", ctx -> {
                throw new Exception(dirty);
            })) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                fake.awaitCalls("Nack", 1, BARRIER);
                assertNull(run.stopAndWait());
            }
            List<NackRequest> nacks = fake.nackRequests();
            assertEquals(1, nacks.size());
            String msg = nacks.get(0).getError();
            assertFalse(msg.contains("\u0000"), "a NUL would fail the Nack itself, not the job");
            assertTrue(msg.getBytes(StandardCharsets.UTF_8).length <= ErrorText.MAX_ERROR_BYTES);
            assertTrue(msg.startsWith("java.lang.Exception: boom  "), msg.substring(0, 40));
            assertEquals(List.of(), fake.ackRequests());

            List<Map<String, Object>> logged = log.fields(LogMessages.HANDLER_FINISHED);
            assertEquals(1, logged.size());
            assertEquals(msg, logged.get(0).get("err"),
                    "the bounded value persisted by Nack is also the value exposed to log handlers");
        }
    }

    /** The handler throws an AssertionError, an Error: the status is PANIC and the Nack carries the stack. */
    @Test
    void aPanickingHandlerIsNackedAndTheWorkerCarriesOn() throws Exception {
        try (var fake = FakeBroker.start()) {
            var calls = new AtomicInteger();
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()), "q", ctx -> {
                if (calls.incrementAndGet() == 1) {
                    throw new AssertionError("payload made no sense");
                }
            })) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                fake.awaitCalls("Nack", 1, BARRIER);

                String err = fake.nackRequests().get(0).getError();
                assertTrue(err.startsWith("java.lang.AssertionError: payload made no sense\n"), err);
                assertTrue(err.contains("Execution.invoke"), "the stack goes in the error too: " + err);
                assertTrue(err.getBytes(StandardCharsets.UTF_8).length <= ErrorText.MAX_ERROR_BYTES);

                // The token came back and the worker is still working.
                s.awaitCredit(1, BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                fake.awaitCalls("Ack", 1, BARRIER);
                assertNull(run.stopAndWait());
            }
        }
    }

    /** On the system clock: a whole worker through confirmation retries with backoff. */
    @Test
    void transientConfirmationRetriesKeepTheSameIdentifiers() throws Exception {
        try (var fake = FakeBroker.start()) {
            var attempts = new AtomicInteger();
            fake.onAck((req, ctx) -> {
                if (attempts.incrementAndGet() < 3) {
                    // The write may well have committed before the response was lost; that is
                    // exactly why the retry must present the same token rather than giving up
                    // or re-running the handler.
                    throw Status.UNAVAILABLE.withDescription("broker is shutting down").asException();
                }
            });
            var runs = new AtomicInteger();
            Fixture fx = Fixture.of("q");
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()), "q", ctx -> runs.incrementAndGet())) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(fx.proto(), BARRIER);
                fake.awaitCalls("Ack", 3, BARRIER);
                assertNull(run.stopAndWait());
            }
            List<AckRequest> acks = fake.ackRequests();
            assertEquals(3, acks.size());
            for (int i = 0; i < acks.size(); i++) {
                assertEquals(fx.id().toString(), acks.get(i).getId(), "attempt " + i);
                assertEquals(fx.leaseId().toString(), acks.get(i).getLeaseId(), "attempt " + i);
            }
            assertEquals(1, runs.get(), "a lost response is not a reason to run the handler again");
        }
    }

    @ParameterizedTest
    @EnumSource(value = Status.Code.class, names = {"ABORTED", "FAILED_PRECONDITION", "NOT_FOUND", "INTERNAL"})
    void terminalConfirmationCodesAreNotRetried(Status.Code code) throws Exception {
        try (var fake = FakeBroker.start()) {
            fake.onAck((req, ctx) -> {
                throw code.toStatus().withDescription(code.toString()).asException();
            });
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()), "q", ctx -> {})) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                fake.awaitCalls("Ack", 1, BARRIER);
                s.awaitCredit(1, BARRIER); // the delivery ends either way
                assertNull(run.stopAndWait());
            }
            assertEquals(1, fake.started("Ack"), code + " is an answer, not a network hiccup");
        }
    }

    @Test
    void noConfirmationAfterTheLeaseIsLost() throws Exception {
        try (var fake = FakeBroker.start()) {
            // The first beat succeeds; every later beat says the token is stale.
            var beats = new AtomicInteger();
            fake.onHeartbeat((req, ctx) -> {
                if (beats.incrementAndGet() > 1) {
                    throw Status.ABORTED.withDescription("lease lost").asException();
                }
            });
            var cause = new LinkedBlockingQueue<StopReason>();
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).leaseDuration(Duration.ofMillis(150)),
                    "q", ctx -> {
                        try {
                            new CountDownLatch(1).await();
                        } catch (InterruptedException e) {
                            cause.add(ctx.stopReason().orElseThrow());
                            throw e;
                        }
                    })) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").grant(Duration.ofMillis(150)).proto(), BARRIER);

                assertEquals(StopReason.LEASE_LOST, cause.poll(BARRIER.toSeconds(), TimeUnit.SECONDS),
                        "the handler is told why it was stopped");
                s.awaitCredit(1, BARRIER); // the delivery finished and returned its token
                assertNull(run.stopAndWait());
            }
            assertEquals(List.of(), fake.ackRequests(), "a job we no longer hold is not ours to confirm");
            assertEquals(List.of(), fake.nackRequests());
        }
    }

    /**
     * The regression this guards: when the handler finishes, the SDK cancels the heartbeat RPC in
     * flight. That produces a CANCELLED status which looks exactly like a rejection, and treating
     * it as one would suppress the Ack for a job that really did succeed.
     */
    @Test
    void aHeartbeatCancelledByNormalCompletionStillAcks() throws Exception {
        try (var fake = FakeBroker.start(); var g = new Gate()) {
            var blocked = new CountDownLatch(1);
            var beats = new AtomicInteger();
            fake.onHeartbeat((req, ctx) -> {
                if (beats.incrementAndGet() == 1) {
                    return; // the first beat answers
                }
                blocked.countDown();
                g.await(ctx); // a periodic beat that never answers
            });
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()), "q", ctx -> {
                // Return only once a renewal is definitely stuck mid-flight.
                blocked.await(BARRIER.toSeconds(), TimeUnit.SECONDS);
            })) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").grant(Duration.ofMillis(600)).proto(), BARRIER);
                fake.awaitCalls("Ack", 1, BARRIER);
                assertNull(run.stopAndWait());
            }
            assertEquals(1, fake.ackRequests().size(), "the lease was still live under its last confirmed deadline");
            assertEquals(List.of(), fake.nackRequests());
        }
    }

    @Test
    void executionPinsHeartbeatsToTheDeliveredGrant() throws Exception {
        try (var fake = FakeBroker.start()) {
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel())
                    .leaseDuration(Duration.ofSeconds(12)).checkEveryDelivery(true), "q", ctx -> {})) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").grant(Duration.ofSeconds(30)).proto(), BARRIER);
                fake.awaitCalls("Ack", 1, BARRIER);
                assertNull(run.stopAndWait());
            }
            var requests = fake.heartbeatRequests();
            assertFalse(requests.isEmpty());
            assertEquals(com.google.protobuf.Duration.newBuilder().setSeconds(30).build(), requests.get(0).getLeaseDuration(),
                    "a broker configuration change must not replace the grant this delivery actually received");
        }
    }

    @Test
    void anUnregisteredQueueIsNackedUnstarted() throws Exception {
        try (var fake = FakeBroker.start()) {
            var ran = new AtomicInteger();
            Fixture fx = Fixture.of("unknown");
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).checkEveryDelivery(true), "known",
                    ctx -> ran.incrementAndGet())) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(fx.proto(), BARRIER);
                fake.awaitCalls("Nack", 1, BARRIER);
                assertNull(run.stopAndWait());
            }
            List<NackRequest> nacks = fake.nackRequests();
            assertEquals(1, nacks.size());
            assertEquals(fx.leaseId().toString(), nacks.get(0).getLeaseId());
            assertTrue(nacks.get(0).getError().contains("no handler is registered for queue \"unknown\""));
            assertEquals(0, ran.get(), "user code never ran");
            assertEquals(1, fake.started("Heartbeat"), "ownership is proven even for a job we will only hand back");
        }
    }

    @Test
    void aDeliveryThatLosesToShutdownIsNackedUnstarted() throws Exception {
        try (var fake = FakeBroker.start(); var g = new Gate()) {
            // Hold acquisition until drain has begun, so the delivery is valid and owned but
            // arrives on the wrong side of the shutdown transition.
            var acquiring = new CountDownLatch(1);
            fake.onHeartbeat((req, ctx) -> {
                acquiring.countDown();
                g.await(ctx);
            });
            var ran = new AtomicInteger();
            var w = Harness.builder(fake.channel()).concurrency(1).shutdownTimeout(Duration.ofSeconds(5))
                    .checkEveryDelivery(true).handle("q", ctx -> ran.incrementAndGet()).build();
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                assertTrue(acquiring.await(BARRIER.toSeconds(), TimeUnit.SECONDS), "acquisition never started");

                run.stop();
                s.awaitEOF(BARRIER); // drain has begun: intake is closed
                g.release();

                fake.awaitCalls("Nack", 1, BARRIER);
                assertNull(run.await(), "the drain completed inside its budget");
            }
            List<NackRequest> nacks = fake.nackRequests();
            assertEquals(1, nacks.size());
            assertTrue(nacks.get(0).getError().contains("never started"));
            assertEquals(0, ran.get(), "once drain wins the race, user code does not start");
            assertEquals(List.of(), fake.ackRequests());
        }
    }

    /** On the manual clock: the shutdown deadline is a TimeSource timer the test advances. */
    @Test
    void forcedShutdownEndsABlockedConfirmation() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        try (var fake = FakeBroker.start(); var g = new Gate()) {
            var acking = new CountDownLatch(1);
            fake.onAck((req, ctx) -> {
                acking.countDown();
                g.await(ctx);
            });
            Duration budget = Duration.ofMillis(400);
            var w = Harness.builder(fake.channel()).timeSource(time).concurrency(1).shutdownTimeout(budget)
                    .handle("q", ctx -> {}).build();
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                assertTrue(acking.await(BARRIER.toSeconds(), TimeUnit.SECONDS), "the Ack never reached the broker");

                long start = time.nanoTime();
                run.stop();
                time.awaitTimers(1, BARRIER); // the shutdown deadline
                time.advance(budget);
                var failed = assertInstanceOf(WorkerFailedException.class, run.await());
                assertEquals(budget.toNanos(), time.nanoTime() - start);
                assertTrue(failed.getMessage().contains("1 awaiting confirmation"),
                        "a pending confirmation is counted apart from a running handler: " + failed.getMessage());
            }
            // And nothing keeps retrying behind the worker's back.
            int before = fake.started("Ack");
            Thread.sleep(150);
            assertEquals(before, fake.started("Ack"), "the SDK stops when it says it has stopped");
        }
    }

    /** On the system clock: a whole worker through confirmation retries with backoff. */
    @Test
    void forcedShutdownEndsConfirmationBackoff() throws Exception {
        try (var fake = FakeBroker.start()) {
            fake.onAck((req, ctx) -> {
                throw Status.UNAVAILABLE.withDescription("broker is shutting down").asException();
            });
            Duration budget = Duration.ofMillis(400);
            var w = Harness.builder(fake.channel()).concurrency(1).shutdownTimeout(budget).handle("q", ctx -> {}).build();
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                fake.awaitCalls("Ack", 2, BARRIER);

                long start = System.nanoTime();
                run.stop();
                var failed = assertInstanceOf(WorkerFailedException.class, run.await());
                assertTrue(failed.abandoned().isPresent());
                assertTrue(System.nanoTime() - start < 2 * budget.toNanos(), "the retry timer is interruptible");
            }
            int before = fake.started("Ack");
            Thread.sleep(150);
            assertEquals(before, fake.started("Ack"), "no retries continue after abandonment");
        }
    }

    /** On the manual clock. The handler deliberately ignores both interruption and isStopped(). */
    @Test
    void aHandlerReturningAfterAbandonmentConfirmsNothing() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        try (var fake = FakeBroker.start()) {
            var release = new CountDownLatch(1);
            var started = new CountDownLatch(1);
            Duration budget = Duration.ofMillis(300);
            var w = Harness.builder(fake.channel()).timeSource(time).concurrency(1).shutdownTimeout(budget)
                    .handle("q", ctx -> {
                        started.countDown();
                        // Deliberately ignores cancellation, as a handler doing blocking work
                        // with no interruption support would.
                        while (true) {
                            try {
                                release.await();
                                return;
                            } catch (InterruptedException ignored) {
                                // carries on
                            }
                        }
                    }).build();
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                awaitHandler(started);

                run.stop();
                time.awaitTimers(3, BARRIER); // expiry, the first renewal, and the shutdown deadline
                time.advance(budget);
                var failed = assertInstanceOf(WorkerFailedException.class, run.await());
                assertTrue(failed.getMessage().contains("1 running handlers"), failed.getMessage());

                // Only now does the handler finish. Its job was already counted as abandoned and
                // its lease is expiring; writing now would be writing after the worker reported
                // it had stopped.
                release.countDown();
                Thread.sleep(200);
                assertEquals(List.of(), fake.ackRequests());
                assertEquals(List.of(), fake.nackRequests());
            }
        }
    }

    /** On the system clock: a whole worker through confirmation retries with backoff. */
    @Test
    void confirmationStopsAtTheLocalLeaseDeadline() throws Exception {
        try (var fake = FakeBroker.start()) {
            fake.onAck((req, ctx) -> {
                throw Status.UNAVAILABLE.withDescription("broker is shutting down").asException();
            });
            // A short grant means the local deadline arrives long before the shutdown budget
            // would. Retrying past it would only ask the broker about a token the reaper is
            // already free to take.
            var w = Harness.builder(fake.channel()).concurrency(1).shutdownTimeout(Duration.ofSeconds(10))
                    .handle("q", ctx -> {}).build();
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").grant(Duration.ofMillis(400)).proto(), BARRIER);

                s.awaitCredit(1, BARRIER); // the delivery ends when its lease does
                int before = fake.started("Ack");
                Thread.sleep(200);
                assertEquals(before, fake.started("Ack"), "retries stop at the lease, not at the shutdown budget");
                assertNull(run.stopAndWait(), "and the worker still drains cleanly");
            }
        }
    }
}

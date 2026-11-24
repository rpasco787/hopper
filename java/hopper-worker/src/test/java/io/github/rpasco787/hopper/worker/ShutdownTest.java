package io.github.rpasco787.hopper.worker;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import io.github.rpasco787.hopper.testing.Gate;
import io.grpc.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Ports of worker/shutdown_test.go, all four, and the Java-only shutdown surface. */
class ShutdownTest {
    private static final Duration BARRIER = Harness.BARRIER;

    @Test
    void shutdownDrainsRunningHandlerThroughHeartbeatAndAck() throws Exception {
        try (var fake = FakeBroker.start(); var finishHandler = new Gate()) {
            var renewed = new CountDownLatch(1);
            var beats = new AtomicInteger();
            fake.onHeartbeat((req, ctx) -> {
                // Go's value. Since the start rule a fresh delivery takes no check, so the second
                // beat is the second renewal (docs/plans §15 notes the same of window_test.go).
                if (beats.incrementAndGet() >= 2) {
                    renewed.countDown();
                }
            });
            var handlerStarted = new CompletableFuture<JobContext>();
            var w = HopperWorker.builder(fake.channel()).concurrency(1).shutdownTimeout(Duration.ofSeconds(2))
                    .log(Harness.QUIET).handle("q", ctx -> {
                        handlerStarted.complete(ctx);
                        finishHandler.await(io.grpc.Context.ROOT);
                    }).build();
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").grant(Duration.ofMillis(600)).proto(), BARRIER);
                JobContext handlerCtx = handlerStarted.get(10, SECONDS);

                run.stop();
                s.awaitEOF(BARRIER); // normal shutdown half-closes intake
                assertFalse(handlerCtx.isStopped(), "graceful drain stopped the handler: " + handlerCtx.stopReason());
                assertTrue(renewed.await(10, SECONDS), "the running handler did not retain heartbeat renewal during drain");

                finishHandler.release();
                fake.awaitCalls("Ack", 1, BARRIER);
                assertNull(run.await());
            }
            assertEquals(1, fake.ackRequests().size());
            assertEquals(List.of(), fake.nackRequests());
        }
    }

    /** On the manual clock: the budget is a TimeSource timer the test advances. */
    @Test
    void permanentConsumeFailureIsJoinedWithHandlerAbandonment() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        try (var fake = FakeBroker.start(); var failStream = new Gate()) {
            fake.onLease((s, ctx) -> {
                failStream.await(ctx);
                throw Status.INVALID_ARGUMENT.withDescription("permanent stream failure").asException();
            });
            var handlerStarted = new CountDownLatch(1);
            var handlerDone = new CountDownLatch(1);
            var releaseHandler = new CountDownLatch(1);
            Duration budget = Duration.ofMillis(300);
            var w = HopperWorker.builder(fake.channel()).concurrency(1).shutdownTimeout(budget).log(Harness.QUIET)
                    .timeSource(time).handle("q", ctx -> {
                        try {
                            handlerStarted.countDown();
                            while (true) { // deliberately ignores cancellation
                                try {
                                    releaseHandler.await();
                                    return;
                                } catch (InterruptedException ignored) {
                                    // carries on
                                }
                            }
                        } finally {
                            handlerDone.countDown();
                        }
                    }).build();
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                assertTrue(handlerStarted.await(10, SECONDS), "the handler never started");

                long started = time.nanoTime();
                failStream.release();
                time.awaitTimers(3, BARRIER); // expiry, the first renewal, and the shutdown deadline
                time.advance(budget);
                var err = assertInstanceOf(WorkerFailedException.class, run.await());
                assertEquals(Status.Code.INVALID_ARGUMENT, err.permanentFailure().orElseThrow().getCode(),
                        "the permanent consume status is preserved");
                assertTrue(err.abandoned().isPresent());
                assertTrue(err.getMessage().contains("1 running handlers"), err.getMessage());
                assertTrue(err.getMessage().startsWith("worker: lease stream: INVALID_ARGUMENT: permanent stream failure\n"),
                        "the stream failure comes first: " + err.getMessage());
                assertEquals(budget.toNanos(), time.nanoTime() - started);

                releaseHandler.countDown();
                assertTrue(handlerDone.await(10, SECONDS), "the released handler did not return");
            }
            assertEquals(List.of(), fake.ackRequests());
            assertEquals(List.of(), fake.nackRequests());
        }
    }

    /** On the manual clock. With checkEveryDelivery(true), as in Go. */
    @Test
    void forcedShutdownCountsBlockedAcquisition() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        try (var fake = FakeBroker.start()) {
            var acquiring = new CountDownLatch(1);
            fake.onHeartbeat((req, ctx) -> {
                acquiring.countDown();
                new Gate().await(ctx); // until the call is cancelled
            });
            Duration budget = Duration.ofMillis(250);
            var ran = new AtomicInteger();
            var w = HopperWorker.builder(fake.channel()).concurrency(1).shutdownTimeout(budget).log(Harness.QUIET)
                    .timeSource(time).checkEveryDelivery(true).handle("q", ctx -> ran.incrementAndGet()).build();
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                assertTrue(acquiring.await(10, SECONDS), "acquisition never started");

                long started = time.nanoTime();
                run.stop();
                time.awaitTimers(1, BARRIER); // the shutdown deadline
                time.advance(budget);
                var err = assertInstanceOf(WorkerFailedException.class, run.await());
                assertTrue(err.getMessage().contains("1 acquiring a lease"), err.getMessage());
                assertEquals(budget.toNanos(), time.nanoTime() - started);
            }
            assertEquals(List.of(), fake.ackRequests());
            assertEquals(List.of(), fake.nackRequests());
            assertEquals(0, ran.get());
        }
    }

    /** With checkEveryDelivery(true), as in Go. On the system clock: Go asserts no elapsed time. */
    @Test
    void forcedShutdownCountsBlockedUnstartedNack() throws Exception {
        try (var fake = FakeBroker.start(); var releaseAcquisition = new Gate()) {
            var acquiring = new CountDownLatch(1);
            var nacking = new CountDownLatch(1);
            fake.onHeartbeat((req, ctx) -> {
                acquiring.countDown();
                releaseAcquisition.await(ctx);
            });
            fake.onNack((req, ctx) -> {
                nacking.countDown();
                new Gate().await(ctx); // until the call is cancelled
            });
            Duration budget = Duration.ofMillis(300);
            var ran = new AtomicInteger();
            var w = HopperWorker.builder(fake.channel()).concurrency(1).shutdownTimeout(budget).log(Harness.QUIET)
                    .checkEveryDelivery(true).handle("q", ctx -> ran.incrementAndGet()).build();
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                assertTrue(acquiring.await(10, SECONDS), "acquisition never started");

                run.stop();
                s.awaitEOF(BARRIER);
                releaseAcquisition.release();
                assertTrue(nacking.await(10, SECONDS), "the unstarted Nack never began");
                var err = assertInstanceOf(WorkerFailedException.class, run.await());
                assertTrue(err.getMessage().contains("1 awaiting confirmation"), err.getMessage());
            }
            assertEquals(1, fake.nackRequests().size());
            assertEquals(0, ran.get());
        }
    }

    // --- Java-only ---

    /** Go: TestAnAlreadyCancelledContextDrainsBeforeNewLifecycleReturns. */
    @Test
    void aShutdownBeforeRunOpensNoStream() {
        try (var fake = FakeBroker.start()) {
            var log = new CapturingLog();
            var w = HopperWorker.builder(fake.channel()).log(log).handle("q", ctx -> {}).build();
            w.shutdown();
            assertDoesNotThrow(w::run);
            assertEquals(0, fake.started("LeaseJobs"), "whether a doomed worker opens a stream must not depend on scheduling");
            assertEquals("shutdown requested", log.fields(LogMessages.SHUTDOWN_STARTED).get(0).get("reason"));
        }
    }

    @Test
    void interruptingTheRunThreadBeginsTheDrainAndRestoresTheFlag() throws Exception {
        try (var fake = FakeBroker.start()) {
            var log = new CapturingLog();
            var w = HopperWorker.builder(fake.channel()).log(log).concurrency(1).handle("q", ctx -> {}).build();
            var flag = new CompletableFuture<Boolean>();
            var thrown = new CompletableFuture<Throwable>();
            Thread t = Thread.ofPlatform().name("hopper-test-run").start(() -> {
                try {
                    w.run();
                    thrown.complete(null);
                } catch (Throwable e) {
                    thrown.complete(e);
                }
                flag.complete(Thread.currentThread().isInterrupted());
            });
            FakeStream s = fake.awaitStream(BARRIER);
            t.interrupt();
            s.awaitEOF(BARRIER); // the interrupt began the drain, which half-closes
            assertNull(thrown.get(10, SECONDS), "run() returns normally after a clean drain");
            assertTrue(flag.get(10, SECONDS), "run() re-interrupts its thread before returning");
            assertEquals("caller interrupted", log.fields(LogMessages.SHUTDOWN_STARTED).get(0).get("reason"));
            t.join(BARRIER.toMillis());
        }
    }

    @Test
    void awaitTerminationReturnsWhenRunReturns() throws Exception {
        try (var fake = FakeBroker.start()) {
            var w = HopperWorker.builder(fake.channel()).log(Harness.QUIET).handle("q", ctx -> {}).build();
            assertFalse(w.awaitTermination(Duration.ofMillis(20)), "run() has not even started");
            try (var run = Harness.runWorker(w)) {
                fake.awaitStream(BARRIER);
                assertFalse(w.awaitTermination(Duration.ofMillis(50)), "run() is still running");
                w.shutdown();
                assertTrue(w.awaitTermination(BARRIER), "run() returned after the drain");
                assertNull(run.await());
            }
        }
    }

    @Test
    void shutdownIsIdempotentAndSafeFromAnyThread() throws Exception {
        try (var fake = FakeBroker.start()) {
            var log = new CapturingLog();
            var w = HopperWorker.builder(fake.channel()).log(log).handle("q", ctx -> {}).build();
            try (var run = Harness.runWorker(w)) {
                fake.awaitStream(BARRIER);
                var together = new CyclicBarrier(8);
                List<Thread> threads = new ArrayList<>();
                for (int i = 0; i < 8; i++) {
                    threads.add(Thread.ofVirtual().start(() -> {
                        try {
                            together.await();
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                        w.shutdown();
                    }));
                }
                for (Thread t : threads) {
                    t.join(BARRIER.toMillis());
                }
                assertNull(run.await());
            }
            assertEquals(1, log.count(LogMessages.SHUTDOWN_STARTED), "shutdown started is logged once");
        }
    }
}

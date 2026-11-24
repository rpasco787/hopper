package io.github.rpasco787.hopper.worker;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import io.grpc.Status;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

/** How a handler's ending becomes a confirmation (Decision D22, D21). Java-only. */
class HandlerFailureTest {
    private static final Duration BARRIER = Harness.BARRIER;

    @Test
    void anExceptionIsNackedWithItsTextAndNoStack() throws Exception {
        try (var fake = FakeBroker.start()) {
            var observer = new ObserverTest.Recording();
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).observer(observer), "q", ctx -> {
                throw new IOException("disk");
            })) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                fake.awaitCalls("Nack", 1, BARRIER);
                assertNull(run.stopAndWait());
            }
            assertEquals("java.io.IOException: disk", fake.nackRequests().get(0).getError());
            assertEquals(HandlerStatus.ERROR, observer.finished().get(0).status());
        }
    }

    /** The flag is the SDK's to clear: a stale interrupt would cancel the Ack's blocking call. */
    @Test
    void aHandlerThatInterruptsItselfIsStillAcked() throws Exception {
        var log = new CapturingLog();
        try (var fake = FakeBroker.start()) {
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).log(log), "q",
                    ctx -> Thread.currentThread().interrupt())) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                log.await(LogMessages.JOB_CONFIRMED, 1, BARRIER);
                assertNull(run.stopAndWait());
            }
            assertEquals(1, fake.ackRequests().size(), "the Ack was sent");
            assertEquals(List.of("ack"), log.fields(LogMessages.JOB_CONFIRMED).stream().map(f -> f.get("operation")).toList());
            assertEquals(0, log.count(LogMessages.CONFIRMATION_REFUSED));
        }
    }

    /** A handler stopped for a lost lease, which ends by throwing InterruptedException: nothing is confirmed. */
    @Test
    void aStoppedHandlerThatThrowsInterruptedExceptionIsNotConfirmed() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        var log = new CapturingLog();
        try (var fake = FakeBroker.start()) {
            fake.onHeartbeat((req, ctx) -> {
                throw Status.ABORTED.withDescription("stale token").asException();
            });
            var started = new CountDownLatch(1);
            var stopped = new CompletableFuture<StopReason>();
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).timeSource(time).log(log), "q", ctx -> {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    stopped.complete(ctx.stopReason().orElseThrow());
                    throw e;
                }
            })) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").grant(Duration.ofSeconds(3)).proto(), BARRIER);
                assertTrue(started.await(10, SECONDS));
                time.awaitTimers(2, BARRIER);
                time.advance(Duration.ofSeconds(1)); // the first renewal is refused ABORTED
                assertEquals(StopReason.LEASE_LOST, stopped.get(10, SECONDS));
                log.await(LogMessages.UNCONFIRMED_LEASE_RAN_OUT, 1, BARRIER);
                s.awaitCredit(1, BARRIER);
                assertNull(run.stopAndWait());
            }
            assertEquals(0, fake.started("Ack"));
            assertEquals(0, fake.started("Nack"), "the Nack an InterruptedException would carry is not sent");
            String err = String.valueOf(log.fields(LogMessages.HANDLER_FINISHED).get(0).get("err"));
            assertTrue(err.startsWith("java.lang.InterruptedException"), err);
        }
    }
}

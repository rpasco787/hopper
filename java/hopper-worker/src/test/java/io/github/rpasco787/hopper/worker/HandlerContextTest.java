package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Interrupt hygiene (docs/design/java-worker-sdk.md, Stopping a handler; Decision D21). */
class HandlerContextTest {
    private static final Duration BARRIER = Duration.ofSeconds(10);

    private static HandlerContext context() {
        return new HandlerContext(new Job(UUID.randomUUID(), "q", new byte[0], 1, 5, Instant.EPOCH));
    }

    @Test
    void stopSetsTheReasonBeforeInterrupting() throws Exception {
        var ctx = context();
        var blocked = new CountDownLatch(1);
        var seen = new CompletableFuture<Optional<StopReason>>();
        Thread handler = Thread.ofVirtual().start(() -> {
            ctx.bind(Thread.currentThread());
            blocked.countDown();
            try {
                Thread.sleep(Duration.ofMinutes(1));
                seen.completeExceptionally(new AssertionError("never interrupted"));
            } catch (InterruptedException e) {
                seen.complete(ctx.stopReason()); // read inside the catch: the reason is already there
            }
            ctx.finished();
        });
        assertTrue(blocked.await(10, TimeUnit.SECONDS));
        assertTrue(ctx.stop(StopReason.LEASE_EXPIRED));
        assertEquals(Optional.of(StopReason.LEASE_EXPIRED), seen.get(10, TimeUnit.SECONDS));
        assertTrue(handler.join(BARRIER));
    }

    @Test
    void theFirstReasonWins() {
        var ctx = context();
        assertFalse(ctx.isStopped());
        assertEquals(Optional.empty(), ctx.stopReason());
        assertTrue(ctx.stop(StopReason.JOB_CANCELLED));
        assertFalse(ctx.stop(StopReason.SHUTTING_DOWN), "the first cause is the true one");
        assertTrue(ctx.isStopped());
        assertEquals(Optional.of(StopReason.JOB_CANCELLED), ctx.stopReason());
    }

    @Test
    void noInterruptReachesTheThreadAfterTheHandlerReturned() throws Exception {
        var ctx = context();
        var returned = new CountDownLatch(1);
        var stopped = new CountDownLatch(1);
        var interrupted = new CompletableFuture<Boolean>();
        Thread handler = Thread.ofVirtual().start(() -> {
            ctx.bind(Thread.currentThread());
            ctx.finished(); // the handler returned
            returned.countDown();
            try {
                assertTrue(stopped.await(10, TimeUnit.SECONDS)); // the confirmation would block here
                interrupted.complete(Thread.currentThread().isInterrupted());
            } catch (InterruptedException e) {
                interrupted.complete(true);
            }
        });
        assertTrue(returned.await(10, TimeUnit.SECONDS));
        ctx.stop(StopReason.LEASE_EXPIRED);
        stopped.countDown();
        assertFalse(interrupted.get(10, TimeUnit.SECONDS), "a late stop must not cancel the Ack's blocking call");
        assertTrue(ctx.isStopped(), "the reason is still recorded");
        assertTrue(handler.join(BARRIER));
    }

    @Test
    void finishingClearsAStaleInterrupt() throws Exception {
        var ctx = context();
        var after = new CompletableFuture<Boolean>();
        Thread handler = Thread.ofVirtual().start(() -> {
            ctx.bind(Thread.currentThread());
            Thread.currentThread().interrupt(); // the handler interrupts itself and returns
            ctx.finished();
            after.complete(Thread.currentThread().isInterrupted());
        });
        assertFalse(after.get(10, TimeUnit.SECONDS));
        assertTrue(handler.join(BARRIER));
    }

    @Test
    void throwIfStoppedThrowsWithTheReason() throws Exception {
        var ctx = context();
        ctx.throwIfStopped(); // not stopped: nothing happens
        ctx.stop(StopReason.JOB_CANCELLED);
        JobStoppedException e = assertThrows(JobStoppedException.class, ctx::throwIfStopped);
        assertEquals(StopReason.JOB_CANCELLED, e.reason());
        assertEquals("worker: job cancelled", e.getMessage(), "Go's cause text");
    }

    /** As a Go handler handed an already-cancelled context: its first blocking call ends at once. */
    @Test
    void aStopBeforeTheHandlerStartsInterruptsItWhenBound() throws Exception {
        var ctx = context();
        ctx.stop(StopReason.SHUTTING_DOWN);
        var result = new CompletableFuture<Boolean>();
        Thread handler = Thread.ofVirtual().start(() -> {
            ctx.bind(Thread.currentThread());
            try {
                Thread.sleep(Duration.ofMinutes(1));
                result.complete(false);
            } catch (InterruptedException e) {
                result.complete(true);
            }
            ctx.finished();
        });
        assertTrue(result.get(10, TimeUnit.SECONDS));
        assertTrue(handler.join(BARRIER));
    }

    @Test
    void finishedFromAnotherThreadIsAnSdkBug() throws Exception {
        var ctx = context();
        var bound = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        Thread handler = Thread.ofVirtual().start(() -> {
            ctx.bind(Thread.currentThread());
            bound.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(bound.await(10, TimeUnit.SECONDS));
        assertThrows(IllegalStateException.class, ctx::finished);
        release.countDown();
        assertTrue(handler.join(BARRIER));
    }

    @Test
    void theReasonsCarryGosCauseTexts() {
        assertEquals("worker: lease lost", StopReason.LEASE_LOST.text());
        assertEquals("worker: lease expired", StopReason.LEASE_EXPIRED.text());
        assertEquals("worker: job cancelled", StopReason.JOB_CANCELLED.text());
        assertEquals("worker: shutting down", StopReason.SHUTTING_DOWN.text());
    }
}

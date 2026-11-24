package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.testing.Gate;
import io.grpc.Context;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Ports of worker/lifecycle_test.go, on the manual clock, plus the Java-only cases. */
class LifecycleTest {
    private static final Duration BARRIER = Duration.ofSeconds(10);
    private static final Duration NOT_YET = Duration.ofMillis(50);

    private final List<Lifecycle> made = new ArrayList<>();
    private final ManualTimeSource time = new ManualTimeSource(1_000_000_000L);

    private Lifecycle lifecycle(Duration timeout, EventLog log) {
        var lc = new Lifecycle(timeout, time, log);
        made.add(lc);
        return lc;
    }

    private Lifecycle lifecycle(Duration timeout) {
        return lifecycle(timeout, new CapturingLog());
    }

    @AfterEach
    void stopAll() {
        made.forEach(Lifecycle::stop);
    }

    private static Lifecycle.Counts counts(int deliveries, int acquiring, int handlers, int confirming) {
        return new Lifecycle.Counts(deliveries, acquiring, handlers, confirming);
    }

    private static final Lifecycle.Counts NONE = counts(0, 0, 0, 0);

    /** Runs await() on a virtual thread; the future completes with its result. */
    private static CompletableFuture<Optional<AbandonedCounts>> awaitAsync(Lifecycle lc) {
        var done = new CompletableFuture<Optional<AbandonedCounts>>();
        Thread.ofVirtual().start(() -> {
            try {
                done.complete(lc.await());
            } catch (Throwable t) {
                done.completeExceptionally(t);
            }
        });
        return done;
    }

    private static void assertNotDone(CompletableFuture<?> f, String why) throws Exception {
        Thread.sleep(NOT_YET);
        assertFalse(f.isDone(), why);
    }

    @Test
    void drainBeginsOnceAndFixesTheDeadline() {
        var lc = lifecycle(Duration.ofMinutes(1));
        assertFalse(lc.isDraining());
        assertTrue(lc.shutdownDeadline().isEmpty(), "there is no deadline before drain begins");

        lc.beginDrain("first");
        long first = lc.shutdownDeadline().orElseThrow();
        assertTrue(lc.isDraining());
        assertEquals(time.nanoTime() + Duration.ofMinutes(1).toNanos(), first);

        time.jump(Duration.ofSeconds(5));
        lc.beginDrain("second");
        assertEquals(first, lc.shutdownDeadline().orElseThrow(),
                "a later caller must not extend a shutdown already running out of time");
    }

    /** Go: TestCallerCancellationBeginsDrain. Here the trigger is beginDrain from another thread. */
    @Test
    void callerShutdownBeginsDrain() throws Exception {
        var lc = lifecycle(Duration.ofMinutes(1));
        assertFalse(lc.isDraining());

        Thread t = Thread.ofVirtual().start(() -> lc.beginDrain("caller cancelled"));
        t.join(BARRIER);
        assertEquals(Optional.empty(), lc.await(), "nothing was in flight, so the drain completes immediately");
        assertTrue(lc.isDraining());
    }

    @Test
    void awaitWaitsForActiveDeliveries() throws Exception {
        var lc = lifecycle(Duration.ofMinutes(1));
        Work wk = lc.begin();
        assertEquals(counts(1, 1, 0, 0), lc.counts());

        var done = awaitAsync(lc);
        assertNotDone(done, "await returned with a delivery still active");

        wk.end();
        assertEquals(Optional.empty(), done.get(BARRIER.toSeconds(), TimeUnit.SECONDS),
                "await did not return after the last delivery finished");
        assertEquals(NONE, lc.counts());
    }

    @Test
    void drainWaitsForDeliveryReceivedBeforeIntakeFinishes() throws Exception {
        var lc = lifecycle(Duration.ofMinutes(1));
        lc.beginDrain("caller cancelled");
        assertFalse(lc.isDrained(), "drain completed before intake finished and could no longer deliver buffered work");

        Work late = lc.begin();
        var done = awaitAsync(lc);
        assertNotDone(done, "await returned while a delivery received during teardown was active");

        late.end();
        assertEquals(Optional.empty(), done.get(BARRIER.toSeconds(), TimeUnit.SECONDS),
                "await did not return after intake ended and the late delivery finished");
    }

    /** A log whose "shutdown started" line blocks until a gate opens. */
    private static final class BlockedStartLog implements EventLog {
        final CapturingLog inner = new CapturingLog();
        final CountDownLatch entered = new CountDownLatch(1);
        final Gate release;

        BlockedStartLog(Gate release) {
            this.release = release;
        }

        @Override
        public boolean enabled(Level level) {
            return true;
        }

        @Override
        public void log(Level level, String msg, Object... keyValues) {
            if (msg.equals(LogMessages.SHUTDOWN_STARTED)) {
                entered.countDown();
                try {
                    release.await(Context.ROOT);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
            inner.log(level, msg, keyValues);
        }

        @Override
        public EventLog with(Object... keyValues) {
            return this;
        }
    }

    @Test
    void shutdownDeadlineIsEnforcedWhileStartLogIsBlocked() throws Exception {
        Duration budget = Duration.ofMillis(50);
        try (var gate = new Gate()) {
            var log = new BlockedStartLog(gate);
            var lc = lifecycle(budget, log);
            lc.begin();
            Thread.ofVirtual().start(() -> lc.beginDrain("caller cancelled"));

            assertTrue(log.entered.await(BARRIER.toSeconds(), TimeUnit.SECONDS), "shutdown-start log was never reached");
            time.awaitTimers(1, BARRIER); // armed before the log line, so it is there already
            time.advance(budget);
            assertTrue(lc.forceContext().isCancelled(), "blocked logging extended the forced-shutdown deadline");
            assertEquals(StopReason.SHUTTING_DOWN.text(), lc.forceContext().cancellationCause().getMessage());
        }
    }

    @Test
    void awaitReportsAbandonmentWithSeparateCounts() throws Exception {
        Duration budget = Duration.ofMillis(50);
        var lc = lifecycle(budget);

        Work acquiring = lc.begin();
        Work running = lc.begin();
        assertTrue(running.acceptHandler());
        Work confirming = lc.begin();
        assertTrue(confirming.acceptHandler());
        confirming.confirming();
        assertEquals(counts(3, 1, 1, 1), lc.counts());

        long start = time.nanoTime();
        var done = awaitAsync(lc);
        time.awaitTimers(1, BARRIER);
        time.advance(budget);
        AbandonedCounts got = done.get(BARRIER.toSeconds(), TimeUnit.SECONDS).orElseThrow();
        assertEquals(budget.toNanos(), time.nanoTime() - start, "abandonment happens at the deadline, not later");

        assertEquals(new AbandonedCounts(3, 1, 1, 1), got);
        String msg = new WorkerFailedException(null, got).getMessage();
        assertTrue(msg.contains("3 active deliveries"), msg);
        assertTrue(msg.contains("1 running handlers"), msg);
        assertTrue(msg.contains("1 acquiring a lease"), msg);
        assertTrue(msg.contains("1 awaiting confirmation"), msg);
        assertTrue(lc.forceContext().isCancelled(), "the deadline cancels everything the SDK owns");
        assertEquals(StopReason.SHUTTING_DOWN.text(), lc.forceContext().cancellationCause().getMessage());

        // The counters survive so the abandoned work can still be logged, and the
        // deliveries can still finish without corrupting them.
        acquiring.end();
        running.end();
        confirming.end();
        assertEquals(NONE, lc.counts());
    }

    @Test
    void acceptHandlerLosesToDrain() {
        var lc = lifecycle(Duration.ofMinutes(1));

        Work before = lc.begin();
        assertTrue(before.acceptHandler(), "acceptance before drain wins and that handler is in flight");

        lc.beginDrain("cancelled");

        Work after = lc.begin();
        assertFalse(after.acceptHandler(), "once drain wins, user code never starts");
        assertEquals(counts(2, 1, 1, 0), lc.counts());

        // The refused delivery is still active work: it holds its token until its
        // unstarted nack finishes.
        after.confirming();
        assertEquals(counts(2, 0, 1, 1), lc.counts());
        after.end();
        before.end();
        assertEquals(NONE, lc.counts());
    }

    @Test
    void workEndsExactlyOnce() {
        var lc = lifecycle(Duration.ofMinutes(1));
        Work first = lc.begin();
        Work second = lc.begin();

        first.end();
        first.end();
        first.end();
        assertEquals(counts(1, 1, 0, 0), lc.counts());

        assertFalse(first.acceptHandler(), "a finished delivery cannot start a handler");
        second.end();
        assertEquals(NONE, lc.counts());
    }

    @Test
    void phaseTransitionsMoveExactlyOneCount() {
        var lc = lifecycle(Duration.ofMinutes(1));
        Work wk = lc.begin();
        assertEquals(counts(1, 1, 0, 0), lc.counts());

        assertTrue(wk.acceptHandler());
        assertEquals(counts(1, 0, 1, 0), lc.counts());

        wk.confirming();
        assertEquals(counts(1, 0, 0, 1), lc.counts());
        wk.confirming();
        assertEquals(counts(1, 0, 0, 1), lc.counts(), "repeating a phase changes nothing");

        wk.end();
        assertEquals(NONE, lc.counts());
    }

    /** Go: TestDrainSignalIsClosedOnce. The listeners are what the drain signal is in Java. */
    @Test
    void drainListenersRunOnce() throws Exception {
        var lc = lifecycle(Duration.ofMinutes(1));
        var runs = new AtomicInteger();
        lc.onDrain(runs::incrementAndGet);
        Thread a = Thread.ofVirtual().start(() -> lc.beginDrain("a"));
        Thread b = Thread.ofVirtual().start(() -> lc.beginDrain("b"));
        lc.beginDrain("c");
        a.join(BARRIER);
        b.join(BARRIER);
        assertEquals(1, runs.get(), "the drain signal fires once");

        var late = new AtomicInteger();
        lc.onDrain(late::incrementAndGet);
        assertEquals(1, late.get(), "a listener added after the drain began sees it at once, as a closed channel would");
    }

    /**
     * Pins the one ordering that used to lose a dropped job. An abandoned delivery decrements its
     * own counters on the way out, and that single decrement both empties the live counts and
     * closes drained. So by the time await looks, a worker that has just dropped a job is
     * indistinguishable from one that shut down perfectly, and await reported success. The
     * verdict has to come from the snapshot taken when the deadline fired.
     */
    @Test
    void abandonmentSurvivesADeliveryUnwindingAtTheDeadline() {
        Duration budget = Duration.ofMillis(20);
        var lc = lifecycle(budget);
        Work w = lc.begin();
        assertTrue(w.acceptHandler());
        w.confirming();
        lc.beginDrain("test");

        // The budget runs out with the confirmation still in flight...
        time.advance(budget);
        assertTrue(lc.forceContext().isCancelled());
        // ... and only then does the delivery unwind.
        w.end();

        Optional<AbandonedCounts> got = lc.await();
        assertTrue(got.isPresent(), "a job was dropped; a clean exit would lie about it");
        assertTrue(new WorkerFailedException(null, got.get()).getMessage().contains("1 awaiting confirmation"));
    }

    // --- Java-only ---

    @Test
    void theDeadlineStopsRegisteredHandlersAndCancelsTheRootContext() {
        Duration budget = Duration.ofSeconds(1);
        var lc = lifecycle(budget);
        var ctx = new HandlerContext(new Job(UUID.randomUUID(), "q", new byte[0], 1, 5, Instant.EPOCH));
        var other = new HandlerContext(new Job(UUID.randomUUID(), "q", new byte[0], 1, 5, Instant.EPOCH));
        lc.register(ctx);
        lc.register(other);
        lc.unregister(other);
        Context child = lc.forceContext().withCancellation();

        lc.beginDrain("test");
        time.advance(budget.minusNanos(1));
        assertFalse(lc.forceContext().isCancelled(), "not before the deadline");
        assertFalse(ctx.isStopped());

        time.advance(Duration.ofNanos(1));
        assertEquals(Optional.of(StopReason.SHUTTING_DOWN), ctx.stopReason());
        assertEquals(Optional.empty(), other.stopReason(), "an unregistered context is left alone");
        assertTrue(lc.forceContext().isCancelled());
        assertTrue(child.isCancelled(), "every RPC descends from the root, so all of them end");
        assertInstanceOf(IllegalStateException.class, lc.forceContext().cancellationCause());
        assertTrue(lc.forced());

        var late = new HandlerContext(new Job(UUID.randomUUID(), "q", new byte[0], 1, 5, Instant.EPOCH));
        lc.register(late);
        assertEquals(Optional.of(StopReason.SHUTTING_DOWN), late.stopReason(),
                "a context registered after the deadline is stopped at once");
    }

    @Test
    void shutdownStartedCarriesIntegerCounts() {
        var log = new CapturingLog();
        var lc = lifecycle(Duration.ofMinutes(1), log);
        lc.begin();
        Work running = lc.begin();
        assertTrue(running.acceptHandler());
        Instant before = Instant.now();

        lc.beginDrain("shutdown requested");
        lc.beginDrain("again");

        assertEquals(1, log.count(LogMessages.SHUTDOWN_STARTED), "logged once");
        Map<String, Object> f = log.fields(LogMessages.SHUTDOWN_STARTED).get(0);
        assertEquals("shutdown requested", f.get("reason"));
        assertEquals("1m0s", f.get("timeout"));
        assertEquals(2, f.get("active_deliveries"));
        assertEquals(1, f.get("running_handlers"));
        Instant deadline = Instant.parse((String) f.get("deadline"));
        assertFalse(deadline.isBefore(before.plus(Duration.ofMinutes(1))), "the deadline is now + timeout, on the wall clock");
        assertTrue(deadline.isBefore(Instant.now().plus(Duration.ofMinutes(1)).plusSeconds(1)));
        assertEquals(List.of("reason", "deadline", "timeout", "active_deliveries", "running_handlers"),
                List.copyOf(f.keySet()));
        assertEquals(EventLog.Level.INFO, log.lines().get(0).level());
    }

    @Test
    void theDeadlineLineNamesTheFourCounts() {
        var log = new CapturingLog();
        Duration budget = Duration.ofSeconds(2);
        var lc = lifecycle(budget, log);
        lc.begin();
        lc.begin();
        Work running = lc.begin();
        assertTrue(running.acceptHandler());
        Work confirming = lc.begin();
        assertTrue(confirming.acceptHandler());
        confirming.confirming();

        lc.beginDrain("test");
        time.advance(budget);

        assertEquals(1, log.count(LogMessages.SHUTDOWN_DEADLINE_REACHED));
        CapturingLog.Line line = log.lines().stream()
                .filter(l -> l.msg().equals(LogMessages.SHUTDOWN_DEADLINE_REACHED)).findFirst().orElseThrow();
        assertEquals(EventLog.Level.WARN, line.level());
        assertEquals(Map.of("active_deliveries", 4, "running_handlers", 1, "acquiring", 2, "confirming", 1),
                line.fields());
    }

    @Test
    void awaitIsUninterruptibleAndRestoresTheFlag() throws Exception {
        var lc = lifecycle(Duration.ofMinutes(1));
        Work wk = lc.begin();
        var flag = new CompletableFuture<Boolean>();
        var result = new CompletableFuture<Optional<AbandonedCounts>>();
        Thread t = Thread.ofVirtual().start(() -> {
            result.complete(lc.await());
            flag.complete(Thread.currentThread().isInterrupted());
        });
        t.interrupt();
        assertNotDone(result, "an interrupt must not end the wait: the deadline bounds it");

        wk.end();
        assertEquals(Optional.empty(), result.get(BARRIER.toSeconds(), TimeUnit.SECONDS));
        assertTrue(flag.get(BARRIER.toSeconds(), TimeUnit.SECONDS), "the interrupt is restored before await returns");
        assertNotEquals(Thread.State.WAITING, t.getState());
    }
}

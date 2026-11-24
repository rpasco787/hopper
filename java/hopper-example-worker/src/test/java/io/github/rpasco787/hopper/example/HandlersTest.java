package io.github.rpasco787.hopper.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.worker.StopReason;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Ports of cmd/worker/main_test.go's handler tests, plus the Java-only payload rules that Go's
 * encoding/json gives cmd/worker for free (null, case-insensitive keys, duplicates, syntax).
 */
class HandlersTest {
    private static final UUID ID = UUID.fromString("7f8d5e0c-4b8a-4c35-9f7a-2d6f0c1b9e11");

    private static Handlers.Work work(String payload, int attempt) {
        return new Handlers.Work(ID, payload.getBytes(StandardCharsets.UTF_8), attempt);
    }

    @Test
    void durationPayloadDefaultsAndBoundaries() throws Exception {
        assertEquals(Duration.ofMillis(100), Handlers.workDuration(work("{}", 1)));
        assertEquals(Duration.ZERO, Handlers.workDuration(work("{\"ms\":0}", 1)));
        assertEquals(Duration.ofMillis(250), Handlers.workDuration(work("{\"ms\":250}", 1)));

        assertTrue(assertThrows(JobFailure.class, () -> Handlers.workDuration(work("{\"ms\":-1}", 1)))
                .getMessage().contains("non-negative"));
        // Go's fixture: one more than MaxInt64 / 1e6 (TestDurationOverflowFixtureFitsInt64).
        assertTrue(assertThrows(JobFailure.class, () -> Handlers.workDuration(work("{\"ms\":9223372036855}", 1)))
                .getMessage().contains("overflows"));
        assertEquals(Duration.ofMillis(9223372036854L), Handlers.workDuration(work("{\"ms\":9223372036854}", 1)));
    }

    @Test
    void sleepAndCpuBurnHonorCancellation() throws Exception {
        record Named(String name, Handlers.Body body) {}
        for (Named h : List.of(new Named("sleep", Handlers::sleep), new Named("cpu", Handlers::cpuBurn))) {
            // Go's case: a context cancelled before the handler starts returns at once.
            StubJobContext before = new StubJobContext();
            before.stop(StopReason.SHUTTING_DOWN, new Thread(() -> {}));
            long t0 = System.nanoTime();
            assertThrows(Exception.class, () -> h.body().run(before, work("{\"ms\":1000}", 1)), h.name());
            assertTrue(System.nanoTime() - t0 < Duration.ofMillis(500).toNanos(), h.name() + " returned at once");

            // The plan's case: a 60 s job on a virtual thread ends within 1 s of being stopped.
            StubJobContext ctx = new StubJobContext();
            AtomicReference<Throwable> thrown = new AtomicReference<>();
            Thread t = Thread.ofVirtual().start(() -> {
                try {
                    h.body().run(ctx, work("{\"ms\":60000}", 1));
                } catch (Throwable e) {
                    thrown.set(e);
                }
            });
            Thread.sleep(100); // let it get into its wait or its loop
            assertTrue(t.isAlive(), h.name() + " was still working");
            long stopped = System.nanoTime();
            ctx.stop(StopReason.LEASE_LOST, t);
            assertTrue(t.join(Duration.ofSeconds(1)), h.name() + " ended within 1 s of being stopped");
            assertTrue(System.nanoTime() - stopped < Duration.ofSeconds(1).toNanos());
            assertTrue(thrown.get() instanceof Exception, h.name() + " failed: " + thrown.get());
        }
    }

    @Test
    void failNTimesUsesDeliveryAttempt() throws Exception {
        assertThrows(JobFailure.class, () -> Handlers.failNTimes(work("{\"fail\":2}", 1)));
        assertThrows(JobFailure.class, () -> Handlers.failNTimes(work("{\"fail\":2}", 2)));
        Handlers.failNTimes(work("{\"fail\":2}", 3));
        Handlers.failNTimes(work("{}", 1));
        assertTrue(assertThrows(JobFailure.class, () -> Handlers.failNTimes(work("{\"fail\":-1}", 1)))
                .getMessage().contains("non-negative"));
    }

    @Test
    void anEmptyPayloadIsAnError() {
        // worker/job.go's Unmarshal text, which every handler reaches first.
        String want = "worker: job " + ID + " has an empty payload";
        assertEquals(want, assertThrows(JobFailure.class, () -> Handlers.workDuration(work("", 1))).getMessage());
        assertEquals(want, assertThrows(JobFailure.class, () -> Handlers.failNTimes(work("", 1))).getMessage());
    }

    @Test
    void aNonIntegerMsIsAnError() {
        for (String payload : List.of("{\"ms\":1.5}", "{\"ms\":\"5\"}", "{\"ms\":1e3}", "{\"ms\":1.0}",
                "{\"ms\":true}", "{\"ms\":[1]}", "{\"ms\":{}}")) {
            JobFailure e = assertThrows(JobFailure.class, () -> Handlers.workDuration(work(payload, 1)), payload);
            assertEquals("worker: unmarshal payload of job " + ID + ": ms must be an integer", e.getMessage(), payload);
        }
        assertEquals("worker: unmarshal payload of job " + ID + ": ms overflows int64",
                assertThrows(JobFailure.class, () -> Handlers.workDuration(work("{\"ms\":9223372036854775808}", 1)))
                        .getMessage());
    }

    // Java-only: what encoding/json does that a tree lookup wouldn't.

    @Test
    void nullAndAbsentMsUseTheDefault() throws Exception {
        for (String payload : List.of("null", " null ", "{\"ms\":null}", "{\"ms\":5,\"ms\":null}", "{\"other\":1}")) {
            assertEquals(Duration.ofMillis(100), Handlers.workDuration(work(payload, 1)), payload);
        }
    }

    @Test
    void keysMatchCaseInsensitivelyAndTheLastOneWins() throws Exception {
        assertEquals(Duration.ofMillis(250), Handlers.workDuration(work("{\"MS\":250}", 1)));
        assertEquals(Duration.ofMillis(3), Handlers.workDuration(work("{\"ms\":1,\"MS\":2,\"ms\":3}", 1)));
        // A null fail leaves the earlier value, as decoding null into an int32 does.
        assertThrows(JobFailure.class, () -> Handlers.failNTimes(work("{\"fail\":2,\"fail\":null}", 2)));
        assertThrows(JobFailure.class, () -> Handlers.failNTimes(work("{\"Fail\":1}", 1)));
    }

    @Test
    void malformedPayloadsAreErrors() {
        for (String payload : List.of("{", "{} x", "  ", "[]", "\"x\"", "5", "{\"ms\":01}", "{'ms':1}")) {
            JobFailure e = assertThrows(JobFailure.class, () -> Handlers.workDuration(work(payload, 1)), payload);
            assertTrue(e.getMessage().startsWith("worker: unmarshal payload of job " + ID + ": "), e.getMessage());
            e = assertThrows(JobFailure.class, () -> Handlers.failNTimes(work(payload, 1)), payload);
            assertTrue(e.getMessage().startsWith("worker: unmarshal payload of job " + ID + ": "), e.getMessage());
        }
        assertEquals("worker: unmarshal payload of job " + ID + ": fail must be an integer",
                assertThrows(JobFailure.class, () -> Handlers.failNTimes(work("{\"fail\":\"2\"}", 1))).getMessage());
        assertEquals("worker: unmarshal payload of job " + ID + ": fail overflows int32",
                assertThrows(JobFailure.class, () -> Handlers.failNTimes(work("{\"fail\":2147483648}", 1))).getMessage());
    }

    @Test
    void failuresReadAsGoErrorText() {
        // The Nack carries the exception's toString(); Go's carries err.Error().
        JobFailure e = assertThrows(JobFailure.class, () -> Handlers.failNTimes(work("{\"fail\":2}", 1)));
        assertEquals("intentional failure on attempt 1 of 2", e.getMessage());
        assertEquals("intentional failure on attempt 1 of 2", e.toString());
    }

    @Test
    void sleepAndCpuBurnRunForTheirDuration() throws Exception {
        for (Handlers.Body body : List.<Handlers.Body>of(Handlers::sleep, Handlers::cpuBurn)) {
            long t0 = System.nanoTime();
            body.run(new StubJobContext(), work("{\"ms\":60}", 1));
            assertTrue(System.nanoTime() - t0 >= Duration.ofMillis(60).toNanos());
            t0 = System.nanoTime();
            body.run(new StubJobContext(), work("{\"ms\":0}", 1));
            assertTrue(System.nanoTime() - t0 < Duration.ofMillis(500).toNanos());
        }
    }

    @Test
    void anInterruptWithoutAStopIsStillAnEnd() {
        // A stray interrupt isn't the SDK's (it sets a reason first), but a handler must not
        // swallow it: both bodies end and say they were interrupted.
        for (Handlers.Body body : List.<Handlers.Body>of(Handlers::sleep, Handlers::cpuBurn)) {
            Thread.currentThread().interrupt();
            Exception e = assertThrows(Exception.class, () -> body.run(new StubJobContext(), work("{\"ms\":60000}", 1)));
            assertInstanceOf(InterruptedException.class, e);
            assertFalse(Thread.interrupted(), "the flag was consumed by the exception, not left set");
        }
    }

    @Test
    void theThreeQueuesAreCmdWorkers() {
        assertEquals(List.of("sleep", "fail_n_times", "cpu_burn"), List.copyOf(Handlers.ALL.keySet()));
    }
}

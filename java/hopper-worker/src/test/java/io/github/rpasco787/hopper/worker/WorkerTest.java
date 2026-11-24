package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Ports of worker/worker_test.go. Not ported: TestJobUnmarshal (Java's Job has no unmarshal;
 * JobTest covers the payload); TestBackoffGrowsJittersAndCaps and TestSanitizeError were ported
 * in Task 10 (BackoffTest, ErrorTextTest).
 */
class WorkerTest {
    private static final JobHandler NOOP = ctx -> {};

    @Test
    void defaultsApplyToZeroValuesOnly() {
        HopperWorker w = HopperWorker.builder("127.0.0.1:9090").handle("q", NOOP).build();
        assertEquals(8, w.concurrency());
        assertEquals(Duration.ofSeconds(30), w.shutdownTimeout());
        assertFalse(w.workerId().isEmpty());
        assertEquals(Duration.ZERO, w.leaseDuration(), "zero lease duration asks the broker to choose");

        HopperWorker set = HopperWorker.builder("127.0.0.1:9090")
                .workerId("w-1").concurrency(3).leaseDuration(Duration.ofMinutes(1)).shutdownTimeout(Duration.ofSeconds(1))
                .handle("q", NOOP).build();
        assertEquals("w-1", set.workerId());
        assertEquals(3, set.concurrency());
        assertEquals(Duration.ofMinutes(1), set.leaseDuration());
        assertEquals(Duration.ofSeconds(1), set.shutdownTimeout());

        HopperWorker zero = HopperWorker.builder("127.0.0.1:9090")
                .concurrency(0).leaseDuration(Duration.ZERO).shutdownTimeout(Duration.ZERO).handle("q", NOOP).build();
        assertEquals(8, zero.concurrency(), "zero is the default, not a value");
        assertEquals(Duration.ofSeconds(30), zero.shutdownTimeout());

        // A negative is not a zero, so it is not defaulted away; build() rejects it.
        assertThrows(IllegalArgumentException.class,
                () -> HopperWorker.builder("127.0.0.1:9090").concurrency(-1).handle("q", NOOP).build());
    }

    private static void rejects(String target, Consumer<HopperWorker.Builder> configure, boolean register, String want) {
        HopperWorker.Builder b = HopperWorker.builder(target);
        configure.accept(b);
        if (register) {
            b.handle("q", NOOP);
        }
        RuntimeException e = assertThrows(RuntimeException.class, b::build);
        assertTrue(e.getMessage().contains(want), "message " + e.getMessage() + " should contain " + want);
    }

    /** Go: TestRunRejectsConfigurationBeforeDialing. Java rejects at build(), before any channel exists. */
    @Test
    void buildRejectsConfigurationBeforeDialing() {
        rejects("", b -> {}, true, "target is required");
        rejects("a:1", b -> b.concurrency(-1), true, "must not be negative");
        rejects("a:1", b -> b.concurrency(HopperWorker.MAX_CONCURRENCY + 1), true, "exceeds the maximum");
        rejects("a:1", b -> b.leaseDuration(Duration.ofSeconds(-1)), true, "lease duration");
        rejects("a:1", b -> b.shutdownTimeout(Duration.ofSeconds(-1)), true, "shutdown timeout");
        rejects("a:1", b -> {}, false, "worker: no handlers registered");

        assertEquals("worker: concurrency -1 must not be negative", assertThrows(IllegalArgumentException.class,
                () -> HopperWorker.builder("a:1").concurrency(-1).handle("q", NOOP).build()).getMessage());
        assertEquals("worker: lease duration -1s must not be negative", assertThrows(IllegalArgumentException.class,
                () -> HopperWorker.builder("a:1").leaseDuration(Duration.ofSeconds(-1)).handle("q", NOOP).build())
                .getMessage());
        assertEquals("worker: no handlers registered",
                assertThrows(IllegalStateException.class, () -> HopperWorker.builder("a:1").build()).getMessage());

        // The maximum itself is allowed; only above it is rejected.
        assertDoesNotThrow(() -> HopperWorker.builder("a:1").concurrency(HopperWorker.MAX_CONCURRENCY).handle("q", NOOP).build());
    }

    /** Go: run with a cancelled context returns at once. Here shutdown() before run() does. */
    @Test
    void runIsSingleUse() {
        HopperWorker w = HopperWorker.builder("a:1").handle("q", NOOP).log(Harness.QUIET).build();
        w.shutdown();
        assertDoesNotThrow(w::run);
        IllegalStateException e = assertThrows(IllegalStateException.class, w::run);
        assertTrue(e.getMessage().contains("already been called"), e.getMessage());
    }

    /** Java's builder is the only place to register, so Go's "Handle after Run" can't happen. */
    @Test
    void handleRejectsMisuse() {
        assertEquals("worker: handle called with an empty queue",
                assertThrows(IllegalArgumentException.class, () -> HopperWorker.builder("a:1").handle("", NOOP))
                        .getMessage());
        assertEquals("worker: handle called with a null queue",
                assertThrows(NullPointerException.class, () -> HopperWorker.builder("a:1").handle(null, NOOP))
                        .getMessage());
        assertEquals("worker: handle called with a null handler for queue \"q\"",
                assertThrows(NullPointerException.class, () -> HopperWorker.builder("a:1").handle("q", null))
                        .getMessage());
        assertEquals("worker: queue \"q\" already has a handler",
                assertThrows(IllegalArgumentException.class,
                        () -> HopperWorker.builder("a:1").handle("q", NOOP).handle("q", NOOP)).getMessage());
        assertEquals("worker: no handlers registered",
                assertThrows(IllegalStateException.class, () -> HopperWorker.builder("a:1").build()).getMessage());
    }

    @Test
    void queuesAreSorted() {
        HopperWorker w = HopperWorker.builder("a:1").handle("zeta", NOOP).handle("alpha", NOOP).handle("mu", NOOP).build();
        assertEquals(List.of("alpha", "mu", "zeta"), w.queues());
    }
}

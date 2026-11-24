package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Ports of worker/logging_test.go, through CapturingLog, and the delivery scope (Java-only). */
class LoggingTest {
    private static final Duration BARRIER = Harness.BARRIER;

    /** Go drives session.accept directly; here a real session and a spy. */
    @Test
    void malformedDeliveryLogsRawLeaseID() {
        var log = new CapturingLog();
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(1).log(log), spy::run, "q");
            try (var run = Harness.runWorker(w)) {
                FakeStream s = fake.awaitStream(BARRIER);
                String badLeaseId = "not-a-uuid";
                var job = Fixture.of("q").proto().toBuilder().setLeaseId(badLeaseId).build();
                s.deliver(job, BARRIER);
                log.await(LogMessages.MALFORMED_DELIVERY, 1, BARRIER);
                Map<String, Object> line = log.fields(LogMessages.MALFORMED_DELIVERY).get(0);
                assertEquals(badLeaseId, line.get("lease_id"));
                assertEquals(job.getId(), line.get("job_id"));
                assertNull(run.stopAndWait());
            }
        }
    }

    @Test
    void handlerFinishedLogsHandlerTime() throws Exception {
        var log = new CapturingLog();
        try (var fake = FakeBroker.start()) {
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).log(log), "q", ctx -> Thread.sleep(35))) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").proto(), BARRIER);
                fake.awaitCalls("Ack", 1, BARRIER);
                assertNull(run.stopAndWait());
            }
            Map<String, Object> line = log.fields(LogMessages.HANDLER_FINISHED).get(0);
            long handlerMs = assertInstanceOf(Long.class, line.get("handler_ms"));
            long durationMs = assertInstanceOf(Long.class, line.get("duration_ms"));
            assertTrue(handlerMs >= 30, "handler_ms " + handlerMs);
            assertTrue(handlerMs <= durationMs, handlerMs + " > " + durationMs);
            assertTrue(line.containsKey("err"));
            assertNull(line.get("err"), "err is null on success");
        }
    }

    /**
     * Across one delivery's lines, every delivery-scoped message carries job_id, lease_id, queue
     * and attempt (docs/design/java-worker-sdk.md, Logs), and no line carries the payload.
     */
    @Test
    void everyDeliveryLineCarriesItsScope() throws Exception {
        var log = new CapturingLog();
        String secret = "hunter2-payload-text";
        Fixture fx = Fixture.of("q").attempt(2).payload(("{\"secret\":\"" + secret + "\"}").getBytes(StandardCharsets.UTF_8));
        try (var fake = FakeBroker.start()) {
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).log(log), "q", ctx -> {})) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(fx.proto(), BARRIER);
                fake.awaitCalls("Ack", 1, BARRIER);
                log.await(LogMessages.JOB_CONFIRMED, 1, BARRIER);
                assertNull(run.stopAndWait());
            }
        }
        Set<String> scoped = Set.of(LogMessages.JOB_STARTED, LogMessages.HANDLER_FINISHED, LogMessages.JOB_CONFIRMED);
        int seen = 0;
        for (CapturingLog.Line line : log.lines()) {
            assertFalse(line.fields().toString().contains(secret), "a payload reached the log: " + line);
            assertFalse(line.msg().contains(secret));
            if (scoped.contains(line.msg())) {
                seen++;
                assertEquals(fx.id().toString(), line.fields().get("job_id"), line.toString());
                assertEquals(fx.leaseId().toString(), line.fields().get("lease_id"), line.toString());
                assertEquals("q", line.fields().get("queue"), line.toString());
                assertEquals(2, line.fields().get("attempt"), line.toString());
                List<String> keys = List.copyOf(line.fields().keySet());
                assertEquals(List.of("worker_id", "job_id", "lease_id", "queue", "attempt"), keys.subList(0, 5),
                        "the scope comes first, after worker_id: " + keys);
            }
        }
        assertEquals(3, seen, "job started, handler finished and job confirmed");
    }
}

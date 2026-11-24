package io.github.rpasco787.hopper.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The client against a real broker with the Java example worker serving it
 * (scripts/java-smoke.sh worker-smoke, Task 20). Skipped unless -PsmokeBroker is set.
 */
@Tag("compose")
class ComposeWorkerSmokeTest {
    private final String prefix = "jsmoke-" + System.currentTimeMillis();
    private HopperClient client;

    @BeforeEach
    void connect() {
        String broker = System.getProperty("hopper.smoke.broker", "");
        Assumptions.assumeFalse(broker.isEmpty(), "no -PsmokeBroker");
        client = HopperClient.connect(broker);
    }

    @AfterEach
    void close() {
        if (client != null) {
            client.close();
        }
    }

    /** Polls getJob every 100 ms until the job is in state, for at most limit. */
    private void awaitState(UUID id, JobState state, Duration limit) throws InterruptedException {
        long end = System.nanoTime() + limit.toNanos();
        JobState last = null;
        while (System.nanoTime() - end < 0) {
            last = client.getJob(id).state();
            if (last == state) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("job " + id + " not " + state + " within " + limit + "; last " + last);
    }

    @Test
    void aCompletedJobCannotBeCancelled() throws InterruptedException {
        JobResult job = client.enqueue(NewJob.builder("sleep").payloadJson("{\"ms\":10}")
                .idempotencyKey(prefix + "-done").build());
        awaitState(job.id(), JobState.COMPLETED, Duration.ofSeconds(30));
        JobTerminalException e = assertThrows(JobTerminalException.class, () -> client.cancel(job.id()));
        assertFalse(e.outcomeUnknown(), "FAILED_PRECONDITION writes nothing");
    }

    @Test
    void aRunningJobThatIsCancelledStopsAndIsNeverRetried() throws InterruptedException, IOException {
        JobResult job = client.enqueue(NewJob.builder("sleep").payloadJson("{\"ms\":20000}")
                .idempotencyKey(prefix + "-cancel").build());
        awaitState(job.id(), JobState.LEASED, Duration.ofSeconds(30));
        CancelResult cancelled = client.cancel(job.id());
        assertEquals(JobState.CANCELLED, cancelled.state());

        String out = System.getProperty("hopper.smoke.out", "");
        if (!out.isEmpty()) {
            Files.writeString(Path.of(out, "cancelled-id.txt"), job.id().toString(), StandardCharsets.UTF_8);
        }
        // Past G/3 = 10 s at Compose's 30 s lease: the worker's first renewal has seen it.
        Thread.sleep(15_000);
        assertEquals(JobState.CANCELLED, client.getJob(job.id()).state(), "a cancelled job is never retried");
    }
}

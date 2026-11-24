package io.github.rpasco787.hopper.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The client against a real broker (scripts/java-smoke.sh client). The queue is one no worker
 * serves, so every job stays where the test puts it. Skipped unless -PsmokeBroker is set.
 */
@Tag("compose")
class ComposeClientSmokeTest {
    private static final String QUEUE = "java-smoke-unserved";

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

    private NewJob keyed(String key) {
        return NewJob.builder(QUEUE).payloadJson("{}").idempotencyKey(prefix + "-" + key).build();
    }

    @Test
    void aKeyedEnqueueIsDeduplicated() {
        JobResult first = client.enqueue(keyed("dedup"));
        JobResult second = client.enqueue(keyed("dedup"));
        assertEquals(first.id(), second.id(), "the same key names the same job");
        assertFalse(first.deduplicated(), "the first enqueue wrote it");
        assertTrue(second.deduplicated(), "the second found it");
        assertEquals(JobState.AVAILABLE, first.state());
        assertEquals(JobState.AVAILABLE, second.state());
    }

    @Test
    void aBatchRetriedWholeWithItsKeysDeduplicates() {
        List<NewJob> batch = List.of(keyed("batch-0"), keyed("batch-1"), keyed("batch-2"));
        List<JobResult> first = client.enqueueBatch(batch);
        List<JobResult> again = client.enqueueBatch(batch);
        assertEquals(3, first.size());
        assertEquals(3, again.size());
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            assertFalse(first.get(i).deduplicated(), "result " + i + " was written by the first batch");
            assertEquals(first.get(i).id(), again.get(i).id(), "result " + i + " is in order and the same job");
            assertTrue(again.get(i).deduplicated(), "result " + i + " deduplicated on the retry");
            ids.add(first.get(i).id());
        }
        assertEquals(3, ids.stream().distinct().count(), "three distinct jobs");
    }

    @Test
    void cancellingAnAvailableJobIsIdempotent() {
        JobResult job = client.enqueue(keyed("cancel"));
        CancelResult first = client.cancel(job.id());
        assertEquals(JobState.CANCELLED, first.state());
        assertFalse(first.alreadyCancelled());
        CancelResult second = client.cancel(job.id());
        assertEquals(JobState.CANCELLED, second.state());
        assertTrue(second.alreadyCancelled());
    }

    @Test
    void getJobOfAMissingJobIsNotFound() {
        assertThrows(JobNotFoundException.class, () -> client.getJob(UUID.randomUUID()));
    }

    @Test
    void aMalformedPayloadIsInvalidAndWritesNothing() {
        NewJob bad = NewJob.builder(QUEUE).payloadJson("{").idempotencyKey(prefix + "-bad").build();
        InvalidRequestException e = assertThrows(InvalidRequestException.class, () -> client.enqueue(bad));
        assertFalse(e.outcomeUnknown(), "INVALID_ARGUMENT means nothing was written");
    }
}

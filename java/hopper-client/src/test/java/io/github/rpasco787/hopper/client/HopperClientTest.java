package io.github.rpasco787.hopper.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.proto.v1.CancelResponse;
import io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse;
import io.github.rpasco787.hopper.proto.v1.EnqueueResponse;
import io.github.rpasco787.hopper.proto.v1.GetJobResponse;
import io.github.rpasco787.hopper.proto.v1.Job;
import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.Gate;
import io.grpc.Status;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The four calls, against the fake broker, through {@code HopperClient.wrap}. */
class HopperClientTest {
    private static final Duration BARRIER = Duration.ofSeconds(10);

    private FakeBroker fake;
    private HopperClient client;

    @BeforeEach
    void start() {
        fake = FakeBroker.start();
        client = HopperClient.wrap(fake.channel());
    }

    @AfterEach
    void stop() {
        fake.close();
    }

    private static Job job(UUID id, String queue, io.github.rpasco787.hopper.proto.v1.JobState state) {
        return Job.newBuilder().setId(id.toString()).setQueue(queue).setState(state).build();
    }

    private static Job job(String id) {
        return Job.newBuilder().setId(id).setState(io.github.rpasco787.hopper.proto.v1.JobState.JOB_STATE_AVAILABLE).build();
    }

    @Test
    void enqueueSendsTheSpecAndReturnsTheResult() {
        UUID id = UUID.randomUUID();
        fake.onEnqueue((request, ctx) -> EnqueueResponse.newBuilder()
                .setJob(job(id, "q", io.github.rpasco787.hopper.proto.v1.JobState.JOB_STATE_AVAILABLE))
                .setDeduplicated(false)
                .build());
        NewJob newJob = NewJob.builder("q").payloadJson("{\"a\":1}").priority(7).idempotencyKey("k").build();

        JobResult result = client.enqueue(newJob);

        assertEquals(1, fake.enqueueRequests().size());
        assertEquals(newJob.toSpec(), fake.enqueueRequests().get(0).getJob());
        assertEquals(new JobResult(id, JobState.AVAILABLE, false), result);
    }

    @Test
    void aDeduplicatedEnqueueSaysSo() {
        UUID id = UUID.randomUUID();
        fake.onEnqueue((request, ctx) -> EnqueueResponse.newBuilder()
                .setJob(job(id, "q", io.github.rpasco787.hopper.proto.v1.JobState.JOB_STATE_LEASED))
                .setDeduplicated(true)
                .build());

        JobResult result = client.enqueue(NewJob.builder("q").idempotencyKey("k").build());

        assertTrue(result.deduplicated());
        assertEquals(JobState.LEASED, result.state());
    }

    @Test
    void enqueueBatchReturnsResultsInRequestOrder() {
        List<UUID> ids = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        fake.onEnqueueBatch((request, ctx) -> {
            EnqueueBatchResponse.Builder response = EnqueueBatchResponse.newBuilder();
            for (UUID id : ids) {
                response.addResults(EnqueueResponse.newBuilder()
                        .setJob(job(id, "q", io.github.rpasco787.hopper.proto.v1.JobState.JOB_STATE_AVAILABLE)));
            }
            return response.build();
        });

        List<JobResult> results = client.enqueueBatch(List.of(
                NewJob.builder("q").priority(1).build(),
                NewJob.builder("q").priority(2).build(),
                NewJob.builder("q").priority(3).build()));

        assertEquals(ids, results.stream().map(JobResult::id).toList());
        assertEquals(List.of(1, 2, 3),
                fake.batchRequests().get(0).getJobsList().stream().map(s -> s.getPriority()).toList(),
                "specs went out in the caller's order");
    }

    /** Port of client_test.go:79 TestEnqueueBatchNamesTheBadItem. */
    @Test
    void enqueueBatchNamesTheNullItem() {
        NullPointerException e = assertThrows(NullPointerException.class,
                () -> client.enqueueBatch(Arrays.asList(NewJob.builder("q").build(), null)));

        assertEquals("jobs[1] is null", e.getMessage());
        assertEquals(0, fake.started("EnqueueBatch"), "nothing went to the network");
    }

    @Test
    void cancelReportsAlreadyCancelled() {
        UUID id = UUID.randomUUID();
        fake.onCancel((request, ctx) -> CancelResponse.newBuilder()
                .setJob(job(id, "q", io.github.rpasco787.hopper.proto.v1.JobState.JOB_STATE_CANCELLED))
                .setAlreadyCancelled(true)
                .build());

        CancelResult result = client.cancel(id);

        assertEquals(new CancelResult(id, JobState.CANCELLED, true), result);
        assertEquals(id.toString(), fake.cancelRequests().get(0).getId());
    }

    @Test
    void cancelOfACompletedJobIsTerminal() {
        fake.onCancel((request, ctx) -> {
            throw Status.FAILED_PRECONDITION.withDescription("job is completed").asException();
        });

        JobTerminalException e = assertThrows(JobTerminalException.class, () -> client.cancel(UUID.randomUUID()));

        assertFalse(e.outcomeUnknown());
        assertEquals(Status.Code.FAILED_PRECONDITION, e.code());
    }

    @Test
    void getJobOfAMissingJobIsNotFound() {
        fake.onGetJob((request, ctx) -> {
            throw Status.NOT_FOUND.withDescription("no such job").asException();
        });

        JobNotFoundException e = assertThrows(JobNotFoundException.class, () -> client.getJob(UUID.randomUUID()));

        assertFalse(e.outcomeUnknown());
    }

    @Test
    void getJobReturnsTheState() {
        UUID id = UUID.randomUUID();
        fake.onGetJob((request, ctx) -> GetJobResponse.newBuilder()
                .setJob(job(id, "q", io.github.rpasco787.hopper.proto.v1.JobState.JOB_STATE_DEAD))
                .build());

        assertEquals(new JobResult(id, JobState.DEAD, false), client.getJob(id));
    }

    /** Go's toResult (client.go:198-204): the broker's id is data we parse, so it can be wrong. */
    @Test
    void aMalformedIdFromTheBrokerIsAnError() {
        fake.onGetJob((request, ctx) -> GetJobResponse.newBuilder().setJob(job("nope")).build());

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> client.getJob(UUID.randomUUID()));

        assertEquals("hopper: broker returned a malformed id \"nope\"", e.getMessage());
    }

    /** docs/semantics.md 395-399: a batch response over the message limit can follow committed writes. */
    @Test
    void resourceExhaustedFromABatchSaysTheOutcomeIsUnknown() {
        fake.onEnqueueBatch((request, ctx) -> {
            throw Status.RESOURCE_EXHAUSTED.withDescription("response too large").asException();
        });

        ResourceExhaustedException e = assertThrows(ResourceExhaustedException.class,
                () -> client.enqueueBatch(List.of(NewJob.builder("q").build())));

        assertTrue(e.outcomeUnknown());
    }

    /** Decision D13: the client retries nothing. The caller decides, and only with an idempotency key. */
    @Test
    void unavailableIsNotRetried() {
        fake.onEnqueue((request, ctx) -> {
            throw Status.UNAVAILABLE.withDescription("the standby has not caught up").asException();
        });

        BrokerUnavailableException e = assertThrows(BrokerUnavailableException.class,
                () -> client.enqueue(NewJob.builder("q").build()));

        assertTrue(e.outcomeUnknown());
        assertEquals(1, fake.started("Enqueue"));
    }

    @Test
    void everyCallHasADeadline() throws Exception {
        BlockingQueue<Boolean> cancelledAtTheBroker = new LinkedBlockingQueue<>();
        try (Gate stuck = new Gate()) {
            fake.onGetJob((request, ctx) -> {
                try {
                    stuck.await(ctx);
                } finally {
                    cancelledAtTheBroker.add(ctx.isCancelled());
                }
                return GetJobResponse.getDefaultInstance();
            });
            HopperClient impatient = HopperClient.wrap(fake.channel(), Duration.ofMillis(200));

            long begun = System.nanoTime();
            DeadlineExceededException e = assertThrows(DeadlineExceededException.class,
                    () -> impatient.getJob(UUID.randomUUID()));
            Duration took = Duration.ofNanos(System.nanoTime() - begun);

            assertTrue(took.compareTo(Duration.ofSeconds(2)) < 0, "took " + took);
            assertFalse(e.outcomeUnknown(), "a read never leaves an unknown outcome");
            Boolean seen = cancelledAtTheBroker.poll(BARRIER.toMillis(), TimeUnit.MILLISECONDS);
            assertNotNull(seen, "the hook never returned: the deadline did not reach the broker");
            assertTrue(seen, "the hook saw its Context cancelled");
        }
    }

    @Test
    void aPerCallTimeoutOverridesTheDefault() {
        try (Gate stuck = new Gate()) {
            fake.onGetJob((request, ctx) -> {
                stuck.await(ctx);
                return GetJobResponse.getDefaultInstance();
            });

            long begun = System.nanoTime();
            assertThrows(DeadlineExceededException.class,
                    () -> client.getJob(UUID.randomUUID(), Duration.ofMillis(200)));
            Duration took = Duration.ofNanos(System.nanoTime() - begun);

            assertTrue(took.compareTo(Duration.ofSeconds(2)) < 0, "took " + took + " against a 30 s default");
        }
    }

    @Test
    void aNonPositiveTimeoutIsRejectedBeforeAnyCall() {
        assertThrows(IllegalArgumentException.class, () -> HopperClient.wrap(fake.channel(), Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> client.getJob(UUID.randomUUID(), Duration.ofSeconds(-1)));
        assertEquals(0, fake.started("GetJob"));
    }
}

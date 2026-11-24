package io.github.rpasco787.hopper.testing;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.github.rpasco787.hopper.proto.v1.Job;
import io.github.rpasco787.hopper.proto.v1.JobState;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * A well-formed leased row, the port of worker/fake_test.go's {@code fixture}.
 * Tests change the fields they care about, so a malformed-delivery case says
 * exactly what is wrong with it.
 */
public final class Fixture {
    private UUID id = UUID.randomUUID();
    private UUID leaseId = UUID.randomUUID();
    private String queue;
    private byte[] payload = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private int attempt = 1;
    private int maxAttempts = 5;
    private Duration grant = Duration.ofSeconds(30);
    private Instant leasedAt = Instant.now();

    private Fixture(String queue) {
        this.queue = queue;
    }

    /** A leased row on {@code queue}: random ids, payload {@code {}}, attempt 1 of 5, a 30 s grant, leased now. */
    public static Fixture of(String queue) {
        return new Fixture(queue);
    }

    public UUID id() {
        return id;
    }

    public UUID leaseId() {
        return leaseId;
    }

    public String queue() {
        return queue;
    }

    public byte[] payload() {
        return payload.clone();
    }

    public int attempt() {
        return attempt;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public Duration grant() {
        return grant;
    }

    public Instant leasedAt() {
        return leasedAt;
    }

    public Fixture id(UUID id) {
        this.id = id;
        return this;
    }

    /** {@code null} leaves {@code lease_id} unset on the wire, the malformed case. */
    public Fixture leaseId(UUID leaseId) {
        this.leaseId = leaseId;
        return this;
    }

    public Fixture queue(String queue) {
        this.queue = queue;
        return this;
    }

    public Fixture payload(byte[] payload) {
        this.payload = payload.clone();
        return this;
    }

    public Fixture attempt(int attempt) {
        this.attempt = attempt;
        return this;
    }

    public Fixture maxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
        return this;
    }

    public Fixture grant(Duration grant) {
        this.grant = grant;
        return this;
    }

    public Fixture leasedAt(Instant leasedAt) {
        this.leasedAt = leasedAt;
        return this;
    }

    /** The row the way the broker's own conversion would render it. */
    public Job proto() {
        Timestamp leased = timestamp(leasedAt);
        Job.Builder job = Job.newBuilder()
                .setId(id.toString())
                .setQueue(queue)
                .setPayload(ByteString.copyFrom(payload))
                .setState(JobState.JOB_STATE_LEASED)
                .setAttempts(attempt)
                .setMaxAttempts(maxAttempts)
                .setLeasedAt(leased)
                .setLeaseExpires(timestamp(leasedAt.plus(grant)))
                .setRunAt(leased)
                .setCreatedAt(leased)
                .setUpdatedAt(leased);
        if (leaseId != null) {
            job.setLeaseId(leaseId.toString());
        }
        return job.build();
    }

    static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }
}

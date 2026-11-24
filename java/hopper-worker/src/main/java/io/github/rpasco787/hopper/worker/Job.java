package io.github.rpasco787.hopper.worker;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * One delivery, as a handler sees it. Immutable. It deliberately omits the lease token: the SDK
 * owns the lease (worker/job.go). {@link #attempt()} counts deliveries from 1; it is not an
 * idempotency key, since two deliveries of one job differ in it. Use {@link #id()}, or a
 * business key in the payload.
 */
public final class Job {
    private final UUID id;
    private final String queue;
    private final byte[] payload;
    private final int attempt;
    private final int maxAttempts;
    private final Instant enqueuedAt;

    Job(UUID id, String queue, byte[] payload, int attempt, int maxAttempts, Instant enqueuedAt) {
        this.id = id;
        this.queue = queue;
        this.payload = payload.clone();
        this.attempt = attempt;
        this.maxAttempts = maxAttempts;
        this.enqueuedAt = enqueuedAt;
    }

    public UUID id() {
        return id;
    }

    public String queue() {
        return queue;
    }

    /** The JSON the producer enqueued, as stored; a copy. */
    public byte[] payload() {
        return payload.clone();
    }

    /** The payload decoded as UTF-8. */
    public String payloadUtf8() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    /** Which delivery this is, from 1. attempt == maxAttempts means a failure sends it to dead. */
    public int attempt() {
        return attempt;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    /** When the row was created, not when this delivery started. */
    public Instant enqueuedAt() {
        return enqueuedAt;
    }

    /** Names the job, never its payload (docs/semantics.md §14). */
    @Override
    public String toString() {
        return "Job[id=" + id + ", queue=" + queue + ", attempt=" + attempt + "/" + maxAttempts + "]";
    }
}

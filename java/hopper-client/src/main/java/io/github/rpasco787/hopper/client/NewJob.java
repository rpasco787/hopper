package io.github.rpasco787.hopper.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.github.rpasco787.hopper.proto.v1.JobSpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * A job to enqueue: what a producer sends. Immutable; build one with
 * {@link #builder(String)}.
 *
 * <p>The payload is raw JSON, sent as given (Decision D33): the client has no JSON
 * dependency and does not re-encode. The broker checks the syntax.
 */
public final class NewJob {
    private static final int PRIORITY_MIN = Short.MIN_VALUE;
    private static final int PRIORITY_MAX = Short.MAX_VALUE;

    private final JobSpec spec;

    private NewJob(JobSpec spec) {
        this.spec = spec;
    }

    public static Builder builder(String queue) {
        return new Builder(queue);
    }

    /** The wire form. Package-private: proto types stay out of calling code. */
    JobSpec toSpec() {
        return spec;
    }

    public String queue() {
        return spec.getQueue();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof NewJob job && spec.equals(job.spec);
    }

    @Override
    public int hashCode() {
        return spec.hashCode();
    }

    @Override
    public String toString() {
        // The payload is the caller's data; it stays out of messages and logs.
        return "NewJob[queue=" + spec.getQueue() + ", priority=" + spec.getPriority() + "]";
    }

    public static final class Builder {
        private final String queue;
        private byte[] payload = new byte[0];
        private int priority;
        private Instant runAt;
        private int maxAttempts;
        private String idempotencyKey;

        private Builder(String queue) {
            this.queue = queue;
        }

        /** Raw JSON, as UTF-8. Null sends no payload, which the broker stores as {@code {}}. */
        public Builder payloadJson(String json) {
            this.payload = json == null ? new byte[0] : json.getBytes(StandardCharsets.UTF_8);
            return this;
        }

        /** Raw JSON bytes, copied. Null sends no payload, which the broker stores as {@code {}}. */
        public Builder payloadJson(byte[] json) {
            this.payload = json == null ? new byte[0] : json.clone();
            return this;
        }

        /** Higher runs first. Must fit 16 bits, as the broker's column does. Default 0. */
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        /** Not before this instant. Unset means now. */
        public Builder runAt(Instant runAt) {
            this.runAt = runAt;
            return this;
        }

        /** Zero takes the broker's default. */
        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        /**
         * Unset means no deduplication. Set it and an enqueue becomes safe to retry: the
         * second call returns the first job with {@code deduplicated} set.
         */
        public Builder idempotencyKey(String key) {
            this.idempotencyKey = key;
            return this;
        }

        /** @throws IllegalArgumentException if the queue is missing, the priority does not fit 16 bits, or maxAttempts is negative */
        public NewJob build() {
            if (queue == null || queue.isEmpty()) {
                throw new IllegalArgumentException("queue is required");
            }
            if (priority < PRIORITY_MIN || priority > PRIORITY_MAX) {
                throw new IllegalArgumentException(
                        "priority " + priority + " does not fit 16 bits (" + PRIORITY_MIN + ".." + PRIORITY_MAX + ")");
            }
            if (maxAttempts < 0) {
                throw new IllegalArgumentException("maxAttempts must not be negative, got " + maxAttempts);
            }
            JobSpec.Builder spec = JobSpec.newBuilder()
                    .setQueue(queue)
                    .setPayload(ByteString.copyFrom(payload))
                    .setPriority(priority)
                    .setMaxAttempts(maxAttempts);
            if (runAt != null) {
                spec.setRunAt(Timestamp.newBuilder().setSeconds(runAt.getEpochSecond()).setNanos(runAt.getNano()));
            }
            if (idempotencyKey != null && !idempotencyKey.isEmpty()) {
                spec.setIdempotencyKey(idempotencyKey);
            }
            return new NewJob(spec.build());
        }
    }
}

package io.github.rpasco787.hopper.client;

import io.github.rpasco787.hopper.proto.v1.CancelRequest;
import io.github.rpasco787.hopper.proto.v1.CancelResponse;
import io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest;
import io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse;
import io.github.rpasco787.hopper.proto.v1.EnqueueRequest;
import io.github.rpasco787.hopper.proto.v1.EnqueueResponse;
import io.github.rpasco787.hopper.proto.v1.GetJobRequest;
import io.github.rpasco787.hopper.proto.v1.GetJobResponse;
import io.github.rpasco787.hopper.proto.v1.HopperServiceGrpc;
import io.github.rpasco787.hopper.proto.v1.Job;
import io.github.rpasco787.hopper.proto.v1.JobSpec;
import io.github.rpasco787.hopper.proto.ChannelDefaults;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * A producer-side client for the broker: enqueue, enqueue a batch, cancel and read
 * a job, and nothing a worker needs. The counterpart of client/client.go. It keeps
 * proto types, base64 and raw status codes out of calling code.
 *
 * <p><b>Calls block, and are meant for virtual threads.</b> Each one runs under a
 * deadline: the client's call timeout, or the {@code Duration} you pass. Java has
 * no {@code context.Context} to carry one, and a call with none would wait on a
 * stuck broker until keepalive gave up. The deadline travels to the broker, so a
 * call that times out is cancelled there too.
 *
 * <p><b>Nothing is retried for you</b> (docs/semantics.md §8). A failed write can be
 * ambiguous: see {@link HopperException#outcomeUnknown()}. An {@link #enqueue} with
 * an idempotency key is safe to retry whenever the outcome is unknown; without one
 * it is not. {@link #enqueueBatch} is not atomic: retry a failed batch whole, with
 * the same stable per-item keys, and never resend an unkeyed batch blindly.
 * {@link #cancel} is idempotent at the broker.
 *
 * <p>Thread-safe.
 */
public final class HopperClient implements AutoCloseable {
    /** What a call waits when it is not given its own timeout. */
    static final Duration DEFAULT_CALL_TIMEOUT = Duration.ofSeconds(30);

    /** How long close() lets calls in flight finish before cancelling them. */
    private static final Duration CLOSE_GRACE = Duration.ofSeconds(5);

    private final HopperServiceGrpc.HopperServiceBlockingStub stub;
    private final Duration callTimeout;
    /** The channel this client dialled and must close; null for a wrapped one. */
    private final ManagedChannel owned;

    private HopperClient(Channel channel, Duration callTimeout, ManagedChannel owned) {
        this.stub = HopperServiceGrpc.newBlockingStub(channel);
        this.callTimeout = callTimeout;
        this.owned = owned;
    }

    /**
     * Connects to the brokers {@code target} resolves to, in plaintext, with 30 s calls.
     * A name that resolves to several addresses gets a connection to each, and unary
     * calls rotate across them. The connection is lazy: this does not fail because the
     * broker is down, the first call does, which is what you want in a producer that
     * starts before its broker does. The client owns the channel; {@link #close()} shuts it down.
     */
    public static HopperClient connect(String target) {
        return builder(target).build();
    }

    /** Options for a client that dials its own channel. */
    public static Builder builder(String target) {
        return new Builder(target);
    }

    public static final class Builder {
        private final String target;
        private ChannelCredentials credentials = InsecureChannelCredentials.create();
        private Duration callTimeout = DEFAULT_CALL_TIMEOUT;

        private Builder(String target) {
            this.target = Objects.requireNonNull(target, "target");
        }

        /**
         * Replaces the plaintext default (the broker is a trusted-network prototype that
         * serves plaintext), for example with {@code TlsChannelCredentials.create()}.
         */
        public Builder credentials(ChannelCredentials credentials) {
            this.credentials = Objects.requireNonNull(credentials, "credentials");
            return this;
        }

        /** What a call waits when it is not given its own timeout. Default 30 s. */
        public Builder callTimeout(Duration callTimeout) {
            this.callTimeout = requirePositive(callTimeout);
            return this;
        }

        /** Dials the target with {@link ChannelDefaults}. */
        public HopperClient build() {
            ManagedChannel channel = ChannelDefaults.open(target, credentials, b -> {});
            return new HopperClient(channel, callTimeout, channel);
        }
    }

    /**
     * A client over a channel the caller owns: {@link #close()} leaves it open.
     * Calls take 30 s unless given their own timeout.
     */
    public static HopperClient wrap(Channel channel) {
        return wrap(channel, DEFAULT_CALL_TIMEOUT);
    }

    /** As {@link #wrap(Channel)}, with {@code callTimeout} for calls that are not given their own. */
    public static HopperClient wrap(Channel channel, Duration callTimeout) {
        Objects.requireNonNull(channel, "channel");
        return new HopperClient(channel, requirePositive(callTimeout), null);
    }

    private static Duration requirePositive(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive, got " + timeout);
        }
        return timeout;
    }

    private HopperServiceGrpc.HopperServiceBlockingStub within(Duration timeout) {
        return stub.withDeadlineAfter(requirePositive(timeout).toNanos(), TimeUnit.NANOSECONDS);
    }

    // enqueue

    /**
     * Adds one job. There is no automatic retry: a failure can be ambiguous (the write
     * may have committed), so retrying is the caller's decision, and it is only safe
     * with an idempotency key.
     */
    public JobResult enqueue(NewJob job) {
        return enqueue(job, callTimeout);
    }

    public JobResult enqueue(NewJob job, Duration timeout) {
        Objects.requireNonNull(job, "job");
        EnqueueResponse response;
        try {
            response = within(timeout).enqueue(EnqueueRequest.newBuilder().setJob(job.toSpec()).build());
        } catch (StatusRuntimeException e) {
            throw Errors.wrap("enqueue", e, true);
        }
        return toResult(response.getJob(), response.getDeduplicated());
    }

    // enqueue batch

    /**
     * Adds jobs in one call. It is <b>not atomic</b>: a failure can leave committed
     * jobs, and stable idempotency keys deduplicate a retry while the original rows
     * still exist. Results are in request order.
     *
     * @throws NullPointerException naming the entry, before anything is sent, if one is null
     */
    public List<JobResult> enqueueBatch(List<NewJob> jobs) {
        return enqueueBatch(jobs, callTimeout);
    }

    public List<JobResult> enqueueBatch(List<NewJob> jobs, Duration timeout) {
        Objects.requireNonNull(jobs, "jobs");
        List<JobSpec> specs = new ArrayList<>(jobs.size());
        for (int i = 0; i < jobs.size(); i++) {
            specs.add(Objects.requireNonNull(jobs.get(i), "jobs[" + i + "] is null").toSpec());
        }
        EnqueueBatchResponse response;
        try {
            response = within(timeout).enqueueBatch(EnqueueBatchRequest.newBuilder().addAllJobs(specs).build());
        } catch (StatusRuntimeException e) {
            throw Errors.wrap("enqueue batch", e, true);
        }
        List<JobResult> results = new ArrayList<>(response.getResultsCount());
        for (EnqueueResponse result : response.getResultsList()) {
            results.add(toResult(result.getJob(), result.getDeduplicated()));
        }
        return List.copyOf(results);
    }

    // cancel

    /**
     * Cancels a job. Idempotent: cancelling an already-cancelled job succeeds, and says
     * so. A completed or dead job throws {@link JobTerminalException}.
     */
    public CancelResult cancel(UUID id) {
        return cancel(id, callTimeout);
    }

    public CancelResult cancel(UUID id, Duration timeout) {
        Objects.requireNonNull(id, "id");
        CancelResponse response;
        try {
            response = within(timeout).cancel(CancelRequest.newBuilder().setId(id.toString()).build());
        } catch (StatusRuntimeException e) {
            throw Errors.wrap("cancel", e, true);
        }
        JobResult job = toResult(response.getJob(), false);
        return new CancelResult(job.id(), job.state(), response.getAlreadyCancelled());
    }

    // get job

    /** Reads a job. A read: its failure never leaves an unknown outcome. */
    public JobResult getJob(UUID id) {
        return getJob(id, callTimeout);
    }

    public JobResult getJob(UUID id, Duration timeout) {
        Objects.requireNonNull(id, "id");
        GetJobResponse response;
        try {
            response = within(timeout).getJob(GetJobRequest.newBuilder().setId(id.toString()).build());
        } catch (StatusRuntimeException e) {
            throw Errors.wrap("get job", e, false);
        }
        return toResult(response.getJob(), false);
    }

    /**
     * The broker's id is data we parse, so it can be wrong; that is a broker bug,
     * not a failed call, so it is not a {@link HopperException}.
     */
    private static JobResult toResult(Job job, boolean deduplicated) {
        UUID id;
        try {
            id = UUID.fromString(job.getId());
            // UUID.fromString is lenient ("1-1-1-1-1"); the broker only ever sends the canonical form.
            if (!id.toString().equalsIgnoreCase(job.getId())) {
                throw new IllegalArgumentException("not in canonical form");
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("hopper: broker returned a malformed id \"" + job.getId() + "\"", e);
        }
        return new JobResult(id, JobState.fromProto(job.getState()), deduplicated);
    }

    /**
     * Shuts down the channel this client dialled: it stops accepting calls, waits up to
     * 5 s for those in flight, then cancels them. Does nothing for a client made with
     * {@link #wrap(Channel)}, whose channel belongs to the caller. If the thread is
     * interrupted while waiting, the in-flight calls are cancelled at once and the
     * interrupt is kept.
     */
    @Override
    public void close() {
        if (owned == null) {
            return;
        }
        owned.shutdown();
        try {
            if (!owned.awaitTermination(CLOSE_GRACE.toNanos(), TimeUnit.NANOSECONDS)) {
                owned.shutdownNow();
            }
        } catch (InterruptedException e) {
            owned.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

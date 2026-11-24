package io.github.rpasco787.hopper.worker;

import io.grpc.Status;
import java.util.Optional;

/**
 * Thrown by {@link HopperWorker#run()} when the worker stopped for a permanent stream failure,
 * or when the shutdown deadline passed with work in flight, or both. Neither hides the other,
 * as errors.Join keeps both in the Go SDK: a permanent failure explains why the worker stopped,
 * and an abandonment says what it left behind.
 */
public final class WorkerFailedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final transient Status permanentFailure;
    private final transient AbandonedCounts abandoned;

    /** At least one of the two is non-null. */
    WorkerFailedException(Status permanentFailure, AbandonedCounts abandoned) {
        super(message(permanentFailure, abandoned),
                permanentFailure == null ? null : permanentFailure.asRuntimeException());
        this.permanentFailure = permanentFailure;
        this.abandoned = abandoned;
    }

    /** The status that ended the lease stream for good: a configuration or authentication refusal. */
    public Optional<Status> permanentFailure() {
        return Optional.ofNullable(permanentFailure);
    }

    /** What the shutdown deadline abandoned, if it fired with work in flight. */
    public Optional<AbandonedCounts> abandoned() {
        return Optional.ofNullable(abandoned);
    }

    /** The Go SDK's texts, one per line, the stream failure first (errors.Join's order). */
    private static String message(Status permanentFailure, AbandonedCounts abandoned) {
        StringBuilder b = new StringBuilder();
        if (permanentFailure != null) {
            b.append(streamFailure(permanentFailure));
        }
        if (abandoned != null) {
            if (b.length() > 0) {
                b.append('\n');
            }
            b.append("worker: shutdown deadline reached with ").append(abandoned.deliveries())
                    .append(" active deliveries (").append(abandoned.handlers()).append(" running handlers, ")
                    .append(abandoned.acquiring()).append(" acquiring a lease, ")
                    .append(abandoned.confirming()).append(" awaiting confirmation)");
        }
        return b.toString();
    }

    /** How a stream's status reads in logs and messages: worker: lease stream: CODE: description. */
    static String streamFailure(Status s) {
        String text = "worker: lease stream: " + s.getCode();
        return s.getDescription() == null ? text : text + ": " + s.getDescription();
    }
}

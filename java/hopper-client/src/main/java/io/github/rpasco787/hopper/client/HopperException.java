package io.github.rpasco787.hopper.client;

import io.grpc.Status;

/**
 * A call to the broker failed. The original {@code StatusRuntimeException} is the
 * cause, so nothing is lost; this class says what a producer needs to decide
 * what to do next.
 *
 * <p>The client never retries for you (docs/semantics.md §8). {@link #outcomeUnknown()}
 * is how it tells you whether a retry is a question at all:
 * <ul>
 *   <li>{@code false}: the call did not take effect, or it was a read. Retrying is safe.
 *   <li>{@code true}: the broker may have committed the write before the failure.
 *       Retry an {@code enqueue} only with an idempotency key, and never resend an
 *       unkeyed batch.
 * </ul>
 *
 * <p>The more specific subclasses exist for the codes a producer branches on.
 * Any other code, INTERNAL included, is a plain {@code HopperException}.
 */
public class HopperException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final Status.Code code;
    private final boolean outcomeUnknown;

    HopperException(String message, Status.Code code, boolean outcomeUnknown, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.outcomeUnknown = outcomeUnknown;
    }

    /** The gRPC status code the broker (or the transport) answered with. */
    public Status.Code code() {
        return code;
    }

    /** Whether the call may have taken effect although it failed. Always false for reads. */
    public boolean outcomeUnknown() {
        return outcomeUnknown;
    }
}

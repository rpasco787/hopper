package io.github.rpasco787.hopper.client;

import io.grpc.Status;

/** The call ran out of time (DEADLINE_EXCEEDED). For a write the outcome is unknown. */
public final class DeadlineExceededException extends HopperException {
    private static final long serialVersionUID = 1L;

    DeadlineExceededException(String message, Status.Code code, boolean outcomeUnknown, Throwable cause) {
        super(message, code, outcomeUnknown, cause);
    }
}

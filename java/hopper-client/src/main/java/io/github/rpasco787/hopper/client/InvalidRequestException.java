package io.github.rpasco787.hopper.client;

import io.grpc.Status;

/** The broker rejected the request (INVALID_ARGUMENT). Validation writes nothing, so the outcome is never unknown. */
public final class InvalidRequestException extends HopperException {
    private static final long serialVersionUID = 1L;

    InvalidRequestException(String message, Status.Code code, boolean outcomeUnknown, Throwable cause) {
        super(message, code, outcomeUnknown, cause);
    }
}

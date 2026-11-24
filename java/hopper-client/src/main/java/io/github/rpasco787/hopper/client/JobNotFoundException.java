package io.github.rpasco787.hopper.client;

import io.grpc.Status;

/** No such job (NOT_FOUND). The counterpart of the Go client's ErrNotFound. */
public final class JobNotFoundException extends HopperException {
    private static final long serialVersionUID = 1L;

    JobNotFoundException(String message, Status.Code code, boolean outcomeUnknown, Throwable cause) {
        super(message, code, outcomeUnknown, cause);
    }
}

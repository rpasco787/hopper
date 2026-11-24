package io.github.rpasco787.hopper.client;

import io.grpc.Status;

/** The job is already in a terminal state (FAILED_PRECONDITION): for example, cancelling a completed or dead job. The counterpart of the Go client's ErrTerminal. */
public final class JobTerminalException extends HopperException {
    private static final long serialVersionUID = 1L;

    JobTerminalException(String message, Status.Code code, boolean outcomeUnknown, Throwable cause) {
        super(message, code, outcomeUnknown, cause);
    }
}

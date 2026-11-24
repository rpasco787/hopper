package io.github.rpasco787.hopper.client;

import io.grpc.Status;

/** No broker answered (UNAVAILABLE). The call may have reached one and taken effect, so for a write the outcome is unknown. */
public final class BrokerUnavailableException extends HopperException {
    private static final long serialVersionUID = 1L;

    BrokerUnavailableException(String message, Status.Code code, boolean outcomeUnknown, Throwable cause) {
        super(message, code, outcomeUnknown, cause);
    }
}

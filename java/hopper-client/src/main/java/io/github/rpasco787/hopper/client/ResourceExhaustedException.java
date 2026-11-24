package io.github.rpasco787.hopper.client;

import io.grpc.Status;

/** RESOURCE_EXHAUSTED. For a batch the outcome is unknown: a response over the 4 MiB message limit can follow committed writes (docs/semantics.md §8). */
public final class ResourceExhaustedException extends HopperException {
    private static final long serialVersionUID = 1L;

    ResourceExhaustedException(String message, Status.Code code, boolean outcomeUnknown, Throwable cause) {
        super(message, code, outcomeUnknown, cause);
    }
}

package io.github.rpasco787.hopper.client;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Turns a status back into an exception a caller can branch on, keeping the
 * status underneath for anyone who wants it. The Java counterpart of
 * client.go's {@code wrap}, plus the one thing Go's errors do not carry:
 * whether a failed write may still have happened (see the table in
 * docs/design/java-producer-client.md).
 */
final class Errors {
    private Errors() {}

    /**
     * @param op the operation, as the message names it: {@code enqueue}, {@code enqueue batch},
     *     {@code cancel} or {@code get job}
     * @param mutating whether the call writes. A read's outcome is never unknown.
     */
    static HopperException wrap(String op, StatusRuntimeException e, boolean mutating) {
        Status status = e.getStatus();
        Status.Code code = status.getCode();
        String description = status.getDescription();
        String message = "hopper: " + op + ": " + code + (description == null ? "" : ": " + description);

        // A blocking call whose thread was interrupted is cancelled with the
        // InterruptedException as its cause. Whatever swallowed the interrupt on
        // the way up, the caller must still see it.
        if (e.getCause() instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }

        return switch (code) {
            // Broker validation writes nothing (docs/semantics.md §8).
            case INVALID_ARGUMENT -> new InvalidRequestException(message, code, false, e);
            case NOT_FOUND -> new JobNotFoundException(message, code, false, e);
            case FAILED_PRECONDITION -> new JobTerminalException(message, code, false, e);
            // A response over 4 MiB can follow committed writes (§8).
            case RESOURCE_EXHAUSTED -> new ResourceExhaustedException(message, code, mutating, e);
            // The call may have taken effect (§5).
            case UNAVAILABLE -> new BrokerUnavailableException(message, code, mutating, e);
            case DEADLINE_EXCEEDED -> new DeadlineExceededException(message, code, mutating, e);
            // Refused before the broker looked at the request.
            case UNIMPLEMENTED, UNAUTHENTICATED, PERMISSION_DENIED -> new HopperException(message, code, false, e);
            // INTERNAL, CANCELLED, UNKNOWN and the rest: nothing says it did not happen.
            default -> new HopperException(message, code, mutating, e);
        };
    }
}

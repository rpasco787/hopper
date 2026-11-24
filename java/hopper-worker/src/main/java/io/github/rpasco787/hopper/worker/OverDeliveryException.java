package io.github.rpasco787.hopper.worker;

/**
 * Ends a stream that sent more jobs than it was given credit for (worker/stream.go
 * errOverDelivery, Decision D32). The excess job is not started and not counted: the
 * alternative is to manufacture a token, which would break the one bound the worker promises.
 */
final class OverDeliveryException extends Exception {
    private static final long serialVersionUID = 1L;

    static final String TEXT = "worker: the broker exceeded the credit it was given";

    OverDeliveryException(String detail) {
        super(TEXT + ": " + detail);
    }
}

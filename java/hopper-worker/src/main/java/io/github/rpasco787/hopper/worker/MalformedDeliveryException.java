package io.github.rpasco787.hopper.worker;

/** A leased row the SDK can't vouch for (worker/stream.go parseDelivery); its message is Go's. */
final class MalformedDeliveryException extends Exception {
    private static final long serialVersionUID = 1L;

    MalformedDeliveryException(String message) {
        super(message);
    }
}

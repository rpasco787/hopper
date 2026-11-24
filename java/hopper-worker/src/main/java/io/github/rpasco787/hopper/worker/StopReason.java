package io.github.rpasco787.hopper.worker;

/** Why the SDK stopped a handler: the counterparts of the Go SDK's context causes. */
public enum StopReason {
    /** A Heartbeat was refused ABORTED, NOT_FOUND or otherwise: someone else may hold it now. */
    LEASE_LOST("worker: lease lost"),
    /** The SDK's own conservative deadline passed, concluded without asking the broker. */
    LEASE_EXPIRED("worker: lease expired"),
    /** A Heartbeat answered FAILED_PRECONDITION: a client cancelled the job. */
    JOB_CANCELLED("worker: job cancelled"),
    /** The shutdown deadline fired. */
    SHUTTING_DOWN("worker: shutting down");

    private final String text;

    StopReason(String text) {
        this.text = text;
    }

    /** The Go SDK's error text for this cause (worker/errors.go), as logs and Nacks carry it. */
    String text() {
        return text;
    }
}

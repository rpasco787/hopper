package io.github.rpasco787.hopper.worker;

/** Thrown by {@link JobContext#throwIfStopped()}: the SDK stopped this handler, for {@link #reason()}. */
public final class JobStoppedException extends Exception {
    private static final long serialVersionUID = 1L;

    private final StopReason reason;

    JobStoppedException(StopReason reason) {
        super(reason.text());
        this.reason = reason;
    }

    public StopReason reason() {
        return reason;
    }
}

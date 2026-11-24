package io.github.rpasco787.hopper.example;

import io.github.rpasco787.hopper.worker.Job;
import io.github.rpasco787.hopper.worker.JobContext;
import io.github.rpasco787.hopper.worker.StopReason;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A JobContext a test can stop, the way the SDK does: the reason first, then an interrupt of
 * the handler's thread (docs/design/java-worker-sdk.md, "Stopping a handler").
 *
 * <p>It carries no {@link Job}: Job's constructor is the SDK's own (sealed package), so
 * {@link Handlers} takes the job's values as a {@link Handlers.Work} and uses the context only
 * to learn that it was stopped. For the same reason {@link #throwIfStopped()} can't make a
 * JobStoppedException, and throws an unchecked one instead.
 */
final class StubJobContext implements JobContext {
    private final AtomicReference<StopReason> reason = new AtomicReference<>();

    /** Sets the reason, then interrupts {@code handler}, in the SDK's order. */
    void stop(StopReason why, Thread handler) {
        reason.set(why);
        handler.interrupt();
    }

    @Override
    public Job job() {
        throw new UnsupportedOperationException("the stub carries no Job; Handlers takes a Work");
    }

    @Override
    public boolean isStopped() {
        return reason.get() != null;
    }

    @Override
    public Optional<StopReason> stopReason() {
        return Optional.ofNullable(reason.get());
    }

    @Override
    public void throwIfStopped() {
        StopReason why = reason.get();
        if (why != null) {
            throw new IllegalStateException("stopped: " + why);
        }
    }
}

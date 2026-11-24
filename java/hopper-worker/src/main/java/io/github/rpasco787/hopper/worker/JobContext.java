package io.github.rpasco787.hopper.worker;

import java.util.Optional;

/**
 * What a handler gets: its job, and whether the SDK has stopped it. The SDK stops a handler by
 * setting a reason first and then interrupting its thread, so a handler that catches
 * InterruptedException can read why. A handler that ignores both interruption and
 * {@link #isStopped()} can't be stopped; it keeps its concurrency slot until it returns, and
 * its job may meanwhile be redelivered elsewhere.
 */
public interface JobContext {
    Job job();

    boolean isStopped();

    Optional<StopReason> stopReason();

    /** @throws JobStoppedException if the SDK has stopped this handler */
    void throwIfStopped() throws JobStoppedException;
}

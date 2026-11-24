package io.github.rpasco787.hopper.worker;

/**
 * Every clock reading and every wait in the SDK (docs/design/java-worker-sdk.md, Time).
 * Instants are {@code nanoTime()} readings, compared only by subtracting one from another.
 * Production uses {@link SystemTimeSource}; tests use a manual clock.
 */
interface TimeSource {
    long nanoTime();

    /** Runs task once, delayNanos from now (at once if not positive). Tasks must not block. */
    Timer schedule(Runnable task, long delayNanos);

    interface Timer {
        /** Stops the task if it has not started. Harmless twice, or after it ran. */
        void cancel();
    }
}

package io.github.rpasco787.hopper.worker;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Reconnect and confirmation retry pacing (worker/backoff.go): 100 ms doubling, a 5 s cap on
 * the nominal delay, then ×[0.75, 1.25) jitter, so the longest wait is 6.25 s. Not thread-safe;
 * each retry loop owns one. The jitter keeps a broker restart from having every worker
 * reconnect in the same few milliseconds, again and again.
 */
final class Backoff {
    private static final long BASE_NANOS = 100_000_000L;
    private static final long CAP_NANOS = 5_000_000_000L;
    private static final double JITTER_FLOOR = 0.75;
    private static final double JITTER_RANGE = 0.5;

    private final DoubleSupplier jitter;
    private int attempt;

    Backoff() {
        this(() -> ThreadLocalRandom.current().nextDouble());
    }

    /** jitter returns a value in [0, 1); a test pins it to its extremes. */
    Backoff(DoubleSupplier jitter) {
        this.jitter = jitter;
    }

    /** The next delay; advances the sequence. */
    Duration next() {
        // The shift is bounded before it happens, as in Go: a shift of 63 or more is negative
        // or zero, and a non-positive delay is the opposite of backing off.
        long d = CAP_NANOS;
        if (attempt < 62) {
            long shifted = BASE_NANOS << attempt;
            if (shifted > 0 && shifted < CAP_NANOS) {
                d = shifted;
            }
            attempt++;
        }
        return Duration.ofNanos((long) (d * (JITTER_FLOOR + JITTER_RANGE * jitter.getAsDouble())));
    }

    /** Restarts the sequence: only after an attempt did meaningful work. */
    void reset() {
        attempt = 0;
    }
}

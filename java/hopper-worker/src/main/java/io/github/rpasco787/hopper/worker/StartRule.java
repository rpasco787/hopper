package io.github.rpasco787.hopper.worker;

import java.util.OptionalLong;

/**
 * The start rule (docs/design/java-worker-sdk.md, copied from
 * docs/design/credit-anchored-freshness.md). All instants are TimeSource
 * readings, compared only by subtraction.
 */
final class StartRule {
    /** Scheduling slack subtracted from every grant: not a proof against a pause. */
    static final long LEASE_SAFETY_MARGIN_NANOS = 500_000_000L;
    /** A delivery read less than grant/FRESH_FRACTION after its credit was sent starts without the check. */
    static final long FRESH_FRACTION = 10;

    private StartRule() {}

    /** min(0.5 s, G/10): a short grant doesn't lose a quarter of itself to slack meant for a long one. */
    static long safetyMargin(long grantNanos) {
        return Math.min(LEASE_SAFETY_MARGIN_NANOS, grantNanos / 10);
    }

    /** The local deadline of a lease granted G, measured from the instant its evidence was sent. */
    static long deadlineFrom(long sentNanos, long grantNanos) {
        return sentNanos + (grantNanos - safetyMargin(grantNanos));
    }

    /**
     * The decision. creditSentAtNanos is the send instant of the unit the delivery was matched
     * to, receivedNanos the instant it was read off the stream. Empty means: take the check.
     * The broker leased this row only after receiving that unit, so the lease runs at least
     * until creditSent + G, and deadlineFrom(creditSent, G) is safe whatever the delivery's age;
     * the age only decides whether enough of it is left to be worth skipping the check, which
     * would also have bought a fresh grant.
     */
    static OptionalLong freshDeadline(boolean creditSent, long creditSentAtNanos,
                                      long receivedNanos, long grantNanos) {
        if (!creditSent || grantNanos <= 0) {
            return OptionalLong.empty();
        }
        long age = receivedNanos - creditSentAtNanos;
        if (age < 0 || age >= grantNanos / FRESH_FRACTION) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(deadlineFrom(creditSentAtNanos, grantNanos));
    }
}

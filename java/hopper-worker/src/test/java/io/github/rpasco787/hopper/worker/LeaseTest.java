package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Port of heartbeat_test.go TestLeaseTransitions, on long instants compared by subtraction. */
class LeaseTest {
    private static final long MS = 1_000_000L;
    private static final long S = 1_000 * MS;
    private static final long H = 3_600 * S;
    private final long now = 5_000_000_000L;

    private static Optional<StopReason> reason(Lease l) {
        return l.state().ended().map(Lease.Ended::reason);
    }

    @Test
    void leaseTransitions_aStaleTimerCannotExpireARenewedLease() {
        var l = new Lease();
        long old = now + 100 * MS;
        l.establish(old);
        assertTrue(l.extend(now + S, now));

        // The wakeup armed for the old deadline fires. It must look at the deadline as it is
        // now, not the one it was scheduled against.
        assertFalse(l.expireIfPast(old), "the lease was renewed under it");
        assertTrue(l.live());
    }

    @Test
    void leaseTransitions_aResponseAfterItsUsableDeadlineIsDiscarded() {
        var l = new Lease();
        l.establish(now + S);
        assertFalse(l.extend(now - MS, now), "already spent when it arrived");
        Lease.State state = l.state();
        assertTrue(state.ended().isEmpty(), "a late response is not a lost lease");
        assertEquals(now + S, state.deadline(), "the older, still-live deadline stands");
    }

    @Test
    void leaseTransitions_aRenewalCannotReviveALeaseWhoseDeadlineHasPassed() {
        var l = new Lease();
        l.establish(now - MS);
        assertFalse(l.extend(now + S, now));
        assertEquals(Optional.of(StopReason.LEASE_EXPIRED), reason(l));
    }

    @Test
    void leaseTransitions_terminalIsForever() {
        var l = new Lease();
        l.establish(now + S);
        assertTrue(l.lose(StopReason.LEASE_LOST, "ABORTED: lease mismatch"));

        assertFalse(l.lose(StopReason.JOB_CANCELLED, "later"), "the first cause is the true one");
        assertFalse(l.extend(now + H, now), "no late response may revive it");
        assertFalse(l.expireIfPast(now + H));
        assertEquals(Optional.of(StopReason.LEASE_LOST), reason(l));
        assertEquals("ABORTED: lease mismatch", l.state().ended().orElseThrow().detail());

        l.establish(now + H);
        assertFalse(l.live());
    }

    @Test
    void leaseTransitions_expiryFiresExactlyOnceAtTheDeadline() {
        var l = new Lease();
        l.establish(now);
        assertFalse(l.expireIfPast(now - 1), "not yet");
        assertTrue(l.expireIfPast(now));
        assertFalse(l.expireIfPast(now), "and not again");
    }

    /** Instants anywhere in the long range: a deadline past the wrap is still in the future. */
    @Test
    void instantsCompareBySubtractionAcrossTheOverflow() {
        var l = new Lease();
        long t = Long.MAX_VALUE - MS;
        l.establish(t + 2 * MS); // wraps negative
        assertFalse(l.expireIfPast(t), "1 ms before the wrapped deadline is not past it");
        assertTrue(l.extend(t + 3 * S, t + MS));
        assertEquals(t + 3 * S, l.state().deadline());
        assertTrue(l.expireIfPast(t + 3 * S));
    }

    /** Go's zero deadline is in the past: a lease never established has nothing to extend. */
    @Test
    void aLeaseNeverEstablishedCannotBeExtended() {
        var l = new Lease();
        assertFalse(l.extend(now + S, now));
        assertEquals(Optional.of(StopReason.LEASE_EXPIRED), reason(l));
    }
}

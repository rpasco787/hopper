package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** The start rule's Stamp and Match (docs/design/java-worker-sdk.md). Ports of stream_test.go. */
class CreditLedgerTest {
    /** Port of stream_test.go TestCreditIsMatchedToDeliveriesOldestFirst. */
    @Test
    void creditIsMatchedToDeliveriesOldestFirst() {
        var time = new ManualTimeSource(1_000_000_000L);
        var ledger = new CreditLedger();
        long t0 = time.nanoTime();
        ledger.reserveAndSend(2, time, () -> {});
        time.jump(Duration.ofSeconds(1));
        long t1 = time.nanoTime();
        ledger.reserveAndSend(1, time, () -> {});
        ledger.reserveUnsent(1); // the drain path: reserved, never sent

        assertEquals(new CreditLedger.Match(true, t0), ledger.spend().orElseThrow(), "delivery 0");
        assertEquals(new CreditLedger.Match(true, t0), ledger.spend().orElseThrow(), "delivery 1");
        assertEquals(new CreditLedger.Match(true, t1), ledger.spend().orElseThrow(), "delivery 2");
        assertFalse(ledger.spend().orElseThrow().sent(), "delivery 3 matched a unit never sent");
        assertTrue(ledger.spend().isEmpty(), "a delivery past the credit is still refused");
    }

    /** The stamp is read before the send runs: the transport taking time can't make it late. */
    @Test
    void theInstantIsReadBeforeTheSendRuns() {
        var time = new ManualTimeSource(1_000_000_000L);
        var ledger = new CreditLedger();
        long before = time.nanoTime();
        ledger.reserveAndSend(1, time, () -> time.jump(Duration.ofMillis(1)));
        assertEquals(before, ledger.spend().orElseThrow().sentAtNanos());
    }

    /** Port of stream_test.go TestReconcileForgetsTheSendInstants: unspent credit takes its instants with it. */
    @Test
    void reconcileForgetsTheSendInstants() {
        var time = new ManualTimeSource(1_000_000_000L);
        var pool = new Capacity(2, new CapturingLog());
        var ledger = new CreditLedger();
        assertEquals(2, pool.takeAll());
        ledger.reserveAndSend(2, time, () -> {});
        ledger.reconcile(pool);
        assertTrue(ledger.spend().isEmpty(), "nothing is left to match after reconcile");
        assertEquals(2, pool.free());
    }

    /** Port of stream_test.go TestReconcileReturnsUnspentCreditExactlyOnce. */
    @Test
    void reconcileReturnsUnspentCreditExactlyOnce() {
        var time = new ManualTimeSource(1_000_000_000L);
        var log = new CapturingLog();
        var pool = new Capacity(4, log);
        var ledger = new CreditLedger();

        assertEquals(4, pool.takeAll());
        ledger.reserveAndSend(4, time, () -> {});
        assertTrue(ledger.spend().isPresent(), "one delivery arrived against the credit");

        ledger.reconcile(pool);
        assertEquals(3, pool.free(), "the delivery's own token is returned by its runner, not by reconcile");

        ledger.reconcile(pool);
        ledger.reconcile(pool);
        assertEquals(3, pool.free(), "reconciling twice must not manufacture tokens");
        assertEquals(0, log.count("worker: released more capacity than exists; this is an SDK bug"),
                "and nothing was over-released");
    }

    /** Port of stream_test.go TestSpendRefusesCreditItDoesNotHave. */
    @Test
    void spendRefusesCreditItDoesNotHave() {
        var time = new ManualTimeSource(1_000_000_000L);
        var ledger = new CreditLedger();
        ledger.reserveAndSend(1, time, () -> {});
        assertTrue(ledger.spend().isPresent());
        assertTrue(ledger.spend().isEmpty(), "a delivery past the credit is a protocol violation, not a token");
        assertEquals(1, ledger.deliveredCount(), "and the refused one is not counted");
    }

    /** A send that throws keeps its units: a delivery may still answer it, and reconcile returns the rest once. */
    @Test
    void aSendThatThrowsKeepsItsReservation() {
        var time = new ManualTimeSource(1_000_000_000L);
        var pool = new Capacity(2, new CapturingLog());
        var ledger = new CreditLedger();
        assertEquals(2, pool.takeAll());
        long at = time.nanoTime();
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> ledger.reserveAndSend(2, time, () -> {
                    throw new IllegalStateException("call was cancelled");
                }));
        assertEquals("call was cancelled", e.getMessage());
        assertEquals(new CreditLedger.Match(true, at), ledger.spend().orElseThrow(), "the reservation stood");
        ledger.reconcile(pool);
        assertEquals(1, pool.free(), "the unspent unit came back");
        ledger.reconcile(pool);
        assertEquals(1, pool.free(), "exactly once");
    }

    /**
     * A delivery that answers the send before the send returns finds its units, and the lock
     * is not held across the send: a spender on another thread finishes while the send waits.
     */
    @Test
    void aDeliveryArrivingDuringItsSendFindsItsUnits() {
        var time = new ManualTimeSource(1_000_000_000L);
        var ledger = new CreditLedger();
        long at = time.nanoTime();
        var seen = new AtomicReference<Optional<CreditLedger.Match>>();
        ledger.reserveAndSend(1, time, () -> {
            Thread spender = Thread.ofVirtual().start(() -> seen.set(ledger.spend()));
            try {
                assertTrue(spender.join(Duration.ofSeconds(5)), "the spender was not blocked by the ledger's lock");
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new AssertionError(ie);
            }
        });
        assertEquals(Optional.of(new CreditLedger.Match(true, at)), seen.get(), "matched to the stamped instant");
    }

    @Test
    void zeroCreditStillSendsAndReservesNothing() {
        var time = new ManualTimeSource(1_000_000_000L);
        var ledger = new CreditLedger();
        boolean[] sent = {false};
        ledger.reserveAndSend(0, time, () -> sent[0] = true);
        assertTrue(sent[0], "the subscription still goes out");
        assertTrue(ledger.spend().isEmpty());
        ledger.reserveUnsent(0);
        assertTrue(ledger.spend().isEmpty());
    }
}

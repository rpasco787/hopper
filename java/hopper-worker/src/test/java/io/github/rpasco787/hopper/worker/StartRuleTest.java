package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class StartRuleTest {
    private static final long MS = 1_000_000L;
    private static final long S = 1_000 * MS;

    /**
     * Port of freshness_test.go TestFreshDeadlineCountsFromTheCreditSend: a delivery read off
     * the stream less than G/10 after its credit was sent starts on deadlineFrom(sent, G), which
     * is never later than receipt would give. Anything else takes the check.
     */
    @Test
    void freshDeadlineCountsFromTheCreditSend() {
        long grant = 30 * S;
        long sent = 123_456_789L; // any reading: only differences matter
        record Case(String name, long age, boolean fresh) {}
        for (Case c : List.of(
                new Case("moments after", MS, true),
                new Case("just under a tenth", grant / 10 - 1, true),
                new Case("a tenth", grant / 10, false),
                new Case("half the grant", grant / 2, false),
                new Case("before the send", -MS, false))) {
            long received = sent + c.age();
            OptionalLong d = StartRule.freshDeadline(true, sent, received, grant);
            assertEquals(c.fresh(), d.isPresent(), c.name());
            if (d.isEmpty()) {
                continue;
            }
            assertEquals(StartRule.deadlineFrom(sent, grant), d.getAsLong(),
                    c.name() + ": measured from the credit's send");
            assertTrue(d.getAsLong() - StartRule.deadlineFrom(received, grant) <= 0,
                    c.name() + ": never later than receipt would give");
            assertTrue(d.getAsLong() - received > 0, c.name() + ": and still ahead of the delivery");
        }
        assertTrue(StartRule.freshDeadline(false, 0, sent, grant).isEmpty(), "a unit never sent proves nothing");
        assertTrue(StartRule.freshDeadline(true, sent, sent, 0).isEmpty(), "no grant, no deadline");
    }

    /** Port of heartbeat_test.go TestDeadlineFromUsesTheSendInstantAndScalesItsMargin. */
    @Test
    void deadlineFromUsesTheSendInstantAndScalesItsMargin() {
        long sent = 42;
        assertEquals(sent + 30 * S - 500 * MS, StartRule.deadlineFrom(sent, 30 * S), "a long grant loses the fixed margin");
        assertEquals(sent + S - 100 * MS, StartRule.deadlineFrom(sent, S), "a short one loses a tenth");
        assertEquals(10 * MS, StartRule.safetyMargin(100 * MS));
        assertEquals(500 * MS, StartRule.safetyMargin(3_600 * S));
    }

    /** nanoTime may sit anywhere in the long range, so an age is a difference, never a comparison. */
    @Test
    void freshnessIsMeasuredBySubtractionAcrossTheOverflow() {
        long grant = 30 * S;
        long sent = Long.MAX_VALUE - MS;
        long received = sent + 2 * MS; // wraps negative
        assertTrue(received < sent, "the fixture really does wrap");
        OptionalLong d = StartRule.freshDeadline(true, sent, received, grant);
        assertTrue(d.isPresent(), "2 ms after its credit is fresh, across the wrap too");
        assertEquals(sent + (grant - 500 * MS), d.getAsLong());
    }
}

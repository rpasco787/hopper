package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class BackoffTest {
    /** Port of worker_test.go TestBackoffGrowsJittersAndCaps. */
    @Test
    void backoffGrowsJittersAndCaps() {
        Backoff low = new Backoff(() -> 0);
        List<Long> want = List.of(75L, 150L, 300L, 600L, 1200L, 2400L, 3750L, 3750L);
        for (int i = 0; i < want.size(); i++) {
            assertEquals(Duration.ofMillis(want.get(i)), low.next(), "attempt " + i + " at the jitter floor");
        }

        // The highest value the sequence can produce is 1.25 × the nominal cap.
        Backoff high = new Backoff(() -> 1 - 1e-12);
        Duration peak = Duration.ZERO;
        for (int i = 0; i < 20; i++) {
            Duration d = high.next();
            if (d.compareTo(peak) > 0) {
                peak = d;
            }
        }
        assertTrue(peak.compareTo(Duration.ofMillis(6250)) <= 0,
                "5s is the cap on the nominal delay, not on the jittered one");
        assertTrue(peak.compareTo(Duration.ofMillis(6249)) > 0);

        // And with the real source, every draw stays inside the band.
        Backoff b = new Backoff();
        for (int i = 0; i < 1000; i++) {
            Duration d = b.next();
            assertTrue(d.compareTo(Duration.ofMillis(75)) >= 0, d.toString());
            assertTrue(d.compareTo(Duration.ofMillis(6250)) <= 0, d.toString());
        }

        Backoff floor = new Backoff(() -> 0);
        floor.next();
        floor.next();
        floor.reset();
        assertEquals(Duration.ofMillis(75), floor.next(), "reset restarts the sequence");
    }

    /** The shift is bounded before it happens, as in Go: a huge attempt count stays at the cap. */
    @Test
    void manyAttemptsStayAtTheCap() {
        Backoff low = new Backoff(() -> 0);
        Duration last = Duration.ZERO;
        for (int i = 0; i < 200; i++) {
            last = low.next();
        }
        assertEquals(Duration.ofMillis(3750), last);
    }
}

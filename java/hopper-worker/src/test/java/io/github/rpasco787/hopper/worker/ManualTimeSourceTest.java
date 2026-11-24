package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ManualTimeSourceTest {
    private static final long START = 1_000_000_000L;
    private static final long MS = 1_000_000L;
    private static final Duration BARRIER = Duration.ofSeconds(10);

    @Test
    void advanceRunsDueTasksInDeadlineOrder() {
        var time = new ManualTimeSource(START);
        List<String> ran = new ArrayList<>();
        time.schedule(() -> ran.add("c"), 30 * MS);
        time.schedule(() -> ran.add("a"), 10 * MS);
        time.schedule(() -> ran.add("b1"), 20 * MS);
        time.schedule(() -> ran.add("b2"), 20 * MS); // same due: scheduling order
        time.schedule(() -> ran.add("late"), 31 * MS);
        time.advance(Duration.ofMillis(30));
        assertEquals(List.of("a", "b1", "b2", "c"), ran);
        assertEquals(START + 30 * MS, time.nanoTime(), "the clock ends at start + d");
        assertEquals(1, time.pending(), "the one not yet due waits");
    }

    @Test
    void aTaskSeesTheClockAtItsDueTime() {
        var time = new ManualTimeSource(START);
        long[] seen = new long[2];
        time.schedule(() -> seen[0] = time.nanoTime(), 7 * MS);
        time.schedule(() -> seen[1] = time.nanoTime(), 0);
        time.advance(Duration.ofSeconds(1));
        assertEquals(START + 7 * MS, seen[0]);
        assertEquals(START, seen[1], "a task due now runs at now");
        assertEquals(START + 1_000 * MS, time.nanoTime());
    }

    @Test
    void jumpMovesTheClockWithoutRunningTasks() {
        var time = new ManualTimeSource(START);
        List<Long> ran = new ArrayList<>();
        time.schedule(() -> ran.add(time.nanoTime()), 10 * MS);
        time.jump(Duration.ofSeconds(5));
        assertEquals(START + 5_000 * MS, time.nanoTime());
        assertEquals(List.of(), ran, "no timer fires during a pause");
        assertEquals(1, time.pending());
        time.advance(Duration.ZERO);
        assertEquals(List.of(START + 5_000 * MS), ran, "the overdue timer fires late, at the woken clock");
    }

    @Test
    void aCancelledTaskNeverRuns() {
        var time = new ManualTimeSource(START);
        List<String> ran = new ArrayList<>();
        TimeSource.Timer t = time.schedule(() -> ran.add("cancelled"), 10 * MS);
        time.schedule(() -> ran.add("kept"), 10 * MS);
        t.cancel();
        t.cancel(); // twice is harmless
        assertEquals(1, time.pending());
        time.advance(Duration.ofMillis(20));
        assertEquals(List.of("kept"), ran);
    }

    @Test
    void aTaskScheduledDuringAdvanceRunsIfDue() {
        var time = new ManualTimeSource(START);
        List<Long> ran = new ArrayList<>();
        time.schedule(() -> {
            ran.add(time.nanoTime());
            time.schedule(() -> ran.add(time.nanoTime()), 5 * MS); // due at 15 ms
            time.schedule(() -> ran.add(-1L), 50 * MS); // due at 60 ms: past this advance
        }, 10 * MS);
        time.advance(Duration.ofMillis(20));
        assertEquals(List.of(START + 10 * MS, START + 15 * MS), ran);
        assertEquals(1, time.pending());
    }

    @Test
    void awaitTimersWaitsForATimerArmedOnAnotherThread() throws InterruptedException {
        var time = new ManualTimeSource(START);
        Thread armer = Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            time.schedule(() -> {}, MS);
        });
        time.awaitTimers(1, BARRIER);
        assertEquals(1, time.pending());
        armer.join();
        AssertionError e = assertThrows(AssertionError.class, () -> time.awaitTimers(2, Duration.ofMillis(50)));
        assertTrue(e.getMessage().contains("2 pending timers"), e.getMessage());
    }
}

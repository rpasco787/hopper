package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class SystemTimeSourceTest {
    @Test
    void aScheduledTaskRunsOnTheSchedulerThread() throws Exception {
        try (var time = new SystemTimeSource(new CapturingLog())) {
            var thread = new CompletableFuture<Thread>();
            time.schedule(() -> thread.complete(Thread.currentThread()), Duration.ofMillis(10).toNanos());
            Thread t = thread.get(2, TimeUnit.SECONDS);
            assertEquals("hopper-scheduler", t.getName());
            assertTrue(t.isDaemon(), "it never keeps the JVM alive");
        }
    }

    @Test
    void cancelPreventsIt() throws InterruptedException {
        try (var time = new SystemTimeSource(new CapturingLog())) {
            var ran = new AtomicBoolean();
            time.schedule(() -> ran.set(true), Duration.ofMillis(200).toNanos()).cancel();
            Thread.sleep(500);
            assertFalse(ran.get());
        }
    }

    @Test
    void aThrowingTaskDoesNotStopTheScheduler() throws InterruptedException {
        var log = new CapturingLog();
        try (var time = new SystemTimeSource(log)) {
            time.schedule(() -> {
                throw new IllegalStateException("boom");
            }, 0);
            var later = new CountDownLatch(1);
            time.schedule(later::countDown, Duration.ofMillis(20).toNanos());
            assertTrue(later.await(2, TimeUnit.SECONDS), "the next task still ran");
            log.await(SystemTimeSource.TASK_FAILED, 1, Duration.ofSeconds(2));
            var line = log.lines().stream().filter(l -> l.msg().equals(SystemTimeSource.TASK_FAILED)).findFirst().orElseThrow();
            assertEquals(EventLog.Level.ERROR, line.level());
            assertEquals("java.lang.IllegalStateException: boom", line.fields().get("err"));
        }
    }

    @Test
    void nanoTimeIsTheSystemClock() {
        try (var time = new SystemTimeSource(new CapturingLog())) {
            long before = System.nanoTime();
            long t = time.nanoTime();
            assertTrue(t - before >= 0 && System.nanoTime() - t >= 0);
        }
    }
}

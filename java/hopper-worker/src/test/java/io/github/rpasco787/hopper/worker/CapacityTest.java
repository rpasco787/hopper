package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** The worker's token pool (worker/stream.go capacity). */
class CapacityTest {
    private static final String OVER_RELEASED = "worker: released more capacity than exists; this is an SDK bug";

    @Test
    void takeAllClaimsEveryFreeToken() {
        var pool = new Capacity(3, new CapturingLog());
        assertEquals(3, pool.free());
        assertEquals(3, pool.takeAll());
        assertEquals(0, pool.free());
        assertEquals(0, pool.takeAll(), "and never blocks on an empty pool");
        pool.release();
        assertEquals(1, pool.takeAll());
    }

    @Test
    void takeOneOrWakeReturnsEveryFreeTokenAtOnce() throws Exception {
        var pool = new Capacity(4, new CapturingLog());
        assertEquals(4, pool.takeOneOrWake(() -> false), "one wakeup carries every free token");
        assertEquals(0, pool.free());

        // Blocked on an empty pool, it wakes for a release and takes what is free then.
        var got = new CompletableFuture<Integer>();
        Thread waiter = Thread.ofVirtual().start(() -> {
            try {
                got.complete(pool.takeOneOrWake(() -> false));
            } catch (InterruptedException e) {
                got.completeExceptionally(e);
            }
        });
        Thread.sleep(50);
        assertFalse(got.isDone(), "it blocks while nothing is free");
        pool.release();
        assertEquals(1, got.get(5, TimeUnit.SECONDS));
        waiter.join();
    }

    @Test
    void takeOneOrWakeReturnsZeroWhenStoppedAndWoken() throws Exception {
        var pool = new Capacity(1, new CapturingLog());
        assertEquals(1, pool.takeAll());
        var stop = new AtomicBoolean();
        var got = new CompletableFuture<Integer>();
        Thread waiter = Thread.ofVirtual().start(() -> {
            try {
                got.complete(pool.takeOneOrWake(stop::get));
            } catch (InterruptedException e) {
                got.completeExceptionally(e);
            }
        });
        Thread.sleep(50);
        assertFalse(got.isDone());
        stop.set(true);
        pool.wake();
        assertEquals(0, got.get(5, TimeUnit.SECONDS), "stopped: no token taken");
        waiter.join();
        assertEquals(0, pool.free(), "and none manufactured");

        pool.release();
        assertEquals(0, pool.takeOneOrWake(() -> true), "a stop already set wins over a free token");
        assertEquals(1, pool.free(), "which stays in the pool");
    }

    @Test
    void anOverReleaseIsLoggedAndDropped() {
        var log = new CapturingLog();
        var pool = new Capacity(2, log);
        pool.release();
        assertEquals(2, pool.free(), "the ceiling is never raised");
        assertEquals(List.of(OVER_RELEASED), log.messages());
        assertEquals(EventLog.Level.ERROR, log.lines().get(0).level());
        assertEquals(List.of(Map.<String, Object>of("concurrency", 2)), log.fields(OVER_RELEASED));
    }

    @Test
    void anInterruptedWaitThrows() throws Exception {
        var pool = new Capacity(1, new CapturingLog());
        assertEquals(1, pool.takeAll());
        var thrown = new AtomicReference<Throwable>();
        var entered = new CompletableFuture<Void>();
        Thread waiter = Thread.ofVirtual().start(() -> {
            entered.complete(null);
            try {
                pool.takeOneOrWake(() -> false);
            } catch (Throwable t) {
                thrown.set(t);
            }
        });
        entered.get(5, TimeUnit.SECONDS);
        Thread.sleep(50);
        waiter.interrupt();
        assertTrue(waiter.join(Duration.ofSeconds(5)));
        assertTrue(thrown.get() instanceof InterruptedException, "got " + thrown.get());
        assertEquals(0, pool.free());
    }
}

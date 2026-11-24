package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import io.github.rpasco787.hopper.testing.Gate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Ports of worker/observer_test.go. */
class ObserverTest {
    private static final Duration BARRIER = Harness.BARRIER;

    record Event(String queue, HandlerStatus status, Duration elapsed) {}

    /** Records what it is told; safe for concurrent use. */
    static final class Recording implements WorkerObserver {
        private final List<String> started = new ArrayList<>();
        private final List<Event> finished = new ArrayList<>();

        @Override
        public synchronized void handlerStarted(String queue) {
            started.add(queue);
        }

        @Override
        public synchronized void handlerFinished(String queue, HandlerStatus status, Duration elapsed) {
            finished.add(new Event(queue, status, elapsed));
        }

        synchronized List<String> started() {
            return List.copyOf(started);
        }

        synchronized List<Event> finished() {
            return List.copyOf(finished);
        }
    }

    @Test
    void observerSeesEachHandlerOnce() throws Exception {
        try (var fake = FakeBroker.start()) {
            var observer = new Recording();
            Duration delay = Duration.ofMillis(20);
            var w = HopperWorker.builder(fake.channel()).concurrency(3).shutdownTimeout(Duration.ofSeconds(3))
                    .log(Harness.QUIET).observer(observer)
                    .handle("ok", ctx -> Thread.sleep(delay))
                    .handle("error", ctx -> {
                        Thread.sleep(delay);
                        throw new Exception("failed");
                    })
                    .handle("panic", ctx -> {
                        Thread.sleep(delay);
                        throw new AssertionError("failed");
                    })
                    .build();
            try (var run = Harness.runWorker(w)) {
                FakeStream stream = fake.awaitStream(BARRIER);
                for (String q : List.of("ok", "error", "panic")) {
                    stream.deliver(Fixture.of(q).proto(), BARRIER);
                }
                fake.awaitCalls("Ack", 1, BARRIER);
                fake.awaitCalls("Nack", 2, BARRIER);
                assertNull(run.stopAndWait());
            }
            assertEquals(List.of("error", "ok", "panic"), observer.started().stream().sorted().toList());
            assertEquals(3, observer.finished().size());
            Map<String, HandlerStatus> statuses = new HashMap<>();
            for (Event e : observer.finished()) {
                statuses.put(e.queue(), e.status());
                assertTrue(e.elapsed().compareTo(delay) >= 0, e.toString());
            }
            assertEquals(Map.of("ok", HandlerStatus.OK, "error", HandlerStatus.ERROR, "panic", HandlerStatus.PANIC), statuses);
        }
    }

    @Test
    void observerNeverSeesUnstartedJobs() throws Exception {
        try (var fake = FakeBroker.start(); var gate = new Gate()) {
            var observer = new Recording();
            Fixture known = Fixture.of("known");
            var acquiring = new CountDownLatch(1);
            fake.onHeartbeat((req, ctx) -> {
                if (!req.getId().equals(known.id().toString())) {
                    return;
                }
                acquiring.countDown();
                gate.await(ctx);
            });
            var ran = new AtomicInteger();
            var w = Harness.builder(fake.channel()).concurrency(1).shutdownTimeout(Duration.ofSeconds(5))
                    .observer(observer).checkEveryDelivery(true).handle("known", ctx -> ran.incrementAndGet()).build();
            try (var run = Harness.runWorker(w)) {
                FakeStream stream = fake.awaitStream(BARRIER);
                stream.deliver(Fixture.of("unknown").proto(), BARRIER);
                fake.awaitCalls("Nack", 1, BARRIER);
                stream.deliver(known.proto(), BARRIER);
                assertTrue(acquiring.await(BARRIER.toSeconds(), TimeUnit.SECONDS), "acquisition never started");
                run.stop();
                stream.awaitEOF(BARRIER);
                gate.release();
                fake.awaitCalls("Nack", 2, BARRIER);
                assertNull(run.await());
            }
            assertEquals(0, ran.get(), "unstarted handler ran");
            assertEquals(List.of(), observer.started());
            assertEquals(List.of(), observer.finished());
        }
    }

    /** Tracks handlers in flight, and the least and most it ever saw. */
    static final class Counting implements WorkerObserver {
        final AtomicInteger current = new AtomicInteger();
        final AtomicInteger min = new AtomicInteger();
        final AtomicInteger max = new AtomicInteger();

        @Override
        public void handlerStarted(String queue) {
            int n = current.incrementAndGet();
            max.accumulateAndGet(n, Math::max);
        }

        @Override
        public void handlerFinished(String queue, HandlerStatus status, Duration elapsed) {
            int n = current.decrementAndGet();
            min.accumulateAndGet(n, Math::min);
        }
    }

    @Test
    void inflightNeverGoesNegative() throws Exception {
        try (var fake = FakeBroker.start()) {
            var observer = new Counting();
            var w = Harness.builder(fake.channel()).concurrency(16).shutdownTimeout(Duration.ofSeconds(3))
                    .observer(observer).handle("q", ctx -> Thread.sleep(1)).build();
            try (var run = Harness.runWorker(w)) {
                FakeStream stream = fake.awaitStream(BARRIER);
                for (int i = 0; i < 200; i++) {
                    stream.deliver(Fixture.of("q").proto(), BARRIER);
                }
                fake.awaitCalls("Ack", 200, BARRIER);
                assertNull(run.stopAndWait());
            }
            assertEquals(0, observer.current.get());
            assertTrue(observer.min.get() >= 0);
            assertTrue(observer.max.get() <= 16, "max " + observer.max.get());
        }
    }

    /** An observer changes nothing about the Nack: it is D22's text for an Error. */
    @Test
    void panicNackMessageIsUnchanged() throws Exception {
        try (var fake = FakeBroker.start()) {
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).observer(new Recording()), "q", ctx -> {
                throw new AssertionError("payload made no sense");
            })) {
                FakeStream stream = fake.awaitStream(BARRIER);
                stream.deliver(Fixture.of("q").proto(), BARRIER);
                fake.awaitCalls("Nack", 1, BARRIER);
                assertNull(run.stopAndWait());
            }
            assertEquals(1, fake.nackRequests().size());
            assertTrue(fake.nackRequests().get(0).getError().startsWith("java.lang.AssertionError: payload made no sense\n"));
        }
    }

    /** Go: a nil Observer. Java: no observer set. */
    @Test
    void nilObserverIsFine() throws Exception {
        try (var fake = FakeBroker.start()) {
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()), "q", ctx -> {})) {
                FakeStream stream = fake.awaitStream(BARRIER);
                stream.deliver(Fixture.of("q").proto(), BARRIER);
                fake.awaitCalls("Ack", 1, BARRIER);
                assertNull(run.stopAndWait());
            }
        }
    }
}

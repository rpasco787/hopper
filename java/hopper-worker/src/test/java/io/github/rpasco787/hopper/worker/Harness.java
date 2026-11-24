package io.github.rpasco787.hopper.worker;

import io.github.rpasco787.hopper.testing.FakeBroker;
import io.grpc.ManagedChannel;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** The worker tests' shared setup: the ports of fake_test.go's runWorker and stream_test.go's newTestWorker. */
final class Harness {
    /** The default budget for a harness wait: long enough for a loaded machine, short of the build's timeout. */
    static final Duration BARRIER = Duration.ofSeconds(10);

    /** A log that writes nothing. */
    static final EventLog QUIET = new EventLog() {
        @Override
        public boolean enabled(Level level) {
            return false;
        }

        @Override
        public void log(Level level, String msg, Object... keyValues) {}

        @Override
        public EventLog with(Object... keyValues) {
            return this;
        }
    };

    private Harness() {}

    /** A builder on a caller-owned channel, with a quiet log and a 2 s shutdown, as Go's newTestWorker. */
    static HopperWorker.Builder builder(ManagedChannel channel) {
        return HopperWorker.builder(channel).log(QUIET).shutdownTimeout(Duration.ofSeconds(2));
    }

    /** Registers a no-op handler for each queue and substitutes runner. */
    static HopperWorker newTestWorker(HopperWorker.Builder builder, DeliveryRunner runner, String... queues) {
        for (String q : queues) {
            builder.handle(q, ctx -> {});
        }
        return builder.runner(runner).build();
    }

    /** A worker with one real handler: concurrency 1, a 3 s shutdown, the real runner. */
    static Run runHandler(FakeBroker fake, HopperWorker.Builder builder, String queue, JobHandler handler) {
        return runWorker(builder.concurrency(1).shutdownTimeout(Duration.ofSeconds(3)).handle(queue, handler).build());
    }

    static Run runWorker(HopperWorker worker) {
        return new Run(worker);
    }

    /**
     * A worker running in the background. close() stops it and requires that run() actually
     * returned, because a run() that never returns is the failure most worth catching.
     */
    static final class Run implements AutoCloseable {
        private final HopperWorker worker;
        private final CompletableFuture<RuntimeException> done = new CompletableFuture<>();
        final Thread thread;

        Run(HopperWorker worker) {
            this.worker = worker;
            this.thread = Thread.ofPlatform().name("hopper-test-run").daemon().start(() -> {
                try {
                    worker.run();
                    done.complete(null);
                } catch (RuntimeException e) {
                    done.complete(e);
                } catch (Throwable t) {
                    done.completeExceptionally(t);
                }
            });
        }

        /** Begins a graceful drain. */
        void stop() {
            worker.shutdown();
        }

        /** What run() threw (null if it returned), failing if it hasn't returned within the barrier. */
        RuntimeException await() {
            try {
                return done.get(BARRIER.toNanos(), TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                throw new AssertionError("run() did not return within " + BARRIER);
            } catch (ExecutionException e) {
                throw new AssertionError("run() threw an Error", e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted waiting for run()", e);
            }
        }

        boolean isDone() {
            return done.isDone();
        }

        RuntimeException stopAndWait() {
            stop();
            return await();
        }

        @Override
        public void close() {
            stop();
            try {
                done.get(BARRIER.toNanos(), TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                throw new AssertionError("run() did not return after shutdown()");
            } catch (ExecutionException e) {
                // reported by await() where a test cares
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

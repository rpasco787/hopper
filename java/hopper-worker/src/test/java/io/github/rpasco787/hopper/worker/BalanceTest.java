package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.Gate;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import io.grpc.Status;
import io.grpc.StatusOr;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/** Ports of worker/balance_test.go. */
class BalanceTest {
    private static final Duration BARRIER = Harness.BARRIER;

    /**
     * Resolves hoppertest:///brokers to both fakes, the way DNS resolves a headless Service or a
     * shared Compose alias to every broker. A copy of hopper-proto's test-only PairResolver,
     * which another module's tests can't reach.
     */
    private static final class TwoBrokers extends NameResolverProvider {
        private final List<InetSocketAddress> addresses;

        TwoBrokers(FakeBroker a, FakeBroker b) {
            this.addresses = List.of(address(a), address(b));
        }

        private static InetSocketAddress address(FakeBroker broker) {
            String[] hostAndPort = broker.target().split(":");
            return new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        }

        @Override
        protected boolean isAvailable() {
            return true;
        }

        @Override
        protected int priority() {
            return 5;
        }

        @Override
        public String getDefaultScheme() {
            return "hoppertest";
        }

        @Override
        public NameResolver newNameResolver(URI target, NameResolver.Args args) {
            if (!"hoppertest".equals(target.getScheme())) {
                return null;
            }
            return new NameResolver() {
                @Override
                public String getServiceAuthority() {
                    return "brokers";
                }

                @Override
                public void start(Listener2 listener) {
                    List<EquivalentAddressGroup> groups = new ArrayList<>();
                    for (InetSocketAddress address : addresses) {
                        groups.add(new EquivalentAddressGroup(address));
                    }
                    listener.onResult(NameResolver.ResolutionResult.newBuilder()
                            .setAddressesOrError(StatusOr.fromValue(groups))
                            .build());
                }

                @Override
                public void shutdown() {}
            };
        }
    }

    /**
     * Both fakes end every lease stream at once, as a broker does when a stream reaches its
     * maximum age, so the worker keeps reopening it. Through round robin the reopened streams
     * reach both brokers; under pick_first they would all go back to the first.
     */
    @Test
    void reconnectingStreamsSpreadAcrossBrokers() throws Exception {
        try (var a = FakeBroker.startOnLoopback(); var b = FakeBroker.startOnLoopback(); var spy = new Spy(false)) {
            for (FakeBroker f : List.of(a, b)) {
                f.onLease((s, ctx) -> {
                    throw Status.UNAVAILABLE.withDescription("lease stream reached its maximum age; reconnect").asException();
                });
            }
            var provider = new TwoBrokers(a, b);
            NameResolverRegistry.getDefaultRegistry().register(provider);
            try {
                // builder(String): the worker dials, so the channel has the SDK's own defaults.
                var w = Harness.newTestWorker(HopperWorker.builder("hoppertest:///brokers")
                        .log(Harness.QUIET).shutdownTimeout(Duration.ofSeconds(2)).concurrency(1), spy::run, "q");
                try (var run = Harness.runWorker(w)) {
                    long deadline = System.nanoTime() + BARRIER.toNanos();
                    while (a.subscriptions().isEmpty() || b.subscriptions().isEmpty()) {
                        assertTrue(System.nanoTime() < deadline, "every lease stream went to one broker");
                        Thread.sleep(10);
                    }
                    assertNull(run.stopAndWait());
                }
            } finally {
                NameResolverRegistry.getDefaultRegistry().deregister(provider);
            }
        }
    }

    /**
     * A broker ends every lease stream at its maximum age, and a worker on a quiet queue delivers
     * nothing in between. A stream that stayed up that long shows the broker is healthy, so
     * reconnecting must not climb the backoff toward its 5 s cap; otherwise every routine age-out
     * leaves an idle worker with no stream for seconds. On the system clock, with Go's
     * tolerances: the lease script's lifetime is wall time.
     */
    @Test
    void routineAgeOutsDoNotGrowTheReconnectDelay() throws Exception {
        Duration lived = Duration.ofMillis(60);
        List<Long> opened = new CopyOnWriteArrayList<>();
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            fake.onLease((s, ctx) -> {
                opened.add(System.nanoTime());
                var timer = new Gate();
                Thread.ofVirtual().start(() -> {
                    try {
                        Thread.sleep(lived);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    timer.release();
                });
                timer.await(ctx);
                throw Status.UNAVAILABLE.withDescription("lease stream reached its maximum age; reconnect").asException();
            });
            var w = Harness.newTestWorker(Harness.builder(fake.channel()).concurrency(1)
                    .healthySession(lived.dividedBy(2)), spy::run, "q"); // in production, 5 s
            int streams = 8;
            try (var run = Harness.runWorker(w)) {
                long deadline = System.nanoTime() + BARRIER.toNanos();
                while (opened.size() < streams) {
                    assertTrue(System.nanoTime() < deadline, "the worker reopened its stream too slowly: the reconnect delay grew");
                    Thread.sleep(10);
                }
                assertNull(run.stopAndWait());
            }
            for (int i = 1; i < streams; i++) {
                // lived, plus one un-grown delay (at most 125 ms), plus scheduling slack.
                long gap = opened.get(i) - opened.get(i - 1);
                assertTrue(gap < lived.plusMillis(400).toNanos(),
                        "reopen " + i + " waited " + Duration.ofNanos(gap) + ": the delay grew");
            }
        }
    }
}

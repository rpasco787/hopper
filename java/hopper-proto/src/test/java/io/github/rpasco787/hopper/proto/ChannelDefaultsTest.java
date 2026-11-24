package io.github.rpasco787.hopper.proto;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.proto.v1.GetJobRequest;
import io.github.rpasco787.hopper.proto.v1.HopperServiceGrpc;
import io.github.rpasco787.hopper.testing.FakeBroker;
import io.grpc.EquivalentAddressGroup;
import io.grpc.ForwardingChannelBuilder2;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import io.grpc.StatusOr;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The channel defaults are the one copy of what client/client.go:46-86 and
 * worker/worker.go:318-351 each keep, so the producer client and the worker SDK
 * cannot drift apart.
 */
class ChannelDefaultsTest {
    /** A channel builder that remembers what it was asked, and builds nothing. */
    static final class Recording extends ForwardingChannelBuilder2<Recording> {
        private final ManagedChannelBuilder<?> delegate = InProcessChannelBuilder.forName("recording");
        Map<String, ?> serviceConfig;
        final List<Duration> keepAliveTimes = new ArrayList<>();
        Duration keepAliveTimeout;
        Boolean keepAliveWithoutCalls;
        Integer maxInboundMessageSize;
        boolean directExecutorCalled;

        @Override
        protected ManagedChannelBuilder<?> delegate() {
            return delegate;
        }

        @Override
        public Recording defaultServiceConfig(Map<String, ?> serviceConfig) {
            this.serviceConfig = serviceConfig;
            return super.defaultServiceConfig(serviceConfig);
        }

        @Override
        public Recording keepAliveTime(long time, TimeUnit unit) {
            keepAliveTimes.add(Duration.ofNanos(unit.toNanos(time)));
            return super.keepAliveTime(time, unit);
        }

        @Override
        public Recording keepAliveTimeout(long timeout, TimeUnit unit) {
            keepAliveTimeout = Duration.ofNanos(unit.toNanos(timeout));
            return super.keepAliveTimeout(timeout, unit);
        }

        @Override
        public Recording keepAliveWithoutCalls(boolean enable) {
            keepAliveWithoutCalls = enable;
            return super.keepAliveWithoutCalls(enable);
        }

        @Override
        public Recording maxInboundMessageSize(int bytes) {
            maxInboundMessageSize = bytes;
            return super.maxInboundMessageSize(bytes);
        }

        @Override
        public Recording directExecutor() {
            directExecutorCalled = true;
            return super.directExecutor();
        }
    }

    @Test
    void configureAppliesTheSameDefaultsAsGo() {
        Recording recording = new Recording();

        ChannelDefaults.configure(recording);

        assertEquals(
                Map.of("loadBalancingConfig", List.of(Map.of("round_robin", Map.of()))),
                recording.serviceConfig);
        assertEquals(List.of(Duration.ofSeconds(10)), recording.keepAliveTimes);
        assertEquals(Duration.ofSeconds(5), recording.keepAliveTimeout);
        assertEquals(Boolean.TRUE, recording.keepAliveWithoutCalls);
        assertEquals(4194304, recording.maxInboundMessageSize);
        assertFalse(recording.directExecutorCalled, "inbound callbacks must never run on the transport's thread");
    }

    /** Defaults first, the caller's after, as Go applies them: a caller's option wins. */
    @Test
    void customizationRunsAfterTheDefaults() {
        Recording recording = new Recording();

        ChannelDefaults.apply(recording, b -> b.keepAliveTime(20, SECONDS));

        assertEquals(List.of(Duration.ofSeconds(10), Duration.ofSeconds(20)), recording.keepAliveTimes);
    }

    /** Resolves hoppertest:///pair to the two addresses it was given. */
    static final class PairResolver extends NameResolverProvider {
        private final List<InetSocketAddress> addresses;

        PairResolver(List<InetSocketAddress> addresses) {
            this.addresses = addresses;
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
                    return "pair";
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
     * Port of client_test.go:255 TestDialSpreadsCallsAcrossEveryResolvedBroker:
     * a name that resolves to two brokers must spread unary calls over both,
     * which only happens if round_robin is the default policy.
     */
    @Test
    void aNameThatResolvesToTwoBrokersSpreadsUnaryCallsAcrossBoth() throws Exception {
        try (FakeBroker first = FakeBroker.startOnLoopback();
                FakeBroker second = FakeBroker.startOnLoopback()) {
            PairResolver provider = new PairResolver(List.of(address(first), address(second)));
            NameResolverRegistry.getDefaultRegistry().register(provider);
            ManagedChannel channel = null;
            try {
                channel = ChannelDefaults.open("hoppertest:///pair", InsecureChannelCredentials.create(), b -> {});
                HopperServiceGrpc.HopperServiceBlockingStub stub = HopperServiceGrpc.newBlockingStub(channel);
                GetJobRequest request = GetJobRequest.newBuilder().setId("j").build();
                // The first call waits for both subchannels to be ready.
                stub.withWaitForReady().getJob(request);
                for (int i = 0; i < 8; i++) {
                    stub.getJob(request);
                }
                assertTrue(first.started("GetJob") >= 3, "first broker saw " + first.started("GetJob"));
                assertTrue(second.started("GetJob") >= 3, "second broker saw " + second.started("GetJob"));
            } finally {
                if (channel != null) {
                    channel.shutdownNow();
                }
                NameResolverRegistry.getDefaultRegistry().deregister(provider);
            }
        }
    }

    private static InetSocketAddress address(FakeBroker broker) {
        String[] hostAndPort = broker.target().split(":");
        return new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
    }
}

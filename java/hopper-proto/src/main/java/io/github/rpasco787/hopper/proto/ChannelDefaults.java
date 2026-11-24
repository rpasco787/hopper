package io.github.rpasco787.hopper.proto;

import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The channel settings the producer client and the worker SDK share. It is the
 * one copy of what client/client.go (46-86) and worker/worker.go (318-351) each
 * keep, and say to "keep in step", so the two Java modules cannot drift apart.
 *
 * <p>Nothing here calls {@code directExecutor()} (inbound callbacks must never
 * run on the transport's thread) or sets a retry policy: the clients retry
 * nothing for you (docs/semantics.md §8).
 */
public final class ChannelDefaults {
    /** The broker's message ceiling (internal/broker/service.go:29). */
    public static final int MAX_MESSAGE_BYTES = 4 << 20;

    /** The broker permits client pings every 5 s (internal/broker/lifetime.go:18). */
    public static final Duration KEEPALIVE_TIME = Duration.ofSeconds(10);

    public static final Duration KEEPALIVE_TIMEOUT = Duration.ofSeconds(5);

    private ChannelDefaults() {}

    /** {@code {"loadBalancingConfig":[{"round_robin":{}}]}}: spread calls over every address a name resolves to. */
    public static Map<String, ?> serviceConfig() {
        return Map.of("loadBalancingConfig", List.of(Map.of("round_robin", Map.of())));
    }

    /** Applies every default to {@code builder}. gRPC builders mutate in place. */
    public static void configure(ManagedChannelBuilder<?> builder) {
        builder.defaultServiceConfig(serviceConfig());
        builder.keepAliveTime(KEEPALIVE_TIME.toSeconds(), TimeUnit.SECONDS);
        builder.keepAliveTimeout(KEEPALIVE_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        builder.keepAliveWithoutCalls(true);
        builder.maxInboundMessageSize(MAX_MESSAGE_BYTES);
    }

    /**
     * Opens a channel to {@code target} with the defaults, then lets {@code customize}
     * change the builder, so a caller's option wins over a default.
     */
    public static ManagedChannel open(
            String target, ChannelCredentials credentials, Consumer<ManagedChannelBuilder<?>> customize) {
        ManagedChannelBuilder<?> builder = Grpc.newChannelBuilder(target, credentials);
        apply(builder, customize);
        return builder.build();
    }

    /** The defaults first, the caller's after, as Go applies them. */
    static void apply(ManagedChannelBuilder<?> builder, Consumer<ManagedChannelBuilder<?>> customize) {
        configure(builder);
        customize.accept(builder);
    }
}

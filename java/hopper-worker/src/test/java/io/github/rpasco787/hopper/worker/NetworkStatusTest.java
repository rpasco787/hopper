package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import java.nio.channels.ClosedChannelException;
import org.junit.jupiter.api.Test;

/**
 * grpc-netty reports a write onto a connection that has just been killed as UNKNOWN with the
 * description "channel closed" and a ClosedChannelException cause (Utils.statusFromThrowable),
 * where Go's gRPC reports UNAVAILABLE. Phase 3c's fifth chaos pass against the Java worker lost
 * an Ack to it when the broker was killed: the confirmation was not retried, the lease ran out
 * and the handler ran again. It is the network, so it is retried and never ends a lease.
 */
class NetworkStatusTest {
    private static Status channelClosed() {
        return Status.UNKNOWN.withDescription("channel closed").withCause(new ClosedChannelException());
    }

    @Test
    void aClosedChannelIsTheNetwork() {
        assertTrue(Heartbeat.isNetwork(channelClosed()));
        assertTrue(Heartbeat.isNetwork(Status.UNAVAILABLE));
        assertTrue(Heartbeat.isNetwork(Status.DEADLINE_EXCEEDED));
    }

    @Test
    void anUnknownWithoutThatCauseIsNot() {
        assertFalse(Heartbeat.isNetwork(Status.UNKNOWN));
        assertFalse(Heartbeat.isNetwork(Status.UNKNOWN.withDescription("channel closed")));
        assertFalse(Heartbeat.isNetwork(Status.UNKNOWN.withCause(new IllegalStateException("x"))));
        assertFalse(Heartbeat.isNetwork(Status.INTERNAL.withCause(new ClosedChannelException())));
    }

    @Test
    void aRenewalThatMeetsAClosedChannelRetriesInsteadOfLosingTheLease() {
        assertNull(Heartbeat.classify(channelClosed()));
        assertNotNull(Heartbeat.classify(Status.UNKNOWN));
    }
}

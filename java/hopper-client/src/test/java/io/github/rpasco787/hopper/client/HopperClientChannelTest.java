package io.github.rpasco787.hopper.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.proto.v1.GetJobResponse;
import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.Gate;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.TlsChannelCredentials;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Who owns the channel, what credentials it uses, and what an interrupt does,
 * over real Netty on loopback (the channel defaults are what is under test).
 *
 * <p>Not ported: TestIdleConnectionSurvivesKeepalive, TestSilentBrokerIsNoticedInAboutFifteenSeconds
 * and TestConnectionAgeFindsANewBroker. They test grpc's keepalive and the broker's connection age,
 * need 15 s or more of wall time each, and the settings that drive them are pinned by
 * ChannelDefaultsTest.
 */
class HopperClientChannelTest {
    private static final Duration BARRIER = Duration.ofSeconds(10);

    private FakeBroker fake;

    @BeforeEach
    void start() {
        fake = FakeBroker.startOnLoopback();
    }

    @AfterEach
    void stop() {
        fake.close();
    }

    @Test
    void connectOwnsItsChannelAndCloseShutsItDown() {
        HopperClient client = HopperClient.connect(fake.target());
        UUID id = UUID.randomUUID();
        assertEquals(id, client.getJob(id).id());

        long begun = System.nanoTime();
        client.close();
        Duration took = Duration.ofNanos(System.nanoTime() - begun);

        assertTrue(took.compareTo(Duration.ofSeconds(6)) < 0, "close took " + took);
        HopperException e = assertThrows(HopperException.class, () -> client.getJob(id));
        assertEquals(Status.Code.UNAVAILABLE, e.code(), "the channel is shut down");
    }

    @Test
    void wrapLeavesTheCallersChannelOpen() {
        ManagedChannel channel = fake.channel();

        HopperClient.wrap(channel).close();

        assertFalse(channel.isShutdown(), "the caller owns a wrapped channel");
        assertNotNull(HopperClient.wrap(channel).getJob(UUID.randomUUID()), "and it still carries calls");
    }

    /** Port of client_test.go:155 TestDialDefaultsToPlaintextAlongsideOtherOptions. */
    @Test
    void plaintextIsTheDefault() {
        try (HopperClient client = HopperClient.builder(fake.target()).callTimeout(Duration.ofSeconds(5)).build()) {
            UUID id = UUID.randomUUID();
            assertEquals(id, client.getJob(id).id());
        }
    }

    /** Port of TestDialLetsExplicitCredentialsOverrideTheDefault: the TLS handshake fails against plaintext. */
    @Test
    void explicitCredentialsReplaceThePlaintextDefault() {
        try (HopperClient client = HopperClient.builder(fake.target())
                .credentials(TlsChannelCredentials.create())
                .callTimeout(Duration.ofSeconds(2))
                .build()) {
            assertThrows(BrokerUnavailableException.class, () -> client.getJob(UUID.randomUUID()));
        }
        assertEquals(0, fake.started("GetJob"), "nothing reached the broker over a failed handshake");
    }

    @Test
    void anInterruptedCallStopsAndKeepsTheInterruptFlag() throws Exception {
        BlockingQueue<Boolean> cancelledAtTheBroker = new LinkedBlockingQueue<>();
        try (Gate stuck = new Gate();
                HopperClient client = HopperClient.connect(fake.target())) {
            fake.onGetJob((request, ctx) -> {
                try {
                    stuck.await(ctx);
                } finally {
                    cancelledAtTheBroker.add(ctx.isCancelled());
                }
                return GetJobResponse.getDefaultInstance();
            });
            AtomicReference<Status.Code> code = new AtomicReference<>();
            AtomicBoolean flagSet = new AtomicBoolean();
            Thread caller = Thread.ofVirtual().start(() -> {
                try {
                    client.getJob(UUID.randomUUID());
                } catch (HopperException e) {
                    code.set(e.code());
                    flagSet.set(Thread.currentThread().isInterrupted());
                }
            });
            fake.awaitCalls("GetJob", 1, BARRIER);

            caller.interrupt();
            assertTrue(caller.join(Duration.ofSeconds(2)), "the interrupted call did not stop within 2 s");

            assertEquals(Status.Code.CANCELLED, code.get());
            assertTrue(flagSet.get(), "the caller's interrupt must stay visible to it");
            Boolean seen = cancelledAtTheBroker.poll(BARRIER.toMillis(), TimeUnit.MILLISECONDS);
            assertNotNull(seen, "the hook never returned: the cancellation did not reach the broker");
            assertTrue(seen, "the hook saw its Context cancelled");
        }
    }
}

package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.proto.v1.HeartbeatRequest;
import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.Gate;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Ports of worker/heartbeat_test.go, on the manual clock wherever time passes. leaseTransitions
 * was ported in Task 13 (LeaseTest) and deadlineFromUsesTheSendInstantAndScalesItsMargin in
 * Task 10 (StartRuleTest).
 */
class HeartbeatTest {
    private static final Duration BARRIER = Harness.BARRIER;
    private static final long START = 1_000_000_000L;

    private static com.google.protobuf.Duration proto(Duration d) {
        return com.google.protobuf.Duration.newBuilder().setSeconds(d.getSeconds()).setNanos(d.getNano()).build();
    }

    @Test
    void acquisitionSpendsOneHeartbeatAndEstablishesADeadline() throws Exception {
        var time = new ManualTimeSource(START);
        try (var fake = FakeBroker.start(); var x = new HeartbeatHarness(fake, Duration.ofSeconds(30), time)) {
            long sent = time.nanoTime();
            assertEquals(Optional.empty(), x.h.acquire(x.d.acquisitionBudget()));

            assertEquals(1, fake.started("Heartbeat"), "acquisition is exactly one extra RPC per valid delivery");
            assertEquals(1, fake.finished("Heartbeat"));

            Lease.State st = x.h.lease().state();
            assertTrue(st.ended().isEmpty());
            assertEquals(sent + Duration.ofSeconds(30).minus(Duration.ofMillis(500)).toNanos(), st.deadline());
            assertTrue(st.deadline() - time.nanoTime() > 0, "a handler may only start against a future deadline");

            assertEquals(proto(Duration.ofSeconds(30)), fake.heartbeatRequests().get(0).getLeaseDuration(),
                    "validation pins the duration carried by the delivery instead of consulting a new broker default");
        }
    }

    @Test
    void renewalUsesTheBrokerSGrantNotTheRequest() throws Exception {
        var time = new ManualTimeSource(START);
        // The broker granted 400ms although nothing was requested. Renewal has to pace off the
        // grant: it is the only evidence of what the broker chose.
        try (var fake = FakeBroker.start(); var x = new HeartbeatHarness(fake, Duration.ofMillis(400), time)) {
            assertEquals(Optional.empty(), x.h.acquire(x.d.acquisitionBudget()));
            assertEquals(time.nanoTime() + Duration.ofMillis(400 - 40).toNanos(), x.h.lease().state().deadline(),
                    "the margin scales down for a short grant");
        }
    }

    @Test
    void periodicRenewalKeepsAHandlerAliveAcrossSeveralGrants() throws Exception {
        var time = new ManualTimeSource(START);
        Duration grant = Duration.ofMillis(200);
        try (var fake = FakeBroker.start(); var x = new HeartbeatHarness(fake, grant, time)) {
            assertEquals(Optional.empty(), x.h.acquire(x.d.acquisitionBudget()));
            x.h.start();

            // Four grants' worth of handler. Renewal targets a third of the grant, so the
            // lease should never come close to running out.
            Duration interval = grant.dividedBy(3);
            for (int i = 1; i <= 12; i++) {
                time.awaitTimers(2, BARRIER); // the expiry timer and the next renewal
                time.advance(interval);
                fake.awaitCalls("Heartbeat", 1 + i, BARRIER);
            }
            time.awaitTimers(2, BARRIER);
            assertTrue(x.lost.isEmpty(), "the lease was lost while renewal was healthy: " + x.lost);
            assertTrue(x.h.lease().live());
            assertEquals(13, fake.started("Heartbeat"), "one acquisition plus a beat every G/3");

            x.h.stop();
            assertTrue(x.h.lease().live(), "a normal stop is not a rejection; the Ack that follows is still valid");
            assertTrue(x.lost.isEmpty(), "and the handler is never told anything went wrong");
        }
    }

    @Test
    void everyRenewalPinsTheOriginalBrokerGrant() throws Exception {
        var time = new ManualTimeSource(START);
        Duration grant = Duration.ofMillis(150);
        try (var fake = FakeBroker.start(); var x = new HeartbeatHarness(fake, grant, time)) {
            assertEquals(Optional.empty(), x.h.acquire(x.d.acquisitionBudget()));
            x.h.start();
            for (int i = 1; i <= 2; i++) {
                time.awaitTimers(2, BARRIER);
                time.advance(grant.dividedBy(3));
                fake.awaitCalls("Heartbeat", 1 + i, BARRIER);
            }
            x.h.stop();

            List<HeartbeatRequest> requests = fake.heartbeatRequests();
            assertTrue(requests.size() >= 3);
            for (int i = 0; i < requests.size(); i++) {
                assertEquals(proto(grant), requests.get(i).getLeaseDuration(),
                        "heartbeat " + i + " must not select a different broker's default");
            }
        }
    }

    @Test
    void shortGrantsGetNoMinimumInterval() throws Exception {
        var time = new ManualTimeSource(START);
        try (var fake = FakeBroker.start(); var x = new HeartbeatHarness(fake, Duration.ofMillis(60), time)) {
            assertEquals(Optional.empty(), x.h.acquire(x.d.acquisitionBudget()));
            int before = fake.started("Heartbeat");
            x.h.start();

            time.awaitTimers(2, BARRIER);
            time.advance(Duration.ofMillis(20));
            fake.awaitCalls("Heartbeat", before + 1, BARRIER); // a 60ms grant heartbeats every 20ms
            assertTrue(fake.started("Heartbeat") > before, "a fixed floor would guarantee it expires");
        }
    }

    private record Mapping(String name, Status.Code code, StopReason want) {}

    private static final List<Mapping> TERMINAL = List.of(
            new Mapping("stale token", Status.Code.ABORTED, StopReason.LEASE_LOST),
            new Mapping("row is gone", Status.Code.NOT_FOUND, StopReason.LEASE_LOST),
            new Mapping("job cancelled", Status.Code.FAILED_PRECONDITION, StopReason.JOB_CANCELLED));

    @Test
    void terminalHeartbeatMappings() throws Exception {
        for (Mapping tc : TERMINAL) {
            // acquisition
            var time = new ManualTimeSource(START);
            try (var fake = FakeBroker.start(); var x = new HeartbeatHarness(fake, Duration.ofSeconds(30), time)) {
                fake.onHeartbeat((req, ctx) -> {
                    throw tc.code().toStatus().withDescription(tc.name()).asException();
                });
                Lease.Ended err = x.h.acquire(x.d.acquisitionBudget()).orElseThrow();
                assertEquals(tc.want(), err.reason(), "acquisition: " + tc.name());
                assertFalse(x.h.lease().live(), "a refused acquisition never starts a handler");
            }
            // renewal
            time = new ManualTimeSource(START);
            try (var fake = FakeBroker.start(); var x = new HeartbeatHarness(fake, Duration.ofMillis(150), time)) {
                assertEquals(Optional.empty(), x.h.acquire(x.d.acquisitionBudget()));
                fake.onHeartbeat((req, ctx) -> {
                    throw tc.code().toStatus().withDescription(tc.name()).asException();
                });
                x.h.start();
                time.awaitTimers(2, BARRIER);
                time.advance(Duration.ofMillis(50));
                assertEquals(tc.want(), x.awaitLost(BARRIER).reason(), "renewal: " + tc.name());
                assertFalse(x.h.lease().live());
                x.log.await(LogMessages.HEARTBEAT_REJECTED, 1, BARRIER);
            }
        }
    }

    @Test
    void aGenericRejectionStopsWorkWithItsDiagnostic() throws Exception {
        var time = new ManualTimeSource(START);
        try (var fake = FakeBroker.start(); var x = new HeartbeatHarness(fake, Duration.ofSeconds(30), time)) {
            fake.onHeartbeat((req, ctx) -> {
                throw Status.INTERNAL.withDescription("internal error").asException();
            });
            Lease.Ended err = x.h.acquire(x.d.acquisitionBudget()).orElseThrow();
            assertTrue(err.detail().contains("heartbeat rejected"), err.detail());
            assertTrue(err.detail().contains("INTERNAL"), "the underlying status survives for the log: " + err.detail());
            assertFalse(err.detail().contains(StopReason.LEASE_LOST.text()),
                    "INTERNAL is not evidence the lease went anywhere: " + err.detail());
            assertFalse(x.h.lease().live(), "but it is not retried blindly either");
            assertEquals(1, fake.started("Heartbeat"));
        }
    }

    /** On the manual clock: the two backoff waits are advanced by the test. */
    @Test
    void transientFailuresAreRetriedWithinTheBudget() throws Exception {
        var time = new ManualTimeSource(START);
        try (var fake = FakeBroker.start(); var x = new HeartbeatHarness(fake, Duration.ofSeconds(30), time)) {
            var calls = new AtomicInteger();
            fake.onHeartbeat((req, ctx) -> {
                if (calls.incrementAndGet() < 3) {
                    throw Status.UNAVAILABLE.withDescription("broker is shutting down").asException();
                }
            });
            var result = new CompletableFuture<Optional<Lease.Ended>>();
            Thread.ofVirtual().start(() -> result.complete(x.h.acquire(x.d.acquisitionBudget())));
            for (Duration ceiling : List.of(Duration.ofMillis(125), Duration.ofMillis(250))) {
                time.awaitTimers(1, BARRIER); // the backoff wait: 100 ms, then 200 ms, each ×[0.75, 1.25)
                time.advance(ceiling);
            }
            assertEquals(Optional.empty(), result.get(10, TimeUnit.SECONDS));
            assertEquals(3, fake.started("Heartbeat"), "two transient failures, then ownership is proven");
            assertTrue(x.h.lease().live());
        }
    }

    @Test
    void acquisitionGivesUpWhenItsBudgetRunsOut() throws Exception {
        var time = new ManualTimeSource(START);
        try (var fake = FakeBroker.start(); var g = new Gate();
                var x = new HeartbeatHarness(fake, Duration.ofMillis(300), time)) {
            fake.onHeartbeat((req, ctx) -> g.await(ctx));
            // A 300ms grant leaves a 270ms budget: the whole attempt is bounded by it.
            var result = new CompletableFuture<Optional<Lease.Ended>>();
            long wallStart = System.nanoTime();
            Thread.ofVirtual().start(() -> result.complete(x.h.acquire(x.d.acquisitionBudget())));
            fake.awaitCalls("Heartbeat", 1, BARRIER);
            time.advance(Duration.ofMillis(300)); // the budget passes while the call is stuck

            Lease.Ended err = result.get(10, TimeUnit.SECONDS).orElseThrow();
            long elapsed = System.nanoTime() - wallStart;
            assertEquals(StopReason.LEASE_EXPIRED, err.reason());
            assertFalse(x.h.lease().live(), "no handler runs after an unsuccessful acquisition");
            assertTrue(elapsed < Duration.ofSeconds(1).toNanos(), "the budget bounds the waiting, not the RPC timeout");
        }
    }

    @Test
    void expiryIsIndependentOfHeartbeatProgress() throws Exception {
        var time = new ManualTimeSource(START);
        try (var fake = FakeBroker.start(); var g = new Gate();
                var x = new HeartbeatHarness(fake, Duration.ofMillis(300), time)) {
            assertEquals(Optional.empty(), x.h.acquire(x.d.acquisitionBudget()));
            long deadline = x.h.lease().state().deadline();

            // From here every heartbeat stalls until its own RPC deadline. If expiry depended
            // on an RPC returning, the handler would run on past a lease the reaper is already
            // free to take back.
            fake.onHeartbeat((req, ctx) -> g.await(ctx));
            x.h.start();

            time.awaitTimers(2, BARRIER);
            time.advance(Duration.ofMillis(100)); // the first renewal, which then stalls
            fake.awaitCalls("Heartbeat", 2, BARRIER); // renewals were attempted and got nowhere
            time.advance(Duration.ofNanos(deadline - time.nanoTime() - 1));
            assertTrue(x.lost.isEmpty(), "not before the deadline");
            time.advance(Duration.ofNanos(1));
            assertEquals(StopReason.LEASE_EXPIRED, x.awaitLost(BARRIER).reason());
            assertEquals(deadline, time.nanoTime(), "expiry happens at the deadline, not whenever the network gives up");
            x.log.await(LogMessages.LEASE_EXPIRED_LOCALLY, 1, BARRIER);
        }
    }

    @Test
    void renewalCancelsHandlerWhenItObservesAnExpiredLease() throws Exception {
        var time = new ManualTimeSource(START);
        try (var fake = FakeBroker.start(); var x = new HeartbeatHarness(fake, Duration.ofMillis(3), time)) {
            x.h.lease().establish(time.nanoTime() - Duration.ofSeconds(1).toNanos());
            x.h.armRenewal(); // the renewal path alone, as Go's test runs renew() by itself
            time.awaitTimers(1, BARRIER);
            time.advance(Duration.ofMillis(1));
            assertEquals(StopReason.LEASE_EXPIRED, x.awaitLost(Duration.ofMillis(250)).reason());
        }
    }

    @Test
    void stopDuringABlockedRenewalIsPromptAndNotARejection() throws Exception {
        var time = new ManualTimeSource(START);
        try (var fake = FakeBroker.start(); var g = new Gate();
                var x = new HeartbeatHarness(fake, Duration.ofSeconds(30), time)) {
            assertEquals(Optional.empty(), x.h.acquire(x.d.acquisitionBudget()));
            var entered = new CountDownLatch(1);
            fake.onHeartbeat((req, ctx) -> {
                entered.countDown();
                g.await(ctx);
            });
            x.h.start();
            // The manual clock reaches the first renewal at once; Go shortens the grant instead.
            time.awaitTimers(2, BARRIER);
            time.advance(Duration.ofSeconds(10));
            assertTrue(entered.await(10, TimeUnit.SECONDS), "no renewal was attempted");

            long start = System.nanoTime();
            x.h.stop();
            assertTrue(System.nanoTime() - start < Duration.ofSeconds(2).toNanos(), "stopping cancels the RPC in flight");
            assertTrue(x.h.lease().live(), "our own cancellation is not the broker refusing us");
            assertTrue(x.lost.isEmpty());
            assertEquals(0, x.log.count(LogMessages.HEARTBEAT_REJECTED));
        }
    }

    // --- Java-only ---

    /** The first renewal is measured from start(), not from receipt or the check. */
    @Test
    void theFirstRenewalIsAThirdOfTheGrantAfterStart() throws Exception {
        var time = new ManualTimeSource(START);
        try (var fake = FakeBroker.start(); var x = new HeartbeatHarness(fake, Duration.ofSeconds(3), time)) {
            assertEquals(Optional.empty(), x.h.acquire(x.d.acquisitionBudget()));
            time.jump(Duration.ofMillis(500)); // between the check and the handler's start
            x.h.start();
            time.awaitTimers(2, BARRIER);
            time.advance(Duration.ofSeconds(1).minusNanos(1));
            assertEquals(1, fake.started("Heartbeat"), "no renewal before G/3 after start");
            time.advance(Duration.ofNanos(1));
            fake.awaitCalls("Heartbeat", 2, BARRIER);
        }
    }

    /** On the system clock, so the scheduler is the real hopper-scheduler thread. */
    @Test
    void renewalRpcsRunOnVirtualThreadsNotTheScheduler() throws Exception {
        List<Thread> callers = new CopyOnWriteArrayList<>();
        ClientInterceptor record = new ClientInterceptor() {
            @Override
            public <Q, R> ClientCall<Q, R> interceptCall(MethodDescriptor<Q, R> method, CallOptions options, Channel next) {
                callers.add(Thread.currentThread());
                return next.newCall(method, options);
            }
        };
        try (var time = new SystemTimeSource(Harness.QUIET); var fake = FakeBroker.start();
                var x = new HeartbeatHarness(fake, Duration.ofMillis(300), time, record)) {
            assertEquals(Optional.empty(), x.h.acquire(x.d.acquisitionBudget()));
            x.h.start();
            fake.awaitCalls("Heartbeat", 2, BARRIER);
            x.h.stop();
            Thread renewal = callers.get(1);
            assertTrue(renewal.isVirtual(), "a renewal's RPC runs on a virtual thread: " + renewal);
            assertEquals("hopper-renewal", renewal.getName());
            assertNotEquals("hopper-scheduler", renewal.getName());
        }
    }
}

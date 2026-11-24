package io.github.rpasco787.hopper.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.proto.v1.AckRequest;
import io.github.rpasco787.hopper.proto.v1.AckResponse;
import io.github.rpasco787.hopper.proto.v1.HeartbeatRequest;
import io.github.rpasco787.hopper.proto.v1.HeartbeatResponse;
import io.github.rpasco787.hopper.proto.v1.HopperServiceGrpc;
import io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest;
import io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse;
import io.github.rpasco787.hopper.proto.v1.NackRequest;
import io.github.rpasco787.hopper.proto.v1.Subscription;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Ports of worker/transport_test.go's harness tests. They check the test broker
 * and the stream-control interceptor themselves, so that every later test can
 * trust what they report. Each Go test's comment is kept on its Java port.
 *
 * <p>Streams are driven through the generated async stub with a {@link Events}
 * observer, the counterpart of the Go tests' {@code subscribe} and
 * {@code creditMsg} helpers.
 */
class FakeBrokerTest {
    private static final Duration BARRIER = Duration.ofSeconds(10);

    /** What a client sees on a lease stream, in order. */
    static final class Events implements ClientResponseObserver<LeaseJobsRequest, LeaseJobsResponse> {
        private static final Object COMPLETED = new Object();
        private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        volatile ClientCallStreamObserver<LeaseJobsRequest> requests;

        @Override
        public void beforeStart(ClientCallStreamObserver<LeaseJobsRequest> requestStream) {
            this.requests = requestStream;
        }

        @Override
        public void onNext(LeaseJobsResponse value) {
            queue.add(value);
        }

        @Override
        public void onError(Throwable t) {
            queue.add(t);
        }

        @Override
        public void onCompleted() {
            queue.add(COMPLETED);
        }

        private Object next() {
            try {
                Object event = queue.poll(BARRIER.toMillis(), TimeUnit.MILLISECONDS);
                if (event == null) {
                    throw new AssertionError("no stream event within " + BARRIER);
                }
                return event;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted waiting for a stream event", e);
            }
        }

        LeaseJobsResponse delivery() {
            Object event = next();
            if (event instanceof LeaseJobsResponse response) {
                return response;
            }
            throw new AssertionError("wanted a delivery, got " + event);
        }

        Status error() {
            Object event = next();
            if (event instanceof Throwable t) {
                return Status.fromThrowable(t);
            }
            throw new AssertionError("wanted an error, got " + event);
        }

        void completed() {
            Object event = next();
            if (event != COMPLETED) {
                throw new AssertionError("wanted the stream to complete, got " + event);
            }
        }
    }

    private static LeaseJobsRequest credit(int n) {
        return LeaseJobsRequest.newBuilder().setCredit(n).build();
    }

    private static Subscription subscription(int credit) {
        return Subscription.newBuilder().addQueues("q").setWorkerId("w").setCredit(credit).build();
    }

    /** Opens a lease stream and sends the subscription. */
    private static StreamObserver<LeaseJobsRequest> subscribe(
            ManagedChannel channel, Events events, Subscription subscription) {
        StreamObserver<LeaseJobsRequest> requests = HopperServiceGrpc.newStub(channel).leaseJobs(events);
        requests.onNext(LeaseJobsRequest.newBuilder().setSubscribe(subscription).build());
        return requests;
    }

    @Test
    void fakeUnaryDefaultsSucceedAndRecordCopies() {
        try (FakeBroker broker = FakeBroker.start()) {
            HopperServiceGrpc.HopperServiceBlockingStub stub =
                    HopperServiceGrpc.newBlockingStub(broker.channel());

            Fixture d = Fixture.of("q");
            String id = d.id().toString();
            String leaseId = d.leaseId().toString();
            AckResponse ack = stub.ack(AckRequest.newBuilder().setId(id).setLeaseId(leaseId).build());
            assertNotNull(ack);
            stub.nack(NackRequest.newBuilder().setId(id).setLeaseId(leaseId).setError("boom").build());
            stub.heartbeat(HeartbeatRequest.newBuilder().setId(id).setLeaseId(leaseId).build());

            assertEquals(1, broker.started("Ack"));
            assertEquals(1, broker.finished("Ack"));

            List<AckRequest> acks = broker.ackRequests();
            assertEquals(1, acks.size());
            assertEquals(id, acks.get(0).getId());
            // Protos are immutable, so a snapshot cannot be tampered with; what
            // matters is that it is a snapshot, not a view of the live record.
            assertThrows(UnsupportedOperationException.class, () -> acks.add(AckRequest.getDefaultInstance()));
            stub.ack(AckRequest.newBuilder().setId("later").setLeaseId(leaseId).build());
            assertEquals(1, acks.size(), "snapshots are copies");
            assertEquals(2, broker.ackRequests().size());

            assertEquals("boom", broker.nackRequests().get(0).getError());
            assertEquals(leaseId, broker.heartbeatRequests().get(0).getLeaseId());
        }
    }

    @Test
    void fakeUnaryHookOverridesTheDefault() {
        try (FakeBroker broker = FakeBroker.start()) {
            HopperServiceGrpc.HopperServiceBlockingStub stub =
                    HopperServiceGrpc.newBlockingStub(broker.channel());

            broker.onAck((request, ctx) -> {
                throw Status.ABORTED.withDescription("lease lost").asException();
            });
            StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
                    () -> stub.ack(AckRequest.newBuilder().setId("a").setLeaseId("b").build()));
            assertEquals(Status.Code.ABORTED, e.getStatus().getCode());

            assertEquals(1, broker.started("Ack"));
            assertEquals(1, broker.finished("Ack"), "a hook that returns still completes the call");
        }
    }

    @Test
    void halfCloseProducesEOF() {
        try (FakeBroker broker = FakeBroker.start()) {
            Events events = new Events();
            StreamObserver<LeaseJobsRequest> requests = subscribe(broker.channel(), events, subscription(2));
            FakeStream s = broker.awaitStream(BARRIER);
            assertEquals(2, s.creditTotal(), "the subscription's credit counts");

            requests.onCompleted();
            s.awaitEOF(BARRIER);

            // The default script treats EOF as a clean end, so the client sees
            // the stream finish rather than an error.
            events.completed();
        }
    }

    @Test
    void creditAccountingIsPerStream() {
        try (FakeBroker broker = FakeBroker.start()) {
            ManagedChannel channel = broker.channel();
            Events firstEvents = new Events();
            StreamObserver<LeaseJobsRequest> first = subscribe(channel, firstEvents, subscription(1));
            FakeStream s1 = broker.awaitStream(BARRIER);
            Events secondEvents = new Events();
            StreamObserver<LeaseJobsRequest> second = subscribe(channel, secondEvents, subscription(0));
            FakeStream s2 = broker.awaitStream(BARRIER);

            first.onNext(credit(3));
            s1.awaitCredit(4, BARRIER);
            assertEquals(0, s2.creditTotal(), "one stream's credit is not another's");

            second.onNext(credit(2));
            s2.awaitCredit(2, BARRIER);
            assertEquals(List.of(3), s1.creditMessages());
            assertEquals(List.of(2), s2.creditMessages());
        }
    }

    @Test
    void deliverySpendsCreditAndOverDeliveryDoesNot() {
        try (FakeBroker broker = FakeBroker.start()) {
            Events events = new Events();
            subscribe(broker.channel(), events, subscription(1));
            FakeStream s = broker.awaitStream(BARRIER);

            Fixture d = Fixture.of("q");
            s.deliver(d.proto(), BARRIER);
            assertEquals(d.id().toString(), events.delivery().getJob().getId());
            assertEquals(0, s.creditTotal());

            AssertionError e = assertThrows(AssertionError.class, () -> s.spend(1));
            assertTrue(e.getMessage().contains("only 0 outstanding"), e.getMessage());

            // send bypasses credit, which is how a test manufactures over-delivery.
            s.send(Fixture.of("q").proto());
            events.delivery();
        }
    }

    @Test
    void nonresponsiveHalfCloseStillRespectsCancellation() throws Exception {
        try (FakeBroker broker = FakeBroker.start()) {
            // A script that deliberately ignores the client's half-close, the way
            // a broker wedged on a database call would.
            BlockingQueue<Boolean> observed = new LinkedBlockingQueue<>();
            broker.onLease((stream, ctx) -> {
                try (Gate never = new Gate()) {
                    never.await(ctx);
                } finally {
                    observed.add(ctx.isCancelled());
                }
            });

            Events events = new Events();
            StreamObserver<LeaseJobsRequest> requests = subscribe(broker.channel(), events, subscription(1));
            FakeStream s = broker.awaitStream(BARRIER);

            requests.onCompleted();
            s.awaitEOF(BARRIER); // the reader sees EOF even though the script ignores it

            assertEquals(1, broker.started("LeaseJobs"));
            assertEquals(0, broker.finished("LeaseJobs"), "the handler is still stuck, which is the point");

            events.requests.cancel("the test cancels the stream", null);
            Boolean cancelled = observed.poll(BARRIER.toMillis(), TimeUnit.MILLISECONDS);
            assertNotNull(cancelled, "the stuck script never observed transport cancellation");
            assertTrue(cancelled);
        }
    }

    @Test
    void streamControlFailsToOpen() {
        try (FakeBroker broker = FakeBroker.start()) {
            StreamControl ctl = new StreamControl();
            ManagedChannel channel = broker.channel(ctl);

            ctl.onOpen(forward -> {
                throw Status.UNAVAILABLE.withDescription("no route").asException();
            });
            Events events = new Events();
            HopperServiceGrpc.newStub(channel).leaseJobs(events);
            assertEquals(Status.Code.UNAVAILABLE, events.error().getCode());

            assertEquals(0, broker.started("LeaseJobs"), "the stream never reached the broker");
        }
    }

    @Test
    void streamControlBlocksOpeningUntilCancelled() throws Exception {
        try (FakeBroker broker = FakeBroker.start();
                Gate gate = new Gate()) {
            StreamControl ctl = new StreamControl();
            ManagedChannel channel = broker.channel(ctl);
            CountDownLatch blocked = new CountDownLatch(1);
            ctl.onOpen(forward -> {
                blocked.countDown();
                gate.await(Context.current());
                forward.run();
            });

            Context.CancellableContext opening = Context.current().withCancellation();
            Events events = new Events();
            Thread opener = Thread.ofVirtual().start(() -> opening.run(() -> HopperServiceGrpc.newStub(channel).leaseJobs(events)));
            assertTrue(blocked.await(BARRIER.toMillis(), TimeUnit.MILLISECONDS), "the open never reached its hook");
            opening.cancel(null);

            assertEquals(Status.Code.CANCELLED, events.error().getCode(), "a blocked stream open ignored cancellation");
            opener.join(BARRIER.toMillis());
            assertFalse(opener.isAlive());
        }
    }

    /**
     * Go's Send returns the injected error itself. grpc-java's request observer
     * reports a failed write on the response observer, so the injected status
     * arrives there. The Go test's {@code started == 0} cannot be asserted: the
     * call's headers have already gone out, so the broker may have begun the
     * handler. "A failed Send delivers nothing" is asserted as: no subscription
     * ever reached the broker.
     */
    @Test
    void streamControlFailsASend() {
        try (FakeBroker broker = FakeBroker.start()) {
            StreamControl ctl = new StreamControl();
            ManagedChannel channel = broker.channel(ctl);

            ctl.onSend((message, forward) -> {
                throw Status.UNAVAILABLE.withDescription("write failed").asException();
            });
            Events events = new Events();
            subscribe(channel, events, subscription(1));

            Status status = events.error();
            assertEquals(Status.Code.UNAVAILABLE, status.getCode());
            assertEquals("write failed", status.getDescription());
            assertEquals(List.of(), broker.subscriptions(), "a failed Send delivers nothing");
        }
    }

    @Test
    void streamControlDeliversBeforeASendReturns() throws Exception {
        try (FakeBroker broker = FakeBroker.start();
                Gate gate = new Gate()) {
            StreamControl ctl = new StreamControl();
            ManagedChannel channel = broker.channel(ctl);
            Events events = new Events();
            StreamObserver<LeaseJobsRequest> requests = subscribe(channel, events, subscription(0));
            FakeStream s = broker.awaitStream(BARRIER);

            // From here, a Send reaches the broker and only then blocks. That is
            // the ordering the SDK has to survive: the answer to a credit message
            // can arrive while the code that sent it is still inside Send.
            ctl.onSend((message, forward) -> {
                forward.run();
                gate.await(Context.current());
            });

            CompletableFuture<Void> sent = CompletableFuture.runAsync(() -> requests.onNext(credit(1)));

            s.awaitCredit(1, BARRIER); // the credit arrived although Send has not returned
            Fixture d = Fixture.of("q");
            s.deliver(d.proto(), BARRIER);

            assertEquals(d.id().toString(), events.delivery().getJob().getId());
            assertFalse(sent.isDone(), "Send returned before the gate opened");

            gate.release();
            sent.get(BARRIER.toMillis(), TimeUnit.MILLISECONDS); // Send returns after the gate opens
        }
    }

    @Test
    void streamControlBlocksCloseSend() throws Exception {
        try (FakeBroker broker = FakeBroker.start();
                Gate gate = new Gate()) {
            StreamControl ctl = new StreamControl();
            ManagedChannel channel = broker.channel(ctl);

            ctl.onHalfClose(forward -> {
                gate.await(Context.current());
                forward.run();
            });

            Events events = new Events();
            StreamObserver<LeaseJobsRequest> requests = subscribe(channel, events, subscription(1));
            FakeStream s = broker.awaitStream(BARRIER);

            CompletableFuture<Void> closed = CompletableFuture.runAsync(requests::onCompleted);
            gate.release();
            closed.get(BARRIER.toMillis(), TimeUnit.MILLISECONDS); // CloseSend ran
            s.awaitEOF(BARRIER);
        }
    }

    @Test
    void subscriptionRejectionReachesTheClient() {
        try (FakeBroker broker = FakeBroker.start()) {
            broker.onSubscribe((subscription, ctx) -> {
                throw Status.INVALID_ARGUMENT.withDescription("queues is required").asException();
            });
            Events events = new Events();
            subscribe(broker.channel(), events, Subscription.newBuilder().setWorkerId("w").build());
            assertEquals(Status.Code.INVALID_ARGUMENT, events.error().getCode());
        }
    }

    @Test
    void nonSubscriptionFirstMessageIsRejected() {
        try (FakeBroker broker = FakeBroker.start()) {
            Events events = new Events();
            HopperServiceGrpc.newStub(broker.channel()).leaseJobs(events).onNext(credit(1));
            assertEquals(Status.Code.INVALID_ARGUMENT, events.error().getCode());
        }
    }

    /**
     * Not a port: a handler blocked on a gate must not outlive its broker.
     * close() cancels it, and then proves every started call finished.
     */
    @Test
    void closeJoinsAHandlerBlockedOnAGate() {
        FakeBroker broker = FakeBroker.start();
        Gate gate = new Gate();
        try {
            broker.onHeartbeat((request, ctx) -> gate.await(ctx));
            HopperServiceGrpc.newStub(broker.channel()).heartbeat(
                    HeartbeatRequest.newBuilder().setId("a").setLeaseId("b").build(),
                    new StreamObserver<HeartbeatResponse>() {
                        @Override
                        public void onNext(HeartbeatResponse value) {}

                        @Override
                        public void onError(Throwable t) {}

                        @Override
                        public void onCompleted() {}
                    });
            broker.awaitCalls("Heartbeat", 1, BARRIER);
            assertEquals(0, broker.finished("Heartbeat"), "the hook is blocked");

            long begun = System.nanoTime();
            broker.close();
            Duration took = Duration.ofNanos(System.nanoTime() - begun);

            assertTrue(took.compareTo(BARRIER) < 0, "close took " + took);
            assertEquals(1, broker.started("Heartbeat"));
            assertEquals(broker.started("Heartbeat"), broker.finished("Heartbeat"));
        } finally {
            gate.release();
            broker.close();
        }
    }
}

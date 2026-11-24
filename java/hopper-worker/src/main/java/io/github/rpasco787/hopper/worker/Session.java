package io.github.rpasco787.hopper.worker;

import io.github.rpasco787.hopper.proto.v1.HopperServiceGrpc;
import io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest;
import io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse;
import io.github.rpasco787.hopper.proto.v1.Subscription;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One lease stream and its accounting (worker/stream.go session). A new stream is a new session
 * with a new ledger; deliveries from the old one keep running and keep their tokens.
 *
 * <p>Threads (docs/design/java-worker-sdk.md): the caller of {@link #run} opens the call and
 * sends the subscription; the inbound {@code onNext}, on grpc's callback executor, matches,
 * parses and starts each delivery's virtual thread and never blocks; one virtual writer thread
 * is the only caller of the request observer's onNext and onCompleted after the subscription.
 * The subscription's send and the whole writer loop run inside the stream's context, so a
 * forced shutdown, which cancels it, ends whatever is blocked on the transport.
 */
final class Session {
    /** What the session needs from its worker, fixed for its life. */
    record Config(List<String> queues, String workerId, Duration leaseDuration,
                  Map<String, JobHandler> handlers, DeliveryRunner runner) {}

    private static final AtomicLong DELIVERY_NUMBER = new AtomicLong();

    private final Config config;
    private final Lifecycle lc;
    private final Channel channel;
    private final Capacity capacity;
    private final TimeSource time;
    private final EventLog log;
    private final Runnable onInterrupt;
    private final CreditLedger ledger;

    private final Context.CancellableContext streamCtx;
    private final CountDownLatch readDone = new CountDownLatch(1);
    private final Object causeLock = new Object();
    private Status cause;
    private boolean causeSet;
    private volatile ClientCallStreamObserver<LeaseJobsRequest> request;

    /** onInterrupt runs when the calling thread is interrupted while it waits; the wait goes on. */
    Session(Config config, Lifecycle lc, Channel channel, Capacity capacity, TimeSource time, EventLog log,
            Runnable onInterrupt) {
        this(config, lc, channel, capacity, time, log, onInterrupt, new CreditLedger());
    }

    /** A session over a given ledger; the worker always passes a new one (the start rule's Reconnect). */
    Session(Config config, Lifecycle lc, Channel channel, Capacity capacity, TimeSource time, EventLog log,
            Runnable onInterrupt, CreditLedger ledger) {
        this.config = config;
        this.lc = lc;
        this.channel = channel;
        this.capacity = capacity;
        this.time = time;
        this.log = log;
        this.onInterrupt = onInterrupt;
        this.ledger = ledger;
        // It exists before the call can block on it, so a forced shutdown during establishment
        // cancels the attempt rather than waiting for it.
        this.streamCtx = lc.forceContext().withCancellation();
    }

    int deliveryCount() {
        return ledger.deliveredCount();
    }

    /**
     * Holds one stream until it ends. Returns null for a clean end, and also when the forced
     * shutdown ended it (that is the SDK stopping it on purpose, not a stream failure); otherwise
     * the status it failed with.
     */
    Status run() {
        Thread writer = null;
        try {
            writer = streamCtx.call(() -> {
                open();
                subscribe();
                return Thread.ofVirtual().name("hopper-writer").start(streamCtx.wrap(this::write));
            });
        } catch (Exception e) {
            end(Status.fromThrowable(e));
        }
        // Wait for the final status where there is one. A broker that never answers the
        // half-close is bounded by the shared deadline, which cancels streamCtx, not by a
        // second timeout of its own.
        awaitUninterruptibly(readDone);
        streamCtx.cancel(null);
        capacity.wake();
        if (writer != null) {
            joinUninterruptibly(writer);
        }
        ledger.reconcile(capacity);
        log.log(EventLog.Level.INFO, LogMessages.STREAM_CLOSED, "delivered", ledger.deliveredCount());

        if (lc.forced()) {
            return null;
        }
        synchronized (causeLock) {
            return cause;
        }
    }

    private void open() {
        HopperServiceGrpc.newStub(channel).leaseJobs(new Reader());
    }

    /**
     * Advertises only what is free now. After a reconnect that is concurrency minus the handlers
     * still running from the previous stream, whose leases this new stream does not revoke.
     */
    private void subscribe() {
        int initial = capacity.takeAll();
        Subscription.Builder sub = Subscription.newBuilder()
                .addAllQueues(config.queues())
                .setWorkerId(config.workerId())
                .setCredit(initial);
        if (config.leaseDuration().isPositive()) {
            Duration d = config.leaseDuration();
            sub.setLeaseDuration(com.google.protobuf.Duration.newBuilder()
                    .setSeconds(d.getSeconds()).setNanos(d.getNano()).build());
        }
        LeaseJobsRequest msg = LeaseJobsRequest.newBuilder().setSubscribe(sub).build();
        ClientCallStreamObserver<LeaseJobsRequest> out = request;
        try {
            ledger.reserveAndSend(initial, time, () -> out.onNext(msg));
        } catch (IllegalStateException cancelled) {
            return; // the call already ended; its status is on its way to the reader
        }
        log.log(EventLog.Level.INFO, LogMessages.STREAM_OPENED, "queues", config.queues(), "credit", initial);
    }

    private boolean ended() {
        return readDone.getCount() == 0 || streamCtx.isCancelled();
    }

    /**
     * The one owner of every send after the subscription, and of the half-close. A token freeing
     * at the same instant the drain begins must not buy more work, so drain is checked again
     * once the tokens are held.
     */
    private void write() {
        ClientCallStreamObserver<LeaseJobsRequest> out = request;
        try {
            while (true) {
                int taken = capacity.takeOneOrWake(() -> lc.isDraining() || ended());
                if (lc.isDraining()) {
                    ledger.reserveUnsent(taken); // never sent: carries no instant
                    out.onCompleted();
                    return;
                }
                if (ended()) {
                    ledger.reserveUnsent(taken); // reconcile returns them
                    return;
                }
                // Reserve before sending: if this send is in flight when a delivery answers it,
                // the accounting is already there; if it fails, the reservation stays and
                // reconcile returns it exactly once.
                LeaseJobsRequest credit = LeaseJobsRequest.newBuilder().setCredit(taken).build();
                ledger.reserveAndSend(taken, time, () -> out.onNext(credit));
            }
        } catch (IllegalStateException cancelled) {
            // onNext or onCompleted on a call already cancelled: the stream has ended.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Records why the stream ended, if nothing has yet, and lets the session finish. */
    private void end(Status why) {
        synchronized (causeLock) {
            if (!causeSet) {
                causeSet = true;
                cause = why;
            }
        }
        readDone.countDown();
        capacity.wake();
    }

    private boolean over() {
        synchronized (causeLock) {
            return causeSet;
        }
    }

    private final class Reader implements ClientResponseObserver<LeaseJobsRequest, LeaseJobsResponse> {
        @Override
        public void beforeStart(ClientCallStreamObserver<LeaseJobsRequest> requestStream) {
            request = requestStream;
        }

        /** Never blocks and never calls the network: the reader must stay free for the next message. */
        @Override
        public void onNext(LeaseJobsResponse resp) {
            if (over()) {
                return; // a stream being ended carries nothing more worth starting
            }
            io.github.rpasco787.hopper.proto.v1.Job p = resp.getJob();
            var match = ledger.spend();
            if (match.isEmpty()) {
                var e = new OverDeliveryException("job \"" + p.getId() + "\" arrived with no credit outstanding");
                Status s = Status.CANCELLED.withDescription(e.getMessage()).withCause(e);
                synchronized (causeLock) {
                    if (!causeSet) {
                        causeSet = true;
                        cause = s;
                    }
                }
                request.cancel(e.getMessage(), e);
                return;
            }
            long received = time.nanoTime();
            Delivery d;
            try {
                d = Delivery.parse(p, received);
            } catch (MalformedDeliveryException e) {
                // Nothing in a malformed message can be trusted, so there is no id and lease
                // pair worth nacking. Return the capacity and say what arrived.
                log.log(EventLog.Level.ERROR, LogMessages.MALFORMED_DELIVERY, "err", e.getMessage(),
                        "job_id", p.getId(), "lease_id", p.getLeaseId());
                capacity.release();
                return;
            }
            d.matched(match.get());
            d.handler(config.handlers().get(d.job().queue()));
            dispatch(d);
        }

        @Override
        public void onError(Throwable t) {
            end(Status.fromThrowable(t));
        }

        @Override
        public void onCompleted() {
            end(null);
        }
    }

    /**
     * Runs a delivery on its own virtual thread: the check, the handler and the confirmation all
     * happen there, so a slow broker cannot stall the reader. The token goes back before the
     * delivery is struck from the counts, so nothing can observe a drained worker still short a
     * slot.
     */
    private void dispatch(Delivery d) {
        Work wk = lc.begin();
        Thread.ofVirtual().name("hopper-delivery-" + DELIVERY_NUMBER.incrementAndGet()).start(() -> {
            try {
                config.runner().run(lc, wk, channel, d);
            } finally {
                try {
                    capacity.release();
                } finally {
                    wk.end();
                }
            }
        });
    }

    private void awaitUninterruptibly(CountDownLatch latch) {
        while (true) {
            try {
                latch.await();
                return;
            } catch (InterruptedException e) {
                onInterrupt.run();
            }
        }
    }

    private void joinUninterruptibly(Thread t) {
        while (true) {
            try {
                t.join();
                return;
            } catch (InterruptedException e) {
                onInterrupt.run();
            }
        }
    }
}

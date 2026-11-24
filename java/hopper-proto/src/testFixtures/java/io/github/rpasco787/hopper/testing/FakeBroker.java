package io.github.rpasco787.hopper.testing;

import io.github.rpasco787.hopper.proto.ChannelDefaults;
import io.github.rpasco787.hopper.proto.v1.AckRequest;
import io.github.rpasco787.hopper.proto.v1.AckResponse;
import io.github.rpasco787.hopper.proto.v1.CancelRequest;
import io.github.rpasco787.hopper.proto.v1.CancelResponse;
import io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest;
import io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse;
import io.github.rpasco787.hopper.proto.v1.EnqueueRequest;
import io.github.rpasco787.hopper.proto.v1.EnqueueResponse;
import io.github.rpasco787.hopper.proto.v1.GetJobRequest;
import io.github.rpasco787.hopper.proto.v1.GetJobResponse;
import io.github.rpasco787.hopper.proto.v1.HeartbeatRequest;
import io.github.rpasco787.hopper.proto.v1.HeartbeatResponse;
import io.github.rpasco787.hopper.proto.v1.HopperServiceGrpc;
import io.github.rpasco787.hopper.proto.v1.Job;
import io.github.rpasco787.hopper.proto.v1.JobSpec;
import io.github.rpasco787.hopper.proto.v1.JobState;
import io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest;
import io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse;
import io.github.rpasco787.hopper.proto.v1.NackRequest;
import io.github.rpasco787.hopper.proto.v1.NackResponse;
import io.github.rpasco787.hopper.proto.v1.Subscription;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.InsecureChannelCredentials;
import io.grpc.InsecureServerCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A scriptable hopper broker, the port of worker/fake_test.go's {@code fake}, and
 * the one test broker for the client and the worker. It is a real gRPC server
 * speaking the real generated service, so the code under test goes through real
 * status codes and stream semantics, but every response is scripted by the test
 * rather than by a database.
 *
 * <p>Two rules shape it, both carried over from Go. Nothing on a server thread
 * calls a test assertion: a failure there is recorded and thrown by
 * {@link #close()} on the test thread, where it can be seen. And every wait takes
 * a timeout, so a script that goes wrong fails with a message naming what it was
 * waiting for instead of hanging the build.
 *
 * <p>{@link #start()} serves in-process, which is fast and deterministic: nothing
 * the tests assert depends on HTTP/2 framing. {@link #startOnLoopback()} serves
 * the same service over Netty on 127.0.0.1, for the few tests that exercise the
 * real channel defaults.
 *
 * <p>Hooks run on the server's call thread and may block, on a {@link Gate}, with
 * the call's {@link Context}. {@link #close()} cancels every call, and then proves
 * every call that started has finished.
 */
public final class FakeBroker implements AutoCloseable {
    /** The default budget for a harness wait; it beats the build's per-test timeout. */
    private static final Duration JOIN = Duration.ofSeconds(10);

    private static final AtomicInteger NAMES = new AtomicInteger();

    /** Runs when an Ack, Nack, Heartbeat or subscription arrives; throwing a status fails the call with it. */
    @FunctionalInterface
    public interface Hook<Q> {
        void apply(Q request, Context ctx) throws StatusException;
    }

    /** Answers a unary call; throwing a status fails the call with it. */
    @FunctionalInterface
    public interface UnaryHook<Q, R> {
        R apply(Q request, Context ctx) throws StatusException;
    }

    /**
     * Drives one accepted lease stream. Returning ends the stream OK; throwing a
     * {@link StatusException} ends it with that status.
     */
    @FunctionalInterface
    public interface LeaseScript {
        void run(FakeStream stream, Context ctx) throws Exception;
    }

    private final boolean loopback;
    private final String name = "fake-hopper-" + NAMES.incrementAndGet();

    /** Guards everything below it; also what {@link #awaitCalls} waits on. */
    private final Object mu = new Object();

    private Server server;
    private int port;
    private boolean closed;
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final Map<String, Integer> started = new HashMap<>();
    private final Map<String, Integer> finished = new HashMap<>();
    private final List<Throwable> failures = new ArrayList<>();

    private final List<Subscription> subscriptions = new ArrayList<>();
    private final List<HeartbeatRequest> heartbeats = new ArrayList<>();
    private final List<AckRequest> acks = new ArrayList<>();
    private final List<NackRequest> nacks = new ArrayList<>();
    private final List<EnqueueRequest> enqueues = new ArrayList<>();
    private final List<EnqueueBatchRequest> batches = new ArrayList<>();
    private final List<CancelRequest> cancels = new ArrayList<>();
    private final List<GetJobRequest> getJobs = new ArrayList<>();

    private UnaryHook<HeartbeatRequest, HeartbeatResponse> heartbeatHook;
    private UnaryHook<AckRequest, AckResponse> ackHook;
    private UnaryHook<NackRequest, NackResponse> nackHook;
    private UnaryHook<EnqueueRequest, EnqueueResponse> enqueueHook;
    private UnaryHook<EnqueueBatchRequest, EnqueueBatchResponse> batchHook;
    private UnaryHook<CancelRequest, CancelResponse> cancelHook;
    private UnaryHook<GetJobRequest, GetJobResponse> getJobHook;
    private Hook<Subscription> subscribeHook;
    private LeaseScript leaseScript = FakeStream::idle;

    /**
     * Publishes each accepted lease stream to the test thread, which then drives
     * it. It is bounded well past what any test opens so a handler never blocks
     * trying to announce itself.
     */
    private final LinkedBlockingQueue<FakeStream> streams = new LinkedBlockingQueue<>(32);

    private final AtomicInteger streamNumber = new AtomicInteger();

    private FakeBroker(boolean loopback) {
        this.loopback = loopback;
    }

    /** An in-process broker with a unique name and the default executor. */
    public static FakeBroker start() {
        return listening(new FakeBroker(false));
    }

    /** The same service over Netty on 127.0.0.1, on a free port. */
    public static FakeBroker startOnLoopback() {
        return listening(new FakeBroker(true));
    }

    private static FakeBroker listening(FakeBroker broker) {
        try {
            broker.listen();
        } catch (IOException e) {
            throw new UncheckedIOException("the fake broker could not listen", e);
        }
        return broker;
    }

    private void listen() throws IOException {
        Service service = new Service();
        if (loopback) {
            server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0), InsecureServerCredentials.create())
                    // What the real broker permits (internal/broker/lifetime.go:18).
                    .permitKeepAliveTime(5, TimeUnit.SECONDS)
                    .permitKeepAliveWithoutCalls(true)
                    .addService(service)
                    .build()
                    .start();
            port = server.getPort();
        } else {
            server = InProcessServerBuilder.forName(name).addService(service).build().start();
        }
    }

    /**
     * A channel to this broker, closed by {@link #close()}. In-process for
     * {@link #start()}, and through {@link ChannelDefaults#open} for a loopback
     * broker. {@code interceptors} sit on the channel, as with
     * {@code ManagedChannelBuilder.intercept}.
     */
    public ManagedChannel channel(ClientInterceptor... interceptors) {
        ManagedChannel channel = loopback
                ? ChannelDefaults.open(target(), InsecureChannelCredentials.create(), b -> b.intercept(interceptors))
                : InProcessChannelBuilder.forName(name).intercept(interceptors).build();
        synchronized (mu) {
            channels.add(channel);
        }
        return channel;
    }

    /** {@code 127.0.0.1:<port>}. Only a loopback broker has one. */
    public String target() {
        if (!loopback) {
            throw new IllegalStateException("only a broker started on loopback has a target");
        }
        return "127.0.0.1:" + port;
    }

    // Hooks. Each replaces the default; null restores it.

    public void onHeartbeat(Hook<HeartbeatRequest> hook) {
        synchronized (mu) {
            heartbeatHook = hook == null ? null : (request, ctx) -> {
                hook.apply(request, ctx);
                return HeartbeatResponse.getDefaultInstance();
            };
        }
    }

    public void onAck(Hook<AckRequest> hook) {
        synchronized (mu) {
            ackHook = hook == null ? null : (request, ctx) -> {
                hook.apply(request, ctx);
                return AckResponse.getDefaultInstance();
            };
        }
    }

    public void onNack(Hook<NackRequest> hook) {
        synchronized (mu) {
            nackHook = hook == null ? null : (request, ctx) -> {
                hook.apply(request, ctx);
                return NackResponse.getDefaultInstance();
            };
        }
    }

    /** Runs after the subscription is recorded and before the stream is published. */
    public void onSubscribe(Hook<Subscription> hook) {
        synchronized (mu) {
            subscribeHook = hook;
        }
    }

    /** What to do with each accepted lease stream. The default is {@link FakeStream#idle}. */
    public void onLease(LeaseScript script) {
        synchronized (mu) {
            leaseScript = script == null ? FakeStream::idle : script;
        }
    }

    public void onEnqueue(UnaryHook<EnqueueRequest, EnqueueResponse> hook) {
        synchronized (mu) {
            enqueueHook = hook;
        }
    }

    public void onEnqueueBatch(UnaryHook<EnqueueBatchRequest, EnqueueBatchResponse> hook) {
        synchronized (mu) {
            batchHook = hook;
        }
    }

    public void onCancel(UnaryHook<CancelRequest, CancelResponse> hook) {
        synchronized (mu) {
            cancelHook = hook;
        }
    }

    public void onGetJob(UnaryHook<GetJobRequest, GetJobResponse> hook) {
        synchronized (mu) {
            getJobHook = hook;
        }
    }

    // Counts. "started" means a call arrived; "finished" means its handler returned.
    // They are separate because they answer different questions: whether a client
    // sent a call at all, and whether it waited for the answer.

    public int started(String method) {
        synchronized (mu) {
            return started.getOrDefault(method, 0);
        }
    }

    public int finished(String method) {
        synchronized (mu) {
            return finished.getOrDefault(method, 0);
        }
    }

    /** Blocks until at least {@code n} calls to {@code method} have arrived. */
    public void awaitCalls(String method, int n, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (mu) {
            try {
                while (started.getOrDefault(method, 0) < n) {
                    long left = deadline - System.nanoTime();
                    if (left <= 0) {
                        throw new AssertionError("waiting for " + n + " " + method + " calls, have "
                                + started.getOrDefault(method, 0));
                    }
                    TimeUnit.NANOSECONDS.timedWait(mu, left);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted waiting for " + n + " " + method + " calls", e);
            }
        }
    }

    // Recorded requests, as snapshots. Protos are immutable, so a copy of the list is enough.

    public List<HeartbeatRequest> heartbeatRequests() {
        return snapshot(heartbeats);
    }

    public List<AckRequest> ackRequests() {
        return snapshot(acks);
    }

    public List<NackRequest> nackRequests() {
        return snapshot(nacks);
    }

    public List<Subscription> subscriptions() {
        return snapshot(subscriptions);
    }

    public List<EnqueueRequest> enqueueRequests() {
        return snapshot(enqueues);
    }

    public List<EnqueueBatchRequest> batchRequests() {
        return snapshot(batches);
    }

    public List<CancelRequest> cancelRequests() {
        return snapshot(cancels);
    }

    public List<GetJobRequest> getJobRequests() {
        return snapshot(getJobs);
    }

    private <T> List<T> snapshot(List<T> recorded) {
        synchronized (mu) {
            return List.copyOf(recorded);
        }
    }

    /** The next lease stream the broker accepted. */
    public FakeStream awaitStream(Duration timeout) {
        try {
            FakeStream stream = streams.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
            if (stream == null) {
                throw new AssertionError("no lease stream opened within " + timeout);
            }
            return stream;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for a lease stream", e);
        }
    }

    /**
     * Cancels every call and closes every channel, then proves every call that
     * started has finished: a handler that outlives its test is a leak the next
     * test would pay for. Throws {@link AssertionError} if one did not, or if a
     * server thread recorded a failure. Idempotent.
     */
    @Override
    public void close() {
        List<ManagedChannel> toClose;
        synchronized (mu) {
            if (closed) {
                return;
            }
            closed = true;
            toClose = List.copyOf(channels);
        }
        AssertionError problem = null;
        try {
            for (ManagedChannel channel : toClose) {
                channel.shutdownNow();
            }
            server.shutdownNow();
            if (!server.awaitTermination(JOIN.toMillis(), TimeUnit.MILLISECONDS)) {
                problem = new AssertionError("the fake broker's server did not terminate within " + JOIN);
            }
            for (ManagedChannel channel : toClose) {
                if (!channel.awaitTermination(JOIN.toMillis(), TimeUnit.MILLISECONDS)) {
                    problem = new AssertionError("a channel to the fake broker did not terminate within " + JOIN);
                }
            }
            String stuck = awaitHandlers();
            if (stuck != null) {
                problem = new AssertionError("fake broker RPC handlers did not return after shutdown: " + stuck);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            problem = new AssertionError("interrupted while closing the fake broker", e);
        }
        synchronized (mu) {
            if (problem == null && !failures.isEmpty()) {
                problem = new AssertionError(
                        "the fake broker recorded " + failures.size() + " failure(s) on its server threads");
            }
            for (Throwable failure : failures) {
                if (problem != null) {
                    problem.addSuppressed(failure);
                }
            }
        }
        if (problem != null) {
            throw problem;
        }
    }

    /** Waits for finished to catch up with started; returns a description of what is stuck, or null. */
    private String awaitHandlers() throws InterruptedException {
        long deadline = System.nanoTime() + JOIN.toNanos();
        synchronized (mu) {
            while (true) {
                StringBuilder stuck = new StringBuilder();
                for (Map.Entry<String, Integer> e : started.entrySet()) {
                    int done = finished.getOrDefault(e.getKey(), 0);
                    if (done < e.getValue()) {
                        stuck.append(e.getKey()).append(" started ").append(e.getValue()).append(" finished ")
                                .append(done).append("; ");
                    }
                }
                if (stuck.length() == 0) {
                    return null;
                }
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    return stuck.toString();
                }
                TimeUnit.NANOSECONDS.timedWait(mu, left);
            }
        }
    }

    // Server side.

    /** Records an arrival and returns the function that records completion (idempotently). */
    private Runnable enter(String method) {
        synchronized (mu) {
            started.merge(method, 1, Integer::sum);
            mu.notifyAll();
        }
        AtomicBoolean once = new AtomicBoolean();
        return () -> {
            if (once.compareAndSet(false, true)) {
                synchronized (mu) {
                    finished.merge(method, 1, Integer::sum);
                    mu.notifyAll();
                }
            }
        };
    }

    private void fail(String what, Throwable t) {
        synchronized (mu) {
            failures.add(new AssertionError(what + ": " + t, t));
        }
    }

    /**
     * One unary call: record the request, run the hook (or the default), mark the
     * call finished, and only then answer. Finished is recorded before the answer
     * is sent, as Go's deferred completion runs before its response is written, so
     * a client that has its answer can rely on the count.
     */
    private <Q, R> void serve(
            String method,
            Q request,
            StreamObserver<R> out,
            List<Q> recorded,
            Supplier<UnaryHook<Q, R>> hook,
            Function<Q, R> otherwise) {
        Runnable done = enter(method);
        UnaryHook<Q, R> scripted;
        synchronized (mu) {
            recorded.add(request);
            scripted = hook.get();
        }
        R response;
        try {
            response = scripted != null ? scripted.apply(request, Context.current()) : otherwise.apply(request);
        } catch (StatusException e) {
            done.run();
            out.onError(e);
            return;
        } catch (StatusRuntimeException e) {
            done.run();
            out.onError(e);
            return;
        } catch (RuntimeException | Error e) {
            fail(method + " hook", e);
            done.run();
            out.onError(Status.INTERNAL.withDescription(String.valueOf(e)).asRuntimeException());
            return;
        }
        done.run();
        out.onNext(response);
        out.onCompleted();
    }

    private static Job job(String queue, String id, JobState state) {
        return Job.newBuilder().setId(id).setQueue(queue).setState(state).build();
    }

    private final class Service extends HopperServiceGrpc.HopperServiceImplBase {
        @Override
        public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> out) {
            serve("Heartbeat", request, out, heartbeats, () -> heartbeatHook,
                    q -> HeartbeatResponse.getDefaultInstance());
        }

        @Override
        public void ack(AckRequest request, StreamObserver<AckResponse> out) {
            serve("Ack", request, out, acks, () -> ackHook, q -> AckResponse.getDefaultInstance());
        }

        @Override
        public void nack(NackRequest request, StreamObserver<NackResponse> out) {
            serve("Nack", request, out, nacks, () -> nackHook, q -> NackResponse.getDefaultInstance());
        }

        @Override
        public void enqueue(EnqueueRequest request, StreamObserver<EnqueueResponse> out) {
            serve("Enqueue", request, out, enqueues, () -> enqueueHook, FakeBroker::defaultEnqueue);
        }

        @Override
        public void enqueueBatch(EnqueueBatchRequest request, StreamObserver<EnqueueBatchResponse> out) {
            serve("EnqueueBatch", request, out, batches, () -> batchHook, q -> {
                EnqueueBatchResponse.Builder response = EnqueueBatchResponse.newBuilder();
                for (JobSpec spec : q.getJobsList()) {
                    response.addResults(defaultEnqueue(EnqueueRequest.newBuilder().setJob(spec).build()));
                }
                return response.build();
            });
        }

        @Override
        public void cancel(CancelRequest request, StreamObserver<CancelResponse> out) {
            serve("Cancel", request, out, cancels, () -> cancelHook, q -> CancelResponse.newBuilder()
                    .setJob(job("", q.getId(), JobState.JOB_STATE_CANCELLED))
                    .setAlreadyCancelled(false)
                    .build());
        }

        @Override
        public void getJob(GetJobRequest request, StreamObserver<GetJobResponse> out) {
            serve("GetJob", request, out, getJobs, () -> getJobHook, q -> GetJobResponse.newBuilder()
                    .setJob(job("", q.getId(), JobState.JOB_STATE_AVAILABLE))
                    .build());
        }

        /**
         * Accepts the subscription, hands the stream to the test's script on a
         * virtual thread, and feeds it credit as it arrives. The default script
         * idles until the client half-closes or the call is cancelled, which is
         * what an unloaded broker looks like to a worker with nothing to do.
         */
        @Override
        public StreamObserver<LeaseJobsRequest> leaseJobs(StreamObserver<LeaseJobsResponse> responseObserver) {
            Runnable done = enter("LeaseJobs");
            ServerCallStreamObserver<LeaseJobsResponse> call = (ServerCallStreamObserver<LeaseJobsResponse>) responseObserver;
            // Without a handler, writing to a cancelled call throws; a cancel is not a fixture failure.
            call.setOnCancelHandler(() -> {});
            Context ctx = Context.current();

            return new StreamObserver<>() {
                private FakeStream stream;
                private boolean rejected;

                @Override
                public void onNext(LeaseJobsRequest message) {
                    if (rejected) {
                        return;
                    }
                    if (stream == null) {
                        if (message.getMsgCase() != LeaseJobsRequest.MsgCase.SUBSCRIBE) {
                            reject("the first message must be a subscription");
                            return;
                        }
                        Subscription sub = message.getSubscribe();
                        synchronized (mu) {
                            subscriptions.add(sub);
                        }
                        FakeStream s = new FakeStream(sub, call);
                        stream = s;
                        Thread.ofVirtual()
                                .name("fake-lease-" + streamNumber.incrementAndGet())
                                .start(ctx.wrap(() -> runLease(s, ctx, done)));
                        return;
                    }
                    if (message.getMsgCase() == LeaseJobsRequest.MsgCase.CREDIT) {
                        stream.onCredit(message.getCredit());
                    } else {
                        stream.onProtocolError("only credit messages may follow subscribe");
                    }
                }

                @Override
                public void onError(Throwable t) {
                    if (stream != null) {
                        stream.onCancel(Status.fromThrowable(t));
                    } else {
                        done.run();
                    }
                }

                @Override
                public void onCompleted() {
                    if (stream != null) {
                        stream.onHalfClose();
                    } else if (!rejected) {
                        reject("the stream ended before a subscription");
                    }
                }

                private void reject(String message) {
                    rejected = true;
                    done.run();
                    call.onError(Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException());
                }
            };
        }
    }

    private static EnqueueResponse defaultEnqueue(EnqueueRequest request) {
        return EnqueueResponse.newBuilder()
                .setJob(job(request.getJob().getQueue(), UUID.randomUUID().toString(), JobState.JOB_STATE_AVAILABLE))
                .setDeduplicated(false)
                .build();
    }

    /** The body of one lease stream's handler: the subscribe hook, publication, then the script. */
    private void runLease(FakeStream stream, Context ctx, Runnable done) {
        Status status = null;
        try {
            Hook<Subscription> hook;
            LeaseScript script;
            synchronized (mu) {
                hook = subscribeHook;
                script = leaseScript;
            }
            if (hook != null) {
                hook.apply(stream.sub(), ctx);
            }
            if (!streams.offer(stream)) {
                throw Status.RESOURCE_EXHAUSTED
                        .withDescription("the fake broker's stream buffer is full")
                        .asException();
            }
            script.run(stream, ctx);
        } catch (StatusException e) {
            status = e.getStatus();
        } catch (StatusRuntimeException e) {
            status = e.getStatus();
        } catch (Exception | Error e) {
            fail("lease script", e);
            status = Status.INTERNAL.withDescription(String.valueOf(e));
        }
        done.run();
        stream.finish(status);
    }
}

package io.github.rpasco787.hopper.worker;

import io.github.rpasco787.hopper.proto.ChannelDefaults;
import io.grpc.ChannelCredentials;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * hopper's worker: one lease stream at a time, a bounded number of handlers, each delivery's
 * lease kept alive while its handler runs, and the outcome confirmed (worker/worker.go).
 *
 * <p>Delivery is at least once (docs/semantics.md §1). The SDK narrows the window where a
 * duplicate can happen, but cannot close it: handlers must be idempotent, keyed on
 * {@link Job#id()} or a business key in the payload.
 *
 * <pre>{@code
 * HopperWorker w = HopperWorker.builder("127.0.0.1:9090")
 *         .concurrency(16)
 *         .handle("email", ctx -> sendEmail(ctx.job()))
 *         .build();
 * w.run(); // blocks until shutdown() and the drain finish, or a permanent failure
 * }</pre>
 */
public final class HopperWorker {
    static final int DEFAULT_CONCURRENCY = 8;
    static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);
    /** Credit is uint32 on the wire; 65,536 handlers is already far past where the database is the constraint. */
    static final int MAX_CONCURRENCY = 1 << 16;
    /** How long a stream must stay up for its end to count as routine; the backoff cap in production. */
    static final Duration DEFAULT_HEALTHY_SESSION = Duration.ofSeconds(5);

    private final String target;
    private final ManagedChannel callerChannel;
    private final ChannelCredentials credentials;
    private final int concurrency;
    private final Duration leaseDuration;
    private final Duration shutdownTimeout;
    private final String workerId;
    private final EventLog log;
    private final Map<String, JobHandler> handlers;
    private final DeliveryRunner runner;
    private final TimeSource timeSource;
    private final Duration healthySession;

    private final AtomicBoolean started = new AtomicBoolean();
    private final ReentrantLock lock = new ReentrantLock();
    private boolean shutdownRequested;
    private Lifecycle lifecycle;
    private final CountDownLatch terminated = new CountDownLatch(1);

    private HopperWorker(Builder b) {
        this.target = b.target;
        this.callerChannel = b.channel;
        this.credentials = b.credentials;
        this.concurrency = b.concurrency == 0 ? DEFAULT_CONCURRENCY : b.concurrency;
        this.leaseDuration = b.leaseDuration;
        this.shutdownTimeout = b.shutdownTimeout.isZero() ? DEFAULT_SHUTDOWN_TIMEOUT : b.shutdownTimeout;
        this.workerId = b.workerId == null || b.workerId.isEmpty() ? DefaultId.VALUE : b.workerId;
        EventLog base = b.log != null ? b.log : new JsonLinesLog(System.err, EventLog.Level.INFO);
        this.log = base.with("worker_id", workerId);
        this.handlers = Map.copyOf(b.handlers);
        this.runner = b.runner != null ? b.runner : (lc, wk, ch, d) -> {
            throw new UnsupportedOperationException("worker: the real delivery runner arrives in Task 17");
        };
        this.timeSource = b.timeSource;
        this.healthySession = b.healthySession;
    }

    /** A worker that dials target with the SDK's channel defaults, and owns that channel. */
    public static Builder builder(String target) {
        return new Builder(target, null);
    }

    /** A worker on a caller-owned channel, which it never shuts down. */
    public static Builder builder(ManagedChannel channel) {
        return new Builder(null, Objects.requireNonNull(channel, "worker: a channel is required"));
    }

    // Package-private views, for tests and the session.

    int concurrency() {
        return concurrency;
    }

    Duration leaseDuration() {
        return leaseDuration;
    }

    Duration shutdownTimeout() {
        return shutdownTimeout;
    }

    String workerId() {
        return workerId;
    }

    /** The registered queues in a stable order, so a subscription reads the same on every run. */
    List<String> queues() {
        return List.copyOf(new TreeMap<>(handlers).keySet());
    }

    /**
     * Consumes jobs until {@link #shutdown()} and the drain finish, or a permanent failure stops
     * it. The drain stops accepting work, half-closes the stream, and gives running handlers and
     * their confirmations until the shutdown timeout to finish.
     *
     * @throws WorkerFailedException for a permanent stream failure, or when the shutdown deadline
     *     passed with work in flight, or both
     * @throws IllegalStateException if run() was already called: it is single-use
     */
    public void run() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("worker: run has already been called");
        }
        try {
            consume();
        } finally {
            terminated.countDown();
        }
    }

    /** Begins the drain, from any thread, any number of times. Before run(), run() drains at once. */
    public void shutdown() {
        Lifecycle lc;
        lock.lock();
        try {
            shutdownRequested = true;
            lc = lifecycle;
        } finally {
            lock.unlock();
        }
        if (lc != null) {
            lc.beginDrain("shutdown requested");
        }
    }

    /** Waits for run() to return; true if it has. */
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        return terminated.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Holds lease streams and drains on the way out (worker.go consume). The lifecycle exists
     * before anything can block on the network, so a shutdown during the dial gets the same
     * bounded drain as one an hour later.
     */
    private void consume() {
        SystemTimeSource ownedTime = timeSource == null ? new SystemTimeSource(log) : null;
        TimeSource time = timeSource != null ? timeSource : ownedTime;
        Lifecycle lc = new Lifecycle(shutdownTimeout, time, log);
        boolean before;
        lock.lock();
        try {
            lifecycle = lc;
            before = shutdownRequested;
        } finally {
            lock.unlock();
        }
        AtomicBoolean interrupted = new AtomicBoolean();
        Runnable onInterrupt = () -> {
            interrupted.set(true);
            lc.beginDrain("caller interrupted");
        };
        if (before) {
            lc.beginDrain("shutdown requested");
        }
        if (Thread.interrupted()) {
            onInterrupt.run();
        }
        ManagedChannel channel = null;
        try {
            Capacity capacity = new Capacity(concurrency, log);
            lc.onDrain(capacity::wake);
            Status permanent = null;
            if (!lc.isDraining()) {
                try {
                    channel = callerChannel != null ? callerChannel
                            : ChannelDefaults.open(target, credentials, b -> {});
                } catch (RuntimeException e) {
                    lc.beginDrain("dial failed");
                    lc.await();
                    throw e;
                }
                permanent = sessions(lc, time, channel, capacity, onInterrupt);
            }
            if (lc.isDraining() && permanent == null) {
                log.log(EventLog.Level.INFO, LogMessages.NOT_REOPENING);
            }
            lc.beginDrain("lease stream ended");
            lc.finishIntake();
            Optional<AbandonedCounts> abandoned = lc.await();
            if (permanent != null || abandoned.isPresent()) {
                throw new WorkerFailedException(permanent, abandoned.orElse(null));
            }
        } finally {
            lc.stop();
            if (channel != null && channel != callerChannel) {
                channel.shutdownNow();
            }
            if (ownedTime != null) {
                ownedTime.close();
            }
            if (interrupted.get() || Thread.interrupted()) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Opens session after session until the drain or a permanent code; returns that code's status. */
    private Status sessions(Lifecycle lc, TimeSource time, ManagedChannel channel, Capacity capacity,
                            Runnable onInterrupt) {
        Session.Config config = new Session.Config(queues(), workerId, leaseDuration, handlers, runner);
        Backoff backoff = new Backoff();
        Pause pause = new Pause(lc, time);
        while (!lc.isDraining()) {
            Session s = new Session(config, lc, channel, capacity, time, log, onInterrupt);
            long opened = time.nanoTime();
            Status err = s.run();
            if (lc.isDraining()) {
                break;
            }
            if (permanent(err)) {
                return err;
            }
            // A stream that delivered, or that stayed up a while, reached a healthy broker, so
            // the next attempt starts from the shortest delay. The second case is the broker
            // ending the stream at its maximum age, which on a quiet queue delivers nothing;
            // treating it as a failure would climb the backoff toward its cap.
            boolean healthy = time.nanoTime() - opened >= healthySession.toNanos();
            if (healthy || s.deliveryCount() > 0) {
                backoff.reset();
            }
            if (err != null) {
                EventLog.Level level = healthy && err.getCode() == Status.Code.UNAVAILABLE
                        ? EventLog.Level.INFO : EventLog.Level.WARN;
                log.log(level, LogMessages.STREAM_FAILED_RECONNECTING, "err", WorkerFailedException.streamFailure(err));
            } else {
                log.log(EventLog.Level.WARN, LogMessages.STREAM_CLOSED_RECONNECTING);
            }
            pause.sleep(backoff.next(), onInterrupt);
        }
        return null;
    }

    /**
     * A configuration or authentication refusal, not a transport interruption. UNAVAILABLE is
     * deliberately absent: the broker uses it while draining, and a replacement may be up.
     */
    private static boolean permanent(Status s) {
        if (s == null) {
            return false;
        }
        return switch (s.getCode()) {
            case INVALID_ARGUMENT, UNIMPLEMENTED, PERMISSION_DENIED, UNAUTHENTICATED -> true;
            default -> false;
        };
    }

    /** The reconnect wait, on the TimeSource, ending early on the drain or the forced shutdown. */
    private static final class Pause {
        private final Lifecycle lc;
        private final TimeSource time;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition changed = lock.newCondition();

        Pause(Lifecycle lc, TimeSource time) {
            this.lc = lc;
            this.time = time;
            lc.onDrain(this::signal);
            lc.forceContext().addListener(c -> signal(), Runnable::run);
        }

        private void signal() {
            lock.lock();
            try {
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        }

        void sleep(Duration d, Runnable onInterrupt) {
            AtomicBoolean fired = new AtomicBoolean();
            TimeSource.Timer t = time.schedule(() -> {
                fired.set(true);
                signal();
            }, d.toNanos());
            lock.lock();
            try {
                while (!fired.get() && !lc.isDraining() && !lc.forced()) {
                    try {
                        changed.await();
                    } catch (InterruptedException e) {
                        onInterrupt.run();
                    }
                }
            } finally {
                lock.unlock();
                t.cancel();
            }
        }
    }

    /** The default worker id: hostname-pid, computed once (a hostname lookup can be slow). */
    private static final class DefaultId {
        static final String VALUE = compute();

        private static String compute() {
            String host;
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                host = "";
            }
            if (host == null || host.isEmpty()) {
                host = "unknown";
            }
            return host + "-" + ProcessHandle.current().pid();
        }
    }

    /**
     * Configures a worker. Zero values select defaults; a negative is rejected at build(), not
     * silently corrected, because hiding it would hide the bug that produced it.
     */
    public static final class Builder {
        private final String target;
        private final ManagedChannel channel;
        private ChannelCredentials credentials = InsecureChannelCredentials.create();
        private int concurrency;
        private Duration leaseDuration = Duration.ZERO;
        private Duration shutdownTimeout = Duration.ZERO;
        private String workerId;
        private EventLog log;
        private final Map<String, JobHandler> handlers = new TreeMap<>();
        private DeliveryRunner runner;
        private TimeSource timeSource;
        private Duration healthySession = DEFAULT_HEALTHY_SESSION;

        private Builder(String target, ManagedChannel channel) {
            this.target = target;
            this.channel = channel;
        }

        /** The most handlers that may run at once, and the credit advertised: 1..65536, or 0 for 8. */
        public Builder concurrency(int n) {
            this.concurrency = n;
            return this;
        }

        /** The visibility timeout to request. ZERO asks the broker to choose. */
        public Builder leaseDuration(Duration d) {
            this.leaseDuration = Objects.requireNonNull(d, "worker: lease duration");
            return this;
        }

        /**
         * Bounds the whole drain: stream teardown, handlers and confirmations share it. ZERO takes
         * 30 s. It must fit inside the deployment's own grace period.
         */
        public Builder shutdownTimeout(Duration d) {
            this.shutdownTimeout = Objects.requireNonNull(d, "worker: shutdown timeout");
            return this;
        }

        /** Recorded on leased rows as leased_by, for observability only. Empty takes hostname-pid. */
        public Builder workerId(String id) {
            this.workerId = id;
            return this;
        }

        /** Where lifecycle and per-delivery events go. The default is JSON lines on System.err at INFO. */
        public Builder log(EventLog log) {
            this.log = Objects.requireNonNull(log, "worker: log");
            return this;
        }

        /** The transport credentials for a channel the worker dials; plaintext by default. */
        public Builder credentials(ChannelCredentials c) {
            this.credentials = Objects.requireNonNull(c, "worker: credentials");
            return this;
        }

        /**
         * Registers the handler for a queue. An empty queue, a null handler or a duplicate is a
         * programmer error visible at startup, and throws here.
         */
        public Builder handle(String queue, JobHandler handler) {
            if (queue == null) {
                throw new NullPointerException("worker: handle called with a null queue");
            }
            if (queue.isEmpty()) {
                throw new IllegalArgumentException("worker: handle called with an empty queue");
            }
            if (handler == null) {
                throw new NullPointerException("worker: handle called with a null handler for queue " + quote(queue));
            }
            if (handlers.containsKey(queue)) {
                throw new IllegalArgumentException("worker: queue " + quote(queue) + " already has a handler");
            }
            handlers.put(queue, handler);
            return this;
        }

        /** Test seam: the delivery runner (Go: w.runner). */
        Builder runner(DeliveryRunner r) {
            this.runner = r;
            return this;
        }

        /** Test seam: every clock reading and wait (Decision D19). */
        Builder timeSource(TimeSource t) {
            this.timeSource = t;
            return this;
        }

        /** Test seam: how long a stream must stay up for its end to count as routine (Go: w.healthySession). */
        Builder healthySession(Duration d) {
            this.healthySession = d;
            return this;
        }

        /** Validates before any network work, so a bad value never leaves a half-connected worker. */
        public HopperWorker build() {
            if (channel == null && (target == null || target.isEmpty())) {
                throw new IllegalArgumentException("worker: a broker target is required");
            }
            if (handlers.isEmpty()) {
                throw new IllegalStateException("worker: no handlers registered");
            }
            if (concurrency < 0) {
                throw new IllegalArgumentException("worker: concurrency " + concurrency + " must not be negative");
            }
            if (concurrency > MAX_CONCURRENCY) {
                throw new IllegalArgumentException(
                        "worker: concurrency " + concurrency + " exceeds the maximum of " + MAX_CONCURRENCY);
            }
            if (leaseDuration.isNegative()) {
                throw new IllegalArgumentException(
                        "worker: lease duration " + GoDuration.format(leaseDuration) + " must not be negative");
            }
            if (shutdownTimeout.isNegative()) {
                throw new IllegalArgumentException(
                        "worker: shutdown timeout " + GoDuration.format(shutdownTimeout) + " must not be negative");
            }
            return new HopperWorker(this);
        }

        private static String quote(String s) {
            return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        }
    }
}

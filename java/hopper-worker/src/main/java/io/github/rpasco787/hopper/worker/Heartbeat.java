package io.github.rpasco787.hopper.worker;

import io.github.rpasco787.hopper.proto.v1.HeartbeatRequest;
import io.github.rpasco787.hopper.proto.v1.HopperServiceGrpc;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Owns one delivery's lease (worker/heartbeat.go heartbeat): it proves ownership before a
 * handler starts, for a delivery the start rule can't vouch for; renews while the handler runs;
 * and gives up at a deadline it can reach without the network.
 *
 * <p>Expiry and renewal are separate timers on the TimeSource, because expiry must not depend on
 * an RPC making progress: a heartbeat stuck on a wedged broker is exactly when the handler most
 * needs to learn that its lease ran out. Each renewal's RPC runs on its own virtual thread, so
 * the scheduler never blocks. Renewals are G/3 apart, counted from the previous one's return;
 * there is deliberately no minimum interval.
 */
final class Heartbeat {
    /** Bounds one ownership-validation attempt; the acquisition budget bounds the sequence. */
    static final long ACQUISITION_RPC_TIMEOUT_NANOS = 5_000_000_000L;

    private final Channel channel;
    private final EventLog log;
    private final Delivery d;
    private final TimeSource time;
    private final Consumer<Lease.Ended> onLost;
    private final Lease lease = new Lease();
    /** Bounds every heartbeat RPC; cancelling it cancels the one in flight. */
    private final Context.CancellableContext ctx;
    private final HeartbeatRequest request;
    private final long interval;

    private final AtomicBoolean lostOnce = new AtomicBoolean();
    private final ReentrantLock lock = new ReentrantLock();
    // stopping separates the SDK's own cancellation from the broker's refusal. Without it,
    // cancelling the in-flight RPC when a handler finishes normally would look exactly like a
    // rejection and suppress the Ack that follows.
    private volatile boolean stopping;
    private TimeSource.Timer expiryTimer;
    private TimeSource.Timer renewalTimer;
    private Thread renewalThread;

    Heartbeat(Context parent, Channel channel, EventLog log, Delivery d, TimeSource time, Consumer<Lease.Ended> onLost) {
        this.channel = channel;
        this.log = log.with("job_id", d.job().id(), "lease_id", d.leaseId(), "queue", d.job().queue());
        this.d = d;
        this.time = time;
        this.onLost = onLost;
        this.ctx = parent.withCancellation();
        long g = d.grantNanos();
        // The delivery's timestamps are the only demonstrated grant. Pin every validation and
        // renewal to it: asking each broker to choose again would let a rolling configuration
        // change shorten the store lease while this worker scheduled against the older one.
        this.request = HeartbeatRequest.newBuilder()
                .setId(d.job().id().toString())
                .setLeaseId(d.leaseId().toString())
                .setLeaseDuration(com.google.protobuf.Duration.newBuilder()
                        .setSeconds(g / 1_000_000_000L).setNanos((int) (g % 1_000_000_000L)))
                .build();
        this.interval = Math.max(g / 3, 1);
    }

    Lease lease() {
        return lease;
    }

    private void notifyLost(Lease.Ended why) {
        if (why != null && lostOnce.compareAndSet(false, true)) {
            onLost.accept(why);
        }
    }

    /**
     * Proves the lease is still ours before a handler runs, on the delivery's thread. Empty means
     * the lease is established; otherwise the lease has ended with the returned cause. Every
     * failure is terminal for this delivery, including running out of budget: leaving the lease
     * looking live would let a later check mistake "never proven" for "still ours". The budget
     * is fixed at receipt and bounds the waiting; only a successful response claims ownership.
     */
    Optional<Lease.Ended> acquire(long budget) {
        Lease.Ended err = attemptAcquire(budget);
        if (err != null) {
            lease.lose(err.reason(), err.detail());
            return Optional.of(lease.state().ended().orElse(err));
        }
        return Optional.empty();
    }

    private Lease.Ended attemptAcquire(long budget) {
        Backoff b = new Backoff();
        for (int attempt = 1; ; attempt++) {
            long sent = time.nanoTime();
            if (sent - budget >= 0) {
                return exhausted(attempt - 1);
            }
            long timeout = Math.min(ACQUISITION_RPC_TIMEOUT_NANOS, budget - sent);
            StatusRuntimeException failure = call(timeout);
            if (failure == null) {
                long deadline = StartRule.deadlineFrom(sent, d.grantNanos());
                if (time.nanoTime() - deadline >= 0) {
                    // The round trip consumed the grant: there is no time left to run anything in.
                    return new Lease.Ended(StopReason.LEASE_EXPIRED, "worker: job " + d.job().id()
                            + ": the lease was spent before acquisition returned: " + StopReason.LEASE_EXPIRED.text());
                }
                lease.establish(deadline);
                return null;
            }
            if (ctx.isCancelled()) {
                return new Lease.Ended(StopReason.SHUTTING_DOWN, "worker: job " + d.job().id()
                        + ": lease acquisition stopped: " + causeText(ctx.cancellationCause()));
            }
            Lease.Ended refused = classify(failure.getStatus());
            if (refused != null) {
                return refused;
            }
            // Transient. Wait inside what is left of the budget; running out of it is the same
            // answer as being refused, just slower.
            long wait = Math.min(b.next().toNanos(), budget - time.nanoTime());
            if (!sleep(time, wait, ctx)) {
                return exhausted(attempt);
            }
        }
    }

    private Lease.Ended exhausted(int attempts) {
        return new Lease.Ended(StopReason.LEASE_EXPIRED, "worker: job " + d.job().id()
                + ": lease acquisition exhausted its budget after " + attempts + " attempts: "
                + StopReason.LEASE_EXPIRED.text());
    }

    /** One Heartbeat, inside the heartbeat's context, bounded by timeoutNanos. Null on success. */
    private StatusRuntimeException call(long timeoutNanos) {
        try {
            ctx.call(() -> HopperServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(timeoutNanos, TimeUnit.NANOSECONDS)
                    .heartbeat(request));
            return null;
        } catch (StatusRuntimeException e) {
            return e;
        } catch (Exception e) {
            return Status.fromThrowable(e).asRuntimeException();
        }
    }

    /**
     * What a Heartbeat failure means (heartbeat.go classifyHeartbeat): null to retry. ABORTED and
     * NOT_FOUND are definitive; FAILED_PRECONDITION means the job was cancelled; UNAVAILABLE and
     * DEADLINE_EXCEEDED describe the network, not the lease. Anything else, INTERNAL above all,
     * stops the work: a generic store failure may be a permanent rejection or an ambiguous
     * commit, and retrying blindly is how a worker keeps hold of a job it has already lost.
     */
    static Lease.Ended classify(Status s) {
        return switch (s.getCode()) {
            case ABORTED, NOT_FOUND -> new Lease.Ended(StopReason.LEASE_LOST,
                    StopReason.LEASE_LOST.text() + ": " + statusText(s));
            case FAILED_PRECONDITION -> new Lease.Ended(StopReason.JOB_CANCELLED,
                    StopReason.JOB_CANCELLED.text() + ": " + statusText(s));
            default -> isNetwork(s)
                    ? null
                    : new Lease.Ended(StopReason.LEASE_LOST, "worker: heartbeat rejected: " + statusText(s));
        };
    }

    /**
     * Whether a failure describes the network rather than the job or the store: UNAVAILABLE and
     * DEADLINE_EXCEEDED, and the one UNKNOWN grpc-netty makes of a write onto a connection that
     * has just closed (Utils.statusFromThrowable: description "channel closed", a
     * ClosedChannelException as its cause). Go's gRPC reports that same moment as UNAVAILABLE, so
     * without this a broker killed under an in-flight Ack or renewal read as a refusal here and
     * the job ran twice (Phase 3c, chaos pass 2 of 5).
     */
    static boolean isNetwork(Status s) {
        return switch (s.getCode()) {
            case UNAVAILABLE, DEADLINE_EXCEEDED -> true;
            case UNKNOWN -> s.getCause() instanceof java.nio.channels.ClosedChannelException;
            default -> false;
        };
    }

    /** CODE: description, the Java counterpart of Go's "rpc error: code = ... desc = ...". */
    static String statusText(Status s) {
        return s.getDescription() == null ? s.getCode().toString() : s.getCode() + ": " + s.getDescription();
    }

    private static String causeText(Throwable t) {
        return t == null ? "cancelled" : t.getMessage();
    }

    /** Begins renewal and expiry monitoring. */
    void start() {
        armExpiry();
        armRenewal();
    }

    private void armExpiry() {
        lock.lock();
        try {
            if (stopping) {
                return;
            }
            long wait = lease.state().deadline() - time.nanoTime();
            expiryTimer = time.schedule(this::onExpiryTimer, wait);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Re-reads the deadline on every wakeup, so a timer armed for a deadline that has since moved
     * cannot expire a renewed lease; it re-arms on the newer one.
     */
    private void onExpiryTimer() {
        if (stopping) {
            return;
        }
        Lease.State st = lease.state();
        if (st.ended().isPresent()) {
            return;
        }
        if (lease.expireIfPast(time.nanoTime())) {
            logExpired(st.deadline());
            notifyLost(lease.state().ended().orElseThrow());
            return;
        }
        armExpiry();
    }

    /** Arms the next renewal one interval from now. Package-private so a test can drive the renewal path alone. */
    void armRenewal() {
        lock.lock();
        try {
            if (!stopping) {
                renewalTimer = time.schedule(this::onRenewalTimer, interval);
            }
        } finally {
            lock.unlock();
        }
    }

    /** Hands the renewal to a fresh virtual thread: the scheduler never blocks on an RPC. */
    private void onRenewalTimer() {
        lock.lock();
        try {
            if (stopping) {
                return;
            }
            renewalThread = Thread.ofVirtual().name("hopper-renewal").start(this::renew);
        } finally {
            lock.unlock();
        }
    }

    private void renew() {
        Lease.State st = lease.state();
        if (st.ended().isPresent()) {
            notifyLost(st.ended().get());
            return;
        }
        long now = time.nanoTime();
        if (now - st.deadline() >= 0) {
            lease.expireIfPast(now);
            logExpired(st.deadline());
            notifyLost(lease.state().ended().orElseThrow());
            return;
        }

        // Each attempt stops at the earlier of its own interval and the lease deadline: there is
        // no point waiting for an answer that could only arrive after its lease has run out.
        long sent = time.nanoTime();
        StatusRuntimeException failure = call(Math.min(interval, st.deadline() - sent));
        if (failure != null) {
            if (stopping) {
                // Our own doing, because the handler finished. The lease stays live and eligible
                // for the Ack that follows.
                return;
            }
            Lease.Ended refused = classify(failure.getStatus());
            if (refused == null) {
                long after = time.nanoTime();
                if (after - st.deadline() < 0 && lease.live()) {
                    armRenewal();
                    return;
                }
                lease.expireIfPast(after);
                if (lease.live()) {
                    lease.lose(StopReason.LEASE_EXPIRED, StopReason.LEASE_EXPIRED.text());
                }
                notifyLost(lease.state().ended().orElseThrow());
                return;
            }
            if (lease.lose(refused.reason(), refused.detail())) {
                log.log(EventLog.Level.WARN, LogMessages.HEARTBEAT_REJECTED, "err", statusText(failure.getStatus()));
            }
            notifyLost(lease.state().ended().orElseThrow());
            return;
        }

        if (!lease.extend(StartRule.deadlineFrom(sent, d.grantNanos()), time.nanoTime())) {
            Optional<Lease.Ended> ended = lease.state().ended();
            if (ended.isEmpty()) {
                // A response too late to use. The older deadline still stands, so try again
                // rather than giving up on a live lease.
                armRenewal();
                return;
            }
            log.log(EventLog.Level.WARN, LogMessages.LEASE_EXPIRED_ON_RENEWAL, "err", ended.get().detail());
            notifyLost(ended.get());
            return;
        }
        armRenewal();
    }

    private void logExpired(long deadline) {
        Instant wall = Instant.now().plusNanos(deadline - time.nanoTime());
        log.log(EventLog.Level.WARN, LogMessages.LEASE_EXPIRED_LOCALLY,
                "deadline", DateTimeFormatter.ISO_INSTANT.format(wall));
    }

    /**
     * Ends renewal and expiry monitoring and joins the renewal in flight. Idempotent. Cancelling
     * the RPC in flight makes it prompt; the stopping flag keeps that cancellation from being
     * mistaken for a lost lease.
     */
    void stop() {
        Thread t;
        TimeSource.Timer e;
        TimeSource.Timer r;
        lock.lock();
        try {
            stopping = true;
            t = renewalThread;
            e = expiryTimer;
            r = renewalTimer;
        } finally {
            lock.unlock();
        }
        if (e != null) {
            e.cancel();
        }
        if (r != null) {
            r.cancel();
        }
        ctx.cancel(null);
        if (t != null && t != Thread.currentThread()) {
            boolean interrupted = false;
            while (true) {
                try {
                    t.join();
                    break;
                } catch (InterruptedException ie) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Waits nanos on the TimeSource, ending early if ctx is cancelled. Returns whether the full
     * wait elapsed: false for a non-positive wait, a cancellation, or an interrupt (which is
     * restored).
     */
    static boolean sleep(TimeSource time, long nanos, Context ctx) {
        if (nanos <= 0 || ctx.isCancelled()) {
            return false;
        }
        CountDownLatch woke = new CountDownLatch(1);
        AtomicBoolean fired = new AtomicBoolean();
        TimeSource.Timer t = time.schedule(() -> {
            fired.set(true);
            woke.countDown();
        }, nanos);
        Context.CancellationListener listener = c -> woke.countDown();
        ctx.addListener(listener, Runnable::run);
        try {
            woke.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            ctx.removeListener(listener);
            t.cancel();
        }
        return fired.get() && !ctx.isCancelled();
    }
}

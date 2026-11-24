package io.github.rpasco787.hopper.testing;

import io.github.rpasco787.hopper.proto.v1.Job;
import io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse;
import io.github.rpasco787.hopper.proto.v1.Subscription;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.ServerCallStreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One accepted lease stream, the port of worker/fake_test.go's {@code fakeStream}.
 *
 * <p>Credit is per stream on purpose: a broker-wide credit count would let a
 * reconnected stream consume the wakeup meant for its predecessor, which is
 * exactly the confusion the reconnect tests exist to detect.
 *
 * <p>Every wait takes a timeout and fails with an {@link AssertionError} naming
 * what it was waiting for. Nothing here calls a JUnit assertion, and nothing
 * throws on a server thread that a test would then never see.
 */
public final class FakeStream {
    private final Subscription sub;
    private final ServerCallStreamObserver<LeaseJobsResponse> out;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private long credit;
    private final List<Integer> credits = new ArrayList<>();
    private boolean ended;
    /** Why the client side ended; null is a half-close, which is a clean end. */
    private Status endStatus;

    /** Completes when the client side ends for any reason. */
    private final CompletableFuture<Void> done = new CompletableFuture<>();

    private final Object sendLock = new Object();
    private boolean finished;
    private volatile boolean cancelled;

    FakeStream(Subscription sub, ServerCallStreamObserver<LeaseJobsResponse> out) {
        this.sub = sub;
        this.out = out;
        this.credit = Integer.toUnsignedLong(sub.getCredit());
    }

    /** The subscription that opened this stream. */
    public Subscription sub() {
        return sub;
    }

    /** The credit outstanding right now: the subscription's, plus every credit message, less what was spent. */
    public long creditTotal() {
        lock.lock();
        try {
            return credit;
        } finally {
            lock.unlock();
        }
    }

    /** Every credit message in arrival order, excluding the subscription's initial grant. */
    public List<Integer> creditMessages() {
        lock.lock();
        try {
            return List.copyOf(credits);
        } finally {
            lock.unlock();
        }
    }

    /** Blocks until the worker has advertised at least {@code n} free slots. */
    public void awaitCredit(long n, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        lock.lock();
        try {
            while (credit < n) {
                if (ended) {
                    throw new AssertionError(
                            "stream ended holding " + credit + " credit, wanted " + n + ": " + describeEnd());
                }
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    throw new AssertionError("waiting for " + n + " credit, have " + credit);
                }
                changed.awaitNanos(left);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for " + n + " credit", e);
        } finally {
            lock.unlock();
        }
    }

    /** Removes credit this stream is about to use. */
    public void spend(long n) {
        lock.lock();
        try {
            if (credit < n) {
                throw new AssertionError("spending " + n + " credit with only " + credit + " outstanding");
            }
            credit -= n;
        } finally {
            lock.unlock();
        }
    }

    /** Writes a delivery without consulting credit, which is how a test manufactures an over-delivery. */
    public void send(Job job) {
        synchronized (sendLock) {
            if (finished || cancelled) {
                throw new IllegalStateException("the stream has already ended");
            }
            out.onNext(LeaseJobsResponse.newBuilder().setJob(job).build());
        }
    }

    /** Waits for one credit, spends it, and sends: the honest path. */
    public void deliver(Job job, Duration timeout) {
        awaitCredit(1, timeout);
        spend(1);
        send(job);
    }

    /**
     * Waits for the client's half-close, which returns, or for its cancellation,
     * which throws {@code CANCELLED}. A half-close is a clean end: it is what a
     * graceful worker shutdown looks like from this side.
     */
    public void idle(Context ctx) throws StatusException {
        Gate.await(done, ctx);
        Status status = endStatus();
        if (status != null) {
            throw status.asException();
        }
    }

    /** Waits for the client's half-close, and fails if the stream ended any other way. */
    public void awaitEOF(Duration timeout) {
        awaitEnded(timeout, "waiting for half-close");
        Status status = endStatus();
        if (status != null) {
            throw new AssertionError("stream ended without a half-close: " + status);
        }
    }

    /** Waits for the client side to end for any reason: a half-close, a failure or a cancel. */
    public void awaitEnd(Duration timeout) {
        awaitEnded(timeout, "waiting for the stream to end");
    }

    private void awaitEnded(Duration timeout, String what) {
        long deadline = System.nanoTime() + timeout.toNanos();
        lock.lock();
        try {
            while (!ended) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    throw new AssertionError(what + ": timed out after " + timeout);
                }
                changed.awaitNanos(left);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted " + what, e);
        } finally {
            lock.unlock();
        }
    }

    private Status endStatus() {
        lock.lock();
        try {
            return endStatus;
        } finally {
            lock.unlock();
        }
    }

    private String describeEnd() {
        return endStatus == null ? "half-closed" : endStatus.toString();
    }

    // The broker's side. These run on the server's call threads.

    void onCredit(int n) {
        lock.lock();
        try {
            credit += Integer.toUnsignedLong(n);
            credits.add(n);
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void onHalfClose() {
        end(null);
    }

    void onCancel(Status status) {
        cancelled = true;
        end(status);
    }

    void onProtocolError(String message) {
        end(Status.INVALID_ARGUMENT.withDescription(message));
    }

    private void end(Status status) {
        lock.lock();
        try {
            if (!ended) {
                ended = true;
                endStatus = status;
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        done.complete(null);
    }

    /** Ends the response side: OK for {@code null}, else with that status. Idempotent. */
    void finish(Status status) {
        synchronized (sendLock) {
            if (finished) {
                return;
            }
            finished = true;
            try {
                if (status == null || status.isOk()) {
                    out.onCompleted();
                } else {
                    out.onError(status.asRuntimeException());
                }
            } catch (RuntimeException alreadyEnded) {
                // The call was cancelled or closed under us; there is no one left to tell.
            }
        }
    }
}

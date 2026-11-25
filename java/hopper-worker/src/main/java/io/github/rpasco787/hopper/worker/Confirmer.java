package io.github.rpasco787.hopper.worker;

import io.github.rpasco787.hopper.proto.v1.AckRequest;
import io.github.rpasco787.hopper.proto.v1.HopperServiceGrpc;
import io.github.rpasco787.hopper.proto.v1.NackRequest;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Reports one delivery's outcome (worker/confirm.go). Ack and Nack share it so retry policy,
 * bounds and identifier stability cannot drift apart. Each request is built once, so every
 * retry presents identical identifiers.
 */
final class Confirmer {
    /** Bounds one Ack or Nack attempt; the lease deadline and the shutdown deadline bound the sequence. */
    static final long CONFIRM_RPC_TIMEOUT_NANOS = 5_000_000_000L;

    /**
     * How an attempt to report an outcome ended. Kept apart because they mean different things to
     * whoever reads the log: a refused confirmation is a job someone else may now hold, while an
     * unconfirmed one very likely ran and is about to be redelivered anyway.
     */
    enum Outcome {
        CONFIRMED("confirmed"), REFUSED("refused"), UNCONFIRMED("unconfirmed"), ABANDONED("abandoned");

        private final String text;

        Outcome(String text) {
            this.text = text;
        }

        String text() {
            return text;
        }
    }

    /** err is null when confirmed. */
    record Result(Outcome outcome, String err) {}

    private final Channel channel;
    private final EventLog log;
    private final Lifecycle lc;
    private final Lease lease;
    private final Delivery d;
    private final TimeSource time;

    Confirmer(Channel channel, EventLog log, Lifecycle lc, Lease lease, Delivery d, TimeSource time) {
        this.channel = channel;
        this.log = log;
        this.lc = lc;
        this.lease = lease;
        this.d = d;
        this.time = time;
    }

    Result ack() {
        AckRequest req = AckRequest.newBuilder()
                .setId(d.job().id().toString()).setLeaseId(d.leaseId().toString()).build();
        return confirm("ack", stub -> stub.ack(req));
    }

    Result nack(String message) {
        NackRequest req = NackRequest.newBuilder()
                .setId(d.job().id().toString()).setLeaseId(d.leaseId().toString())
                .setError(ErrorText.sanitize(message)).build();
        return confirm("nack", stub -> stub.nack(req));
    }

    /**
     * Runs one confirmation to a conclusion. It stops at the earlier of the local lease deadline
     * and the forced-shutdown deadline: retrying past the lease is pointless (the broker would
     * refuse a token the reaper has taken back), and retrying past the shutdown deadline is what
     * makes a drain overrun its budget. The clock is read immediately before each send, which is
     * what keeps a result whose deadline passed during a pause from being confirmed.
     */
    private Result confirm(String op, Function<HopperServiceGrpc.HopperServiceBlockingStub, ?> call) {
        Backoff b = new Backoff();
        for (int attempt = 1; ; attempt++) {
            Lease.State st = lease.state();
            if (st.ended().isPresent()) {
                return new Result(Outcome.UNCONFIRMED, st.ended().get().detail());
            }
            if (lc.forced()) {
                return abandoned();
            }
            long now = time.nanoTime();
            if (now - st.deadline() >= 0) {
                return new Result(Outcome.UNCONFIRMED, StopReason.LEASE_EXPIRED.text());
            }

            long timeout = Math.min(CONFIRM_RPC_TIMEOUT_NANOS, st.deadline() - now);
            StatusRuntimeException failure;
            try {
                lc.forceContext().call(() -> call.apply(
                        HopperServiceGrpc.newBlockingStub(channel).withDeadlineAfter(timeout, TimeUnit.NANOSECONDS)));
                return new Result(Outcome.CONFIRMED, null);
            } catch (StatusRuntimeException e) {
                failure = e;
            } catch (Exception e) {
                failure = io.grpc.Status.fromThrowable(e).asRuntimeException();
            }
            if (lc.forced()) {
                return abandoned();
            }
            if (!retryable(failure)) {
                return new Result(Outcome.REFUSED, Heartbeat.statusText(failure.getStatus()));
            }
            log.log(EventLog.Level.DEBUG, LogMessages.RETRYING_CONFIRMATION,
                    "operation", op, "attempt", attempt, "err", Heartbeat.statusText(failure.getStatus()));

            // The wait is interruptible by the forced shutdown and never outlives the lease it is about.
            long delay = Math.min(b.next().toNanos(), st.deadline() - time.nanoTime());
            if (delay <= 0) {
                return new Result(Outcome.UNCONFIRMED, StopReason.LEASE_EXPIRED.text());
            }
            if (!Heartbeat.sleep(time, delay, lc.forceContext())) {
                return abandoned();
            }
        }
    }

    private Result abandoned() {
        Throwable cause = lc.forceContext().cancellationCause();
        return new Result(Outcome.ABANDONED, cause == null ? StopReason.SHUTTING_DOWN.text() : cause.getMessage());
    }

    /**
     * Deliberately narrow: UNAVAILABLE and DEADLINE_EXCEEDED describe the network; everything
     * else describes the job or the store. INTERNAL in particular is not retried: a generic store
     * failure may be a permanent rejection or an ambiguous commit, and hammering it recovers
     * neither.
     */
    private static boolean retryable(StatusRuntimeException e) {
        return Heartbeat.isNetwork(e.getStatus());
    }

    /**
     * Records what happened. "job confirmed" appears only after the broker accepted the call:
     * logging completion at handler return would report success for jobs about to be redelivered.
     */
    static void logConfirmation(EventLog log, String op, Result r) {
        switch (r.outcome()) {
            case CONFIRMED -> log.log(EventLog.Level.INFO, LogMessages.JOB_CONFIRMED,
                    "operation", op, "outcome", r.outcome().text());
            case REFUSED -> log.log(EventLog.Level.WARN, LogMessages.CONFIRMATION_REFUSED,
                    "operation", op, "outcome", r.outcome().text(), "err", r.err());
            case UNCONFIRMED -> log.log(EventLog.Level.WARN, LogMessages.UNCONFIRMED_LEASE_RAN_OUT,
                    "operation", op, "outcome", r.outcome().text(), "err", r.err());
            case ABANDONED -> log.log(EventLog.Level.WARN, LogMessages.UNCONFIRMED_SHUTTING_DOWN,
                    "operation", op, "outcome", r.outcome().text(), "err", r.err());
        }
    }
}

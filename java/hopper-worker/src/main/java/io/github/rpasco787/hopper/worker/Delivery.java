package io.github.rpasco787.hopper.worker;

import com.google.protobuf.Timestamp;
import io.github.rpasco787.hopper.proto.v1.JobState;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * One validated job off the stream, with everything the SDK needs to own its lease
 * (worker/stream.go delivery and parseDelivery). The session sets the match and the handler
 * before handing it to its thread, so the thread start publishes them.
 */
final class Delivery {
    /** Timestamp validity, as protobuf's CheckValid: 0001-01-01 to 9999-12-31, nanos in range. */
    private static final long MIN_SECONDS = -62_135_596_800L;
    private static final long MAX_SECONDS = 253_402_300_799L;
    private static final Pattern CANONICAL_UUID =
            Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    private final Job job;
    private final UUID leaseId;
    private final long grantNanos;
    private final long receivedAt;
    private boolean creditSent;
    private long creditSentAt;
    private JobHandler handler;

    private Delivery(Job job, UUID leaseId, long grantNanos, long receivedAt) {
        this.job = job;
        this.leaseId = leaseId;
        this.grantNanos = grantNanos;
        this.receivedAt = receivedAt;
    }

    /**
     * Validates a leased row the way the broker's own conversion built it. Everything a delivery
     * needs must be present and sensible: an invented default would be the SDK asserting
     * ownership of a job on no evidence. receivedNanos is when it was read off the stream.
     */
    static Delivery parse(io.github.rpasco787.hopper.proto.v1.Job p, long receivedNanos)
            throws MalformedDeliveryException {
        if (p == null) {
            throw new MalformedDeliveryException("the message carried no job");
        }
        UUID id = uuid(p.getId(), "job id " + quote(p.getId()));
        if (p.getQueue().isEmpty()) {
            throw new MalformedDeliveryException("job " + id + " has no queue");
        }
        if (p.getState() != JobState.JOB_STATE_LEASED) {
            throw new MalformedDeliveryException("job " + id + " arrived in state " + p.getState() + ", not leased");
        }
        if (!p.hasLeaseId()) {
            throw new MalformedDeliveryException("job " + id + " has no lease id");
        }
        UUID leaseId = uuid(p.getLeaseId(), "job " + id + " lease id " + quote(p.getLeaseId()));
        if (leaseId.getMostSignificantBits() == 0 && leaseId.getLeastSignificantBits() == 0) {
            throw new MalformedDeliveryException("job " + id + " has the nil lease id");
        }
        Instant leasedAt = instant(id, "leased_at", p.hasLeasedAt(), p.getLeasedAt());
        Instant expires = instant(id, "lease_expires", p.hasLeaseExpires(), p.getLeaseExpires());
        long grant = saturatingNanos(Duration.between(leasedAt, expires));
        if (grant <= 0) {
            throw new MalformedDeliveryException("job " + id + " has a lease of " + GoDuration.format(Duration.ofNanos(grant)));
        }
        Instant createdAt = instant(id, "created_at", p.hasCreatedAt(), p.getCreatedAt());
        // attempts is counted at lease time, so a leased row is on at least its first delivery;
        // max_attempts is never stored as zero (the store substitutes its default at enqueue).
        if (p.getAttempts() < 1) {
            throw new MalformedDeliveryException("job " + id + " reports attempt " + p.getAttempts());
        }
        if (p.getMaxAttempts() < 1) {
            throw new MalformedDeliveryException("job " + id + " reports max attempts " + p.getMaxAttempts());
        }
        Job job = new Job(id, p.getQueue(), p.getPayload().toByteArray(), p.getAttempts(), p.getMaxAttempts(), createdAt);
        return new Delivery(job, leaseId, grant, receivedNanos);
    }

    /** Records the credit unit this delivery was matched to; the session calls it after spend(). */
    void matched(CreditLedger.Match match) {
        this.creditSent = match.sent();
        this.creditSentAt = match.sentAtNanos();
    }

    /** The handler registered for its queue; null leaves it to be nacked unstarted. */
    void handler(JobHandler handler) {
        this.handler = handler;
    }

    Job job() {
        return job;
    }

    UUID leaseId() {
        return leaseId;
    }

    /** lease_expires − leased_at: both from the database clock, so skew-free. Not what is left. */
    long grantNanos() {
        return grantNanos;
    }

    long receivedAt() {
        return receivedAt;
    }

    boolean creditSent() {
        return creditSent;
    }

    long creditSentAt() {
        return creditSentAt;
    }

    JobHandler handler() {
        return handler;
    }

    /**
     * How long the check may spend proving ownership, fixed at receipt so a slow broker can't
     * walk a delivery forward: receivedAt + G − margin.
     */
    long acquisitionBudget() {
        return StartRule.deadlineFrom(receivedAt, grantNanos);
    }

    private static UUID uuid(String text, String what) throws MalformedDeliveryException {
        if (!CANONICAL_UUID.matcher(text).matches()) {
            throw new MalformedDeliveryException(what + ": invalid UUID");
        }
        return UUID.fromString(text);
    }

    private static Instant instant(UUID id, String field, boolean present, Timestamp ts)
            throws MalformedDeliveryException {
        if (!present) {
            throw new MalformedDeliveryException("job " + id + ": " + field + " is absent");
        }
        if (ts.getSeconds() < MIN_SECONDS || ts.getSeconds() > MAX_SECONDS) {
            throw new MalformedDeliveryException("job " + id + ": " + field + " is invalid: seconds "
                    + ts.getSeconds() + " outside 0001-01-01 to 9999-12-31");
        }
        if (ts.getNanos() < 0 || ts.getNanos() > 999_999_999) {
            throw new MalformedDeliveryException("job " + id + ": " + field + " is invalid: nanos " + ts.getNanos()
                    + " out of range");
        }
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    /** As Go's Time.Sub: a difference beyond a Duration's range saturates. */
    private static long saturatingNanos(Duration d) {
        try {
            return d.toNanos();
        } catch (ArithmeticException e) {
            return d.isNegative() ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
    }

    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}

package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Timestamp;
import io.github.rpasco787.hopper.proto.v1.JobState;
import io.github.rpasco787.hopper.testing.Fixture;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

/** Port of stream_test.go TestParseDeliveryRejectsMalformedRows. */
class DeliveryTest {
    private static final long RECEIVED = 7_000_000_000L;

    private record Case(String name, UnaryOperator<io.github.rpasco787.hopper.proto.v1.Job.Builder> corrupt,
                        String want) {}

    @Test
    void parseDeliveryRejectsMalformedRows() throws MalformedDeliveryException {
        var fixture = Fixture.of("q");
        var valid = fixture.proto();
        Timestamp leased = valid.getLeasedAt();
        Instant leasedAt = Instant.ofEpochSecond(leased.getSeconds(), leased.getNanos());
        List<Case> cases = List.of(
                new Case("bad job id", j -> j.setId("not-a-uuid"), "job id"),
                new Case("empty queue", j -> j.setQueue(""), "no queue"),
                new Case("not leased", j -> j.setState(JobState.JOB_STATE_AVAILABLE), "not leased"),
                new Case("absent lease id", j -> j.clearLeaseId(), "no lease id"),
                new Case("bad lease id", j -> j.setLeaseId("nope"), "lease id"),
                new Case("nil lease id", j -> j.setLeaseId(new UUID(0, 0).toString()), "nil lease id"),
                new Case("absent leased_at", j -> j.clearLeasedAt(), "leased_at is absent"),
                new Case("absent lease_expires", j -> j.clearLeaseExpires(), "lease_expires is absent"),
                new Case("invalid leased_at",
                        j -> j.setLeasedAt(Timestamp.newBuilder().setSeconds(-62135596801L)), "leased_at is invalid"),
                new Case("expiry before lease", j -> j.setLeaseExpires(ts(leasedAt.minusSeconds(1))), "lease of -1s"),
                new Case("zero-length lease", j -> j.setLeaseExpires(leased), "lease of 0s"),
                new Case("absent created_at", j -> j.clearCreatedAt(), "created_at is absent"),
                new Case("no attempt", j -> j.setAttempts(0), "attempt 0"),
                new Case("no max attempts", j -> j.setMaxAttempts(0), "max attempts 0"));
        assertEquals(14, cases.size());
        for (Case c : cases) {
            var bad = c.corrupt().apply(valid.toBuilder()).build();
            MalformedDeliveryException e = assertThrows(MalformedDeliveryException.class,
                    () -> Delivery.parse(bad, RECEIVED), c.name());
            assertTrue(e.getMessage().contains(c.want()), c.name() + ": " + e.getMessage());
        }

        MalformedDeliveryException none = assertThrows(MalformedDeliveryException.class,
                () -> Delivery.parse(null, RECEIVED));
        assertTrue(none.getMessage().contains("no job"), none.getMessage());

        Delivery d = Delivery.parse(valid, RECEIVED);
        assertEquals(RECEIVED, d.receivedAt());
        assertEquals(Duration.ofSeconds(30).toNanos(), d.grantNanos());
    }

    @Test
    void aValidRowCarriesEverythingTheHandlerAndLeaseNeed() throws MalformedDeliveryException {
        var fixture = Fixture.of("emails").attempt(2).maxAttempts(7).payload("{\"to\":\"a\"}".getBytes());
        Delivery d = Delivery.parse(fixture.proto(), RECEIVED);
        assertEquals(fixture.id(), d.job().id());
        assertEquals("emails", d.job().queue());
        assertEquals("{\"to\":\"a\"}", d.job().payloadUtf8());
        assertEquals(2, d.job().attempt());
        assertEquals(7, d.job().maxAttempts());
        assertEquals(fixture.leasedAt(), d.job().enqueuedAt(), "the fixture creates the row when it leases it");
        assertEquals(fixture.leaseId(), d.leaseId());
        assertFalse(d.creditSent(), "unmatched until the session says otherwise");
        assertNull(d.handler(), "no handler until the session looks one up");
        assertEquals(StartRule.deadlineFrom(RECEIVED, d.grantNanos()), d.acquisitionBudget(),
                "receivedAt + G - margin, fixed at receipt");

        d.matched(new CreditLedger.Match(true, RECEIVED - 1_000));
        assertTrue(d.creditSent());
        assertEquals(RECEIVED - 1_000, d.creditSentAt());
    }

    @Test
    void timestampsAtTheEdgesOfTheValidRange() throws MalformedDeliveryException {
        var valid = Fixture.of("q").proto();
        var lastNano = valid.toBuilder()
                .setLeaseExpires(Timestamp.newBuilder().setSeconds(253402300799L).setNanos(999_999_999)).build();
        Delivery d = Delivery.parse(lastNano, RECEIVED);
        assertTrue(d.grantNanos() > 0, "a grant past Go's Duration range saturates instead of overflowing");

        var pastEnd = valid.toBuilder().setLeaseExpires(Timestamp.newBuilder().setSeconds(253402300800L)).build();
        assertTrue(assertThrows(MalformedDeliveryException.class, () -> Delivery.parse(pastEnd, RECEIVED))
                .getMessage().contains("lease_expires is invalid"));
        var badNanos = valid.toBuilder().setCreatedAt(Timestamp.newBuilder().setSeconds(1).setNanos(-1)).build();
        assertTrue(assertThrows(MalformedDeliveryException.class, () -> Delivery.parse(badNanos, RECEIVED))
                .getMessage().contains("created_at is invalid"));
    }

    @Test
    void onlyCanonicalUuidTextIsAccepted() {
        var lenient = Fixture.of("q").proto().toBuilder().setId("1-1-1-1-1").build();
        assertTrue(assertThrows(MalformedDeliveryException.class, () -> Delivery.parse(lenient, RECEIVED))
                .getMessage().contains("job id"));
    }

    private static Timestamp ts(Instant i) {
        return Timestamp.newBuilder().setSeconds(i.getEpochSecond()).setNanos(i.getNano()).build();
    }
}

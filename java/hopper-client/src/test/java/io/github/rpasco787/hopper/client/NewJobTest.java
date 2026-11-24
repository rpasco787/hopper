package io.github.rpasco787.hopper.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.github.rpasco787.hopper.proto.v1.JobSpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Ports of client_test.go's TestSpecLeavesUnsetFieldsAbsent and
 * TestSpecSendsRawJSONUntouched, plus the local checks. TestSpecMarshalsAStructPayload
 * and TestSpecReturnsMarshalErrors are not ported: the Java client takes raw JSON
 * only (Decision D33), so there is nothing to marshal.
 */
class NewJobTest {
    @Test
    void specLeavesUnsetFieldsAbsent() {
        JobSpec spec = NewJob.builder("q").build().toSpec();

        assertEquals("q", spec.getQueue());
        assertFalse(spec.hasRunAt(), "no run_at means now");
        assertFalse(spec.hasIdempotencyKey(), "no key means no deduplication");
        assertEquals(0, spec.getPriority());
        assertEquals(0, spec.getMaxAttempts(), "0 takes the broker's default");
        assertTrue(spec.getPayload().isEmpty(), "an empty payload is stored as {}");
    }

    @Test
    void specSendsRawJsonUntouched() {
        String raw = "{\"b\": 1,\"a\":[2]}";

        JobSpec spec = NewJob.builder("q").payloadJson(raw).build().toSpec();

        assertEquals(ByteString.copyFrom(raw, StandardCharsets.UTF_8), spec.getPayload(),
                "the broker sees the caller's bytes, not a re-encoding");
    }

    @Test
    void specCarriesEveryField() {
        JobSpec spec = NewJob.builder("q")
                .runAt(Instant.parse("2026-12-01T10:00:00.123456789Z"))
                .idempotencyKey("k")
                .priority(-5)
                .maxAttempts(3)
                .build()
                .toSpec();

        assertEquals(1796119200L, spec.getRunAt().getSeconds());
        assertEquals(123456789, spec.getRunAt().getNanos());
        assertTrue(spec.hasIdempotencyKey());
        assertEquals("k", spec.getIdempotencyKey());
        assertEquals(-5, spec.getPriority());
        assertEquals(3, spec.getMaxAttempts());
    }

    @Test
    void priorityMustFitSixteenBits() {
        assertEquals(32767, NewJob.builder("q").priority(32767).build().toSpec().getPriority());
        assertEquals(-32768, NewJob.builder("q").priority(-32768).build().toSpec().getPriority());

        for (int bad : new int[] {32768, -32769}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> NewJob.builder("q").priority(bad).build());
            assertTrue(e.getMessage().contains("priority"), e.getMessage());
        }
    }

    @Test
    void queueIsRequired() {
        for (String bad : new String[] {null, ""}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> NewJob.builder(bad).build());
            assertTrue(e.getMessage().contains("queue"), e.getMessage());
        }
    }

    @Test
    void maxAttemptsMustNotBeNegative() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> NewJob.builder("q").maxAttempts(-1).build());
        assertTrue(e.getMessage().contains("maxAttempts"), e.getMessage());
        assertEquals(0, NewJob.builder("q").maxAttempts(0).build().toSpec().getMaxAttempts());
    }

    @Test
    void payloadBytesAreCopied() {
        byte[] payload = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        NewJob.Builder builder = NewJob.builder("q").payloadJson(payload);

        payload[2] = 'X';
        JobSpec spec = builder.build().toSpec();
        assertEquals("{\"a\":1}", spec.getPayload().toStringUtf8(), "the builder kept its own copy");

        payload[2] = 'Y';
        assertEquals("{\"a\":1}", spec.getPayload().toStringUtf8(), "and so did the spec");
    }

    @Test
    void aNullPayloadIsSentEmpty() {
        assertTrue(NewJob.builder("q").payloadJson((byte[]) null).build().toSpec().getPayload().isEmpty());
        assertTrue(NewJob.builder("q").payloadJson((String) null).build().toSpec().getPayload().isEmpty());
    }

    @Test
    void aJobIsImmutableOnceBuilt() {
        NewJob.Builder builder = NewJob.builder("q").priority(1);
        NewJob job = builder.build();

        builder.priority(2).payloadJson("{}");

        assertEquals(1, job.toSpec().getPriority());
        assertTrue(job.toSpec().getPayload().isEmpty());
    }
}

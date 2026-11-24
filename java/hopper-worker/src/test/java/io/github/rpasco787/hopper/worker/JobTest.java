package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class JobTest {
    private static Job job(byte[] payload) {
        return new Job(UUID.fromString("11111111-2222-3333-4444-555555555555"), "emails", payload, 2, 5,
                Instant.parse("2026-11-23T12:00:00Z"));
    }

    @Test
    void payloadIsCopiedBothWays() {
        byte[] in = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        Job j = job(in);
        in[0] = 'x';
        assertArrayEquals("{\"a\":1}".getBytes(StandardCharsets.UTF_8), j.payload(), "the caller's array is not shared");
        j.payload()[0] = 'y';
        assertArrayEquals("{\"a\":1}".getBytes(StandardCharsets.UTF_8), j.payload(), "nor is the returned one");
    }

    @Test
    void payloadUtf8DecodesThePayload() {
        assertEquals("{\"name\":\"é😀\"}", job("{\"name\":\"é😀\"}".getBytes(StandardCharsets.UTF_8)).payloadUtf8());
        assertEquals("", job(new byte[0]).payloadUtf8());
    }

    /** docs/semantics.md §14: payloads are never logged. */
    @Test
    void toStringNamesTheJobButNeverThePayload() {
        String s = job("{\"secret\":\"hunter2\"}".getBytes(StandardCharsets.UTF_8)).toString();
        assertTrue(s.contains("11111111-2222-3333-4444-555555555555"), s);
        assertTrue(s.contains("emails"), s);
        assertTrue(s.contains("2"), s);
        assertFalse(s.contains("hunter2"), s);
        assertFalse(s.contains("secret"), s);
    }

    @Test
    void theAccessorsReturnWhatWasParsed() {
        Job j = job(new byte[0]);
        assertEquals(UUID.fromString("11111111-2222-3333-4444-555555555555"), j.id());
        assertEquals("emails", j.queue());
        assertEquals(2, j.attempt());
        assertEquals(5, j.maxAttempts());
        assertEquals(Instant.parse("2026-11-23T12:00:00Z"), j.enqueuedAt());
    }
}

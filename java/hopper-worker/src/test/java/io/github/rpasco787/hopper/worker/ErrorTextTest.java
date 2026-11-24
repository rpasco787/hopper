package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Port of worker_test.go TestSanitizeError. Java strings are UTF-16, so Go's "invalid UTF-8"
 * is an unpaired surrogate here; every length is the UTF-8 byte length.
 */
class ErrorTextTest {
    private static final int MAX = ErrorText.MAX_ERROR_BYTES;
    private static final String SUFFIX = ErrorText.TRUNCATION_SUFFIX;

    private static int bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8).length;
    }

    /** Valid UTF-16 survives an encode and decode unchanged; a lone surrogate does not. */
    private static void assertValid(String s, String msg) {
        assertEquals(s, new String(s.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8), msg);
    }

    @Test
    void sanitizeError_removesNulAndFixesInvalidUtf8() {
        assertEquals("ab", ErrorText.sanitize("a\u0000b"));
        assertEquals("a�b", ErrorText.sanitize("a\uD800b"));
        assertEquals("ok é", ErrorText.sanitize("ok \u0000é"));
        assertEquals("a�b", ErrorText.sanitize("a\uDC00b"), "a lone low surrogate too");
        assertEquals("😀", ErrorText.sanitize("😀"), "a real pair is left alone");
        assertValid(ErrorText.sanitize("\uDC00\uD800"), "a reversed pair is two lone surrogates");
    }

    @Test
    void sanitizeError_leavesAMessageAtTheLimitAlone() {
        String at = "x".repeat(MAX);
        assertEquals(at, ErrorText.sanitize(at));
        assertEquals(MAX, bytes(ErrorText.sanitize(at)));
    }

    @Test
    void sanitizeError_truncatesOneByteOverTheLimit() {
        String got = ErrorText.sanitize("x".repeat(MAX + 1));
        assertTrue(bytes(got) <= MAX);
        assertTrue(got.endsWith(SUFFIX));
        assertEquals(MAX, bytes(got), "the suffix is counted inside the budget, not added to it");
    }

    @Test
    void sanitizeError_cutsAtARuneBoundary() {
        // Two-byte runes make the cut point land mid-rune: the budget before the suffix is
        // 4081 bytes, an odd number of bytes into a string of 2-byte runes.
        String got = ErrorText.sanitize("é".repeat(MAX));
        assertValid(got, "a cut inside a rune would be rejected by Postgres");
        assertTrue(bytes(got) <= MAX);
        assertTrue(got.endsWith(SUFFIX));

        // Same for a 4-byte rune, where three of the four alignments are bad.
        for (int pad = 0; pad < 4; pad++) {
            String out = ErrorText.sanitize("a".repeat(pad) + "😀".repeat(MAX));
            assertValid(out, "pad " + pad);
            assertTrue(bytes(out) <= MAX, "pad " + pad);
            assertTrue(out.endsWith(SUFFIX), "pad " + pad);
        }
    }

    @Test
    void sanitizeError_sanitizesBeforeMeasuring() {
        // Lone surrogates become 3-byte replacement characters, so a string under the limit
        // crosses it during sanitizing and must still come out valid and bounded.
        String got = ErrorText.sanitize("\uD800".repeat(MAX - 1));
        assertValid(got, "valid");
        assertTrue(bytes(got) <= MAX);
        assertFalse(got.contains("\u0000"));
        assertTrue(got.endsWith(SUFFIX), "it really crossed the limit");
    }

    @Test
    void nullIsEmpty() {
        assertEquals("", ErrorText.sanitize(null));
    }
}

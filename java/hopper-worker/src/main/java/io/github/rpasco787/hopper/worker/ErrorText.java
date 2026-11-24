package io.github.rpasco787.hopper.worker;

import java.nio.charset.StandardCharsets;

/**
 * Makes a handler's error safe to store and to log (worker/errors.go, sanitizeError). A NUL
 * can't be stored in a text column, invalid text is rejected by the server encoding, and an
 * unbounded length is a resource problem: all three would fail the Nack rather than the job.
 */
final class ErrorText {
    /** last_error exists to be read by a human. Measured in UTF-8 bytes. */
    static final int MAX_ERROR_BYTES = 4096;
    /** Counted inside MAX_ERROR_BYTES, not added on top of it. */
    static final String TRUNCATION_SUFFIX = "… (truncated)";
    private static final int SUFFIX_BYTES = TRUNCATION_SUFFIX.getBytes(StandardCharsets.UTF_8).length;

    private ErrorText() {}

    /**
     * Strips NUL, replaces each unpaired surrogate (Java's invalid UTF-8) with U+FFFD, and cuts
     * a result over 4,096 UTF-8 bytes back to a code-point boundary before appending the suffix.
     * Null is the empty string.
     */
    static String sanitize(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder clean = new StringBuilder(s.length());
        int bytes = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\u0000') {
                continue;
            }
            if (Character.isHighSurrogate(c) && i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1))) {
                clean.append(c).append(s.charAt(++i));
                bytes += 4;
            } else if (Character.isSurrogate(c)) {
                clean.append('�');
                bytes += 3;
            } else {
                clean.append(c);
                bytes += utf8Length(c);
            }
        }
        if (bytes <= MAX_ERROR_BYTES) {
            return clean.toString();
        }
        int budget = MAX_ERROR_BYTES - SUFFIX_BYTES;
        int used = 0;
        int end = 0;
        while (end < clean.length()) {
            int cp = clean.codePointAt(end);
            int n = cp < 0x80 ? 1 : cp < 0x800 ? 2 : cp < 0x10000 ? 3 : 4;
            if (used + n > budget) {
                break;
            }
            used += n;
            end += Character.charCount(cp);
        }
        return clean.substring(0, end) + TRUNCATION_SUFFIX;
    }

    private static int utf8Length(char c) {
        return c < 0x80 ? 1 : c < 0x800 ? 2 : 3;
    }
}

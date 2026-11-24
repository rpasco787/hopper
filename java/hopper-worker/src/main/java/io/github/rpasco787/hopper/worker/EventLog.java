package io.github.rpasco787.hopper.worker;

/**
 * The worker's structured log (Decision D10): a level, an exact message, and key-value fields.
 * The messages and fields test/accounting and the chaos suite read are {@link LogMessages}.
 * The SDK's default is {@link JsonLinesLog} on System.err at INFO.
 */
public interface EventLog {
    enum Level { DEBUG, INFO, WARN, ERROR }

    /** Whether a line at this level would be written; callers may skip building it. */
    boolean enabled(Level level);

    /**
     * Writes one line. keyValues alternate a String key and its value.
     *
     * @throws IllegalArgumentException for an odd count or a non-String key: an SDK bug
     */
    void log(Level level, String msg, Object... keyValues);

    /** A log whose every line carries these fields first, after any this one already carries. */
    EventLog with(Object... keyValues);
}

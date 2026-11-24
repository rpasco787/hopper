package io.github.rpasco787.hopper.example;

/**
 * A configuration cmd/worker would refuse, with its message. Two kinds are special, as Go's
 * flag package makes them: help ({@code flag.ErrHelp}: print the usage, exit 0), and a flag
 * parse error, which flag reports with the usage before main prints {@code worker: <error>}.
 */
final class ConfigException extends Exception {
    private static final long serialVersionUID = 1L;

    private final boolean help;
    private final boolean usage;

    private ConfigException(String message, boolean help, boolean usage) {
        super(message);
        this.help = help;
        this.usage = usage;
    }

    /** A validation failure: only main's {@code worker: <message>} line. */
    static ConfigException invalid(String message) {
        return new ConfigException(message, false, false);
    }

    /** A flag parse error: flag prints the message and the usage, then main prints its line. */
    static ConfigException flag(String message) {
        return new ConfigException(message, false, true);
    }

    /** -h, -help or --help: the usage, and exit 0. */
    static ConfigException helpRequested() {
        return new ConfigException("flag: help requested", true, true);
    }

    boolean help() {
        return help;
    }

    /** Whether the usage is printed (before the message, unless this is help). */
    boolean usage() {
        return usage;
    }
}

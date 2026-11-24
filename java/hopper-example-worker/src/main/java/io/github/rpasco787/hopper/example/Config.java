package io.github.rpasco787.hopper.example;

import io.github.rpasco787.hopper.worker.EventLog;
import io.github.rpasco787.hopper.worker.GoDuration;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * cmd/worker's configuration (cmd/worker/main.go:24-90): the same flags, environment variables,
 * defaults, validation and messages. Go's flag package, strconv.ParseInt, slog's level syntax
 * and net.SplitHostPort are ported as far as cmd/worker depends on them, so a configuration
 * that works for one worker works, or fails with the same words, for the other.
 *
 * @param logLevel the text as given; {@link #slogLevel} reads it
 */
record Config(String broker, String workerId, int concurrency, Duration leaseDuration,
        Duration shutdownTimeout, String logLevel, String metricsListen) {

    static final int MAX_CONCURRENCY = 1 << 16;

    /** flag.PrintDefaults for cmd/worker's flag set, named "worker". */
    static final String USAGE = """
            Usage of worker:
              -broker string
                \tbroker gRPC address (HOPPER_BROKER) (default "127.0.0.1:9090")
              -concurrency int
                \tmaximum active deliveries (HOPPER_CONCURRENCY) (default 8)
              -lease-duration duration
                \trequested lease duration; zero uses broker default (HOPPER_LEASE_DURATION)
              -log-level string
                \tdebug/info/warn/error (HOPPER_LOG_LEVEL) (default "info")
              -metrics-listen string
                \taccepted as cmd/worker accepts it; the Java example worker serves no metrics (HOPPER_METRICS_LISTEN)
              -shutdown-timeout duration
                \tgraceful drain budget (HOPPER_SHUTDOWN_TIMEOUT) (default 30s)
              -worker-id string
                \tworker identity; empty uses hostname/PID (HOPPER_WORKER_ID)
            """;

    /** main.go's envVars, in its order: the first invalid one is the one reported. */
    private static final List<String[]> ENV = List.of(
            new String[] {"broker", "HOPPER_BROKER"},
            new String[] {"worker-id", "HOPPER_WORKER_ID"},
            new String[] {"concurrency", "HOPPER_CONCURRENCY"},
            new String[] {"lease-duration", "HOPPER_LEASE_DURATION"},
            new String[] {"shutdown-timeout", "HOPPER_SHUTDOWN_TIMEOUT"},
            new String[] {"log-level", "HOPPER_LOG_LEVEL"},
            new String[] {"metrics-listen", "HOPPER_METRICS_LISTEN"});

    private static final Set<String> FLAGS = Set.of(
            "broker", "worker-id", "concurrency", "lease-duration", "shutdown-timeout", "log-level", "metrics-listen");

    /** The values being parsed; a flag.Value per field. */
    private static final class Values {
        String broker = "127.0.0.1:9090";
        String workerId = "";
        long concurrency = 8;
        Duration leaseDuration = Duration.ZERO;
        Duration shutdownTimeout = Duration.ofSeconds(30);
        String logLevel = "info";
        String metricsListen = "";

        /** flag.FlagSet.Set: false where the value doesn't parse. */
        boolean set(String name, String value) {
            switch (name) {
                case "broker" -> broker = value;
                case "worker-id" -> workerId = value;
                case "metrics-listen" -> metricsListen = value;
                case "log-level" -> logLevel = value;
                case "concurrency" -> {
                    Long n = parseGoInt(value);
                    if (n == null) {
                        return false;
                    }
                    concurrency = n;
                }
                case "lease-duration" -> {
                    Duration d = parseGoDuration(value);
                    if (d == null) {
                        return false;
                    }
                    leaseDuration = d;
                }
                case "shutdown-timeout" -> {
                    Duration d = parseGoDuration(value);
                    if (d == null) {
                        return false;
                    }
                    shutdownTimeout = d;
                }
                default -> throw new IllegalArgumentException(name);
            }
            return true;
        }
    }

    /**
     * Parses cmd/worker's arguments, then applies each {@code HOPPER_*} variable present in
     * {@code env} whose flag wasn't given, then validates, all as main.go's parseConfig does.
     *
     * @throws ConfigException with Go's message; {@link ConfigException#help()} for -h/-help
     */
    static Config parse(String[] args, Map<String, String> env) throws ConfigException {
        Values v = new Values();
        Set<String> supplied = new HashSet<>();
        int i = 0;
        while (i < args.length) {
            String s = args[i];
            // flag.FlagSet.parseOne.
            if (s.length() < 2 || s.charAt(0) != '-') {
                break; // the first non-flag argument ends the flags
            }
            int minuses = 1;
            if (s.charAt(1) == '-') {
                minuses = 2;
                if (s.length() == 2) { // "--" ends the flags, and is consumed
                    i++;
                    break;
                }
            }
            String name = s.substring(minuses);
            if (name.isEmpty() || name.charAt(0) == '-' || name.charAt(0) == '=') {
                throw ConfigException.flag("bad flag syntax: " + s);
            }
            i++;
            String value = null;
            int eq = name.indexOf('=', 1);
            if (eq >= 0) {
                value = name.substring(eq + 1);
                name = name.substring(0, eq);
            }
            if (!FLAGS.contains(name)) {
                if (name.equals("help") || name.equals("h")) {
                    throw ConfigException.helpRequested();
                }
                throw ConfigException.flag("flag provided but not defined: -" + name);
            }
            if (value == null) { // every flag here takes a value: the next argument, whatever it is
                if (i >= args.length) {
                    throw ConfigException.flag("flag needs an argument: -" + name);
                }
                value = args[i++];
            }
            if (!v.set(name, value)) {
                String why = name.equals("concurrency") && isRangeError(value) ? "value out of range" : "parse error";
                throw ConfigException.flag("invalid value " + quote(value) + " for flag -" + name + ": " + why);
            }
            supplied.add(name);
        }
        if (i < args.length) {
            throw ConfigException.invalid("unexpected positional arguments");
        }
        for (String[] pair : ENV) {
            String value = env.get(pair[1]);
            if (value != null && !supplied.contains(pair[0]) && !v.set(pair[0], value)) {
                throw ConfigException.invalid("invalid " + pair[1]);
            }
        }
        if (v.broker.isEmpty()) {
            throw ConfigException.invalid("broker address is required");
        }
        if (v.concurrency <= 0) {
            throw ConfigException.invalid("concurrency must be positive");
        }
        if (v.concurrency > MAX_CONCURRENCY) {
            throw ConfigException.invalid("concurrency " + v.concurrency + " exceeds the maximum of " + MAX_CONCURRENCY);
        }
        if (v.leaseDuration.isNegative()) {
            throw ConfigException.invalid("lease-duration must not be negative");
        }
        if (v.shutdownTimeout.isNegative()) {
            throw ConfigException.invalid("shutdown-timeout must not be negative");
        }
        if (!v.metricsListen.isEmpty() && !splitsAsHostPort(v.metricsListen)) {
            throw ConfigException.invalid("invalid metrics-listen address");
        }
        slogLevel(v.logLevel);
        return new Config(v.broker, v.workerId, (int) v.concurrency, v.leaseDuration, v.shutdownTimeout,
                v.logLevel, v.metricsListen);
    }

    /** The lowest level this configuration logs, or empty for none (a level above ERROR). */
    Optional<EventLog.Level> minimum() {
        try {
            return minimum(slogLevel(logLevel));
        } catch (ConfigException e) {
            throw new IllegalStateException("parse() validated the level", e);
        }
    }

    /**
     * slog.Level.UnmarshalText: debug, info, warn or error in any case, with an optional signed
     * decimal offset ("info+2", "error-1"), as slog's numbers (DEBUG -4, INFO 0, WARN 4, ERROR 8).
     */
    static long slogLevel(String text) throws ConfigException {
        String name = text;
        long offset = 0;
        int sign = indexOfSign(text);
        if (sign >= 0) {
            name = text.substring(0, sign);
            Long n = parseDecimal(text.substring(sign));
            if (n == null) {
                throw badLevel(text);
            }
            offset = n;
        }
        long base = switch (name.toUpperCase(Locale.ROOT)) {
            case "DEBUG" -> -4;
            case "INFO" -> 0;
            case "WARN" -> 4;
            case "ERROR" -> 8;
            default -> throw badLevel(text);
        };
        return base + offset; // slog's Level is an int: an absurd offset wraps there too
    }

    /**
     * A slog handler at {@code level} writes a record whose level is at least it. The SDK logs at
     * four levels, so the effective minimum is the first of them at or above {@code level}.
     */
    static Optional<EventLog.Level> minimum(long level) {
        if (level <= -4) {
            return Optional.of(EventLog.Level.DEBUG);
        } else if (level <= 0) {
            return Optional.of(EventLog.Level.INFO);
        } else if (level <= 4) {
            return Optional.of(EventLog.Level.WARN);
        } else if (level <= 8) {
            return Optional.of(EventLog.Level.ERROR);
        }
        return Optional.empty();
    }

    private static ConfigException badLevel(String text) {
        return ConfigException.invalid("invalid log level " + quote(text) + " (want debug, info, warn or error)");
    }

    private static int indexOfSign(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '+' || s.charAt(i) == '-') {
                return i;
            }
        }
        return -1;
    }

    /** strconv.Atoi: an optional sign, then decimal digits only; null on a syntax or range error. */
    private static Long parseDecimal(String s) {
        int i = 0;
        if (!s.isEmpty() && (s.charAt(0) == '+' || s.charAt(0) == '-')) {
            i = 1;
        }
        if (i == s.length()) {
            return null;
        }
        for (int j = i; j < s.length(); j++) {
            if (s.charAt(j) < '0' || s.charAt(j) > '9') {
                return null;
            }
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Duration parseGoDuration(String s) {
        try {
            return GoDuration.parse(s);
        } catch (IllegalArgumentException e) {
            return null; // flag's durationValue.Set reports any failure as "parse error"
        }
    }

    /** strconv.ParseInt(s, 0, 64), as flag.IntVar parses; null on any error. */
    static Long parseGoInt(String s) {
        return goInt(s) instanceof Long n ? n : null;
    }

    private static boolean isRangeError(String s) {
        return goInt(s) == GoIntError.RANGE;
    }

    private enum GoIntError { SYNTAX, RANGE }

    /** A Long, or which strconv error ParseInt(s, 0, 64) gives. */
    private static Object goInt(String s) {
        if (s.isEmpty()) {
            return GoIntError.SYNTAX;
        }
        boolean neg = false;
        String u = s;
        if (u.charAt(0) == '+' || u.charAt(0) == '-') {
            neg = u.charAt(0) == '-';
            u = u.substring(1);
        }
        Object un = goUint(u);
        if (!(un instanceof Long n)) {
            return un;
        }
        // n is unsigned; ParseInt's cutoff is 1<<63.
        if (!neg && Long.compareUnsigned(n, Long.MIN_VALUE) >= 0) {
            return GoIntError.RANGE;
        }
        if (neg && Long.compareUnsigned(n, Long.MIN_VALUE) > 0) {
            return GoIntError.RANGE;
        }
        return neg ? -n : n;
    }

    /** strconv.ParseUint(s, 0, 64), unsigned in a long. */
    private static Object goUint(String s0) {
        if (s0.isEmpty()) {
            return GoIntError.SYNTAX;
        }
        String s = s0;
        int base = 10;
        if (s.charAt(0) == '0') {
            char p = s.length() >= 3 ? lower(s.charAt(1)) : 0;
            if (p == 'b') {
                base = 2;
                s = s.substring(2);
            } else if (p == 'o') {
                base = 8;
                s = s.substring(2);
            } else if (p == 'x') {
                base = 16;
                s = s.substring(2);
            } else {
                base = 8;
                s = s.substring(1);
            }
        }
        long cutoff = Long.divideUnsigned(-1L, base) + 1; // maxUint64/base + 1
        long n = 0;
        boolean underscores = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            int d;
            if (c == '_') {
                underscores = true;
                continue;
            } else if (c >= '0' && c <= '9') {
                d = c - '0';
            } else if (lower(c) >= 'a' && lower(c) <= 'z') {
                d = lower(c) - 'a' + 10;
            } else {
                return GoIntError.SYNTAX;
            }
            if (d >= base) {
                return GoIntError.SYNTAX;
            }
            if (Long.compareUnsigned(n, cutoff) >= 0) {
                return GoIntError.RANGE; // n*base overflows
            }
            n *= base;
            long n1 = n + d;
            if (Long.compareUnsigned(n1, n) < 0) {
                return GoIntError.RANGE;
            }
            n = n1;
        }
        if (underscores && !underscoreOk(s0)) {
            return GoIntError.SYNTAX;
        }
        return n;
    }

    /** strconv.underscoreOK: underscores only between digits, or between a base prefix and a digit. */
    private static boolean underscoreOk(String s) {
        char saw = '^';
        int i = 0;
        if (!s.isEmpty() && (s.charAt(0) == '-' || s.charAt(0) == '+')) {
            s = s.substring(1);
        }
        boolean hex = false;
        if (s.length() >= 2 && s.charAt(0) == '0') {
            char p = lower(s.charAt(1));
            if (p == 'b' || p == 'o' || p == 'x') {
                i = 2;
                saw = '0';
                hex = p == 'x';
            }
        }
        for (; i < s.length(); i++) {
            char c = s.charAt(i);
            char l = lower(c);
            if ((c >= '0' && c <= '9') || (hex && l >= 'a' && l <= 'f')) {
                saw = '0';
                continue;
            }
            if (c == '_') {
                if (saw != '0') {
                    return false;
                }
                saw = '_';
                continue;
            }
            if (saw == '_') {
                return false;
            }
            saw = '!';
        }
        return saw != '_';
    }

    /** strconv's lower: ASCII only, as Go folds bytes, not runes. */
    private static char lower(char c) {
        return c >= 'A' && c <= 'Z' ? (char) (c + ('a' - 'A')) : c;
    }

    /** Whether net.SplitHostPort accepts it (main.go only asks that, not whether it resolves). */
    static boolean splitsAsHostPort(String hostport) {
        int i = hostport.lastIndexOf(':');
        if (i < 0) {
            return false; // missing port
        }
        int j = 0;
        int k = 0;
        if (hostport.charAt(0) == '[') {
            int end = hostport.indexOf(']');
            if (end < 0) {
                return false; // missing ']'
            }
            if (end + 1 == hostport.length()) {
                return false; // missing port
            } else if (end + 1 != i) {
                return false; // too many colons, or missing port
            }
            j = 1;
            k = end + 1;
        } else if (hostport.substring(0, i).indexOf(':') >= 0) {
            return false; // too many colons
        }
        return hostport.indexOf('[', j) < 0 && hostport.indexOf(']', k) < 0;
    }

    /** strconv.Quote, for the characters a flag value can hold. */
    static String quote(String s) {
        StringBuilder q = new StringBuilder(s.length() + 2).append('"');
        s.codePoints().forEach(cp -> {
            switch (cp) {
                case '"' -> q.append("\\\"");
                case '\\' -> q.append("\\\\");
                case '\n' -> q.append("\\n");
                case '\r' -> q.append("\\r");
                case '\t' -> q.append("\\t");
                default -> {
                    if (cp < 0x20 || cp == 0x7f) {
                        q.append(String.format("\\x%02x", cp));
                    } else if (Character.isISOControl(cp) || Character.getType(cp) == Character.SURROGATE) {
                        q.append(String.format("\\u%04x", cp));
                    } else {
                        q.appendCodePoint(cp);
                    }
                }
            }
        });
        return q.append('"').toString();
    }
}

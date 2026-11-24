package io.github.rpasco787.hopper.worker;

import java.io.PrintStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;

/**
 * The log contract's encoding: one JSON object per line, keys in the order time, level, msg,
 * then the {@link #with} fields in the order they were added, then the line's own fields.
 * time is RFC 3339 UTC from the wall clock, because accounting orders lines across processes
 * by it. Each line is written as UTF-8 bytes in one write and flushed, so concurrent lines
 * never interleave and the stream's charset can't mangle them.
 */
public final class JsonLinesLog implements EventLog {
    private final PrintStream out;
    private final Level minimum;
    private final Object[] scope;

    public JsonLinesLog(PrintStream out, Level minimum) {
        this(out, minimum, new Object[0]);
    }

    private JsonLinesLog(PrintStream out, Level minimum, Object[] scope) {
        this.out = out;
        this.minimum = minimum;
        this.scope = scope;
    }

    @Override
    public boolean enabled(Level level) {
        return level.compareTo(minimum) >= 0;
    }

    @Override
    public void log(Level level, String msg, Object... keyValues) {
        check(msg, keyValues);
        if (!enabled(level)) {
            return;
        }
        StringBuilder line = new StringBuilder(128);
        line.append("{\"time\":");
        string(line, DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
        line.append(",\"level\":");
        string(line, level.name());
        line.append(",\"msg\":");
        string(line, msg);
        fields(line, scope);
        fields(line, keyValues);
        line.append("}\n");
        byte[] bytes = line.toString().getBytes(StandardCharsets.UTF_8);
        synchronized (out) {
            out.write(bytes, 0, bytes.length);
            out.flush();
        }
    }

    @Override
    public EventLog with(Object... keyValues) {
        check("with", keyValues);
        Object[] more = Arrays.copyOf(scope, scope.length + keyValues.length);
        System.arraycopy(keyValues, 0, more, scope.length, keyValues.length);
        return new JsonLinesLog(out, minimum, more);
    }

    /** An odd count or a non-String key is an SDK bug, reported before anything is written. */
    private static void check(String msg, Object[] keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("odd number of key-values logging \"" + msg + "\"");
        }
        for (int i = 0; i < keyValues.length; i += 2) {
            if (!(keyValues[i] instanceof String)) {
                throw new IllegalArgumentException("key " + i / 2 + " is not a String logging \"" + msg + "\"");
            }
        }
    }

    private static void fields(StringBuilder line, Object[] keyValues) {
        for (int i = 0; i < keyValues.length; i += 2) {
            line.append(',');
            string(line, (String) keyValues[i]);
            line.append(':');
            value(line, keyValues[i + 1]);
        }
    }

    private static void value(StringBuilder line, Object v) {
        switch (v) {
            case null -> line.append("null");
            case Boolean b -> line.append(b.booleanValue());
            case Integer n -> line.append(n.intValue());
            case Long n -> line.append(n.longValue());
            case Short n -> line.append(n.shortValue());
            case Byte n -> line.append(n.byteValue());
            case Double d -> number(line, d);
            case Float f -> number(line, f.doubleValue());
            case Enum<?> e -> string(line, e.name());
            case Duration d -> string(line, GoDuration.format(d));
            case Throwable t -> string(line, ErrorText.sanitize(t.toString()));
            case Collection<?> c -> {
                line.append('[');
                boolean first = true;
                for (Object item : c) {
                    if (!first) {
                        line.append(',');
                    }
                    first = false;
                    value(line, item);
                }
                line.append(']');
            }
            default -> {
                if (v.getClass().isArray()) {
                    line.append('[');
                    for (int i = 0, n = Array.getLength(v); i < n; i++) {
                        if (i > 0) {
                            line.append(',');
                        }
                        value(line, Array.get(v, i));
                    }
                    line.append(']');
                } else {
                    string(line, v.toString());
                }
            }
        }
    }

    private static void number(StringBuilder line, double d) {
        if (Double.isFinite(d)) {
            line.append(d);
        } else {
            line.append("null");
        }
    }

    /** A JSON string: quotes, backslash, control characters, U+2028/9 escaped; lone surrogates as U+FFFD. */
    private static void string(StringBuilder line, String s) {
        line.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> line.append("\\\"");
                case '\\' -> line.append("\\\\");
                case '\n' -> line.append("\\n");
                case '\r' -> line.append("\\r");
                case '\t' -> line.append("\\t");
                case ' ' -> line.append("\\u2028");
                case ' ' -> line.append("\\u2029");
                default -> {
                    if (c < 0x20) {
                        line.append(String.format("\\u%04x", (int) c));
                    } else if (Character.isHighSurrogate(c) && i + 1 < s.length()
                            && Character.isLowSurrogate(s.charAt(i + 1))) {
                        line.append(c).append(s.charAt(++i));
                    } else if (Character.isSurrogate(c)) {
                        line.append('�');
                    } else {
                        line.append(c);
                    }
                }
            }
        }
        line.append('"');
    }
}

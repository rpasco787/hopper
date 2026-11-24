package io.github.rpasco787.hopper.worker;

import java.time.Duration;
import java.util.Map;

/**
 * Go's {@code time.Duration} text, so the example worker reads the same configuration as
 * cmd/worker ({@code HOPPER_LEASE_DURATION=30s}) and logs durations the way Go does. A port of
 * {@code time.ParseDuration} and {@code Duration.String}, unsigned arithmetic included, with
 * Go's error messages.
 */
public final class GoDuration {
    private static final long NS = 1L;
    private static final long US = 1_000L * NS;
    private static final long MS = 1_000L * US;
    private static final long S = 1_000L * MS;
    private static final long MIN = 60L * S;
    private static final long H = 60L * MIN;
    private static final Map<String, Long> UNITS = Map.of(
            "ns", NS, "us", US, "µs", US, "μs", US, "ms", MS, "s", S, "m", MIN, "h", H);
    /** 1<<63 as an unsigned value. */
    private static final long TWO_63 = Long.MIN_VALUE;

    private GoDuration() {}

    /**
     * Parses a duration string such as "300ms", "-1.5h" or "2h45m": a possibly signed sequence
     * of decimal numbers, each with an optional fraction and a unit suffix. Valid units are
     * "ns", "us" (or "µs"), "ms", "s", "m" and "h".
     *
     * @throws IllegalArgumentException with Go's message, such as {@code time: invalid duration "x"}
     */
    public static Duration parse(String text) {
        if (text == null) {
            throw new IllegalArgumentException("time: invalid duration null");
        }
        String s = text;
        boolean neg = false;
        if (!s.isEmpty() && (s.charAt(0) == '-' || s.charAt(0) == '+')) {
            neg = s.charAt(0) == '-';
            s = s.substring(1);
        }
        if (s.equals("0")) {
            return Duration.ZERO;
        }
        if (s.isEmpty()) {
            throw invalid(text);
        }
        long d = 0; // unsigned
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (!(c == '.' || (c >= '0' && c <= '9'))) {
                throw invalid(text);
            }
            // The integer part.
            int start = i;
            long v = 0; // unsigned
            while (i < s.length() && isDigit(s.charAt(i))) {
                if (Long.compareUnsigned(v, Long.divideUnsigned(TWO_63, 10)) > 0) {
                    throw invalid(text);
                }
                v = v * 10 + (s.charAt(i) - '0');
                if (Long.compareUnsigned(v, TWO_63) > 0) {
                    throw invalid(text);
                }
                i++;
            }
            boolean pre = i != start;
            // The fraction.
            long f = 0;
            double scale = 1;
            boolean post = false;
            if (i < s.length() && s.charAt(i) == '.') {
                i++;
                int fracStart = i;
                boolean overflow = false;
                while (i < s.length() && isDigit(s.charAt(i))) {
                    int digit = s.charAt(i) - '0';
                    i++;
                    if (overflow) {
                        continue;
                    }
                    if (Long.compareUnsigned(f, Long.MAX_VALUE / 10) > 0) {
                        overflow = true;
                        continue;
                    }
                    long y = f * 10 + digit; // unsigned: at most 2^63 + 1
                    if (Long.compareUnsigned(y, TWO_63) > 0) {
                        overflow = true;
                        continue;
                    }
                    f = y;
                    scale *= 10;
                }
                post = i != fracStart;
            }
            if (!pre && !post) {
                throw invalid(text);
            }
            // The unit.
            int unitStart = i;
            while (i < s.length() && s.charAt(i) != '.' && !isDigit(s.charAt(i))) {
                i++;
            }
            if (i == unitStart) {
                throw new IllegalArgumentException("time: missing unit in duration " + quote(text));
            }
            String u = s.substring(unitStart, i);
            Long unit = UNITS.get(u);
            if (unit == null) {
                throw new IllegalArgumentException("time: unknown unit " + quote(u) + " in duration " + quote(text));
            }
            if (Long.compareUnsigned(v, Long.divideUnsigned(TWO_63, unit)) > 0) {
                throw invalid(text);
            }
            v *= unit;
            if (f != 0) {
                v += (long) (unsignedToDouble(f) * ((double) unit / scale));
                if (Long.compareUnsigned(v, TWO_63) > 0) {
                    throw invalid(text);
                }
            }
            d += v;
            if (Long.compareUnsigned(d, TWO_63) > 0) {
                throw invalid(text);
            }
        }
        if (neg) {
            return Duration.ofNanos(-d);
        }
        if (Long.compareUnsigned(d, Long.MAX_VALUE) > 0) {
            throw invalid(text);
        }
        return Duration.ofNanos(d);
    }

    /**
     * Go's {@code Duration.String()}: "72h3m0.5s", "1.5µs", "0s".
     *
     * @throws IllegalArgumentException if d is outside a Go Duration's range (±292 years)
     */
    public static String format(Duration d) {
        long n;
        try {
            n = d.toNanos();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("duration " + d + " is outside a Go Duration's range", e);
        }
        boolean neg = n < 0;
        long u = neg ? -n : n; // unsigned: -MIN_VALUE is 1<<63
        StringBuilder out = new StringBuilder(32); // built backwards
        if (Long.compareUnsigned(u, S) < 0) {
            int prec;
            out.append('s');
            if (u == 0) {
                return "0s";
            } else if (u < US) {
                prec = 0;
                out.append('n');
            } else if (u < MS) {
                prec = 3;
                out.append('µ');
            } else {
                prec = 6;
                out.append('m');
            }
            u = fraction(out, u, prec);
            integer(out, u);
        } else {
            out.append('s');
            u = fraction(out, u, 9);
            integer(out, Long.remainderUnsigned(u, 60));
            u = Long.divideUnsigned(u, 60);
            if (u > 0) {
                out.append('m');
                integer(out, u % 60);
                u /= 60;
                if (u > 0) {
                    out.append('h');
                    integer(out, u);
                }
            }
        }
        if (neg) {
            out.append('-');
        }
        return out.reverse().toString();
    }

    /** Appends (reversed) v's last prec digits as a fraction, without trailing zeros; returns the rest. */
    private static long fraction(StringBuilder out, long v, int prec) {
        boolean print = false;
        for (int i = 0; i < prec; i++) {
            long digit = Long.remainderUnsigned(v, 10);
            print = print || digit != 0;
            if (print) {
                out.append((char) ('0' + digit));
            }
            v = Long.divideUnsigned(v, 10);
        }
        if (print) {
            out.append('.');
        }
        return v;
    }

    /** Appends (reversed) v in decimal, unsigned. */
    private static void integer(StringBuilder out, long v) {
        if (v == 0) {
            out.append('0');
            return;
        }
        while (v != 0) {
            out.append((char) ('0' + Long.remainderUnsigned(v, 10)));
            v = Long.divideUnsigned(v, 10);
        }
    }

    /** Go's float64(uint64): exact up to 2^53, rounded beyond, and never negative. */
    private static double unsignedToDouble(long v) {
        return v >= 0 ? (double) v : ((double) (v >>> 1)) * 2.0 + (v & 1);
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static IllegalArgumentException invalid(String text) {
        return new IllegalArgumentException("time: invalid duration " + quote(text));
    }

    /** Go's strconv.Quote, for the characters a configuration value can hold. */
    private static String quote(String s) {
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
                    } else if (Character.isISOControl(cp) || (cp >= Character.MIN_SURROGATE && cp <= Character.MAX_SURROGATE)) {
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

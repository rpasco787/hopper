package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Go's time.Duration syntax, for configuration parity with cmd/worker. Expected texts are Go 1.27.1's. */
class GoDurationTest {
    @Test
    void parseAcceptsGoSyntax() {
        Map<String, Duration> ok = new LinkedHashMap<>();
        ok.put("0", Duration.ZERO);
        ok.put("1ns", Duration.ofNanos(1));
        ok.put("1us", Duration.ofNanos(1_000));
        ok.put("1µs", Duration.ofNanos(1_000));
        ok.put("1μs", Duration.ofNanos(1_000)); // Greek mu, which Go also accepts
        ok.put("500ms", Duration.ofMillis(500));
        ok.put("30s", Duration.ofSeconds(30));
        ok.put("45s", Duration.ofSeconds(45));
        ok.put("5m", Duration.ofMinutes(5));
        ok.put("1m30s", Duration.ofSeconds(90));
        ok.put("1.5s", Duration.ofMillis(1_500));
        ok.put("1h", Duration.ofHours(1));
        ok.put("-1s", Duration.ofSeconds(-1));
        ok.put("+5s", Duration.ofSeconds(5));
        ok.put("1.s", Duration.ofSeconds(1));
        ok.put("1h1h", Duration.ofHours(2));
        ok.put("-0", Duration.ZERO);
        ok.put("9223372036854775807ns", Duration.ofNanos(Long.MAX_VALUE));
        ok.put("-9223372036854775808ns", Duration.ofNanos(Long.MIN_VALUE));
        ok.put("2562047h47m16.854775807s", Duration.ofNanos(Long.MAX_VALUE));
        ok.put("1.0000000001s", Duration.ofSeconds(1)); // digits past a nanosecond are dropped
        ok.put("0.5ns", Duration.ZERO);
        ok.forEach((in, want) -> assertEquals(want, GoDuration.parse(in), in));
    }

    @Test
    void parseRejectsWithGosMessages() {
        Map<String, String> bad = new LinkedHashMap<>();
        bad.put("", "time: invalid duration \"\"");
        bad.put("5", "time: missing unit in duration \"5\"");
        bad.put("1x", "time: unknown unit \"x\" in duration \"1x\"");
        bad.put("s", "time: invalid duration \"s\"");
        bad.put("9223372036854775808ns", "time: invalid duration \"9223372036854775808ns\"");
        bad.put(".s", "time: invalid duration \".s\"");
        bad.put("1e3s", "time: unknown unit \"e\" in duration \"1e3s\"");
        bad.put("-9223372036854775809ns", "time: invalid duration \"-9223372036854775809ns\"");
        bad.put("2562047h47m16.854775808s", "time: invalid duration \"2562047h47m16.854775808s\"");
        bad.forEach((in, want) -> {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> GoDuration.parse(in), in);
            assertEquals(want, e.getMessage(), in);
        });
        assertThrows(IllegalArgumentException.class, () -> GoDuration.parse(null));
    }

    @Test
    void formatIsGosString() {
        Map<Duration, String> want = new LinkedHashMap<>();
        want.put(Duration.ZERO, "0s");
        want.put(Duration.ofNanos(1), "1ns");
        want.put(Duration.ofNanos(1_500), "1.5µs");
        want.put(Duration.ofMillis(100), "100ms");
        want.put(Duration.ofSeconds(30), "30s");
        want.put(Duration.ofSeconds(90), "1m30s");
        want.put(Duration.ofHours(1), "1h0m0s");
        want.put(Duration.ofSeconds(-1), "-1s");
        want.put(Duration.ofMillis(1_500), "1.5s");
        want.put(Duration.ofNanos(1_001_000), "1.001ms");
        want.put(Duration.ofHours(2).plusMillis(3), "2h0m0.003s");
        want.put(Duration.ofNanos(Long.MAX_VALUE), "2562047h47m16.854775807s");
        want.put(Duration.ofNanos(Long.MIN_VALUE), "-2562047h47m16.854775808s");
        want.forEach((d, s) -> assertEquals(s, GoDuration.format(d), s));
    }

    @Test
    void formatAndParseRoundTrip() {
        for (long n : new long[] {0, 1, 999, 1_500, 123_456_789, 30_000_000_000L, 3_723_000_000_123L, -45_000_000_000L}) {
            assertEquals(Duration.ofNanos(n), GoDuration.parse(GoDuration.format(Duration.ofNanos(n))), Long.toString(n));
        }
    }

    @Test
    void aDurationBeyondGosRangeIsRejectedByFormat() {
        assertThrows(IllegalArgumentException.class, () -> GoDuration.format(Duration.ofSeconds(Long.MAX_VALUE)));
    }
}

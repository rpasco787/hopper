package io.github.rpasco787.hopper.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.worker.EventLog;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Ports of cmd/worker/main_test.go's configuration tests, plus Java-only checks that the flag
 * syntax, integer syntax, log levels and messages are Go's flag, strconv and slog behaviour.
 * The environment is a map argument, so nothing here reads or changes the process's.
 */
class ConfigTest {
    private static final Map<String, String> NO_ENV = Map.of();

    private static Config parse(Map<String, String> env, String... args) throws ConfigException {
        return Config.parse(args, env);
    }

    private static ConfigException fails(Map<String, String> env, String... args) {
        return assertThrows(ConfigException.class, () -> Config.parse(args, env), List.of(args).toString());
    }

    @Test
    void configDefaults() throws Exception {
        assertEquals(new Config("127.0.0.1:9090", "", 8, Duration.ZERO, Duration.ofSeconds(30), "info", ""),
                parse(NO_ENV));
    }

    @Test
    void configReadsEveryEnvironmentVariable() throws Exception {
        Map<String, String> env = Map.of(
                "HOPPER_BROKER", "broker.internal:9090",
                "HOPPER_WORKER_ID", "worker-9",
                "HOPPER_CONCURRENCY", "17",
                "HOPPER_LEASE_DURATION", "45s",
                "HOPPER_SHUTDOWN_TIMEOUT", "9s",
                "HOPPER_LOG_LEVEL", "debug");
        assertEquals(new Config("broker.internal:9090", "worker-9", 17, Duration.ofSeconds(45),
                Duration.ofSeconds(9), "debug", ""), parse(env));
    }

    @Test
    void configFlagsOverrideInvalidEnvironment() throws Exception {
        Map<String, String> env = Map.of("HOPPER_CONCURRENCY", "many");
        assertTrue(fails(env).getMessage().contains("HOPPER_CONCURRENCY"));

        assertEquals(3, parse(env, "-concurrency", "3").concurrency());
    }

    @Test
    void metricsListenConfiguration() throws Exception {
        assertEquals("", parse(NO_ENV).metricsListen());
        assertEquals("127.0.0.1:9191", parse(Map.of("HOPPER_METRICS_LISTEN", "127.0.0.1:9191")).metricsListen());
        assertEquals("127.0.0.1:9292", parse(NO_ENV, "-metrics-listen", "127.0.0.1:9292").metricsListen());
        assertEquals("invalid metrics-listen address", fails(NO_ENV, "-metrics-listen", "nohost").getMessage());
    }

    @Test
    void configValidation() {
        record Case(String name, List<String> args, String want) {}
        for (Case tc : List.of(
                new Case("positionals", List.of("extra"), "unexpected positional"),
                new Case("empty broker", List.of("-broker", ""), "broker"),
                new Case("zero concurrency", List.of("-concurrency", "0"), "concurrency"),
                new Case("negative concurrency", List.of("-concurrency", "-1"), "concurrency"),
                new Case("too much concurrency", List.of("-concurrency", "65537"), "maximum"),
                new Case("negative lease", List.of("-lease-duration", "-1s"), "lease-duration"),
                new Case("negative shutdown", List.of("-shutdown-timeout", "-1s"), "shutdown-timeout"),
                new Case("bad log level", List.of("-log-level", "loud"), "log level"))) {
            ConfigException e = fails(NO_ENV, tc.args().toArray(String[]::new));
            assertTrue(e.getMessage().contains(tc.want()), tc.name() + ": " + e.getMessage());
        }
        for (List<String> args : List.of(
                List.of("-lease-duration", "0"),
                List.of("-lease-duration", "1ns"),
                List.of("-shutdown-timeout", "0"),
                List.of("-concurrency", "65536"))) {
            try {
                Config.parse(args.toArray(String[]::new), NO_ENV);
            } catch (ConfigException e) {
                throw new AssertionError(args + ": " + e.getMessage(), e);
            }
        }
    }

    // Java-only: the exact texts, which configValidation only samples.

    @Test
    void errorsUseGoText() {
        Map<List<String>, String> cases = new java.util.LinkedHashMap<>();
        cases.put(List.of("extra"), "unexpected positional arguments");
        cases.put(List.of("-x"), "flag provided but not defined: -x");
        cases.put(List.of("-broker", ""), "broker address is required");
        cases.put(List.of("-concurrency", "0"), "concurrency must be positive");
        cases.put(List.of("-concurrency", "65537"), "concurrency 65537 exceeds the maximum of 65536");
        cases.put(List.of("-lease-duration", "-1s"), "lease-duration must not be negative");
        cases.put(List.of("-shutdown-timeout", "-1s"), "shutdown-timeout must not be negative");
        cases.put(List.of("-metrics-listen", "nohost"), "invalid metrics-listen address");
        cases.put(List.of("-log-level", "loud"), "invalid log level \"loud\" (want debug, info, warn or error)");
        cases.put(List.of("-concurrency", "many"), "invalid value \"many\" for flag -concurrency: parse error");
        cases.put(List.of("-concurrency", "9223372036854775808"),
                "invalid value \"9223372036854775808\" for flag -concurrency: value out of range");
        cases.put(List.of("-lease-duration", "5"), "invalid value \"5\" for flag -lease-duration: parse error");
        cases.put(List.of("-broker"), "flag needs an argument: -broker");
        cases.put(List.of("---broker", "x"), "bad flag syntax: ---broker");
        cases.put(List.of("-=x"), "bad flag syntax: -=x");
        cases.forEach((args, want) -> assertEquals(want, fails(NO_ENV, args.toArray(String[]::new)).getMessage(),
                args.toString()));

        assertEquals("invalid HOPPER_LEASE_DURATION", fails(Map.of("HOPPER_LEASE_DURATION", "soon")).getMessage());
        // Go applies the environment in its table's order, so the first bad one is named.
        assertEquals("invalid HOPPER_CONCURRENCY",
                fails(Map.of("HOPPER_LEASE_DURATION", "soon", "HOPPER_CONCURRENCY", "many")).getMessage());
        // A set but empty variable is applied, as os.LookupEnv reports it.
        assertEquals("broker address is required", fails(Map.of("HOPPER_BROKER", "")).getMessage());
    }

    @Test
    void flagErrorsAskForUsageAndValidationErrorsDoNot() {
        // Go's flag package prints the error and the usage itself for a parse error
        // (ContinueOnError's failf); a validation error is only returned.
        assertTrue(fails(NO_ENV, "-x").usage());
        assertTrue(fails(NO_ENV, "-concurrency", "many").usage());
        assertTrue(fails(NO_ENV, "-broker").usage());
        assertFalse(fails(NO_ENV, "-concurrency", "0").usage());
        assertFalse(fails(NO_ENV, "extra").usage());
        assertFalse(fails(Map.of("HOPPER_CONCURRENCY", "many")).usage());
        assertFalse(fails(NO_ENV, "-x").help());
    }

    @Test
    void flagSpellings() throws Exception {
        for (List<String> args : List.of(
                List.of("-concurrency", "3"), List.of("-concurrency=3"),
                List.of("--concurrency", "3"), List.of("--concurrency=3"),
                List.of("-concurrency", "9", "-concurrency", "3"))) {
            assertEquals(3, parse(NO_ENV, args.toArray(String[]::new)).concurrency(), args.toString());
        }
        // A non-boolean flag takes the next argument whatever it looks like.
        assertEquals("-x", parse(NO_ENV, "-worker-id", "-x").workerId());
        // The value is everything after the first '='.
        assertEquals("a=b", parse(NO_ENV, "-worker-id=a=b").workerId());
        // "--" ends the flags; what follows is positional. So is "-" and anything after it.
        assertEquals("unexpected positional arguments", fails(NO_ENV, "--", "-concurrency", "3").getMessage());
        assertEquals("unexpected positional arguments", fails(NO_ENV, "-", "-concurrency", "3").getMessage());
        assertEquals(8, parse(NO_ENV, "--").concurrency());
    }

    @Test
    void helpFlags() {
        for (List<String> args : List.of(List.of("-h"), List.of("-help"), List.of("--help"), List.of("--h"),
                List.of("-help=yes"), List.of("-concurrency", "3", "-h"))) {
            ConfigException e = fails(NO_ENV, args.toArray(String[]::new));
            assertTrue(e.help(), args.toString());
        }
    }

    @Test
    void integersParseAsGoFlagDoes() throws Exception {
        // flag.IntVar parses with strconv.ParseInt(s, 0, 64): base prefixes, a leading 0 for
        // octal, and underscores between digits.
        Map<String, Integer> ok = Map.of("0x10", 16, "020", 16, "0b11", 3, "0o17", 15, "1_0", 10,
                "+5", 5, "0X1_0", 16, "65_536", 65536);
        for (var e : ok.entrySet()) {
            assertEquals(e.getValue(), parse(NO_ENV, "-concurrency", e.getKey()).concurrency(), e.getKey());
        }
        for (String bad : List.of("1__0", "_1", "1_", "0x", "08", "0b2", "1.0", "", "+", "- 1", "１")) {
            assertEquals("invalid value \"" + bad + "\" for flag -concurrency: parse error",
                    fails(NO_ENV, "-concurrency", bad).getMessage(), bad);
        }
        assertEquals("concurrency 99999999999 exceeds the maximum of 65536",
                fails(NO_ENV, "-concurrency", "99999999999").getMessage());
        assertEquals("concurrency must be positive", fails(NO_ENV, "-concurrency", "-0x10").getMessage());
    }

    @Test
    void logLevelsParseAsSlogDoes() throws Exception {
        Map<String, Long> ok = Map.of("DEBUG", -4L, "Info", 0L, "warn", 4L, "error", 8L,
                "info+2", 2L, "warn-1", 3L, "error+1", 9L, "debug-8", -12L);
        for (var e : ok.entrySet()) {
            assertEquals(e.getValue(), Config.slogLevel(e.getKey()), e.getKey());
            assertEquals(e.getKey(), parse(NO_ENV, "-log-level", e.getKey()).logLevel());
        }
        for (String bad : List.of("loud", "trace", "info+", "info+x", "", "+1", "warning")) {
            ConfigException e = fails(NO_ENV, "-log-level", bad);
            assertEquals("invalid log level \"" + bad + "\" (want debug, info, warn or error)", e.getMessage());
        }
        // The SDK's four levels sit at slog's values; a level between them shows the next one
        // up, and one above ERROR shows nothing, as a slog handler would.
        assertEquals(Optional.of(EventLog.Level.DEBUG), Config.minimum(-12));
        assertEquals(Optional.of(EventLog.Level.DEBUG), Config.minimum(-4));
        assertEquals(Optional.of(EventLog.Level.INFO), Config.minimum(-3));
        assertEquals(Optional.of(EventLog.Level.INFO), Config.minimum(0));
        assertEquals(Optional.of(EventLog.Level.WARN), Config.minimum(2));
        assertEquals(Optional.of(EventLog.Level.ERROR), Config.minimum(8));
        assertEquals(Optional.empty(), Config.minimum(9));
    }

    @Test
    void metricsListenAcceptsWhatSplitHostPortAccepts() throws Exception {
        for (String ok : List.of("127.0.0.1:9191", ":9092", "[::1]:9092", "host:", "0.0.0.0:9092", "[fe80::1%en0]:1")) {
            assertEquals(ok, parse(NO_ENV, "-metrics-listen", ok).metricsListen());
        }
        for (String bad : List.of("nohost", "::1", "[::1]", "[::1]9092", "a:b:c", "[a]:b:c", "a[b:1", "a]:1",
                "[a:1", "[a]b:1")) {
            assertEquals("invalid metrics-listen address", fails(NO_ENV, "-metrics-listen", bad).getMessage(), bad);
        }
    }

    @Test
    void usageIsGoFlagsFormat() {
        // flag.PrintDefaults: sorted by name, the type word, the usage on a tab-indented line,
        // and the default unless it is the type's zero value.
        assertEquals("""
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
                """, Config.USAGE);
    }
}

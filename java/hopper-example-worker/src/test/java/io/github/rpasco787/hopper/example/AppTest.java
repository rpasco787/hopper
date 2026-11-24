package io.github.rpasco787.hopper.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import io.grpc.Status;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The example worker's run, in process, against the fake broker on loopback: ports of
 * cmd/worker/main_test.go's run tests and the stdout contract of plan §6. The exit codes a
 * signal gives a real JVM are MainProcessTest's.
 */
class AppTest {
    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final ObjectMapper JSON = new ObjectMapper();

    private ByteArrayOutputStream out;
    private ByteArrayOutputStream err;
    private PrintStream outStream;
    private PrintStream errStream;

    // App installs two process-wide things; each test puts them back.
    private final Logger root = Logger.getLogger("");
    private Handler[] savedHandlers;
    private Thread.UncaughtExceptionHandler savedUncaught;
    private PrintStream savedErr;

    @BeforeEach
    void save() {
        reset();
        savedHandlers = root.getHandlers();
        savedUncaught = Thread.getDefaultUncaughtExceptionHandler();
        savedErr = System.err;
    }

    @AfterEach
    void restore() {
        System.setErr(savedErr);
        Thread.setDefaultUncaughtExceptionHandler(savedUncaught);
        for (Handler h : root.getHandlers()) {
            root.removeHandler(h);
        }
        for (Handler h : savedHandlers) {
            root.addHandler(h);
        }
    }

    private void reset() {
        out = new ByteArrayOutputStream();
        err = new ByteArrayOutputStream();
        outStream = new PrintStream(out, true, StandardCharsets.UTF_8);
        errStream = new PrintStream(err, true, StandardCharsets.UTF_8);
    }

    private App app(Map<String, String> env, String... args) {
        return new App(args, env, outStream, errStream);
    }

    private String stdout() {
        return out.toString(StandardCharsets.UTF_8);
    }

    private String stderr() {
        return err.toString(StandardCharsets.UTF_8);
    }

    /** Every stdout line, each of which must be one JSON object. */
    private List<JsonNode> lines() throws IOException {
        List<JsonNode> lines = new ArrayList<>();
        for (String line : stdout().split("\n", -1)) {
            if (line.isEmpty()) {
                continue;
            }
            JsonNode n = JSON.readTree(line);
            assertTrue(n.isObject(), line);
            lines.add(n);
        }
        return lines;
    }

    private List<JsonNode> withMsg(String msg) throws IOException {
        return lines().stream().filter(n -> msg.equals(n.path("msg").asText())).toList();
    }

    private static List<String> names(JsonNode n) {
        List<String> names = new ArrayList<>();
        n.fieldNames().forEachRemaining(names::add);
        return names;
    }

    /** run() on its own thread. */
    private static final class Running {
        final App app;
        final Thread thread;
        final AtomicInteger code = new AtomicInteger(-1);

        Running(App app) {
            this.app = app;
            this.thread = Thread.ofPlatform().name("app-run").start(() -> code.set(app.run()));
        }

        int join(Duration timeout) throws InterruptedException {
            assertTrue(thread.join(timeout), "run() returned within " + timeout);
            return code.get();
        }
    }

    @Test
    void runReturnsWorkerErrorsAndCleanStops() throws Exception {
        try (FakeBroker fake = FakeBroker.startOnLoopback()) {
            fake.onSubscribe((sub, ctx) -> {
                throw Status.INVALID_ARGUMENT.withDescription("worker stopped badly").asException();
            });
            assertEquals(1, app(Map.of(), "-broker", fake.target(), "-worker-id", "w1").run());
            assertTrue(stderr().startsWith("worker: "), stderr());
            assertTrue(stderr().contains("worker stopped badly"), stderr());
            assertTrue(withMsg("worker stopped").isEmpty(), "a failed run doesn't say it stopped");
        }

        reset();
        try (FakeBroker fake = FakeBroker.startOnLoopback()) {
            Running r = new Running(app(Map.of(), "-broker", fake.target(), "-worker-id", "w2"));
            fake.awaitStream(WAIT);
            r.app.shutdown();
            assertEquals(0, r.join(WAIT));
            assertEquals("", stderr());
            List<JsonNode> stopped = withMsg("worker stopped");
            assertEquals(1, stopped.size(), stdout());
            assertEquals("w2", stopped.get(0).get("worker_id").asText());
        }
    }

    @Test
    void startupBeforeBrokerAndShutdownDrainsPromptly() throws Exception {
        Running r = new Running(app(Map.of(), "-broker", "127.0.0.1:1", "-worker-id", "w3"));
        Thread.sleep(200);
        long t0 = System.nanoTime();
        r.app.shutdown();
        assertEquals(0, r.join(Duration.ofSeconds(2)));
        assertTrue(System.nanoTime() - t0 < Duration.ofSeconds(2).toNanos());
        assertEquals(1, withMsg("worker stopped").size());
    }

    @Test
    void everyStdoutLineIsJson() throws Exception {
        try (FakeBroker fake = FakeBroker.startOnLoopback()) {
            Running r = new Running(app(Map.of(), "-broker", fake.target(), "-worker-id", "w4"));
            FakeStream stream = fake.awaitStream(WAIT);
            stream.deliver(Fixture.of("sleep").payload("{\"ms\":20}".getBytes(StandardCharsets.UTF_8)).proto(), WAIT);
            fake.awaitCalls("Ack", 1, WAIT);
            r.app.shutdown();
            assertEquals(0, r.join(WAIT));
        }
        assertEquals("", stderr());
        List<String> msgs = lines().stream().map(n -> n.get("msg").asText()).toList();
        for (String want : List.of("worker starting", "lease stream opened", "job started", "handler finished",
                "job confirmed", "shutdown started", "worker stopped")) {
            assertTrue(msgs.contains(want), want + " in " + msgs);
        }
        assertEquals("worker stopped", msgs.get(msgs.size() - 1), "nothing after the last line");
        for (JsonNode n : lines()) {
            assertEquals(List.of("time", "level", "msg"), names(n).subList(0, 3), n.toString());
        }
    }

    @Test
    void metricsListenIsAcceptedAndNotServed() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        String addr = "127.0.0.1:" + port;
        try (FakeBroker fake = FakeBroker.startOnLoopback()) {
            Running r = new Running(app(Map.of("HOPPER_METRICS_LISTEN", addr), "-broker", fake.target(),
                    "-worker-id", "w5"));
            fake.awaitStream(WAIT);
            assertThrows(ConnectException.class, () -> new Socket("127.0.0.1", port).close(), "nothing listens");
            r.app.shutdown();
            assertEquals(0, r.join(WAIT));
        }
        List<JsonNode> warned = withMsg("metrics are not served by the Java example worker");
        assertEquals(1, warned.size(), stdout());
        assertEquals("WARN", warned.get(0).get("level").asText());
        assertEquals(addr, warned.get(0).get("addr").asText());
    }

    @Test
    void aBadConfigurationExitsOneWithTheErrorOnStderr() {
        assertEquals(1, app(Map.of(), "-concurrency", "0").run());
        assertEquals("worker: concurrency must be positive\n", stderr());
        assertEquals("", stdout());
    }

    // Java-only.

    @Test
    void helpPrintsUsageToStderrAndExitsZero() {
        assertEquals(0, app(Map.of(), "-help").run());
        assertEquals(Config.USAGE, stderr());
        assertEquals("", stdout());
    }

    @Test
    void aFlagErrorPrintsItAndTheUsageThenTheWorkerLine() {
        // What cmd/worker's stderr holds: flag's own report (failf), then main's.
        assertEquals(1, app(Map.of(), "-x").run());
        assertEquals("flag provided but not defined: -x\n" + Config.USAGE
                + "worker: flag provided but not defined: -x\n", stderr());
        assertEquals("", stdout());
    }

    @Test
    void workerStartingAndStoppedCarryCmdWorkersFields() throws Exception {
        try (FakeBroker fake = FakeBroker.startOnLoopback()) {
            Running r = new Running(app(Map.of(), "-broker", fake.target(), "-worker-id", "w6", "-concurrency", "3",
                    "-lease-duration", "1m30s", "-shutdown-timeout", "0"));
            fake.awaitStream(WAIT);
            r.app.shutdown();
            assertEquals(0, r.join(WAIT));

            JsonNode starting = withMsg("worker starting").get(0);
            assertEquals(List.of("time", "level", "msg", "worker_id", "broker", "concurrency", "lease_duration",
                    "shutdown_timeout"), names(starting));
            assertEquals("INFO", starting.get("level").asText());
            assertEquals("w6", starting.get("worker_id").asText());
            assertEquals(fake.target(), starting.get("broker").asText());
            assertTrue(starting.get("concurrency").isInt());
            assertEquals(3, starting.get("concurrency").asInt());
            assertEquals("1m30s", starting.get("lease_duration").asText());
            assertEquals("0s", starting.get("shutdown_timeout").asText(), "the configured value, as Go logs it");

            JsonNode stopped = withMsg("worker stopped").get(0);
            assertEquals(List.of("time", "level", "msg", "worker_id"), names(stopped));
            assertEquals("INFO", stopped.get("level").asText());
            assertEquals("w6", withMsg("lease stream opened").get(0).get("worker_id").asText());
        }
    }

    @Test
    void aShutdownBeforeRunDrainsWithoutAStream() throws Exception {
        try (FakeBroker fake = FakeBroker.startOnLoopback()) {
            App app = app(Map.of(), "-broker", fake.target(), "-worker-id", "w7");
            app.shutdown();
            assertEquals(0, app.run());
            assertEquals(0, fake.started("LeaseJobs"));
        }
        List<String> msgs = lines().stream().map(n -> n.get("msg").asText()).toList();
        assertTrue(msgs.containsAll(List.of("worker starting", "shutdown started", "worker stopped")), msgs.toString());
    }

    @Test
    void shutdownTimeoutIsTheBudgetTheSdkDrainsWith() {
        App seven = app(Map.of(), "-broker", "127.0.0.1:1", "-shutdown-timeout", "7s");
        assertEquals(Duration.ofSeconds(30), seven.shutdownTimeout(), "30 s before parsing");
        seven.shutdown();
        assertEquals(0, seven.run());
        assertEquals(Duration.ofSeconds(7), seven.shutdownTimeout());

        // Zero means the SDK's default, so the hook must wait for that, not for 0 + 5 s.
        App zero = app(Map.of(), "-broker", "127.0.0.1:1", "-shutdown-timeout", "0");
        zero.shutdown();
        assertEquals(0, zero.run());
        assertEquals(Duration.ofSeconds(30), zero.shutdownTimeout());
    }

    @Test
    void awaitExitIsOneUntilRunReturnsThenItsCode() throws Exception {
        App app = app(Map.of(), "-broker", "127.0.0.1:1", "-worker-id", "w8");
        assertEquals(1, app.awaitExit(Duration.ofMillis(50)), "not run yet");
        Running r = new Running(app);
        Thread.sleep(100);
        app.shutdown();
        assertEquals(0, app.awaitExit(WAIT));
        assertEquals(0, r.join(WAIT));

        App bad = app(Map.of(), "-concurrency", "0");
        assertEquals(1, bad.run());
        assertEquals(1, bad.awaitExit(Duration.ZERO));
    }

    @Test
    void julRecordsAndUncaughtExceptionsDuringARunAreJsonOnStdout() throws Exception {
        ByteArrayOutputStream stray = new ByteArrayOutputStream();
        System.setErr(new PrintStream(stray, true, StandardCharsets.UTF_8));
        try (FakeBroker fake = FakeBroker.startOnLoopback()) {
            Running r = new Running(app(Map.of(), "-broker", fake.target(), "-worker-id", "w9"));
            fake.awaitStream(WAIT);
            Logger.getLogger("io.grpc.Probe").warning("grpc says hello");
            Thread t = Thread.ofPlatform().name("probe").start(() -> {
                throw new IllegalStateException("boom");
            });
            t.join();
            r.app.shutdown();
            assertEquals(0, r.join(WAIT));
        }
        assertEquals("", stray.toString(StandardCharsets.UTF_8), "nothing on the process's stderr");
        List<JsonNode> jul = withMsg("grpc says hello");
        assertEquals(1, jul.size(), stdout());
        assertEquals("WARN", jul.get(0).get("level").asText());
        assertEquals("io.grpc.Probe", jul.get(0).get("logger").asText());
        List<JsonNode> uncaught = withMsg("uncaught exception");
        assertEquals(1, uncaught.size(), stdout());
        assertEquals("ERROR", uncaught.get(0).get("level").asText());
        assertEquals("probe", uncaught.get(0).get("thread").asText());
        assertEquals("java.lang.IllegalStateException: boom", uncaught.get(0).get("err").asText());
    }

    @Test
    void theDefaultWorkerIdIsTheHostnameAndPid() throws Exception {
        App app = app(Map.of(), "-broker", "127.0.0.1:1");
        app.shutdown();
        assertEquals(0, app.run());
        String id = withMsg("worker starting").get(0).get("worker_id").asText();
        String suffix = "-" + ProcessHandle.current().pid();
        assertTrue(id.endsWith(suffix) && id.length() > suffix.length(), id);
        assertFalse(id.startsWith("-"), id);
    }
}

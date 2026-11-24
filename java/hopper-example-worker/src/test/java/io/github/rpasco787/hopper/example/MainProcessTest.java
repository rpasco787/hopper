package io.github.rpasco787.hopper.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/**
 * The example worker as its own JVM, the way the image runs it: the classpath and JVM flags of
 * the installDist start script (Gradle passes both), a real SIGTERM (Process.destroy), and the
 * exit code the JVM reports. Java-only (design, "Testing"): a JVM ended by a signal exits 143
 * unless the shutdown hook halts, and Go's worker exits 0 after a clean drain.
 */
class MainProcessTest {
    private static final Duration WAIT = Duration.ofSeconds(20);
    private static final ObjectMapper JSON = new ObjectMapper();

    /** A child JVM whose stdout (and, if merged, stderr) is read line by line as it arrives. */
    private static final class Child implements AutoCloseable {
        final Process process;
        final List<String> out = new CopyOnWriteArrayList<>();
        final List<String> err = new CopyOnWriteArrayList<>();
        private final Thread outReader;
        private final Thread errReader;

        Child(String mainClass, boolean mergeStderr, String... args) throws IOException {
            String classpath = System.getProperty("hopper.example.classpath");
            String jvmArgs = System.getProperty("hopper.example.jvmArgs");
            assertNotNull(classpath, "Gradle passes hopper.example.classpath");
            assertNotNull(jvmArgs, "Gradle passes hopper.example.jvmArgs");
            List<String> command = new ArrayList<>();
            command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
            command.addAll(Arrays.asList(jvmArgs.split(" ")));
            command.add("-Xmx128m");
            command.add("-cp");
            command.add(classpath);
            command.add(mainClass);
            command.addAll(Arrays.asList(args));
            ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(mergeStderr);
            // cmd/worker reads HOPPER_*; none may leak in from the test's own environment.
            pb.environment().keySet().removeIf(k -> k.startsWith("HOPPER_"));
            process = pb.start();
            outReader = reader("child-stdout", process.getInputStream(), out);
            errReader = reader("child-stderr", process.getErrorStream(), err);
        }

        private static Thread reader(String name, InputStream in, List<String> into) {
            return Thread.ofPlatform().daemon().name(name).start(() -> {
                try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    for (String line; (line = r.readLine()) != null; ) {
                        into.add(line);
                    }
                } catch (IOException e) {
                    into.add("<read failed: " + e + ">");
                }
            });
        }

        void awaitLine(String what, Predicate<String> match) throws InterruptedException {
            long until = System.nanoTime() + WAIT.toNanos();
            while (out.stream().noneMatch(match)) {
                assertTrue(System.nanoTime() - until < 0, "waiting for " + what + "; have " + out + " " + err);
                assertTrue(process.isAlive(), "the child exited waiting for " + what + ": " + out + " " + err);
                Thread.sleep(20);
            }
        }

        /**
         * SIGTERM, then the exit code, once every line has been read. Through the handle: on
         * Unix, Process.destroy() also closes this side of the child's pipes, so everything the
         * drain prints after the signal would be lost.
         */
        int terminate() throws InterruptedException {
            assertTrue(process.toHandle().destroy(), "SIGTERM was sent");
            return awaitExit();
        }

        int awaitExit() throws InterruptedException {
            assertTrue(process.waitFor(WAIT.toSeconds(), TimeUnit.SECONDS), "the child exited: " + out + " " + err);
            assertTrue(outReader.join(WAIT), "stdout read to its end");
            assertTrue(errReader.join(WAIT), "stderr read to its end");
            return process.exitValue();
        }

        @Override
        public void close() {
            process.destroyForcibly();
        }
    }

    private static List<JsonNode> json(List<String> lines) throws IOException {
        List<JsonNode> nodes = new ArrayList<>();
        for (String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            JsonNode n;
            try {
                n = JSON.readTree(line);
            } catch (IOException e) {
                throw new AssertionError("a line that isn't JSON: " + line + "\nall: " + lines, e);
            }
            assertTrue(n != null && n.isObject(), "a line that isn't a JSON object: " + line);
            nodes.add(n);
        }
        return nodes;
    }

    private static List<String> msgs(List<JsonNode> nodes) {
        return nodes.stream().map(n -> n.path("msg").asText()).toList();
    }

    private static Predicate<String> msg(String msg) {
        return line -> line.contains("\"msg\":\"" + msg + "\"");
    }

    @Test
    void aSignalDuringARunDrainsAndExitsZero() throws Exception {
        try (FakeBroker fake = FakeBroker.startOnLoopback();
                Child child = new Child(Main.class.getName(), true,
                        "-broker", fake.target(), "-worker-id", "proc-1", "-shutdown-timeout", "15s")) {
            FakeStream stream = fake.awaitStream(WAIT);
            stream.deliver(Fixture.of("sleep").payload("{\"ms\":1500}".getBytes(StandardCharsets.UTF_8)).proto(), WAIT);
            child.awaitLine("job started", msg("job started"));

            assertEquals(0, child.terminate(), "a clean drain after SIGTERM exits 0, as cmd/worker does");

            List<JsonNode> lines = json(child.out); // stderr merged in, as docker logs merges it
            List<String> msgs = msgs(lines);
            assertEquals("worker stopped", msgs.get(msgs.size() - 1), msgs.toString());
            JsonNode started = lines.stream().filter(n -> n.path("msg").asText().equals("shutdown started"))
                    .findFirst().orElseThrow();
            assertEquals(1, started.get("running_handlers").asInt(), started.toString());
            assertTrue(lines.stream().anyMatch(n -> n.path("msg").asText().equals("job confirmed")
                    && n.path("operation").asText().equals("ack")), msgs.toString());
            assertEquals(1, fake.ackRequests().size());
        }
    }

    @Test
    void aSignalThatAbandonsWorkExitsOne() throws Exception {
        try (FakeBroker fake = FakeBroker.startOnLoopback();
                Child child = new Child(Main.class.getName(), false,
                        "-broker", fake.target(), "-worker-id", "proc-2", "-shutdown-timeout", "1s")) {
            FakeStream stream = fake.awaitStream(WAIT);
            stream.deliver(Fixture.of("sleep").payload("{\"ms\":60000}".getBytes(StandardCharsets.UTF_8)).proto(), WAIT);
            child.awaitLine("job started", msg("job started"));

            assertEquals(1, child.terminate(), "an abandoned drain exits 1");

            assertEquals(List.of("worker: worker: shutdown deadline reached with 1 active deliveries "
                    + "(1 running handlers, 0 acquiring a lease, 0 awaiting confirmation)"), child.err);
            List<String> msgs = msgs(json(child.out));
            assertTrue(msgs.contains("shutdown deadline reached"), msgs.toString());
            assertTrue(!msgs.contains("worker stopped"), msgs.toString());
        }
    }

    @Test
    void julRecordsAndUncaughtExceptionsReachTheLogAsJson() throws Exception {
        try (FakeBroker fake = FakeBroker.startOnLoopback();
                Child child = new Child(ProbeMain.class.getName(), true, "-broker", fake.target(), "-worker-id", "proc-3")) {
            fake.awaitStream(WAIT);
            // Either form of each probe ends the wait: the JSON line, or the default printer's.
            child.awaitLine("the JUL probe", line -> line.contains("grpc says hello"));
            child.awaitLine("the uncaught probe", line -> line.contains("probe-thread"));

            assertEquals(0, child.terminate());

            List<JsonNode> lines = json(child.out);
            assertTrue(lines.stream().anyMatch(n -> n.path("msg").asText().equals("grpc says hello")
                    && n.path("level").asText().equals("WARN")
                    && n.path("logger").asText().equals("io.grpc.Probe")), child.out.toString());
            assertTrue(lines.stream().anyMatch(n -> n.path("msg").asText().equals("uncaught exception")
                    && n.path("thread").asText().equals("probe-thread")
                    && n.path("err").asText().equals("java.lang.IllegalStateException: probe")), child.out.toString());
        }
    }

    @Test
    void exitCodesWithoutASignal() throws Exception {
        // main's own System.exit runs the hook too, which finds the code ready and halts with it.
        try (Child child = new Child(Main.class.getName(), false, "-concurrency", "0")) {
            assertEquals(1, child.awaitExit());
            assertEquals(List.of("worker: concurrency must be positive"), child.err);
            assertEquals(List.of(), child.out);
        }
        try (Child child = new Child(Main.class.getName(), false, "-help")) {
            assertEquals(0, child.awaitExit());
            assertEquals(String.join("\n", child.err) + "\n", Config.USAGE);
            assertEquals(List.of(), child.out);
        }
    }
}

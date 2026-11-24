package io.github.rpasco787.hopper.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rpasco787.hopper.worker.EventLog;
import io.github.rpasco787.hopper.worker.JsonLinesLog;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * grpc-java logs through java.util.logging, whose default console handler writes plain text to
 * stderr; docker logs merges that into the stream test/accounting reads (plan §6). The bridge
 * turns each record into one JSON line instead.
 */
class JulBridgeTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Logger root = Logger.getLogger("");
    private Handler[] saved;
    private PrintStream savedErr;

    @BeforeEach
    void save() {
        saved = root.getHandlers();
        savedErr = System.err;
    }

    @AfterEach
    void restore() {
        System.setErr(savedErr);
        for (Handler h : root.getHandlers()) {
            root.removeHandler(h);
        }
        for (Handler h : saved) {
            root.addHandler(h);
        }
    }

    private static List<JsonNode> lines(ByteArrayOutputStream out) throws IOException {
        List<JsonNode> lines = new ArrayList<>();
        for (String line : out.toString(StandardCharsets.UTF_8).split("\n")) {
            if (!line.isEmpty()) {
                lines.add(JSON.readTree(line));
            }
        }
        return lines;
    }

    @Test
    void aJulWarningBecomesOneJsonLine() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JulBridge.install(new JsonLinesLog(new PrintStream(out, true, StandardCharsets.UTF_8), EventLog.Level.DEBUG));

        Logger.getLogger("io.grpc.internal.Probe").log(Level.WARNING, "dial {0} failed after {1} tries",
                new Object[] {"broker:9090", 3});

        List<JsonNode> lines = lines(out);
        assertEquals(1, lines.size(), out.toString(StandardCharsets.UTF_8));
        JsonNode line = lines.get(0);
        assertEquals("WARN", line.get("level").asText());
        assertEquals("dial broker:9090 failed after 3 tries", line.get("msg").asText());
        assertEquals("io.grpc.internal.Probe", line.get("logger").asText());
        assertFalse(line.has("err"));
        assertEquals(List.of("time", "level", "msg", "logger"), names(line));
    }

    @Test
    void levelsMapToTheLogsFour() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Handler bridge = JulBridge.handler(new JsonLinesLog(new PrintStream(out, true, StandardCharsets.UTF_8),
                EventLog.Level.DEBUG));
        for (Level l : List.of(Level.SEVERE, Level.WARNING, Level.INFO, Level.CONFIG, Level.FINE, Level.FINEST)) {
            bridge.publish(new LogRecord(l, l.getName()));
        }
        List<String> levels = lines(out).stream().map(n -> n.get("level").asText()).toList();
        assertEquals(List.of("ERROR", "WARN", "INFO", "DEBUG", "DEBUG", "DEBUG"), levels);
    }

    @Test
    void nothingReachesStderr() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
        // A console handler made now writes to the captured stderr, as the JVM's default one
        // writes to the real one; the bridge must take it off the root logger.
        root.addHandler(new ConsoleHandler());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JulBridge.install(new JsonLinesLog(new PrintStream(out, true, StandardCharsets.UTF_8), EventLog.Level.INFO));

        Logger.getLogger("io.grpc.netty.Probe").log(Level.SEVERE, "connection reset", new IOException("boom"));

        assertEquals("", err.toString(StandardCharsets.UTF_8));
        List<JsonNode> lines = lines(out);
        assertEquals(1, lines.size());
        assertEquals("ERROR", lines.get(0).get("level").asText());
        assertEquals("connection reset", lines.get(0).get("msg").asText());
        assertEquals("io.grpc.netty.Probe", lines.get(0).get("logger").asText());
        assertEquals("java.io.IOException: boom", lines.get(0).get("err").asText());
        assertTrue(root.getHandlers().length == 1, "only the bridge is left on the root logger");
    }

    private static List<String> names(JsonNode n) {
        List<String> names = new ArrayList<>();
        n.fieldNames().forEachRemaining(names::add);
        return names;
    }
}

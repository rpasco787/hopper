package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

/** The log contract's encoding (docs/design/java-worker-sdk.md, Logs). */
class JsonLinesLogTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);

    private List<String> rawLines() {
        String all = bytes.toString(StandardCharsets.UTF_8);
        assertTrue(all.isEmpty() || all.endsWith("\n"), "every line ends with a newline");
        return all.isEmpty() ? List.of() : List.of(all.split("\n", -1)).subList(0, all.split("\n", -1).length - 1);
    }

    private JsonNode only() throws Exception {
        List<String> lines = rawLines();
        assertEquals(1, lines.size(), lines.toString());
        return JSON.readTree(lines.get(0));
    }

    private static List<String> keys(JsonNode node) {
        List<String> keys = new ArrayList<>();
        Iterator<String> it = node.fieldNames();
        it.forEachRemaining(keys::add);
        return keys;
    }

    @Test
    void aLineIsOneJsonObjectWithTimeLevelAndMsgFirst() throws Exception {
        new JsonLinesLog(out, EventLog.Level.INFO).log(EventLog.Level.INFO, "job started", "job_id", "j");
        String raw = rawLines().get(0);
        assertTrue(raw.startsWith("{\"time\":\""), raw);
        assertTrue(raw.contains("\",\"level\":\"INFO\",\"msg\":\"job started\""), raw);
        assertTrue(raw.endsWith("}"), raw);
        assertEquals(List.of("time", "level", "msg", "job_id"), keys(only()));
    }

    @Test
    void timeIsRfc3339InUtc() throws Exception {
        Instant before = Instant.now();
        new JsonLinesLog(out, EventLog.Level.INFO).log(EventLog.Level.WARN, "m");
        String time = only().get("time").asText();
        assertTrue(time.endsWith("Z"), time);
        Instant t = Instant.parse(time);
        assertFalse(t.isBefore(before.minusSeconds(1)), "it is the wall clock now: " + time);
        assertEquals("WARN", only().get("level").asText());
    }

    @Test
    void stringsAreEscapedAndParseBack() throws Exception {
        String tricky = "q\"b\\n\nt\tz\u0000u\u001fr\rls ps é😀";
        var log = new JsonLinesLog(out, EventLog.Level.INFO);
        log.log(EventLog.Level.INFO, "m", "v", tricky, "lone", "a\uD800b", "k\"ey", 1);
        String raw = rawLines().get(0);
        assertTrue(raw.contains("\\n") && raw.contains("\\t") && raw.contains("\\r"), "short escapes: " + raw);
        assertTrue(raw.contains("\\u0000") && raw.contains("\\u001f"), "control characters: " + raw);
        assertTrue(raw.contains("\\u2028") && raw.contains("\\u2029"), "line and paragraph separators: " + raw);
        assertFalse(raw.contains(" "), "never raw, so no reader splits the line there");
        JsonNode node = only();
        assertEquals(tricky, node.get("v").asText());
        assertEquals("a�b", node.get("lone").asText());
        assertEquals(1, node.get("k\"ey").asInt(), "keys are escaped too");
    }

    private enum Color { RED }

    @Test
    void valuesHaveTheirJsonTypes() throws Exception {
        UUID id = UUID.randomUUID();
        new JsonLinesLog(out, EventLog.Level.INFO).log(EventLog.Level.INFO, "m",
                "string", "s",
                "enum", Color.RED,
                "uuid", id,
                "int", 3,
                "running_handlers", 0,
                "long", 1L << 40,
                "short", (short) 7,
                "byte", (byte) -2,
                "double", 1.5,
                "float", 2.5f,
                "nan", Double.NaN,
                "inf", Float.POSITIVE_INFINITY,
                "true", true,
                "false", false,
                "null", null,
                "list", List.of("a", 1),
                "array", new String[] {"x", "y"},
                "ints", new int[] {1, 2},
                "duration", Duration.ofSeconds(90),
                "err", new IllegalStateException("bad\u0000 thing"),
                "other", new StringBuilder("sb"));
        JsonNode n = only();
        assertTrue(n.get("string").isTextual());
        assertEquals("RED", n.get("enum").asText());
        assertEquals(id.toString(), n.get("uuid").asText());
        assertTrue(n.get("int").isInt());
        assertEquals(3, n.get("int").asInt());
        assertTrue(n.get("running_handlers").isIntegralNumber(), "so Go's *int decodes it");
        assertEquals(0, n.get("running_handlers").asInt());
        assertTrue(n.get("long").isIntegralNumber());
        assertEquals(1L << 40, n.get("long").asLong());
        assertEquals(7, n.get("short").asInt());
        assertEquals(-2, n.get("byte").asInt());
        assertTrue(n.get("double").isNumber());
        assertEquals(1.5, n.get("double").asDouble());
        assertEquals(2.5, n.get("float").asDouble());
        assertTrue(n.get("nan").isNull(), "not finite is null");
        assertTrue(n.get("inf").isNull());
        assertTrue(n.get("true").isBoolean() && n.get("true").asBoolean());
        assertTrue(n.get("false").isBoolean() && !n.get("false").asBoolean());
        assertTrue(n.get("null").isNull());
        assertTrue(n.get("list").isArray());
        assertEquals("a", n.get("list").get(0).asText());
        assertEquals(1, n.get("list").get(1).asInt());
        assertEquals("y", n.get("array").get(1).asText());
        assertTrue(n.get("ints").get(0).isInt());
        assertEquals("1m30s", n.get("duration").asText(), "Go's duration text");
        assertEquals("java.lang.IllegalStateException: bad thing", n.get("err").asText(), "sanitized toString");
        assertEquals("sb", n.get("other").asText());
    }

    @Test
    void withAddsFieldsInOrderBeforeTheLinesOwn() throws Exception {
        EventLog log = new JsonLinesLog(out, EventLog.Level.INFO);
        EventLog scoped = log.with("worker_id", "w").with("job_id", "j");
        scoped.log(EventLog.Level.INFO, "m", "a", 1);
        assertEquals(List.of("time", "level", "msg", "worker_id", "job_id", "a"), keys(only()));
        assertEquals("w", only().get("worker_id").asText());
        bytes.reset();
        log.log(EventLog.Level.INFO, "m");
        assertEquals(List.of("time", "level", "msg"), keys(only()), "with returns a new log; the parent is unchanged");
    }

    @Test
    void levelsBelowTheMinimumAreDropped() {
        var log = new JsonLinesLog(out, EventLog.Level.WARN);
        assertFalse(log.enabled(EventLog.Level.DEBUG));
        assertFalse(log.enabled(EventLog.Level.INFO));
        assertTrue(log.enabled(EventLog.Level.WARN));
        assertTrue(log.enabled(EventLog.Level.ERROR));
        log.log(EventLog.Level.DEBUG, "d");
        log.log(EventLog.Level.INFO, "i");
        log.with("k", "v").log(EventLog.Level.INFO, "i2");
        log.log(EventLog.Level.WARN, "w");
        log.log(EventLog.Level.ERROR, "e");
        List<String> lines = rawLines();
        assertEquals(2, lines.size(), lines.toString());
        assertTrue(lines.get(0).contains("\"msg\":\"w\""));
        assertTrue(lines.get(1).contains("\"msg\":\"e\""));
    }

    @Test
    void concurrentLinesNeverInterleave() throws Exception {
        var log = new JsonLinesLog(out, EventLog.Level.INFO);
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            int thread = t;
            threads.add(Thread.ofVirtual().start(() -> {
                EventLog mine = log.with("thread", thread);
                for (int i = 0; i < 1_000; i++) {
                    mine.log(EventLog.Level.INFO, "line", "i", i, "pad", "x".repeat(200));
                }
            }));
        }
        for (Thread t : threads) {
            assertTrue(t.join(Duration.ofSeconds(20)));
        }
        List<String> lines = rawLines();
        assertEquals(8_000, lines.size());
        Set<String> seen = ConcurrentHashMap.newKeySet();
        for (String line : lines) {
            JsonNode n = JSON.readTree(line);
            seen.add(n.get("thread").asInt() + "/" + n.get("i").asInt());
        }
        assertEquals(8_000, seen.size(), "every line parsed, none lost or merged");
    }

    @Test
    void anOddNumberOfKeyValuesIsRejected() {
        var log = new JsonLinesLog(out, EventLog.Level.INFO);
        IllegalArgumentException odd = assertThrows(IllegalArgumentException.class,
                () -> log.log(EventLog.Level.INFO, "job started", "job_id"));
        assertTrue(odd.getMessage().contains("job started"), odd.getMessage());
        IllegalArgumentException key = assertThrows(IllegalArgumentException.class,
                () -> log.log(EventLog.Level.INFO, "job confirmed", 1, "v"));
        assertTrue(key.getMessage().contains("job confirmed"), key.getMessage());
        assertThrows(IllegalArgumentException.class, () -> log.with("worker_id"));
        assertEquals(List.of(), rawLines(), "nothing half-written");
    }

    @Test
    void aLineIsUtf8WhateverTheStreamsCharset() throws Exception {
        var latin = new PrintStream(bytes, true, StandardCharsets.US_ASCII);
        new JsonLinesLog(latin, EventLog.Level.INFO).log(EventLog.Level.INFO, "m", "v", "é😀");
        assertEquals("é😀", only().get("v").asText());
    }
}

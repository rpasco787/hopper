package io.github.rpasco787.hopper.example;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rpasco787.hopper.worker.Job;
import io.github.rpasco787.hopper.worker.JobContext;
import io.github.rpasco787.hopper.worker.JobHandler;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * cmd/worker/handlers.go's three queues. The payload rules are Go's encoding/json decoding into
 * cmd/worker's structs: a top-level null is an empty object, keys match case-insensitively and
 * the last match wins, a null ms means the default, and a null fail leaves the value before it.
 *
 * <p>Each handler stops when the SDK stops it: the SDK sets a reason, then interrupts the
 * handler's thread (plan §7 item 3e).
 */
final class Handlers {
    static final Duration DEFAULT_WORK = Duration.ofMillis(100);

    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    /** Keeps cpu_burn's arithmetic observable, as handlers.go's cpuChecksum does. */
    private static final AtomicLong CPU_CHECKSUM = new AtomicLong();

    /** The queues in cmd/worker's order (main.go workerHandlers). */
    static final Map<String, JobHandler> ALL;

    static {
        Map<String, JobHandler> all = new LinkedHashMap<>();
        all.put("sleep", ctx -> sleep(ctx, Work.of(ctx.job())));
        all.put("fail_n_times", ctx -> failNTimes(Work.of(ctx.job())));
        all.put("cpu_burn", ctx -> cpuBurn(ctx, Work.of(ctx.job())));
        ALL = Collections.unmodifiableMap(all);
    }

    private Handlers() {}

    /** What a handler reads from its job. Job can only be made by the SDK, so tests make these. */
    record Work(UUID id, byte[] payload, int attempt) {
        static Work of(Job job) {
            return new Work(job.id(), job.payload(), job.attempt());
        }
    }

    /** A handler body, for tests that run each of them the same way. */
    @FunctionalInterface
    interface Body {
        void run(JobContext ctx, Work work) throws Exception;
    }

    /** handlers.go workDuration: ms, or 100 ms when absent or null. */
    static Duration workDuration(Work work) throws JobFailure {
        Field ms = field(work, "ms", Long.MIN_VALUE, Long.MAX_VALUE, "int64");
        if (ms.value == null) {
            return DEFAULT_WORK;
        }
        long n = ms.value;
        if (n < 0) {
            throw new JobFailure("ms must be non-negative");
        }
        if (n > Long.MAX_VALUE / 1_000_000) {
            throw new JobFailure("ms overflows time.Duration");
        }
        return Duration.ofMillis(n);
    }

    /** Sleeps the job's duration; ends early, by throwing, when the SDK stops it. */
    static void sleep(JobContext ctx, Work work) throws Exception {
        Duration d = workDuration(work);
        ctx.throwIfStopped();
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            ctx.throwIfStopped();
            throw e;
        }
    }

    /** Fails while attempt <= fail. */
    static void failNTimes(Work work) throws JobFailure {
        Field fail = field(work, "fail", Integer.MIN_VALUE, Integer.MAX_VALUE, "int32");
        long n = fail.value == null ? 0 : fail.value;
        if (n < 0) {
            throw new JobFailure("fail must be non-negative");
        }
        if (work.attempt() <= n) {
            throw new JobFailure("intentional failure on attempt " + work.attempt() + " of " + n);
        }
    }

    /**
     * Spins handlers.go's LCG until the job's duration ends, looking every 1,024 steps for a
     * stop or an interrupt.
     */
    static void cpuBurn(JobContext ctx, Work work) throws Exception {
        Duration d = workDuration(work);
        long budget = d.toNanos();
        long start = System.nanoTime();
        long x = (long) work.attempt() + 1; // uint64(job.Attempt) + 1; Java's long wraps as uint64 does
        while (System.nanoTime() - start < budget) {
            for (int i = 0; i < 1024; i++) {
                x = x * 6364136223846793005L + 1442695040888963407L;
            }
            if (ctx.isStopped() || Thread.currentThread().isInterrupted()) {
                CPU_CHECKSUM.set(x);
                ctx.throwIfStopped();
                Thread.interrupted(); // the exception carries the interrupt from here
                throw new InterruptedException("cpu_burn interrupted");
            }
        }
        CPU_CHECKSUM.set(x);
    }

    /** One integer field: null when absent or null (a pointer left nil). */
    private record Field(Long value) {}

    /**
     * Reads one integer field of the payload's top-level object, as json.Unmarshal into a struct
     * would. A value of the wrong JSON type is "must be an integer"; one outside the Go type's
     * range "overflows". Both, like syntax errors, are wrapped as job.go's Unmarshal wraps them.
     */
    private static Field field(Work work, String name, long min, long max, String goType) throws JobFailure {
        byte[] payload = work.payload();
        if (payload.length == 0) {
            throw new JobFailure("worker: job " + work.id() + " has an empty payload"); // job.go:42-44
        }
        String prefix = "worker: unmarshal payload of job " + work.id() + ": ";
        // Syntax first, over the whole payload, as encoding/json validates before decoding.
        JsonNode tree;
        try {
            tree = JSON.readTree(payload);
        } catch (IOException e) {
            throw new JobFailure(prefix + reason(e));
        }
        if (tree == null || tree.isMissingNode()) {
            throw new JobFailure(prefix + "unexpected end of JSON input");
        }
        if (tree.isNull()) {
            return new Field(null); // json.Unmarshal of null into a struct does nothing
        }
        if (!tree.isObject()) {
            throw new JobFailure(prefix + "payload must be a JSON object");
        }
        // Then the keys in order, so duplicates and case-insensitive matches resolve as Go's do.
        Long value = null;
        try (JsonParser p = JSON.createParser(payload)) {
            p.nextToken(); // START_OBJECT
            for (JsonToken t = p.nextToken(); t == JsonToken.FIELD_NAME; t = p.nextToken()) {
                String key = p.currentName();
                JsonToken v = p.nextToken();
                if (!key.equalsIgnoreCase(name)) {
                    p.skipChildren();
                    continue;
                }
                switch (v) {
                    case VALUE_NULL -> {
                        if (goType.equals("int64")) {
                            value = null; // *int64: null sets the pointer back to nil
                        } // int32: null leaves the value as it was
                    }
                    case VALUE_NUMBER_INT -> {
                        if (p.getNumberType() == JsonParser.NumberType.BIG_INTEGER) {
                            throw new JobFailure(prefix + name + " overflows " + goType);
                        }
                        long n = p.getLongValue();
                        if (n < min || n > max) {
                            throw new JobFailure(prefix + name + " overflows " + goType);
                        }
                        value = n;
                    }
                    default -> throw new JobFailure(prefix + name + " must be an integer");
                }
            }
        } catch (IOException e) {
            throw new JobFailure(prefix + reason(e));
        }
        return new Field(value);
    }

    /** Jackson's message without its source-location suffix, which would quote the payload. */
    private static String reason(IOException e) {
        return e instanceof JsonProcessingException j ? j.getOriginalMessage() : String.valueOf(e.getMessage());
    }
}

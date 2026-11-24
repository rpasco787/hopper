package io.github.rpasco787.hopper.example;

import io.github.rpasco787.hopper.worker.EventLog;
import io.github.rpasco787.hopper.worker.GoDuration;
import io.github.rpasco787.hopper.worker.HopperWorker;
import io.github.rpasco787.hopper.worker.JsonLinesLog;
import io.github.rpasco787.hopper.worker.LogMessages;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One run of the example worker: cmd/worker/main.go's runWithContext and main, without the
 * metrics server (Decision D24). stdout gets JSON lines only; stderr gets the usage and
 * {@code worker: <error>}, as cmd/worker's does. {@link Main} adds the signal handling.
 */
final class App {
    static final String METRICS_NOT_SERVED = "metrics are not served by the Java example worker";
    static final String UNCAUGHT = "uncaught exception";
    /** The SDK's drain budget when none is configured (Decision D31), and the hook's before parsing. */
    static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    private final String[] args;
    private final Map<String, String> env;
    private final PrintStream out;
    private final PrintStream err;

    private final ReentrantLock lock = new ReentrantLock();
    private HopperWorker worker;
    private boolean shutdownRequested;
    private volatile Duration shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;
    private final CompletableFuture<Integer> exit = new CompletableFuture<>();

    App(String[] args, Map<String, String> env, PrintStream out, PrintStream err) {
        this.args = args.clone();
        this.env = Map.copyOf(env);
        this.out = out;
        this.err = err;
    }

    /** Runs to the end; the process's exit code. Single-use. */
    int run() {
        int code = 1; // anything that escapes below is a failure
        try {
            code = execute();
            return code;
        } finally {
            exit.complete(code);
        }
    }

    private int execute() {
        Config cfg;
        try {
            cfg = Config.parse(args, env);
        } catch (ConfigException e) {
            if (e.help()) {
                err.print(Config.USAGE);
                err.flush();
                return 0;
            }
            if (e.usage()) {
                err.println(e.getMessage());
                err.print(Config.USAGE);
            }
            err.println("worker: " + e.getMessage());
            return 1;
        }
        shutdownTimeout = cfg.shutdownTimeout().isZero() ? DEFAULT_SHUTDOWN_TIMEOUT : cfg.shutdownTimeout();

        EventLog log = cfg.minimum().<EventLog>map(level -> new JsonLinesLog(out, level)).orElse(Silent.LOG);
        JulBridge.install(log);
        Thread.setDefaultUncaughtExceptionHandler((thread, e) ->
                log.log(EventLog.Level.ERROR, UNCAUGHT, "thread", thread.getName(), "err", e));

        String id = cfg.workerId().isEmpty() ? resolvedWorkerId() : cfg.workerId();
        if (!cfg.metricsListen().isEmpty()) {
            log.log(EventLog.Level.WARN, METRICS_NOT_SERVED, "addr", cfg.metricsListen());
        }
        HopperWorker.Builder b = HopperWorker.builder(cfg.broker())
                .workerId(id)
                .concurrency(cfg.concurrency())
                .leaseDuration(cfg.leaseDuration())
                .shutdownTimeout(cfg.shutdownTimeout())
                .log(log);
        Handlers.ALL.forEach(b::handle);
        HopperWorker w;
        try {
            w = b.build();
        } catch (RuntimeException e) {
            err.println("worker: " + text(e));
            return 1;
        }
        boolean early;
        lock.lock();
        try {
            worker = w;
            early = shutdownRequested;
        } finally {
            lock.unlock();
        }
        if (early) {
            w.shutdown(); // a signal before the worker existed: run() drains at once
        }
        log.log(EventLog.Level.INFO, LogMessages.WORKER_STARTING, "worker_id", id, "broker", cfg.broker(),
                "concurrency", cfg.concurrency(), "lease_duration", GoDuration.format(cfg.leaseDuration()),
                "shutdown_timeout", GoDuration.format(cfg.shutdownTimeout()));
        try {
            w.run();
        } catch (RuntimeException e) {
            err.println("worker: " + text(e));
            return 1;
        }
        log.log(EventLog.Level.INFO, LogMessages.WORKER_STOPPED, "worker_id", id);
        return 0;
    }

    /** Begins the drain, from any thread; before the worker exists, it drains as soon as it does. */
    void shutdown() {
        HopperWorker w;
        lock.lock();
        try {
            shutdownRequested = true;
            w = worker;
        } finally {
            lock.unlock();
        }
        if (w != null) {
            w.shutdown();
        }
    }

    /** The budget the worker drains within: 30 s until parsed, and when configured as zero. */
    Duration shutdownTimeout() {
        return shutdownTimeout;
    }

    /** run()'s exit code, or 1 if it isn't ready within {@code timeout}. */
    int awaitExit(Duration timeout) {
        try {
            return exit.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            return 1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 1;
        } catch (ExecutionException e) {
            return 1; // never: the future is only completed with a value
        }
    }

    private static String text(RuntimeException e) {
        return e.getMessage() != null ? e.getMessage() : e.toString();
    }

    /** main.go resolvedWorkerID: the hostname, or "worker", then the PID. */
    static String resolvedWorkerId() {
        String host = hostname();
        if (host.isEmpty()) {
            host = "worker";
        }
        return host + "-" + ProcessHandle.current().pid();
    }

    /**
     * The kernel's hostname, as os.Hostname reads it on Linux (the image), and otherwise the
     * JDK's, which looks it up and can be slow on macOS.
     */
    private static String hostname() {
        try {
            return Files.readString(Path.of("/proc/sys/kernel/hostname"), StandardCharsets.UTF_8).strip();
        } catch (IOException | RuntimeException e) {
            // not Linux
        }
        try {
            String host = InetAddress.getLocalHost().getHostName();
            return host == null ? "" : host;
        } catch (IOException | RuntimeException e) {
            return "";
        }
    }

    /** The log for a level above ERROR: slog writes nothing at such a level, and neither does this. */
    private enum Silent implements EventLog {
        LOG;

        @Override
        public boolean enabled(Level level) {
            return false;
        }

        @Override
        public void log(Level level, String msg, Object... keyValues) {}

        @Override
        public EventLog with(Object... keyValues) {
            return this;
        }
    }
}

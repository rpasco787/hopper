package io.github.rpasco787.hopper.example;

import io.github.rpasco787.hopper.worker.EventLog;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Routes java.util.logging, which grpc-java logs through, into the JSON log. The JVM's default
 * root handler prints plain text to stderr, and docker logs merges stderr into the stream
 * test/accounting reads, where a non-JSON line fails the run (plan §6).
 */
final class JulBridge {
    private JulBridge() {}

    /** Replaces every handler on the root logger with one that writes to {@code log}. */
    static void install(EventLog log) {
        Logger root = Logger.getLogger("");
        for (Handler h : root.getHandlers()) {
            root.removeHandler(h);
        }
        root.addHandler(handler(log));
    }

    /** The bridging handler alone. */
    static Handler handler(EventLog log) {
        return new Bridge(log);
    }

    /** SEVERE is ERROR, WARNING is WARN, INFO is INFO, and anything finer is DEBUG. */
    static EventLog.Level level(Level level) {
        int v = level.intValue();
        if (v >= Level.SEVERE.intValue()) {
            return EventLog.Level.ERROR;
        } else if (v >= Level.WARNING.intValue()) {
            return EventLog.Level.WARN;
        } else if (v >= Level.INFO.intValue()) {
            return EventLog.Level.INFO;
        }
        return EventLog.Level.DEBUG;
    }

    private static final class Bridge extends Handler {
        private final EventLog log;
        /** Used only for formatMessage: parameters and resource bundles, no layout. */
        private final Formatter formatter = new SimpleFormatter();

        Bridge(EventLog log) {
            this.log = log;
        }

        @Override
        public void publish(LogRecord r) {
            if (r == null || !isLoggable(r)) {
                return;
            }
            try {
                EventLog.Level level = level(r.getLevel());
                if (!log.enabled(level)) {
                    return;
                }
                String msg = formatter.formatMessage(r);
                String logger = r.getLoggerName() == null ? "" : r.getLoggerName();
                if (r.getThrown() != null) {
                    log.log(level, msg == null ? "" : msg, "logger", logger, "err", r.getThrown());
                } else {
                    log.log(level, msg == null ? "" : msg, "logger", logger);
                }
            } catch (RuntimeException e) {
                // Nowhere is left to report it without breaking the JSON-only stream; JUL's own
                // ErrorManager would print to stderr, which is exactly what this class prevents.
            }
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}

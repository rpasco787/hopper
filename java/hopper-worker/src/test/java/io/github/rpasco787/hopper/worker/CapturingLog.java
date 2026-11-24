package io.github.rpasco787.hopper.worker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A test EventLog that records every line, with(...) fields included. Each value is stored as
 * JsonLinesLog renders it: a UUID, an Enum or any other object as its string, a Duration as
 * its Go text, a Throwable as its sanitized toString(), collections and arrays as lists, and
 * numbers, booleans and null as they are. So a test compares job_id with a proto's getId().
 */
final class CapturingLog implements EventLog {
    record Line(EventLog.Level level, String msg, Map<String, Object> fields) {}

    private static final class Store {
        final ReentrantLock lock = new ReentrantLock();
        final Condition added = lock.newCondition();
        final List<Line> lines = new ArrayList<>();
    }

    private final Store store;
    private final Object[] scope;

    CapturingLog() {
        this(new Store(), new Object[0]);
    }

    private CapturingLog(Store store, Object[] scope) {
        this.store = store;
        this.scope = scope;
    }

    @Override
    public boolean enabled(EventLog.Level level) {
        return true;
    }

    @Override
    public void log(EventLog.Level level, String msg, Object... keyValues) {
        Map<String, Object> fields = new LinkedHashMap<>();
        put(fields, msg, scope);
        put(fields, msg, keyValues);
        store.lock.lock();
        try {
            store.lines.add(new Line(level, msg, Collections.unmodifiableMap(fields)));
            store.added.signalAll();
        } finally {
            store.lock.unlock();
        }
    }

    @Override
    public EventLog with(Object... keyValues) {
        put(new LinkedHashMap<>(), "with", keyValues); // validates
        Object[] more = Arrays.copyOf(scope, scope.length + keyValues.length);
        System.arraycopy(keyValues, 0, more, scope.length, keyValues.length);
        return new CapturingLog(store, more);
    }

    private static void put(Map<String, Object> into, String msg, Object[] keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("odd number of key-values logging \"" + msg + "\"");
        }
        for (int i = 0; i < keyValues.length; i += 2) {
            if (!(keyValues[i] instanceof String key)) {
                throw new IllegalArgumentException("a non-string key logging \"" + msg + "\"");
            }
            into.put(key, render(keyValues[i + 1]));
        }
    }

    static Object render(Object v) {
        if (v == null || v instanceof Boolean || v instanceof Integer || v instanceof Long
                || v instanceof Short || v instanceof Byte || v instanceof Double || v instanceof Float) {
            return v;
        }
        if (v instanceof Enum<?> e) {
            return e.name();
        }
        if (v instanceof Duration d) {
            return GoDuration.format(d);
        }
        if (v instanceof Throwable t) {
            return ErrorText.sanitize(t.toString());
        }
        if (v instanceof Collection<?> c) {
            return c.stream().map(CapturingLog::render).toList();
        }
        if (v.getClass().isArray()) {
            int n = java.lang.reflect.Array.getLength(v);
            List<Object> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                out.add(render(java.lang.reflect.Array.get(v, i)));
            }
            return Collections.unmodifiableList(out);
        }
        return v.toString();
    }

    List<Line> lines() {
        store.lock.lock();
        try {
            return List.copyOf(store.lines);
        } finally {
            store.lock.unlock();
        }
    }

    List<String> messages() {
        return lines().stream().map(Line::msg).toList();
    }

    int count(String msg) {
        return (int) lines().stream().filter(l -> l.msg().equals(msg)).count();
    }

    /** One map per line logged with msg, in order. */
    List<Map<String, Object>> fields(String msg) {
        return lines().stream().filter(l -> l.msg().equals(msg)).map(Line::fields).toList();
    }

    /** Blocks until msg has been logged at least n times; AssertionError after timeout. */
    void await(String msg, int n, Duration timeout) {
        long left = timeout.toNanos();
        store.lock.lock();
        try {
            while (store.lines.stream().filter(l -> l.msg().equals(msg)).count() < n) {
                if (left <= 0) {
                    throw new AssertionError("waited " + timeout + " for " + n + " \"" + msg + "\" lines; have "
                            + store.lines.stream().map(Line::msg).toList());
                }
                try {
                    left = store.added.awaitNanos(left);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted", e);
                }
            }
        } finally {
            store.lock.unlock();
        }
    }
}

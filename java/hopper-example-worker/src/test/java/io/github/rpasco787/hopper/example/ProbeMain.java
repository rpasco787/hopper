package io.github.rpasco787.hopper.example;

import java.time.Duration;
import java.util.logging.Logger;

/**
 * The real {@link Main}, plus the two stray outputs a JVM makes by default: a
 * java.util.logging record (grpc-java's logger) and an uncaught exception. MainProcessTest runs
 * it as a child JVM and requires every line it prints to be JSON (plan §6).
 */
final class ProbeMain {
    private ProbeMain() {}

    public static void main(String[] args) {
        Thread.ofPlatform().daemon().name("probe-driver").start(ProbeMain::probe);
        Main.main(args);
    }

    private static void probe() {
        // App installs the uncaught-exception handler right after the JUL bridge. Wait for it,
        // but not forever: without it (a broken App) the probes must still fire, and show.
        long until = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (Thread.getDefaultUncaughtExceptionHandler() == null && System.nanoTime() - until < 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                return;
            }
        }
        Logger.getLogger("io.grpc.Probe").warning("grpc says hello");
        Thread t = Thread.ofPlatform().name("probe-thread").start(() -> {
            throw new IllegalStateException("probe");
        });
        try {
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

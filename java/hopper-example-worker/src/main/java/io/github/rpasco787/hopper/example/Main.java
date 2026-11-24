package io.github.rpasco787.hopper.example;

/**
 * hopper-example-worker: cmd/worker's three queues on the Java worker SDK, the Java side of the
 * Phase 3c conformance runs. Same flags, environment, log lines and exit codes as cmd/worker.
 *
 * <p>SIGTERM and SIGINT start the drain from a shutdown hook (Decision D23). The hook waits for
 * run()'s exit code, flushes, and halts with it: a JVM ended by a signal would otherwise exit
 * 143 after its hooks, where cmd/worker exits 0 after a clean drain. When main's own
 * System.exit runs the hook instead, the code is already there.
 */
public final class Main {
    private Main() {}

    public static void main(String[] args) {
        App app = new App(args, System.getenv(), System.out, System.err);
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().name("hopper-shutdown").unstarted(() -> {
            app.shutdown(); // starts the one deadline
            int code = app.awaitExit(app.shutdownTimeout().plusSeconds(5)); // main logs "worker stopped" first
            System.out.flush();
            System.err.flush();
            Runtime.getRuntime().halt(code);
        }));
        int code = app.run();
        System.exit(code); // runs the hook, which finds the code ready and halts with it
    }
}

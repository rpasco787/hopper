/**
 * Phase 3d's positive control for the pinning count (docs/bench/java.md). A virtual thread
 * parks for 5 ms inside a class initializer, which still pins its carrier on JDK 24 and later:
 * JEP 491 lifted pinning for synchronized, not for class initialization. JFR records the pin
 * only when jdk.VirtualThreadPinned's threshold is under 5 ms, as the benchmarks' is (0 ms);
 * at JFR's default of 20 ms it records nothing.
 */
public class PinControl {
    static final class Slow {
        static {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        static void touch() {}
    }

    public static void main(String[] args) throws InterruptedException {
        Thread t = Thread.ofVirtual().start(Slow::touch);
        t.join();
    }
}

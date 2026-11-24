package io.github.rpasco787.hopper.example;

/**
 * A handler's failure, whose text is exactly cmd/worker's error text. The SDK nacks a thrown
 * exception with its toString(), so this one's toString() is its message alone, without a class
 * name: the job's last error reads the same whichever worker ran it.
 */
final class JobFailure extends Exception {
    private static final long serialVersionUID = 1L;

    JobFailure(String message) {
        super(message);
    }

    @Override
    public String toString() {
        return getMessage();
    }
}

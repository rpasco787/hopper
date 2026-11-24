package io.github.rpasco787.hopper.testing;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * A one-shot release, the port of worker/transport_test.go's {@code gate}.
 * Closing it releases it, so a test that declares it in a try-with-resources
 * block, after the broker it blocks, frees whatever it stranded before the
 * broker's own close() has to cancel it.
 *
 * <p>{@link #await} takes the context to give up on, because a bare wait would
 * ignore transport cancellation and then the harness itself would be the thing
 * that cannot be shut down.
 */
public final class Gate implements AutoCloseable {
    private final CompletableFuture<Void> opened = new CompletableFuture<>();

    /** Opens the gate. Idempotent. */
    public void release() {
        opened.complete(null);
    }

    /**
     * Blocks until the gate opens, or fails with {@code CANCELLED} if {@code ctx}
     * is cancelled first (or the thread is interrupted, which also restores the flag).
     */
    public void await(Context ctx) throws StatusException {
        await(opened, ctx);
    }

    @Override
    public void close() {
        release();
    }

    /** Waits for {@code done} or for {@code ctx} to be cancelled, whichever is first. */
    static void await(CompletableFuture<?> done, Context ctx) throws StatusException {
        CompletableFuture<Void> cancelled = new CompletableFuture<>();
        Context.CancellationListener listener = c -> cancelled.complete(null);
        ctx.addListener(listener, Runnable::run);
        try {
            CompletableFuture.anyOf(done, cancelled).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED.withDescription("interrupted while waiting").withCause(e).asException();
        } catch (ExecutionException e) {
            throw new AssertionError("a gate's futures never fail", e);
        } finally {
            ctx.removeListener(listener);
        }
        if (!done.isDone()) {
            throw Status.CANCELLED
                    .withDescription("the context was cancelled")
                    .withCause(ctx.cancellationCause())
                    .asException();
        }
    }
}

package io.github.rpasco787.hopper.worker;

import java.time.Duration;

/**
 * Receives handler lifecycle events, for metrics (worker/observer.go). It is called synchronously
 * on the delivery's thread, so an implementation must be safe for concurrent use, fast and
 * non-blocking, and must not throw: the SDK does not catch an observer. Time spent in
 * handlerFinished delays the Ack or Nack and uses up lease time.
 */
public interface WorkerObserver {
    /** Called once the handler has been accepted to run; never for a job nacked unstarted. */
    void handlerStarted(String queue);

    /** Called when the handler returns, with how it returned and its time from start to return. */
    void handlerFinished(String queue, HandlerStatus status, Duration elapsed);
}

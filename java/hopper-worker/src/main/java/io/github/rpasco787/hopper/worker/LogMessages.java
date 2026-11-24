package io.github.rpasco787.hopper.worker;

/**
 * The exact messages of the log contract (docs/design/java-worker-sdk.md, Logs): the Go SDK's
 * and cmd/worker's text, which test/accounting, the chaos suite and scripts/bench.sh read. A
 * renamed message makes completed jobs look never started, so these never change alone.
 *
 * <ul>
 *   <li>Example worker: {@link #WORKER_STARTING}, {@link #WORKER_STOPPED}.
 *   <li>Stream: {@link #STREAM_OPENED}, {@link #STREAM_CLOSED}, {@link #STREAM_FAILED_RECONNECTING},
 *       {@link #STREAM_CLOSED_RECONNECTING}, {@link #NOT_REOPENING}.
 *   <li>Shutdown: {@link #SHUTDOWN_STARTED}, {@link #SHUTDOWN_DEADLINE_REACHED}.
 *   <li>Delivery: {@link #JOB_STARTED}, {@link #HANDLER_FINISHED}, {@link #JOB_CONFIRMED},
 *       {@link #NACKING_UNSTARTED}, {@link #LEASE_NOT_CONFIRMED}, {@link #MALFORMED_DELIVERY}.
 *   <li>Losses (accounting's lossMessages): {@link #LEASE_EXPIRED_LOCALLY}, {@link #HEARTBEAT_REJECTED},
 *       {@link #LEASE_EXPIRED_ON_RENEWAL}, {@link #CONFIRMATION_REFUSED}, {@link #UNCONFIRMED_LEASE_RAN_OUT},
 *       {@link #UNCONFIRMED_SHUTTING_DOWN}, {@link #RETURNED_AFTER_DEADLINE}.
 *   <li>Diagnostics: {@link #RETRYING_CONFIRMATION}, {@link #CAPACITY_OVER_RELEASED}.
 * </ul>
 */
public final class LogMessages {
    public static final String WORKER_STARTING = "worker starting";
    public static final String WORKER_STOPPED = "worker stopped";

    public static final String STREAM_OPENED = "lease stream opened";
    public static final String STREAM_CLOSED = "lease stream closed";
    public static final String STREAM_FAILED_RECONNECTING = "lease stream failed; reconnecting";
    public static final String STREAM_CLOSED_RECONNECTING = "lease stream closed; reconnecting";
    public static final String NOT_REOPENING = "not opening another lease stream; the worker is draining";

    public static final String SHUTDOWN_STARTED = "shutdown started";
    public static final String SHUTDOWN_DEADLINE_REACHED = "shutdown deadline reached";

    public static final String JOB_STARTED = "job started";
    public static final String HANDLER_FINISHED = "handler finished";
    public static final String JOB_CONFIRMED = "job confirmed";
    public static final String NACKING_UNSTARTED = "nacking an unstarted job";
    public static final String LEASE_NOT_CONFIRMED = "lease not confirmed; leaving the job to be redelivered";
    public static final String MALFORMED_DELIVERY = "discarding a malformed delivery";

    public static final String LEASE_EXPIRED_LOCALLY = "lease expired locally; cancelling the handler";
    public static final String HEARTBEAT_REJECTED = "heartbeat rejected; cancelling the handler";
    public static final String LEASE_EXPIRED_ON_RENEWAL =
            "lease expired while applying a renewal; cancelling the handler";

    public static final String CONFIRMATION_REFUSED = "job confirmation refused; someone else may have it now";
    public static final String UNCONFIRMED_LEASE_RAN_OUT =
            "job left unconfirmed; its lease ran out and it will be redelivered";
    public static final String UNCONFIRMED_SHUTTING_DOWN = "job left unconfirmed; the worker was shutting down";
    public static final String RETURNED_AFTER_DEADLINE = "handler returned after the shutdown deadline; not confirming";

    public static final String RETRYING_CONFIRMATION = "retrying a confirmation";
    public static final String CAPACITY_OVER_RELEASED = "worker: released more capacity than exists; this is an SDK bug";

    private LogMessages() {}
}

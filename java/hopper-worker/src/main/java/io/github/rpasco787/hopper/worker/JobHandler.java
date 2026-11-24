package io.github.rpasco787.hopper.worker;

/**
 * Runs one job. Returning acks it. Throwing an Exception nacks it with the exception's text,
 * sanitized; throwing an Error nacks it with the stack trace too, and the worker carries on.
 */
@FunctionalInterface
public interface JobHandler {
    void handle(JobContext ctx) throws Exception;
}

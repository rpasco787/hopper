package io.github.rpasco787.hopper.worker;

/** How a handler returned (worker/observer.go HandlerStatus). */
public enum HandlerStatus {
    /** It returned normally; the job is acked. */
    OK,
    /** It threw an Exception; the job is nacked with its text. */
    ERROR,
    /** It threw an Error; the job is nacked with its text and stack, and the worker carries on. */
    PANIC
}

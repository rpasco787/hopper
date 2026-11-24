package io.github.rpasco787.hopper.worker;

/**
 * What was still in flight when the shutdown deadline fired. The counts are separate because
 * they call for different responses: handlers still running means the shutdown budget is too
 * small for the slowest handler, while pending confirmations means jobs that very likely ran
 * will be redelivered. The jobs behind them are not lost: their leases expire and the broker
 * redelivers them under the normal retry policy.
 *
 * @param deliveries every active delivery; the other three add up to it
 * @param handlers handlers still running
 * @param acquiring deliveries still proving ownership before their handler
 * @param confirming deliveries still acking or nacking
 */
public record AbandonedCounts(int deliveries, int handlers, int acquiring, int confirming) {}

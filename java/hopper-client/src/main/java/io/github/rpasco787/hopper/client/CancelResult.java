package io.github.rpasco787.hopper.client;

import java.util.UUID;

/**
 * What came back from a cancel. Cancel is idempotent at the broker, so a retried
 * request is not reported as a failure.
 *
 * @param alreadyCancelled true when the job was cancelled before this call
 */
public record CancelResult(UUID id, JobState state, boolean alreadyCancelled) {}

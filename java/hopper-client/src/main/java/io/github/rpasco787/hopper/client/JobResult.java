package io.github.rpasco787.hopper.client;

import java.util.UUID;

/**
 * What came back from an enqueue or a read.
 *
 * @param deduplicated true when an existing job with the same queue and idempotency key was
 *     returned and nothing was inserted (docs/semantics.md §8)
 */
public record JobResult(UUID id, JobState state, boolean deduplicated) {}

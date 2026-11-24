package io.github.rpasco787.hopper.client;

/** Where a job is in its life (docs/semantics.md §2). Proto types stay out of calling code. */
public enum JobState {
    SCHEDULED,
    AVAILABLE,
    LEASED,
    COMPLETED,
    CANCELLED,
    DEAD;

    /**
     * @throws IllegalStateException for {@code JOB_STATE_UNSPECIFIED}, proto3's zero value, which
     *     is never stored, and for a state this client does not know (a newer broker)
     */
    static JobState fromProto(io.github.rpasco787.hopper.proto.v1.JobState state) {
        return switch (state) {
            case JOB_STATE_SCHEDULED -> SCHEDULED;
            case JOB_STATE_AVAILABLE -> AVAILABLE;
            case JOB_STATE_LEASED -> LEASED;
            case JOB_STATE_COMPLETED -> COMPLETED;
            case JOB_STATE_CANCELLED -> CANCELLED;
            case JOB_STATE_DEAD -> DEAD;
            case JOB_STATE_UNSPECIFIED, UNRECOGNIZED ->
                throw new IllegalStateException("hopper: broker returned an unusable job state " + state);
        };
    }
}

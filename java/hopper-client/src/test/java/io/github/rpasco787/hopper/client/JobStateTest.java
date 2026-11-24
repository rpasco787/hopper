package io.github.rpasco787.hopper.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class JobStateTest {
    @Test
    void everyWireStateMapsOneToOne() {
        int checked = 0;
        for (io.github.rpasco787.hopper.proto.v1.JobState wire :
                io.github.rpasco787.hopper.proto.v1.JobState.values()) {
            if (wire == io.github.rpasco787.hopper.proto.v1.JobState.JOB_STATE_UNSPECIFIED
                    || wire == io.github.rpasco787.hopper.proto.v1.JobState.UNRECOGNIZED) {
                continue;
            }
            JobState expected = JobState.valueOf(wire.name().substring("JOB_STATE_".length()));
            assertEquals(expected, JobState.fromProto(wire), wire.name());
            checked++;
        }
        assertEquals(JobState.values().length, checked, "every client state is reachable from the wire");
    }

    /** proto3's zero value is never stored (hopper.proto), so seeing it is the broker's bug. */
    @Test
    void unspecifiedIsABrokerBug() {
        assertThrows(IllegalStateException.class,
                () -> JobState.fromProto(io.github.rpasco787.hopper.proto.v1.JobState.JOB_STATE_UNSPECIFIED));
    }

    @Test
    void aStateThisClientDoesNotKnowIsAlsoAnError() {
        assertThrows(IllegalStateException.class,
                () -> JobState.fromProto(io.github.rpasco787.hopper.proto.v1.JobState.UNRECOGNIZED));
    }
}

package io.github.rpasco787.hopper.proto.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** The generated stubs keep the wire contract of proto/hopper/v1 under a Java package (Decision D5). */
class GeneratedStubsTest {
    @Test
    void theStubsAreInTheJavaPackageAndKeepTheProtoPackage() {
        assertEquals("io.github.rpasco787.hopper.proto.v1", Job.class.getPackageName());
        assertEquals("hopper.v1", HopperProto.getDescriptor().getPackage());
        assertEquals("hopper.v1.HopperService", HopperServiceGrpc.SERVICE_NAME);
    }

    @Test
    void aLeaseRequestRoundTrips() throws Exception {
        LeaseJobsRequest credit = LeaseJobsRequest.newBuilder().setCredit(3).build();
        LeaseJobsRequest back = LeaseJobsRequest.parseFrom(credit.toByteArray());
        assertEquals(LeaseJobsRequest.MsgCase.CREDIT, back.getMsgCase());
        assertEquals(3, back.getCredit());
    }

    @Test
    void anOptionalFieldKnowsWhenItIsAbsent() {
        assertFalse(Job.newBuilder().setId("x").build().hasLeaseId());
        assertTrue(Job.newBuilder().setLeaseId("").build().hasLeaseId());
    }
}

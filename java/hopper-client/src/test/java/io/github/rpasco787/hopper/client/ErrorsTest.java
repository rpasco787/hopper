package io.github.rpasco787.hopper.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.EnumSet;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The table in docs/design/java-producer-client.md ("Errors"), written out so
 * that this test is the contract: which exception each gRPC code becomes, and
 * whether a mutating call that failed with it may still have taken effect.
 */
class ErrorsTest {
    private record Expected(Class<? extends HopperException> type, boolean outcomeUnknown) {}

    private static final Map<Status.Code, Expected> TABLE = Map.ofEntries(
            // Broker validation writes nothing (docs/semantics.md §8).
            Map.entry(Status.Code.INVALID_ARGUMENT, new Expected(InvalidRequestException.class, false)),
            Map.entry(Status.Code.NOT_FOUND, new Expected(JobNotFoundException.class, false)),
            Map.entry(Status.Code.FAILED_PRECONDITION, new Expected(JobTerminalException.class, false)),
            // A response over 4 MiB can follow committed writes (§8).
            Map.entry(Status.Code.RESOURCE_EXHAUSTED, new Expected(ResourceExhaustedException.class, true)),
            // The call may have taken effect (§5).
            Map.entry(Status.Code.UNAVAILABLE, new Expected(BrokerUnavailableException.class, true)),
            Map.entry(Status.Code.DEADLINE_EXCEEDED, new Expected(DeadlineExceededException.class, true)),
            // Refused before the broker looked at the request.
            Map.entry(Status.Code.UNIMPLEMENTED, new Expected(HopperException.class, false)),
            Map.entry(Status.Code.UNAUTHENTICATED, new Expected(HopperException.class, false)),
            Map.entry(Status.Code.PERMISSION_DENIED, new Expected(HopperException.class, false)),
            // Anything else is unknown.
            Map.entry(Status.Code.INTERNAL, new Expected(HopperException.class, true)),
            Map.entry(Status.Code.CANCELLED, new Expected(HopperException.class, true)),
            Map.entry(Status.Code.UNKNOWN, new Expected(HopperException.class, true)),
            Map.entry(Status.Code.ABORTED, new Expected(HopperException.class, true)),
            Map.entry(Status.Code.ALREADY_EXISTS, new Expected(HopperException.class, true)),
            Map.entry(Status.Code.OUT_OF_RANGE, new Expected(HopperException.class, true)),
            Map.entry(Status.Code.DATA_LOSS, new Expected(HopperException.class, true)));

    @Test
    void theTableCoversEveryCodeButOk() {
        EnumSet<Status.Code> all = EnumSet.allOf(Status.Code.class);
        all.remove(Status.Code.OK);
        assertEquals(all, TABLE.keySet(), "a new gRPC code needs a row in the table, and in the design note");
    }

    @Test
    void eachCodeMapsToItsExceptionAndSaysWhetherTheOutcomeIsKnown() {
        for (Map.Entry<Status.Code, Expected> row : TABLE.entrySet()) {
            Status.Code code = row.getKey();
            StatusRuntimeException cause = Status.fromCode(code).withDescription("d").asRuntimeException();

            HopperException written = Errors.wrap("enqueue", cause, true);
            assertEquals(row.getValue().type(), written.getClass(), code.name());
            assertEquals(code, written.code(), code.name());
            assertEquals(row.getValue().outcomeUnknown(), written.outcomeUnknown(), code.name());

            HopperException read = Errors.wrap("get job", cause, false);
            assertEquals(row.getValue().type(), read.getClass(), code.name());
            assertFalse(read.outcomeUnknown(), "a read never leaves an unknown outcome: " + code.name());
        }
    }

    /** Port of client_test.go:89 TestWrapPreservesStatus. */
    @Test
    void theStatusIsKeptAsTheCauseAndNamedInTheMessage() {
        StatusRuntimeException gone = Status.NOT_FOUND.withDescription("gone").asRuntimeException();
        HopperException notFound = Errors.wrap("cancel", gone, true);
        assertEquals(JobNotFoundException.class, notFound.getClass());
        assertEquals("hopper: cancel: NOT_FOUND: gone", notFound.getMessage());
        assertSame(gone, notFound.getCause());

        StatusRuntimeException done = Status.FAILED_PRECONDITION.withDescription("completed").asRuntimeException();
        HopperException terminal = Errors.wrap("cancel", done, true);
        assertEquals(JobTerminalException.class, terminal.getClass());
        assertEquals("hopper: cancel: FAILED_PRECONDITION: completed", terminal.getMessage());
        assertSame(done, terminal.getCause());
    }

    @Test
    void aMissingDescriptionLeavesOutItsSeparator() {
        HopperException e = Errors.wrap("get job", Status.UNAVAILABLE.asRuntimeException(), false);
        assertEquals("hopper: get job: UNAVAILABLE", e.getMessage());
    }

    /** Port of TestWrapPassesOtherCodesThrough. */
    @Test
    void otherCodesPassThroughAsAPlainHopperException() {
        HopperException e = Errors.wrap("enqueue", Status.INTERNAL.withDescription("boom").asRuntimeException(), true);
        assertEquals(HopperException.class, e.getClass());
        assertEquals(Status.Code.INTERNAL, e.code());
    }

    /**
     * A blocking call whose thread is interrupted is cancelled, and the cause is
     * the InterruptedException. The caller's interrupt must stay visible to it.
     */
    @Test
    void aCancellationCausedByInterruptionRestoresTheFlag() {
        assertFalse(Thread.currentThread().isInterrupted(), "the test starts with a clear flag");

        Errors.wrap("get job", Status.CANCELLED.withCause(new InterruptedException()).asRuntimeException(), false);

        assertTrue(Thread.interrupted(), "wrap restores the interrupt flag (and the test clears it again)");
    }

    @Test
    void aCancellationWithoutInterruptionLeavesTheFlagAlone() {
        Errors.wrap("get job", Status.CANCELLED.asRuntimeException(), false);
        assertFalse(Thread.interrupted());
    }
}

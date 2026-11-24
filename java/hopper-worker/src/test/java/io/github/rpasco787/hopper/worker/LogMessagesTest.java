package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The messages test/accounting and the chaos suite read, written out here a second time, so a
 * rename on either side fails a test (plan §6).
 */
class LogMessagesTest {
    @Test
    void theContractMessagesAreExactlyTheGoOnes() {
        assertEquals("worker starting", LogMessages.WORKER_STARTING);
        assertEquals("worker stopped", LogMessages.WORKER_STOPPED);
        assertEquals("lease stream opened", LogMessages.STREAM_OPENED);
        assertEquals("lease stream closed", LogMessages.STREAM_CLOSED);
        assertEquals("lease stream failed; reconnecting", LogMessages.STREAM_FAILED_RECONNECTING);
        assertEquals("lease stream closed; reconnecting", LogMessages.STREAM_CLOSED_RECONNECTING);
        assertEquals("not opening another lease stream; the worker is draining", LogMessages.NOT_REOPENING);
        assertEquals("shutdown started", LogMessages.SHUTDOWN_STARTED);
        assertEquals("shutdown deadline reached", LogMessages.SHUTDOWN_DEADLINE_REACHED);
        assertEquals("job started", LogMessages.JOB_STARTED);
        assertEquals("handler finished", LogMessages.HANDLER_FINISHED);
        assertEquals("job confirmed", LogMessages.JOB_CONFIRMED);
        assertEquals("nacking an unstarted job", LogMessages.NACKING_UNSTARTED);
        assertEquals("lease not confirmed; leaving the job to be redelivered", LogMessages.LEASE_NOT_CONFIRMED);
        assertEquals("discarding a malformed delivery", LogMessages.MALFORMED_DELIVERY);
        assertEquals("lease expired locally; cancelling the handler", LogMessages.LEASE_EXPIRED_LOCALLY);
        assertEquals("heartbeat rejected; cancelling the handler", LogMessages.HEARTBEAT_REJECTED);
        assertEquals("lease expired while applying a renewal; cancelling the handler",
                LogMessages.LEASE_EXPIRED_ON_RENEWAL);
        assertEquals("job confirmation refused; someone else may have it now", LogMessages.CONFIRMATION_REFUSED);
        assertEquals("job left unconfirmed; its lease ran out and it will be redelivered",
                LogMessages.UNCONFIRMED_LEASE_RAN_OUT);
        assertEquals("job left unconfirmed; the worker was shutting down", LogMessages.UNCONFIRMED_SHUTTING_DOWN);
        assertEquals("handler returned after the shutdown deadline; not confirming",
                LogMessages.RETURNED_AFTER_DEADLINE);
        assertEquals("retrying a confirmation", LogMessages.RETRYING_CONFIRMATION);
        assertEquals("worker: released more capacity than exists; this is an SDK bug",
                LogMessages.CAPACITY_OVER_RELEASED);
        assertEquals(24, constants().size(), "exactly the 24 of §6's table, no more");
    }

    /** test/accounting/accounting.go: the eight at 24-31, then the seven lossMessages at 40-46. */
    @Test
    void accountingReadsOnlyMessagesWeEmit() {
        List<String> accounting = List.of(
                "worker starting",
                "job started",
                "handler finished",
                "job confirmed",
                "nacking an unstarted job",
                "shutdown started",
                "shutdown deadline reached",
                "worker stopped",
                "lease expired locally; cancelling the handler",
                "heartbeat rejected; cancelling the handler",
                "lease expired while applying a renewal; cancelling the handler",
                "job confirmation refused; someone else may have it now",
                "job left unconfirmed; its lease ran out and it will be redelivered",
                "job left unconfirmed; the worker was shutting down",
                "handler returned after the shutdown deadline; not confirming");
        assertEquals(15, accounting.size());
        Set<String> ours = constants();
        for (String msg : accounting) {
            assertTrue(ours.contains(msg), "accounting reads \"" + msg + "\", which no constant emits");
        }
    }

    private static Set<String> constants() {
        Set<String> values = new HashSet<>();
        for (Field f : LogMessages.class.getDeclaredFields()) {
            int m = f.getModifiers();
            if (Modifier.isPublic(m) && Modifier.isStatic(m) && Modifier.isFinal(m) && f.getType() == String.class) {
                try {
                    values.add((String) f.get(null));
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
            }
        }
        return values;
    }
}

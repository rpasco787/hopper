package io.github.rpasco787.hopper.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest;
import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import io.github.rpasco787.hopper.testing.StreamControl;
import io.grpc.Context;
import io.grpc.Status;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * The start rule's Stamp, through a real session (Java-only): the instant is read before the
 * transport has the credit, and the units are counted before onNext returns. On the manual
 * clock, with a StreamControl interceptor on the fake's channel.
 */
class CreditStampTest {
    private static boolean isCredit(Object message) {
        return message instanceof LeaseJobsRequest r && r.getMsgCase() == LeaseJobsRequest.MsgCase.CREDIT;
    }

    /** The start rule's Stamp: the instant is read before the transport has the credit. */
    @Test
    void theSubscriptionsCreditIsStampedBeforeItIsHandedToTheTransport() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        long beforeSend = time.nanoTime();
        var control = new StreamControl();
        control.onSend((message, forward) -> {
            time.jump(Duration.ofMillis(1)); // the transport takes a millisecond to accept it
            forward.run();
        });
        try (var fake = FakeBroker.start()) {
            var spy = new Spy(false);
            var worker = Harness.newTestWorker(
                    Harness.builder(fake.channel(control)).timeSource(time), spy::run, "q");
            try (var run = Harness.runWorker(worker)) {
                FakeStream s = fake.awaitStream(Harness.BARRIER);
                s.deliver(Fixture.of("q").proto(), Harness.BARRIER);
                Delivery d = spy.await(Harness.BARRIER);
                assertTrue(d.creditSent());
                assertEquals(beforeSend, d.creditSentAt(), "stamped before onNext, not after it");
                assertNull(run.stopAndWait());
            }
        }
    }

    /** The same for a credit message the writer sends after a delivery returns its token. */
    @Test
    void aReturnedCreditIsStampedBeforeItIsHandedToTheTransport() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        var control = new StreamControl();
        var creditSendAt = new CompletableFuture<Long>();
        control.onSend((message, forward) -> {
            if (isCredit(message)) {
                creditSendAt.complete(time.nanoTime());
            }
            time.jump(Duration.ofMillis(1));
            forward.run();
        });
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            var worker = Harness.newTestWorker(
                    Harness.builder(fake.channel(control)).concurrency(1).timeSource(time), spy::run, "q");
            try (var run = Harness.runWorker(worker)) {
                FakeStream s = fake.awaitStream(Harness.BARRIER);
                s.deliver(Fixture.of("q").proto(), Harness.BARRIER);
                spy.await(Harness.BARRIER); // it returns at once, so its token comes back as credit
                s.deliver(Fixture.of("q").proto(), Harness.BARRIER);
                Delivery second = spy.await(Harness.BARRIER);
                assertTrue(second.creditSent());
                assertEquals(creditSendAt.get(10, TimeUnit.SECONDS), second.creditSentAt(),
                        "matched to the returned credit's pre-send reading");
                assertNull(run.stopAndWait());
            }
        }
    }

    /**
     * The accounting exists before onNext returns: a delivery that answers a credit message
     * while its send is still blocked is matched to it, not refused as an over-delivery.
     */
    @Test
    void aDeliveryThatArrivesBeforeItsSendReturnsIsAccounted() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        var control = new StreamControl();
        var held = new AtomicBoolean();
        var forwarded = new CountDownLatch(1);
        var returned = new CountDownLatch(1);
        try (var fake = FakeBroker.start(); var spy = new Spy(false)) {
            control.onSend((message, forward) -> {
                forward.run();
                if (!isCredit(message) || held.getAndSet(true)) {
                    return;
                }
                forwarded.countDown();
                // Still inside the send: wait until the broker has answered this credit and
                // the delivery has reached the runner.
                long deadline = System.nanoTime() + Harness.BARRIER.toNanos();
                while (spy.count() < 1 && !Context.current().isCancelled()) {
                    if (System.nanoTime() > deadline) {
                        throw Status.INTERNAL.withDescription("the delivery never arrived during the send").asException();
                    }
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                returned.countDown();
            });
            var worker = Harness.newTestWorker(
                    Harness.builder(fake.channel(control)).concurrency(1).timeSource(time), spy::run, "q");
            try (var run = Harness.runWorker(worker)) {
                FakeStream s = fake.awaitStream(Harness.BARRIER);
                s.deliver(Fixture.of("q").proto(), Harness.BARRIER);
                spy.await(Harness.BARRIER);
                assertTrue(forwarded.await(10, TimeUnit.SECONDS), "the returned token was never sent");
                s.deliver(Fixture.of("q").proto(), Harness.BARRIER);
                assertTrue(returned.await(10, TimeUnit.SECONDS), "the delivery was not accounted while its send was blocked");
                Delivery second = spy.await(Harness.BARRIER);
                assertTrue(second.creditSent());
                assertEquals(1, fake.started("LeaseJobs"), "no over-delivery: the stream stayed open");
                assertFalse(run.isDone());
                assertNull(run.stopAndWait());
            }

        }
    }
}

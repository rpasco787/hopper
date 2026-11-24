package io.github.rpasco787.hopper.worker;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rpasco787.hopper.testing.FakeBroker;
import io.github.rpasco787.hopper.testing.FakeStream;
import io.github.rpasco787.hopper.testing.Fixture;
import io.github.rpasco787.hopper.testing.Gate;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

/**
 * A pause (a long GC, a SIGSTOP, a frozen VM) on the manual clock: jump moves the clock and runs
 * no timer. What the SDK must not do is act on a lease it no longer holds once it wakes
 * (docs/design/java-worker-sdk.md, Pauses). Java-only.
 */
class PauseTest {
    private static final Duration BARRIER = Harness.BARRIER;

    @Test
    void aResultWhoseDeadlinePassedDuringAPauseIsNotConfirmed() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        var log = new CapturingLog();
        try (var fake = FakeBroker.start()) {
            var started = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).timeSource(time).log(log), "q", ctx -> {
                started.countDown();
                release.await();
            })) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").grant(Duration.ofSeconds(3)).proto(), BARRIER); // fresh: deadline at 2.7 s
                assertTrue(started.await(10, SECONDS));
                time.jump(Duration.ofSeconds(4)); // the pause: no timer fires
                release.countDown();
                log.await(LogMessages.UNCONFIRMED_LEASE_RAN_OUT, 1, BARRIER);
                s.awaitCredit(1, BARRIER);
                assertNull(run.stopAndWait());
            }
            assertEquals(0, fake.started("Ack"), "the clock is read immediately before the send");
            assertEquals(0, fake.started("Nack"));
        }
    }

    @Test
    void aRenewalThatReturnsAfterAPauseCannotReviveTheLease() throws Exception {
        var time = new ManualTimeSource(1_000_000_000L);
        var log = new CapturingLog();
        try (var fake = FakeBroker.start(); var hold = new Gate()) {
            var entered = new CountDownLatch(1);
            fake.onHeartbeat((req, ctx) -> {
                entered.countDown();
                hold.await(ctx); // answers OK once released
            });
            var stopped = new CompletableFuture<StopReason>();
            try (var run = Harness.runHandler(fake, Harness.builder(fake.channel()).timeSource(time).log(log), "q", ctx -> {
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    stopped.complete(ctx.stopReason().orElseThrow());
                    throw e;
                }
            })) {
                FakeStream s = fake.awaitStream(BARRIER);
                s.deliver(Fixture.of("q").grant(Duration.ofSeconds(3)).proto(), BARRIER); // deadline at 2.7 s
                time.awaitTimers(2, BARRIER);
                time.advance(Duration.ofSeconds(1)); // the first renewal, which the broker holds
                assertTrue(entered.await(10, SECONDS));
                time.jump(Duration.ofSeconds(4)); // the pause, past the deadline
                hold.release(); // the renewal returns OK, too late

                assertEquals(StopReason.LEASE_EXPIRED, stopped.get(10, SECONDS));
                log.await(LogMessages.LEASE_EXPIRED_ON_RENEWAL, 1, BARRIER);
                s.awaitCredit(1, BARRIER);
                assertNull(run.stopAndWait());
            }
            assertEquals(0, fake.started("Ack"), "a lease the renewal could not revive is not confirmed");
            assertEquals(0, fake.started("Nack"));
        }
    }
}

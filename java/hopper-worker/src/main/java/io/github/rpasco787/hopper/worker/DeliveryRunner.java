package io.github.rpasco787.hopper.worker;

import io.grpc.Channel;

/**
 * Owns a delivery from dispatch to completion, on the delivery's own thread (worker/stream.go
 * deliveryRunner). The default is the real execution; tests substitute a controlled runner so
 * dispatch and capacity can be driven without staging a whole execution.
 */
@FunctionalInterface
interface DeliveryRunner {
    void run(Lifecycle lc, Work work, Channel channel, Delivery d);
}

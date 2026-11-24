package io.github.rpasco787.hopper.testing;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A client-side seam for the transport failures that matter to the SDKs, the
 * port of worker/transport_test.go's {@code streamControl}: a stream that will
 * not open, a Send that fails, a Send that blocks, a CloseSend that blocks, and
 * a delivery that arrives before the Send that paid for it has returned.
 *
 * <p>It is an interceptor rather than an attempt to fill real HTTP/2 flow-control
 * windows. Filling a window works only sometimes, at sizes that change with the
 * library; a hook happens exactly when the test says it does.
 *
 * <p>Each hook runs on the caller's thread and decides whether to forward. A hook
 * that blocks should wait on a {@link Gate} with {@code Context.current()}, so a
 * cancelled call can still get out. A hook that throws a {@link StatusException}
 * (or a {@link StatusRuntimeException}) fails the call with that status: the
 * response observer sees it, as it sees a real transport failure. A hook that
 * returns without forwarding drops the message (for send and half-close); for an
 * open, which would otherwise hang forever, that is reported as {@code INTERNAL}.
 *
 * <p>The hooks are settable at any time, so a test can start failing sends after
 * the subscription has gone through.
 */
public final class StreamControl implements ClientInterceptor {
    /** Wraps {@code start}: the stream is opened when {@code forward} runs. */
    @FunctionalInterface
    public interface OpenHook {
        void apply(Runnable forward) throws StatusException;
    }

    /** Wraps {@code sendMessage}: the message goes out when {@code forward} runs. */
    @FunctionalInterface
    public interface SendHook {
        void apply(Object message, Runnable forward) throws StatusException;
    }

    /** Wraps {@code halfClose}: the half-close goes out when {@code forward} runs. */
    @FunctionalInterface
    public interface HalfCloseHook {
        void apply(Runnable forward) throws StatusException;
    }

    private volatile OpenHook onOpen;
    private volatile SendHook onSend;
    private volatile HalfCloseHook onHalfClose;

    public void onOpen(OpenHook hook) {
        this.onOpen = hook;
    }

    public void onSend(SendHook hook) {
        this.onSend = hook;
    }

    public void onHalfClose(HalfCloseHook hook) {
        this.onHalfClose = hook;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new Controlled<>(next.newCall(method, callOptions), callOptions.getExecutor());
    }

    private final class Controlled<ReqT, RespT> extends ForwardingClientCall<ReqT, RespT> {
        private final ClientCall<ReqT, RespT> delegate;
        private final Executor callbacks;
        /** Set when a hook failed the call; replaces whatever status the transport then reports. */
        private volatile Status forced;
        private volatile boolean notStarted;

        Controlled(ClientCall<ReqT, RespT> delegate, Executor callbacks) {
            this.delegate = delegate;
            this.callbacks = callbacks;
        }

        @Override
        protected ClientCall<ReqT, RespT> delegate() {
            return delegate;
        }

        @Override
        public void start(Listener<RespT> listener, Metadata headers) {
            Listener<RespT> wrapped = new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener) {
                @Override
                public void onClose(Status status, Metadata trailers) {
                    Status override = forced;
                    super.onClose(override != null ? override : status, trailers);
                }
            };
            OpenHook hook = onOpen;
            if (hook == null) {
                delegate.start(wrapped, headers);
                return;
            }
            AtomicBoolean forwarded = new AtomicBoolean();
            try {
                hook.apply(() -> {
                    if (forwarded.compareAndSet(false, true)) {
                        delegate.start(wrapped, headers);
                    }
                });
            } catch (StatusException e) {
                failOpen(listener, e.getStatus());
                return;
            } catch (StatusRuntimeException e) {
                failOpen(listener, e.getStatus());
                return;
            }
            if (!forwarded.get()) {
                failOpen(listener, Status.INTERNAL.withDescription("the onOpen hook returned without forwarding"));
            }
        }

        /** The call never reached the transport, so it is closed from here and its other methods do nothing. */
        private void failOpen(Listener<RespT> listener, Status status) {
            notStarted = true;
            Runnable close = () -> listener.onClose(status, new Metadata());
            if (callbacks != null) {
                callbacks.execute(close);
            } else {
                Thread.startVirtualThread(close);
            }
        }

        @Override
        public void request(int numMessages) {
            if (!notStarted) {
                delegate.request(numMessages);
            }
        }

        @Override
        public void cancel(String message, Throwable cause) {
            if (!notStarted) {
                delegate.cancel(message, cause);
            }
        }

        @Override
        public boolean isReady() {
            return !notStarted && delegate.isReady();
        }

        @Override
        public void sendMessage(ReqT message) {
            if (notStarted) {
                return;
            }
            SendHook hook = onSend;
            if (hook == null) {
                delegate.sendMessage(message);
                return;
            }
            try {
                hook.apply(message, () -> delegate.sendMessage(message));
            } catch (StatusException e) {
                fail(e.getStatus());
            } catch (StatusRuntimeException e) {
                fail(e.getStatus());
            }
        }

        @Override
        public void halfClose() {
            if (notStarted) {
                return;
            }
            HalfCloseHook hook = onHalfClose;
            if (hook == null) {
                delegate.halfClose();
                return;
            }
            try {
                hook.apply(delegate::halfClose);
            } catch (StatusException e) {
                fail(e.getStatus());
            } catch (StatusRuntimeException e) {
                fail(e.getStatus());
            }
        }

        /** Cancels the call so that its observer sees {@code status}, not CANCELLED. */
        private void fail(Status status) {
            forced = status;
            delegate.cancel(status.getDescription(), status.getCause());
        }
    }
}

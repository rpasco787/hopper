package io.github.rpasco787.hopper.proto.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * HopperService is the whole API. Producers call Enqueue/EnqueueBatch/Cancel,
 * operators call GetJob/ListJobs, and workers hold a LeaseJobs stream and call
 * Ack/Nack/Heartbeat against it.
 * </pre>
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class HopperServiceGrpc {

  private HopperServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "hopper.v1.HopperService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.EnqueueRequest,
      io.github.rpasco787.hopper.proto.v1.EnqueueResponse> getEnqueueMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Enqueue",
      requestType = io.github.rpasco787.hopper.proto.v1.EnqueueRequest.class,
      responseType = io.github.rpasco787.hopper.proto.v1.EnqueueResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.EnqueueRequest,
      io.github.rpasco787.hopper.proto.v1.EnqueueResponse> getEnqueueMethod() {
    io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.EnqueueRequest, io.github.rpasco787.hopper.proto.v1.EnqueueResponse> getEnqueueMethod;
    if ((getEnqueueMethod = HopperServiceGrpc.getEnqueueMethod) == null) {
      synchronized (HopperServiceGrpc.class) {
        if ((getEnqueueMethod = HopperServiceGrpc.getEnqueueMethod) == null) {
          HopperServiceGrpc.getEnqueueMethod = getEnqueueMethod =
              io.grpc.MethodDescriptor.<io.github.rpasco787.hopper.proto.v1.EnqueueRequest, io.github.rpasco787.hopper.proto.v1.EnqueueResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Enqueue"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.EnqueueRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.EnqueueResponse.getDefaultInstance()))
              .setSchemaDescriptor(new HopperServiceMethodDescriptorSupplier("Enqueue"))
              .build();
        }
      }
    }
    return getEnqueueMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest,
      io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse> getEnqueueBatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "EnqueueBatch",
      requestType = io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest.class,
      responseType = io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest,
      io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse> getEnqueueBatchMethod() {
    io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest, io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse> getEnqueueBatchMethod;
    if ((getEnqueueBatchMethod = HopperServiceGrpc.getEnqueueBatchMethod) == null) {
      synchronized (HopperServiceGrpc.class) {
        if ((getEnqueueBatchMethod = HopperServiceGrpc.getEnqueueBatchMethod) == null) {
          HopperServiceGrpc.getEnqueueBatchMethod = getEnqueueBatchMethod =
              io.grpc.MethodDescriptor.<io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest, io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "EnqueueBatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse.getDefaultInstance()))
              .setSchemaDescriptor(new HopperServiceMethodDescriptorSupplier("EnqueueBatch"))
              .build();
        }
      }
    }
    return getEnqueueBatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.CancelRequest,
      io.github.rpasco787.hopper.proto.v1.CancelResponse> getCancelMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Cancel",
      requestType = io.github.rpasco787.hopper.proto.v1.CancelRequest.class,
      responseType = io.github.rpasco787.hopper.proto.v1.CancelResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.CancelRequest,
      io.github.rpasco787.hopper.proto.v1.CancelResponse> getCancelMethod() {
    io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.CancelRequest, io.github.rpasco787.hopper.proto.v1.CancelResponse> getCancelMethod;
    if ((getCancelMethod = HopperServiceGrpc.getCancelMethod) == null) {
      synchronized (HopperServiceGrpc.class) {
        if ((getCancelMethod = HopperServiceGrpc.getCancelMethod) == null) {
          HopperServiceGrpc.getCancelMethod = getCancelMethod =
              io.grpc.MethodDescriptor.<io.github.rpasco787.hopper.proto.v1.CancelRequest, io.github.rpasco787.hopper.proto.v1.CancelResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Cancel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.CancelRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.CancelResponse.getDefaultInstance()))
              .setSchemaDescriptor(new HopperServiceMethodDescriptorSupplier("Cancel"))
              .build();
        }
      }
    }
    return getCancelMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.GetJobRequest,
      io.github.rpasco787.hopper.proto.v1.GetJobResponse> getGetJobMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetJob",
      requestType = io.github.rpasco787.hopper.proto.v1.GetJobRequest.class,
      responseType = io.github.rpasco787.hopper.proto.v1.GetJobResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.GetJobRequest,
      io.github.rpasco787.hopper.proto.v1.GetJobResponse> getGetJobMethod() {
    io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.GetJobRequest, io.github.rpasco787.hopper.proto.v1.GetJobResponse> getGetJobMethod;
    if ((getGetJobMethod = HopperServiceGrpc.getGetJobMethod) == null) {
      synchronized (HopperServiceGrpc.class) {
        if ((getGetJobMethod = HopperServiceGrpc.getGetJobMethod) == null) {
          HopperServiceGrpc.getGetJobMethod = getGetJobMethod =
              io.grpc.MethodDescriptor.<io.github.rpasco787.hopper.proto.v1.GetJobRequest, io.github.rpasco787.hopper.proto.v1.GetJobResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetJob"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.GetJobRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.GetJobResponse.getDefaultInstance()))
              .setSchemaDescriptor(new HopperServiceMethodDescriptorSupplier("GetJob"))
              .build();
        }
      }
    }
    return getGetJobMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.ListJobsRequest,
      io.github.rpasco787.hopper.proto.v1.ListJobsResponse> getListJobsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListJobs",
      requestType = io.github.rpasco787.hopper.proto.v1.ListJobsRequest.class,
      responseType = io.github.rpasco787.hopper.proto.v1.ListJobsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.ListJobsRequest,
      io.github.rpasco787.hopper.proto.v1.ListJobsResponse> getListJobsMethod() {
    io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.ListJobsRequest, io.github.rpasco787.hopper.proto.v1.ListJobsResponse> getListJobsMethod;
    if ((getListJobsMethod = HopperServiceGrpc.getListJobsMethod) == null) {
      synchronized (HopperServiceGrpc.class) {
        if ((getListJobsMethod = HopperServiceGrpc.getListJobsMethod) == null) {
          HopperServiceGrpc.getListJobsMethod = getListJobsMethod =
              io.grpc.MethodDescriptor.<io.github.rpasco787.hopper.proto.v1.ListJobsRequest, io.github.rpasco787.hopper.proto.v1.ListJobsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListJobs"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.ListJobsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.ListJobsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new HopperServiceMethodDescriptorSupplier("ListJobs"))
              .build();
        }
      }
    }
    return getListJobsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest,
      io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse> getLeaseJobsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "LeaseJobs",
      requestType = io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest.class,
      responseType = io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest,
      io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse> getLeaseJobsMethod() {
    io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest, io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse> getLeaseJobsMethod;
    if ((getLeaseJobsMethod = HopperServiceGrpc.getLeaseJobsMethod) == null) {
      synchronized (HopperServiceGrpc.class) {
        if ((getLeaseJobsMethod = HopperServiceGrpc.getLeaseJobsMethod) == null) {
          HopperServiceGrpc.getLeaseJobsMethod = getLeaseJobsMethod =
              io.grpc.MethodDescriptor.<io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest, io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "LeaseJobs"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new HopperServiceMethodDescriptorSupplier("LeaseJobs"))
              .build();
        }
      }
    }
    return getLeaseJobsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.AckRequest,
      io.github.rpasco787.hopper.proto.v1.AckResponse> getAckMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Ack",
      requestType = io.github.rpasco787.hopper.proto.v1.AckRequest.class,
      responseType = io.github.rpasco787.hopper.proto.v1.AckResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.AckRequest,
      io.github.rpasco787.hopper.proto.v1.AckResponse> getAckMethod() {
    io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.AckRequest, io.github.rpasco787.hopper.proto.v1.AckResponse> getAckMethod;
    if ((getAckMethod = HopperServiceGrpc.getAckMethod) == null) {
      synchronized (HopperServiceGrpc.class) {
        if ((getAckMethod = HopperServiceGrpc.getAckMethod) == null) {
          HopperServiceGrpc.getAckMethod = getAckMethod =
              io.grpc.MethodDescriptor.<io.github.rpasco787.hopper.proto.v1.AckRequest, io.github.rpasco787.hopper.proto.v1.AckResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Ack"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.AckRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.AckResponse.getDefaultInstance()))
              .setSchemaDescriptor(new HopperServiceMethodDescriptorSupplier("Ack"))
              .build();
        }
      }
    }
    return getAckMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.NackRequest,
      io.github.rpasco787.hopper.proto.v1.NackResponse> getNackMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Nack",
      requestType = io.github.rpasco787.hopper.proto.v1.NackRequest.class,
      responseType = io.github.rpasco787.hopper.proto.v1.NackResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.NackRequest,
      io.github.rpasco787.hopper.proto.v1.NackResponse> getNackMethod() {
    io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.NackRequest, io.github.rpasco787.hopper.proto.v1.NackResponse> getNackMethod;
    if ((getNackMethod = HopperServiceGrpc.getNackMethod) == null) {
      synchronized (HopperServiceGrpc.class) {
        if ((getNackMethod = HopperServiceGrpc.getNackMethod) == null) {
          HopperServiceGrpc.getNackMethod = getNackMethod =
              io.grpc.MethodDescriptor.<io.github.rpasco787.hopper.proto.v1.NackRequest, io.github.rpasco787.hopper.proto.v1.NackResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Nack"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.NackRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.NackResponse.getDefaultInstance()))
              .setSchemaDescriptor(new HopperServiceMethodDescriptorSupplier("Nack"))
              .build();
        }
      }
    }
    return getNackMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.HeartbeatRequest,
      io.github.rpasco787.hopper.proto.v1.HeartbeatResponse> getHeartbeatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Heartbeat",
      requestType = io.github.rpasco787.hopper.proto.v1.HeartbeatRequest.class,
      responseType = io.github.rpasco787.hopper.proto.v1.HeartbeatResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.HeartbeatRequest,
      io.github.rpasco787.hopper.proto.v1.HeartbeatResponse> getHeartbeatMethod() {
    io.grpc.MethodDescriptor<io.github.rpasco787.hopper.proto.v1.HeartbeatRequest, io.github.rpasco787.hopper.proto.v1.HeartbeatResponse> getHeartbeatMethod;
    if ((getHeartbeatMethod = HopperServiceGrpc.getHeartbeatMethod) == null) {
      synchronized (HopperServiceGrpc.class) {
        if ((getHeartbeatMethod = HopperServiceGrpc.getHeartbeatMethod) == null) {
          HopperServiceGrpc.getHeartbeatMethod = getHeartbeatMethod =
              io.grpc.MethodDescriptor.<io.github.rpasco787.hopper.proto.v1.HeartbeatRequest, io.github.rpasco787.hopper.proto.v1.HeartbeatResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Heartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.HeartbeatRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.github.rpasco787.hopper.proto.v1.HeartbeatResponse.getDefaultInstance()))
              .setSchemaDescriptor(new HopperServiceMethodDescriptorSupplier("Heartbeat"))
              .build();
        }
      }
    }
    return getHeartbeatMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static HopperServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<HopperServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<HopperServiceStub>() {
        @java.lang.Override
        public HopperServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new HopperServiceStub(channel, callOptions);
        }
      };
    return HopperServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static HopperServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<HopperServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<HopperServiceBlockingV2Stub>() {
        @java.lang.Override
        public HopperServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new HopperServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return HopperServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static HopperServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<HopperServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<HopperServiceBlockingStub>() {
        @java.lang.Override
        public HopperServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new HopperServiceBlockingStub(channel, callOptions);
        }
      };
    return HopperServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static HopperServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<HopperServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<HopperServiceFutureStub>() {
        @java.lang.Override
        public HopperServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new HopperServiceFutureStub(channel, callOptions);
        }
      };
    return HopperServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * HopperService is the whole API. Producers call Enqueue/EnqueueBatch/Cancel,
   * operators call GetJob/ListJobs, and workers hold a LeaseJobs stream and call
   * Ack/Nack/Heartbeat against it.
   * </pre>
   */
  public interface AsyncService {

    /**
     */
    default void enqueue(io.github.rpasco787.hopper.proto.v1.EnqueueRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.EnqueueResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEnqueueMethod(), responseObserver);
    }

    /**
     */
    default void enqueueBatch(io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEnqueueBatchMethod(), responseObserver);
    }

    /**
     */
    default void cancel(io.github.rpasco787.hopper.proto.v1.CancelRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.CancelResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCancelMethod(), responseObserver);
    }

    /**
     */
    default void getJob(io.github.rpasco787.hopper.proto.v1.GetJobRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.GetJobResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetJobMethod(), responseObserver);
    }

    /**
     */
    default void listJobs(io.github.rpasco787.hopper.proto.v1.ListJobsRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.ListJobsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListJobsMethod(), responseObserver);
    }

    /**
     * <pre>
     * LeaseJobs is bidirectional so the worker can return credit as handlers
     * finish. A server-stream cannot carry that signal, and the broker cannot
     * infer it from Ack: brokers are stateless, so a worker's Ack may land on a
     * different replica than its stream.
     * </pre>
     */
    default io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest> leaseJobs(
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getLeaseJobsMethod(), responseObserver);
    }

    /**
     */
    default void ack(io.github.rpasco787.hopper.proto.v1.AckRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.AckResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAckMethod(), responseObserver);
    }

    /**
     */
    default void nack(io.github.rpasco787.hopper.proto.v1.NackRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.NackResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getNackMethod(), responseObserver);
    }

    /**
     */
    default void heartbeat(io.github.rpasco787.hopper.proto.v1.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.HeartbeatResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHeartbeatMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service HopperService.
   * <pre>
   * HopperService is the whole API. Producers call Enqueue/EnqueueBatch/Cancel,
   * operators call GetJob/ListJobs, and workers hold a LeaseJobs stream and call
   * Ack/Nack/Heartbeat against it.
   * </pre>
   */
  public static abstract class HopperServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return HopperServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service HopperService.
   * <pre>
   * HopperService is the whole API. Producers call Enqueue/EnqueueBatch/Cancel,
   * operators call GetJob/ListJobs, and workers hold a LeaseJobs stream and call
   * Ack/Nack/Heartbeat against it.
   * </pre>
   */
  public static final class HopperServiceStub
      extends io.grpc.stub.AbstractAsyncStub<HopperServiceStub> {
    private HopperServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected HopperServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new HopperServiceStub(channel, callOptions);
    }

    /**
     */
    public void enqueue(io.github.rpasco787.hopper.proto.v1.EnqueueRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.EnqueueResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEnqueueMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void enqueueBatch(io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEnqueueBatchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void cancel(io.github.rpasco787.hopper.proto.v1.CancelRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.CancelResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCancelMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getJob(io.github.rpasco787.hopper.proto.v1.GetJobRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.GetJobResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetJobMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listJobs(io.github.rpasco787.hopper.proto.v1.ListJobsRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.ListJobsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListJobsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * LeaseJobs is bidirectional so the worker can return credit as handlers
     * finish. A server-stream cannot carry that signal, and the broker cannot
     * infer it from Ack: brokers are stateless, so a worker's Ack may land on a
     * different replica than its stream.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest> leaseJobs(
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getLeaseJobsMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public void ack(io.github.rpasco787.hopper.proto.v1.AckRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.AckResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAckMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void nack(io.github.rpasco787.hopper.proto.v1.NackRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.NackResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getNackMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void heartbeat(io.github.rpasco787.hopper.proto.v1.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.HeartbeatResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service HopperService.
   * <pre>
   * HopperService is the whole API. Producers call Enqueue/EnqueueBatch/Cancel,
   * operators call GetJob/ListJobs, and workers hold a LeaseJobs stream and call
   * Ack/Nack/Heartbeat against it.
   * </pre>
   */
  public static final class HopperServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<HopperServiceBlockingV2Stub> {
    private HopperServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected HopperServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new HopperServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.EnqueueResponse enqueue(io.github.rpasco787.hopper.proto.v1.EnqueueRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getEnqueueMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse enqueueBatch(io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getEnqueueBatchMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.CancelResponse cancel(io.github.rpasco787.hopper.proto.v1.CancelRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getCancelMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.GetJobResponse getJob(io.github.rpasco787.hopper.proto.v1.GetJobRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetJobMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.ListJobsResponse listJobs(io.github.rpasco787.hopper.proto.v1.ListJobsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getListJobsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * LeaseJobs is bidirectional so the worker can return credit as handlers
     * finish. A server-stream cannot carry that signal, and the broker cannot
     * infer it from Ack: brokers are stateless, so a worker's Ack may land on a
     * different replica than its stream.
     * </pre>
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest, io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse>
        leaseJobs() {
      return io.grpc.stub.ClientCalls.blockingBidiStreamingCall(
          getChannel(), getLeaseJobsMethod(), getCallOptions());
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.AckResponse ack(io.github.rpasco787.hopper.proto.v1.AckRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getAckMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.NackResponse nack(io.github.rpasco787.hopper.proto.v1.NackRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getNackMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.HeartbeatResponse heartbeat(io.github.rpasco787.hopper.proto.v1.HeartbeatRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getHeartbeatMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service HopperService.
   * <pre>
   * HopperService is the whole API. Producers call Enqueue/EnqueueBatch/Cancel,
   * operators call GetJob/ListJobs, and workers hold a LeaseJobs stream and call
   * Ack/Nack/Heartbeat against it.
   * </pre>
   */
  public static final class HopperServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<HopperServiceBlockingStub> {
    private HopperServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected HopperServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new HopperServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.EnqueueResponse enqueue(io.github.rpasco787.hopper.proto.v1.EnqueueRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEnqueueMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse enqueueBatch(io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEnqueueBatchMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.CancelResponse cancel(io.github.rpasco787.hopper.proto.v1.CancelRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCancelMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.GetJobResponse getJob(io.github.rpasco787.hopper.proto.v1.GetJobRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetJobMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.ListJobsResponse listJobs(io.github.rpasco787.hopper.proto.v1.ListJobsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListJobsMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.AckResponse ack(io.github.rpasco787.hopper.proto.v1.AckRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAckMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.NackResponse nack(io.github.rpasco787.hopper.proto.v1.NackRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getNackMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.github.rpasco787.hopper.proto.v1.HeartbeatResponse heartbeat(io.github.rpasco787.hopper.proto.v1.HeartbeatRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHeartbeatMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service HopperService.
   * <pre>
   * HopperService is the whole API. Producers call Enqueue/EnqueueBatch/Cancel,
   * operators call GetJob/ListJobs, and workers hold a LeaseJobs stream and call
   * Ack/Nack/Heartbeat against it.
   * </pre>
   */
  public static final class HopperServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<HopperServiceFutureStub> {
    private HopperServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected HopperServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new HopperServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.github.rpasco787.hopper.proto.v1.EnqueueResponse> enqueue(
        io.github.rpasco787.hopper.proto.v1.EnqueueRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEnqueueMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse> enqueueBatch(
        io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEnqueueBatchMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.github.rpasco787.hopper.proto.v1.CancelResponse> cancel(
        io.github.rpasco787.hopper.proto.v1.CancelRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCancelMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.github.rpasco787.hopper.proto.v1.GetJobResponse> getJob(
        io.github.rpasco787.hopper.proto.v1.GetJobRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetJobMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.github.rpasco787.hopper.proto.v1.ListJobsResponse> listJobs(
        io.github.rpasco787.hopper.proto.v1.ListJobsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListJobsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.github.rpasco787.hopper.proto.v1.AckResponse> ack(
        io.github.rpasco787.hopper.proto.v1.AckRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAckMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.github.rpasco787.hopper.proto.v1.NackResponse> nack(
        io.github.rpasco787.hopper.proto.v1.NackRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getNackMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.github.rpasco787.hopper.proto.v1.HeartbeatResponse> heartbeat(
        io.github.rpasco787.hopper.proto.v1.HeartbeatRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_ENQUEUE = 0;
  private static final int METHODID_ENQUEUE_BATCH = 1;
  private static final int METHODID_CANCEL = 2;
  private static final int METHODID_GET_JOB = 3;
  private static final int METHODID_LIST_JOBS = 4;
  private static final int METHODID_ACK = 5;
  private static final int METHODID_NACK = 6;
  private static final int METHODID_HEARTBEAT = 7;
  private static final int METHODID_LEASE_JOBS = 8;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_ENQUEUE:
          serviceImpl.enqueue((io.github.rpasco787.hopper.proto.v1.EnqueueRequest) request,
              (io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.EnqueueResponse>) responseObserver);
          break;
        case METHODID_ENQUEUE_BATCH:
          serviceImpl.enqueueBatch((io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest) request,
              (io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse>) responseObserver);
          break;
        case METHODID_CANCEL:
          serviceImpl.cancel((io.github.rpasco787.hopper.proto.v1.CancelRequest) request,
              (io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.CancelResponse>) responseObserver);
          break;
        case METHODID_GET_JOB:
          serviceImpl.getJob((io.github.rpasco787.hopper.proto.v1.GetJobRequest) request,
              (io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.GetJobResponse>) responseObserver);
          break;
        case METHODID_LIST_JOBS:
          serviceImpl.listJobs((io.github.rpasco787.hopper.proto.v1.ListJobsRequest) request,
              (io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.ListJobsResponse>) responseObserver);
          break;
        case METHODID_ACK:
          serviceImpl.ack((io.github.rpasco787.hopper.proto.v1.AckRequest) request,
              (io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.AckResponse>) responseObserver);
          break;
        case METHODID_NACK:
          serviceImpl.nack((io.github.rpasco787.hopper.proto.v1.NackRequest) request,
              (io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.NackResponse>) responseObserver);
          break;
        case METHODID_HEARTBEAT:
          serviceImpl.heartbeat((io.github.rpasco787.hopper.proto.v1.HeartbeatRequest) request,
              (io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.HeartbeatResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_LEASE_JOBS:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.leaseJobs(
              (io.grpc.stub.StreamObserver<io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getEnqueueMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.github.rpasco787.hopper.proto.v1.EnqueueRequest,
              io.github.rpasco787.hopper.proto.v1.EnqueueResponse>(
                service, METHODID_ENQUEUE)))
        .addMethod(
          getEnqueueBatchMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.github.rpasco787.hopper.proto.v1.EnqueueBatchRequest,
              io.github.rpasco787.hopper.proto.v1.EnqueueBatchResponse>(
                service, METHODID_ENQUEUE_BATCH)))
        .addMethod(
          getCancelMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.github.rpasco787.hopper.proto.v1.CancelRequest,
              io.github.rpasco787.hopper.proto.v1.CancelResponse>(
                service, METHODID_CANCEL)))
        .addMethod(
          getGetJobMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.github.rpasco787.hopper.proto.v1.GetJobRequest,
              io.github.rpasco787.hopper.proto.v1.GetJobResponse>(
                service, METHODID_GET_JOB)))
        .addMethod(
          getListJobsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.github.rpasco787.hopper.proto.v1.ListJobsRequest,
              io.github.rpasco787.hopper.proto.v1.ListJobsResponse>(
                service, METHODID_LIST_JOBS)))
        .addMethod(
          getLeaseJobsMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              io.github.rpasco787.hopper.proto.v1.LeaseJobsRequest,
              io.github.rpasco787.hopper.proto.v1.LeaseJobsResponse>(
                service, METHODID_LEASE_JOBS)))
        .addMethod(
          getAckMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.github.rpasco787.hopper.proto.v1.AckRequest,
              io.github.rpasco787.hopper.proto.v1.AckResponse>(
                service, METHODID_ACK)))
        .addMethod(
          getNackMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.github.rpasco787.hopper.proto.v1.NackRequest,
              io.github.rpasco787.hopper.proto.v1.NackResponse>(
                service, METHODID_NACK)))
        .addMethod(
          getHeartbeatMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.github.rpasco787.hopper.proto.v1.HeartbeatRequest,
              io.github.rpasco787.hopper.proto.v1.HeartbeatResponse>(
                service, METHODID_HEARTBEAT)))
        .build();
  }

  private static abstract class HopperServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    HopperServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.github.rpasco787.hopper.proto.v1.HopperProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("HopperService");
    }
  }

  private static final class HopperServiceFileDescriptorSupplier
      extends HopperServiceBaseDescriptorSupplier {
    HopperServiceFileDescriptorSupplier() {}
  }

  private static final class HopperServiceMethodDescriptorSupplier
      extends HopperServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    HopperServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (HopperServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new HopperServiceFileDescriptorSupplier())
              .addMethod(getEnqueueMethod())
              .addMethod(getEnqueueBatchMethod())
              .addMethod(getCancelMethod())
              .addMethod(getGetJobMethod())
              .addMethod(getListJobsMethod())
              .addMethod(getLeaseJobsMethod())
              .addMethod(getAckMethod())
              .addMethod(getNackMethod())
              .addMethod(getHeartbeatMethod())
              .build();
        }
      }
    }
    return result;
  }
}

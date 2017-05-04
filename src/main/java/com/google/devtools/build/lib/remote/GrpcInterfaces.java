// Copyright 2017 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.devtools.build.lib.remote;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamBlockingStub;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.devtools.remoteexecution.v1test.ActionCacheGrpc;
import com.google.devtools.remoteexecution.v1test.ActionCacheGrpc.ActionCacheBlockingStub;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.BatchUpdateBlobsRequest;
import com.google.devtools.remoteexecution.v1test.BatchUpdateBlobsResponse;
import com.google.devtools.remoteexecution.v1test.ContentAddressableStorageGrpc;
import com.google.devtools.remoteexecution.v1test.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import com.google.devtools.remoteexecution.v1test.ExecuteRequest;
import com.google.devtools.remoteexecution.v1test.ExecutionGrpc;
import com.google.devtools.remoteexecution.v1test.ExecutionGrpc.ExecutionBlockingStub;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsRequest;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsResponse;
import com.google.devtools.remoteexecution.v1test.GetActionResultRequest;
import com.google.devtools.remoteexecution.v1test.GetTreeRequest;
import com.google.devtools.remoteexecution.v1test.GetTreeResponse;
import com.google.devtools.remoteexecution.v1test.UpdateActionResultRequest;
import com.google.longrunning.Operation;
import com.google.protobuf.util.Durations;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/** Implementations of the gRPC interfaces that actually talk to gRPC. */
public class GrpcInterfaces {
  // TODO(olaola): break these into separate classes?
  /** Create a {@link GrpcCasInterface} instance that actually talks to gRPC. */
  public static GrpcCasInterface casInterface(
      final int grpcTimeoutSeconds, final Channel channel, final ChannelOptions channelOptions) {
    return new GrpcCasInterface() {
      private ContentAddressableStorageBlockingStub getBlockingStub() {
        return ContentAddressableStorageGrpc.newBlockingStub(channel)
            .withCallCredentials(channelOptions.getCallCredentials())
            .withDeadlineAfter(grpcTimeoutSeconds, TimeUnit.SECONDS);
      }

      // TODO(olaola): reuse stubs between calls.
      @Override
      public FindMissingBlobsResponse findMissingBlobs(FindMissingBlobsRequest request) {
        return getBlockingStub().findMissingBlobs(request);
      }

      @Override
      public BatchUpdateBlobsResponse batchUpdateBlobs(BatchUpdateBlobsRequest request) {
        return getBlockingStub().batchUpdateBlobs(request);
      }

      @Override
      public GetTreeResponse getTree(GetTreeRequest request) {
        return getBlockingStub().getTree(request);
      }
    };
  }

  /** Create a {@link GrpcByteStreamInterface} instance that actually talks to gRPC. */
  public static GrpcByteStreamInterface byteStreamInterface(
      final int grpcTimeoutSeconds, final Channel channel, final ChannelOptions channelOptions) {
    return new GrpcByteStreamInterface() {
      @Override
      public Iterator<ReadResponse> read(ReadRequest request) {
        ByteStreamBlockingStub stub =
            ByteStreamGrpc.newBlockingStub(channel)
                .withCallCredentials(channelOptions.getCallCredentials())
                .withDeadlineAfter(grpcTimeoutSeconds, TimeUnit.SECONDS);
        return stub.read(request);
      }

      @Override
      public StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> responseObserver) {
        ByteStreamStub stub =
            ByteStreamGrpc.newStub(channel)
                .withCallCredentials(channelOptions.getCallCredentials())
                .withDeadlineAfter(grpcTimeoutSeconds, TimeUnit.SECONDS);
        return stub.write(responseObserver);
      }
    };
  }

  /** Create a {@link GrpcCasInterface} instance that actually talks to gRPC. */
  public static GrpcActionCacheInterface actionCacheInterface(
      final int grpcTimeoutSeconds, final Channel channel, final ChannelOptions channelOptions) {
    return new GrpcActionCacheInterface() {
      private ActionCacheBlockingStub getBlockingStub() {
        return ActionCacheGrpc.newBlockingStub(channel)
            .withCallCredentials(channelOptions.getCallCredentials())
            .withDeadlineAfter(grpcTimeoutSeconds, TimeUnit.SECONDS);
      }

      @Override
      public ActionResult getActionResult(GetActionResultRequest request) {
        return getBlockingStub().getActionResult(request);
      }

      @Override
      public ActionResult updateActionResult(UpdateActionResultRequest request) {
        return getBlockingStub().updateActionResult(request);
      }
    };
  }

  /** Create a {@link GrpcExecutionInterface} instance that actually talks to gRPC. */
  public static GrpcExecutionInterface executionInterface(
      final int grpcTimeoutSeconds, final Channel channel, final ChannelOptions channelOptions) {
    return new GrpcExecutionInterface() {
      @Override
      public Operation execute(ExecuteRequest request) {
        int actionSeconds = (int)Durations.toSeconds(request.getAction().getTimeout());
        ExecutionBlockingStub stub =
            ExecutionGrpc.newBlockingStub(channel)
                .withCallCredentials(channelOptions.getCallCredentials())
                .withDeadlineAfter(grpcTimeoutSeconds + actionSeconds, TimeUnit.SECONDS);
        return stub.execute(request);
      }
    };
  }
}

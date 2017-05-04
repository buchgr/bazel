// Copyright 2016 The Bazel Authors. All rights reserved.
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

import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import com.google.devtools.build.lib.util.Preconditions;
import com.google.devtools.remoteexecution.v1test.ExecuteRequest;
import com.google.devtools.remoteexecution.v1test.ExecuteResponse;
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannel;
import io.grpc.protobuf.StatusProto;

/** A remote work executor that uses gRPC for communicating the work, inputs and outputs. */
@ThreadSafe
public class GrpcRemoteExecutor extends GrpcActionCache {
  public static boolean isRemoteExecutionOptions(RemoteOptions options) {
    return options.remoteExecutor != null;
  }

  private final GrpcExecutionInterface executionIface;

  public GrpcRemoteExecutor(
      RemoteOptions options,
      GrpcCasInterface casIface,
      GrpcByteStreamInterface bsIface,
      GrpcActionCacheInterface cacheIface,
      GrpcExecutionInterface executionIface) {
    super(options, casIface, bsIface, cacheIface);
    this.executionIface = executionIface;
  }

  public GrpcRemoteExecutor(
      ManagedChannel channel, ChannelOptions channelOptions, RemoteOptions options) {
    super(
        options,
        GrpcInterfaces.casInterface(options.remoteTimeout, channel, channelOptions),
        GrpcInterfaces.byteStreamInterface(options.remoteTimeout, channel, channelOptions),
        GrpcInterfaces.actionCacheInterface(
            options.remoteTimeout, channel, channelOptions));
    this.executionIface =
        GrpcInterfaces.executionInterface(options.remoteTimeout, channel, channelOptions);
  }

  public ExecuteResponse executeRemotely(ExecuteRequest request) {
    // TODO(olaola): handle longrunning Operations by using the Watcher API to wait for results.
    // For now, only support actions with wait_for_completion = true.
    Preconditions.checkArgument(request.getWaitForCompletion());
    Operation op = executionIface.execute(request);
    Preconditions.checkState(op.getDone());
    Preconditions.checkState(op.getResultCase() != Operation.ResultCase.RESULT_NOT_SET);
    if (op.getResultCase() == Operation.ResultCase.ERROR) {
      throw StatusProto.toStatusRuntimeException(op.getError());
    }
    try {
      return op.getResponse().unpack(ExecuteResponse.class);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}

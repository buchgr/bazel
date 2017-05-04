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

import com.google.devtools.remoteexecution.v1test.BatchUpdateBlobsRequest;
import com.google.devtools.remoteexecution.v1test.BatchUpdateBlobsResponse;
import com.google.devtools.remoteexecution.v1test.ContentAddressableStorageGrpc.ContentAddressableStorageBlockingStub;
import com.google.devtools.remoteexecution.v1test.ContentAddressableStorageGrpc.ContentAddressableStorageStub;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsRequest;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsResponse;
import com.google.devtools.remoteexecution.v1test.GetTreeRequest;
import com.google.devtools.remoteexecution.v1test.GetTreeResponse;

/**
 * An abstraction layer between the remote execution client and gRPC to support unit testing. This
 * interface covers the CAS RPC methods, see {@link ContentAddressableStorageBlockingStub} and
 * {@link ContentAddressableStorageStub}.
 */
public interface GrpcCasInterface {
  FindMissingBlobsResponse findMissingBlobs(FindMissingBlobsRequest request);
  BatchUpdateBlobsResponse batchUpdateBlobs(BatchUpdateBlobsRequest request);
  GetTreeResponse getTree(GetTreeRequest request);
}

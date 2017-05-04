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

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ActionInputFileCache;
import com.google.devtools.build.lib.actions.ActionInputHelper;
import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.Artifact.ArtifactExpander;
import com.google.devtools.build.lib.actions.ResourceSet;
import com.google.devtools.build.lib.actions.SimpleSpawn;
import com.google.devtools.build.lib.actions.Spawn;
import com.google.devtools.build.lib.exec.SpawnInputExpander;
import com.google.devtools.build.lib.exec.SpawnResult;
import com.google.devtools.build.lib.exec.SpawnRunner.ProgressStatus;
import com.google.devtools.build.lib.exec.SpawnRunner.SpawnExecutionPolicy;
import com.google.devtools.build.lib.exec.util.FakeOwner;
import com.google.devtools.build.lib.util.io.FileOutErr;
import com.google.devtools.build.lib.vfs.FileSystem;
import com.google.devtools.build.lib.vfs.FileSystemUtils;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.devtools.build.lib.vfs.inmemoryfs.InMemoryFileSystem;
import com.google.devtools.common.options.Options;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.ExecuteRequest;
import com.google.devtools.remoteexecution.v1test.ExecuteResponse;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsRequest;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsResponse;
import com.google.devtools.remoteexecution.v1test.GetActionResultRequest;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Collection;
import java.util.SortedMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;
import org.mockito.MockitoAnnotations.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Tests for {@link RemoteSpawnRunner} in combination with {@link GrpcRemoteExecutor}. */
@RunWith(JUnit4.class)
public class GrpcRemoteExecutionClientTest {
  private static final ArtifactExpander SIMPLE_ARTIFACT_EXPANDER =
      new ArtifactExpander() {
        @Override
        public void expand(Artifact artifact, Collection<? super Artifact> output) {
          output.add(artifact);
        }
      };

  private FileSystem fs;
  private Path execRoot;
  private SimpleSpawn simpleSpawn;
  private FakeActionInputFileCache fakeFileCache;
  @Mock private GrpcCasInterface casIface;
  @Mock private GrpcByteStreamInterface bsIface;
  @Mock private GrpcActionCacheInterface cacheIface;
  @Mock private GrpcExecutionInterface executionIface;
  private Digest inputDigest;
  private RemoteSpawnRunner client;
  private FileOutErr outErr;

  private final SpawnExecutionPolicy simplePolicy =
      new SpawnExecutionPolicy() {
        @Override
        public boolean shouldPrefetchInputsForLocalExecution(Spawn spawn) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void lockOutputFiles() throws InterruptedException {
          throw new UnsupportedOperationException();
        }

        @Override
        public ActionInputFileCache getActionInputFileCache() {
          return fakeFileCache;
        }

        @Override
        public long getTimeoutMillis() {
          return 0;
        }

        @Override
        public FileOutErr getFileOutErr() {
          return outErr;
        }

        @Override
        public SortedMap<PathFragment, ActionInput> getInputMapping() throws IOException {
          return new SpawnInputExpander(/*strict*/ false)
              .getInputMapping(simpleSpawn, SIMPLE_ARTIFACT_EXPANDER, fakeFileCache, "workspace");
        }

        @Override
        public void report(ProgressStatus state) {
          // TODO(ulfjack): Test that the right calls are made.
        }
      };

  @Before
  public final void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    Chunker.setDefaultChunkSizeForTesting(1000); // Enough for everything to be one chunk.
    fs = new InMemoryFileSystem();
    execRoot = fs.getPath("/exec/root");
    FileSystemUtils.createDirectoryAndParents(execRoot);
    fakeFileCache = new FakeActionInputFileCache(execRoot);
    simpleSpawn =
        new SimpleSpawn(
            new FakeOwner("Mnemonic", "Progress Message"),
            ImmutableList.of("/bin/echo", "Hi!"),
            ImmutableMap.of("VARIABLE", "value"),
            /*executionInfo=*/ ImmutableMap.<String, String>of(),
            /*inputs=*/ ImmutableList.of(ActionInputHelper.fromPath("input")),
            /*outputs=*/ ImmutableList.<ActionInput>of(),
            ResourceSet.ZERO);

    Path stdout = fs.getPath("/tmp/stdout");
    Path stderr = fs.getPath("/tmp/stderr");
    FileSystemUtils.createDirectoryAndParents(stdout.getParentDirectory());
    FileSystemUtils.createDirectoryAndParents(stderr.getParentDirectory());
    outErr = new FileOutErr(stdout, stderr);
    RemoteOptions options = Options.getDefaults(RemoteOptions.class);
    GrpcRemoteExecutor executor =
        new GrpcRemoteExecutor(options, casIface, bsIface, cacheIface, executionIface);
    client = new RemoteSpawnRunner(execRoot, options, executor);
    inputDigest = fakeFileCache.createScratchInput(simpleSpawn.getInputFiles().get(0), "xyz");
  }

  @Test
  public void cacheHit() throws Exception {
    when(cacheIface.getActionResult(any(GetActionResultRequest.class)))
        .thenReturn(ActionResult.getDefaultInstance());

    SpawnResult result = client.exec(simpleSpawn, simplePolicy);
    verify(cacheIface).getActionResult(any(GetActionResultRequest.class));
    assertThat(result.setupSuccess()).isTrue();
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(outErr.hasRecordedOutput()).isFalse();
    assertThat(outErr.hasRecordedStderr()).isFalse();
  }

  @Test
  public void cacheHitWithOutput() throws Exception {
    Digest stdOutDigest = Digests.computeDigestUtf8("stdout");
    Digest stdErrDigest = Digests.computeDigestUtf8("stderr");
    when(cacheIface.getActionResult(any(GetActionResultRequest.class)))
        .thenReturn(
            ActionResult.newBuilder()
                .setStdoutDigest(stdOutDigest)
                .setStderrDigest(stdErrDigest)
                .build());
    when(bsIface.read(
            ReadRequest.newBuilder()
                .setResourceName(
                    "blobs/" + stdOutDigest.getHash() + "/" + stdOutDigest.getSizeBytes())
                .build()))
        .thenReturn(
            Iterators.singletonIterator(
                ReadResponse.newBuilder().setData(ByteString.copyFromUtf8("stdout")).build()));
    when(bsIface.read(
            ReadRequest.newBuilder()
                .setResourceName(
                    "blobs/" + stdErrDigest.getHash() + "/" + stdErrDigest.getSizeBytes())
                .build()))
        .thenReturn(
            Iterators.singletonIterator(
                ReadResponse.newBuilder().setData(ByteString.copyFromUtf8("stderr")).build()));

    SpawnResult result = client.exec(simpleSpawn, simplePolicy);
    assertThat(result.setupSuccess()).isTrue();
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(outErr.outAsLatin1()).isEqualTo("stdout");
    assertThat(outErr.errAsLatin1()).isEqualTo("stderr");
  }

  @Test
  public void cacheHitWithInlineOutput() throws Exception {
    when(cacheIface.getActionResult(any(GetActionResultRequest.class)))
        .thenReturn(
            ActionResult.newBuilder()
                .setStdoutRaw(ByteString.copyFromUtf8("stdout"))
                .setStderrRaw(ByteString.copyFromUtf8("stderr"))
                .build());

    SpawnResult result = client.exec(simpleSpawn, simplePolicy);
    assertThat(result.setupSuccess()).isTrue();
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(outErr.outAsLatin1()).isEqualTo("stdout");
    assertThat(outErr.errAsLatin1()).isEqualTo("stderr");
  }

  private Answer<StreamObserver<WriteRequest>> blobWriteAnswer(byte[] data) {
    Digest digest = Digests.computeDigest(data);
    return new Answer<StreamObserver<WriteRequest>>() {
      @Override
      public StreamObserver<WriteRequest> answer(InvocationOnMock invocation) {
        @SuppressWarnings("unchecked")
        StreamObserver<WriteResponse> responseObserver =
            (StreamObserver<WriteResponse>) invocation.getArguments()[0];
        return new StreamObserver<WriteRequest>() {
          @Override
          public void onNext(WriteRequest request) {
            assertThat(request.getResourceName()).contains(digest.getHash());
            assertThat(request.getFinishWrite()).isTrue();
            assertThat(request.getData().toByteArray()).isEqualTo(data);
            responseObserver.onNext(
                WriteResponse.newBuilder().setCommittedSize(request.getData().size()).build());
          }

          @Override
          public void onCompleted() {
            responseObserver.onCompleted();
          }

          @Override
          public void onError(Throwable t) {
            fail("An error occurred: " + t);
          }
        };
      }
    };
  }

  @Test
  public void remotelyExecute() throws Exception {
    // getActionResult mock returns null by default, meaning a cache miss.
    ActionResult actionResult =
        ActionResult.newBuilder()
            .setStdoutRaw(ByteString.copyFromUtf8("stdout"))
            .setStderrRaw(ByteString.copyFromUtf8("stderr"))
            .build();
    when(executionIface.execute(any(ExecuteRequest.class)))
        .thenReturn(
            Operation.newBuilder()
                .setDone(true)
                .setResponse(Any.pack(ExecuteResponse.newBuilder().setResult(actionResult).build()))
                .build());
    Command command =
        Command.newBuilder()
            .addAllArguments(ImmutableList.of("/bin/echo", "Hi!"))
            .addEnvironmentVariables(
                Command.EnvironmentVariable.newBuilder()
                    .setName("VARIABLE")
                    .setValue("value")
                    .build())
            .build();
    when(casIface.findMissingBlobs(any(FindMissingBlobsRequest.class)))
        .thenReturn(
            FindMissingBlobsResponse.newBuilder()
                .addMissingBlobDigests(Digests.computeDigest(command))
                .build())
        .thenReturn(
            FindMissingBlobsResponse.newBuilder().addMissingBlobDigests(inputDigest).build());
    when(bsIface.write(any()))
        .thenAnswer(blobWriteAnswer(command.toByteArray()))
        .thenAnswer(blobWriteAnswer("xyz".getBytes(UTF_8)));

    SpawnResult result = client.exec(simpleSpawn, simplePolicy);
    assertThat(result.setupSuccess()).isTrue();
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(outErr.outAsLatin1()).isEqualTo("stdout");
    assertThat(outErr.errAsLatin1()).isEqualTo("stderr");
  }
}

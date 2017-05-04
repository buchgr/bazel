// Copyright 2015 The Bazel Authors. All rights reserved.
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
import static org.mockito.Mockito.when;

import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.devtools.build.lib.actions.ActionInputHelper;
import com.google.devtools.build.lib.vfs.FileSystem;
import com.google.devtools.build.lib.vfs.FileSystemUtils;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.inmemoryfs.InMemoryFileSystem;
import com.google.devtools.common.options.Options;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsRequest;
import com.google.devtools.remoteexecution.v1test.FindMissingBlobsResponse;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;
import org.mockito.MockitoAnnotations.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Tests for {@link GrpcActionCache}. */
@RunWith(JUnit4.class)
public class GrpcActionCacheTest {
  private FileSystem fs;
  private Path execRoot;
  private FakeActionInputFileCache fakeFileCache;
  @Mock private GrpcCasInterface casIface;
  @Mock private GrpcByteStreamInterface bsIface;
  @Mock private GrpcActionCacheInterface cacheIface;

  @Before
  public final void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    Chunker.setDefaultChunkSizeForTesting(1000);  // Enough for everything to be one chunk.
    fs = new InMemoryFileSystem();
    execRoot = fs.getPath("/exec/root");
    FileSystemUtils.createDirectoryAndParents(execRoot);
    fakeFileCache = new FakeActionInputFileCache(execRoot);
  }

  private GrpcActionCache newClient() throws IOException {
    return newClient(Options.getDefaults(RemoteOptions.class));
  }

  private GrpcActionCache newClient(RemoteOptions options) throws IOException {
    return new GrpcActionCache(options, casIface, bsIface, cacheIface);
  }

  @Test
  public void testDownloadEmptyBlob() throws Exception {
    GrpcActionCache client = newClient();
    Digest emptyDigest = Digests.computeDigest(new byte[0]);
    // Will not call the mock Bytestream interface at all.
    assertThat(client.downloadBlob(emptyDigest)).isEmpty();
  }

  private void addMockReadRequest(Digest digest, String contents) {
    when(bsIface.read(
            ReadRequest.newBuilder()
                .setResourceName("blobs/" + digest.getHash() + "/" + digest.getSizeBytes())
                .build()))
        .thenReturn(
            Iterators.singletonIterator(
                ReadResponse.newBuilder().setData(ByteString.copyFromUtf8(contents)).build()));
  }

  @Test
  public void testDownloadBlobSingleChunk() throws Exception {
    GrpcActionCache client = newClient();
    Digest digest = Digests.computeDigestUtf8("abcdefg");
    addMockReadRequest(digest, "abcdefg");
    assertThat(new String(client.downloadBlob(digest), UTF_8)).isEqualTo("abcdefg");
  }

  @Test
  public void testDownloadBlobMultipleChunks() throws Exception {
    GrpcActionCache client = newClient();
    Digest digest = Digests.computeDigestUtf8("abcdefg");
    when(bsIface.read(
            ReadRequest.newBuilder()
                .setResourceName("blobs/" + digest.getHash() + "/" + digest.getSizeBytes())
                .build()))
        .thenReturn(
            ImmutableList.of(
                    ReadResponse.newBuilder().setData(ByteString.copyFromUtf8("abc")).build(),
                    ReadResponse.newBuilder().setData(ByteString.copyFromUtf8("def")).build(),
                    ReadResponse.newBuilder().setData(ByteString.copyFromUtf8("g")).build())
                .iterator());
    assertThat(new String(client.downloadBlob(digest), UTF_8)).isEqualTo("abcdefg");
  }

  @Test
  public void testDownloadAllResults() throws Exception {
    GrpcActionCache client = newClient();
    Digest fooDigest = Digests.computeDigestUtf8("foo-contents");
    Digest barDigest = Digests.computeDigestUtf8("bar-contents");
    Digest emptyDigest = Digests.computeDigest(new byte[0]);
    addMockReadRequest(fooDigest, "foo-contents");
    addMockReadRequest(barDigest, "bar-contents");

    ActionResult.Builder result = ActionResult.newBuilder();
    result.addOutputFilesBuilder().setPath("a/foo").setDigest(fooDigest);
    result.addOutputFilesBuilder().setPath("b/empty").setDigest(emptyDigest);
    result.addOutputFilesBuilder().setPath("a/bar").setDigest(barDigest).setIsExecutable(true);
    client.downloadAllResults(result.build(), execRoot);
    assertThat(Digests.computeDigest(execRoot.getRelative("a/foo"))).isEqualTo(fooDigest);
    assertThat(Digests.computeDigest(execRoot.getRelative("b/empty"))).isEqualTo(emptyDigest);
    assertThat(Digests.computeDigest(execRoot.getRelative("a/bar"))).isEqualTo(barDigest);
    assertThat(execRoot.getRelative("a/bar").isExecutable()).isTrue();
  }

  @Test
  public void testUploadBlobCacheHit() throws Exception {
    GrpcActionCache client = newClient();
    Digest digest = Digests.computeDigestUtf8("abcdefg");
    when(casIface.findMissingBlobs(
            FindMissingBlobsRequest.newBuilder().addBlobDigests(digest).build()))
        .thenReturn(FindMissingBlobsResponse.getDefaultInstance()); // Nothing is missing.
    assertThat(client.uploadBlob("abcdefg".getBytes(UTF_8))).isEqualTo(digest);
  }

  @Test
  public void testUploadBlobSingleChunk() throws Exception {
    GrpcActionCache client = newClient();
    Digest digest = Digests.computeDigestUtf8("abcdefg");
    when(casIface.findMissingBlobs(
            FindMissingBlobsRequest.newBuilder().addBlobDigests(digest).build()))
        .thenReturn(FindMissingBlobsResponse.newBuilder().addMissingBlobDigests(digest).build());
    when(bsIface.write(any()))
        .thenAnswer(
            new Answer<StreamObserver<WriteRequest>>() {
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
                    assertThat(request.getData().toStringUtf8()).isEqualTo("abcdefg");
                  }

                  @Override
                  public void onCompleted() {
                    responseObserver.onNext(WriteResponse.newBuilder().setCommittedSize(7).build());
                    responseObserver.onCompleted();
                  }

                  @Override
                  public void onError(Throwable t) {
                    fail("An error occurred: " + t);
                  }
                };
              }
            });
    assertThat(client.uploadBlob("abcdefg".getBytes(UTF_8))).isEqualTo(digest);
  }

  static class TestChunkedRequestObserver implements StreamObserver<WriteRequest> {
    private final StreamObserver<WriteResponse> responseObserver;
    private final String contents;
    private Chunker chunker;

    public TestChunkedRequestObserver(
        StreamObserver<WriteResponse> responseObserver, String contents, int chunkSizeBytes) {
      this.responseObserver = responseObserver;
      this.contents = contents;
      try {
        chunker = Chunker.from(contents.getBytes(UTF_8), chunkSizeBytes);
      } catch (IOException e) {
        fail("An error occurred:" + e);
      }
    }

    @Override
    public void onNext(WriteRequest request) {
      assertThat(chunker.hasNext()).isTrue();
      try {
        Chunker.Chunk chunk = chunker.next();
        Digest digest = chunk.getDigest();
        long offset = chunk.getOffset();
        byte[] data = chunk.getData();
        if (offset == 0) {
          assertThat(request.getResourceName()).contains(digest.getHash());
        } else {
          assertThat(request.getResourceName()).isEmpty();
        }
        assertThat(request.getFinishWrite())
            .isEqualTo(offset + data.length == digest.getSizeBytes());
        assertThat(request.getData().toByteArray()).isEqualTo(data);
      } catch (IOException e) {
        fail("An error occurred:" + e);
      }
    }

    @Override
    public void onCompleted() {
      assertThat(chunker.hasNext()).isFalse();
      responseObserver.onNext(
          WriteResponse.newBuilder().setCommittedSize(contents.length()).build());
      responseObserver.onCompleted();
    }

    @Override
    public void onError(Throwable t) {
      fail("An error occurred: " + t);
    }
  }

  private Answer<StreamObserver<WriteRequest>> blobChunkedWriteAnswer(
      String contents, int chunkSize) {
    return new Answer<StreamObserver<WriteRequest>>() {
      @Override
      @SuppressWarnings("unchecked")
      public StreamObserver<WriteRequest> answer(InvocationOnMock invocation) {
        return new TestChunkedRequestObserver(
            (StreamObserver<WriteResponse>) invocation.getArguments()[0], contents, chunkSize);
      }
    };
  }

  @Test
  public void testUploadBlobMultipleChunks() throws Exception {
    Digest digest = Digests.computeDigestUtf8("abcdefg");
    when(casIface.findMissingBlobs(
            FindMissingBlobsRequest.newBuilder().addBlobDigests(digest).build()))
        .thenReturn(FindMissingBlobsResponse.newBuilder().addMissingBlobDigests(digest).build());

    for (int chunkSize = 1; chunkSize <= 7; ++chunkSize) {
      GrpcActionCache client = newClient();
      Chunker.setDefaultChunkSizeForTesting(chunkSize);
      when(bsIface.write(any())).thenAnswer(blobChunkedWriteAnswer("abcdefg", chunkSize));
      assertThat(client.uploadBlob("abcdefg".getBytes(UTF_8))).isEqualTo(digest);
    }
  }

  @Test
  public void testUploadAllResultsCacheHits() throws Exception {
    GrpcActionCache client = newClient();
    Digest fooDigest = fakeFileCache.createScratchInput(ActionInputHelper.fromPath("a/foo"), "xyz");
    Digest barDigest = fakeFileCache.createScratchInput(ActionInputHelper.fromPath("bar"), "x");
    Path fooFile = execRoot.getRelative("a/foo");
    Path barFile = execRoot.getRelative("bar");
    barFile.setExecutable(true);
    when(casIface.findMissingBlobs(
            FindMissingBlobsRequest.newBuilder()
                .addBlobDigests(fooDigest)
                .addBlobDigests(barDigest)
                .build()))
        .thenReturn(FindMissingBlobsResponse.getDefaultInstance());

    ActionResult.Builder result = ActionResult.newBuilder();
    client.uploadAllResults(execRoot, ImmutableList.<Path>of(fooFile, barFile), result);
    ActionResult.Builder expectedResult = ActionResult.newBuilder();
    expectedResult.addOutputFilesBuilder().setPath("a/foo").setDigest(fooDigest);
    expectedResult
        .addOutputFilesBuilder()
        .setPath("bar")
        .setDigest(barDigest)
        .setIsExecutable(true);
    assertThat(result.build()).isEqualTo(expectedResult.build());
  }

  @Test
  public void testUploadAllResultsCacheMisses() throws Exception {
    GrpcActionCache client = newClient();
    Digest fooDigest = fakeFileCache.createScratchInput(ActionInputHelper.fromPath("a/foo"), "xyz");
    Digest barDigest = fakeFileCache.createScratchInput(ActionInputHelper.fromPath("bar"), "x");
    Path fooFile = execRoot.getRelative("a/foo");
    Path barFile = execRoot.getRelative("bar");
    barFile.setExecutable(true);
    when(casIface.findMissingBlobs(
            FindMissingBlobsRequest.newBuilder()
                .addBlobDigests(fooDigest)
                .addBlobDigests(barDigest)
                .build()))
        .thenReturn(
            FindMissingBlobsResponse.newBuilder()
                .addMissingBlobDigests(fooDigest)
                .addMissingBlobDigests(barDigest)
                .build());
    when(bsIface.write(any()))
        .thenAnswer(blobChunkedWriteAnswer("xyz", 3))
        .thenAnswer(blobChunkedWriteAnswer("x", 1));

    ActionResult.Builder result = ActionResult.newBuilder();
    client.uploadAllResults(execRoot, ImmutableList.<Path>of(fooFile, barFile), result);
    ActionResult.Builder expectedResult = ActionResult.newBuilder();
    expectedResult.addOutputFilesBuilder().setPath("a/foo").setDigest(fooDigest);
    expectedResult
        .addOutputFilesBuilder()
        .setPath("bar")
        .setDigest(barDigest)
        .setIsExecutable(true);
    assertThat(result.build()).isEqualTo(expectedResult.build());
  }
}

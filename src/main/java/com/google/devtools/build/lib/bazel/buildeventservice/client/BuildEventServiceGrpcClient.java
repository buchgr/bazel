package com.google.devtools.build.lib.bazel.buildeventservice.client;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.devtools.build.lib.util.Preconditions.checkNotNull;
import static com.google.devtools.build.lib.util.Preconditions.checkState;
import static io.grpc.stub.MetadataUtils.attachHeaders;
import static java.lang.System.getenv;
import static java.nio.file.Files.newInputStream;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.devtools.build.v1.OrderedBuildEvent;
import com.google.devtools.build.v1.PublishBuildEventGrpc;
import com.google.devtools.build.v1.PublishBuildEventGrpc.PublishBuildEventBlockingStub;
import com.google.devtools.build.v1.PublishBuildEventGrpc.PublishBuildEventStub;
import com.google.devtools.build.v1.PublishBuildToolEventStreamResponse;
import com.google.devtools.build.v1.PublishLifecycleEventRequest;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.joda.time.Duration;

/** Implementation of BuildEventServiceClient that uploads data using gRPC. */
public class BuildEventServiceGrpcClient implements BuildEventServiceClient {

  private static final Logger logger =
      Logger.getLogger(BuildEventServiceGrpcClient.class.getName());

  /** Max wait time for a single non-streaming RPC to finish */
  private static final Duration RPC_TIMEOUT = Duration.standardSeconds(15);
  /** See https://developers.google.com/identity/protocols/application-default-credentials * */
  private static final String DEFAULT_APP_CREDENTIALS_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS";
  /** TODO(eduardocolaco): Scope documentation.* */
  private static final String CREDENTIALS_SCOPE =
      "https://www.googleapis.com/auth/cloud-build-service";
  /** See https://cloud.google.com/endpoints/docs/restricting-api-access-with-api-keys-grpc * */
  private static final Metadata.Key<String> API_KEY_HEADER =
      Metadata.Key.of("x-goog-api-key", Metadata.ASCII_STRING_MARSHALLER);

  private final PublishBuildEventStub besAsync;
  private final PublishBuildEventBlockingStub besBlocking;
  private final ManagedChannel channel;
  private final AtomicReference<StreamObserver<OrderedBuildEvent>> streamReference;

  public BuildEventServiceGrpcClient(String serverSpec, String credentialsFile, String apiKey) {
    this(getChannel(serverSpec), getMetadata(apiKey), getCallCredentials(credentialsFile));
  }

  public BuildEventServiceGrpcClient(
      ManagedChannel channel,
      @Nullable Metadata metadata,
      @Nullable CallCredentials callCredentials) {
    this.channel = channel;
    this.besAsync = withMetadataAndCallCredentials(
        PublishBuildEventGrpc.newStub(channel), metadata, callCredentials);
    this.besBlocking = withMetadataAndCallCredentials(
        PublishBuildEventGrpc.newBlockingStub(channel), metadata, callCredentials);
    this.streamReference = new AtomicReference<>(null);
  }

  private static <T extends AbstractStub<T>> T withMetadataAndCallCredentials(
      T stub, @Nullable Metadata metadata, @Nullable CallCredentials callCredentials) {
    stub = metadata != null ? attachHeaders(stub, metadata) : stub;
    stub = callCredentials != null ? stub.withCallCredentials(callCredentials) : stub;
    return stub;
  }

  @Override
  public Status publish(PublishLifecycleEventRequest lifecycleEvent) throws Exception {
    besBlocking
        .withDeadlineAfter(RPC_TIMEOUT.getMillis(), MILLISECONDS)
        .publishLifecycleEvent(lifecycleEvent);
    return Status.OK;
  }

  @Override
  public ListenableFuture<Status> openStream(
      Function<PublishBuildToolEventStreamResponse, Void> ack)
      throws Exception {
    SettableFuture<Status> streamFinished = SettableFuture.create();
    checkState(
        streamReference.compareAndSet(null, createStream(ack, streamFinished)),
        "Starting a new stream without closing the previous one");
    return streamFinished;
  }

  private StreamObserver<OrderedBuildEvent> createStream(
      final Function<PublishBuildToolEventStreamResponse, Void> ack,
      final SettableFuture<Status> streamFinished) {
    return besAsync.publishBuildToolEventStream(
        new StreamObserver<PublishBuildToolEventStreamResponse>() {
          @Override
          public void onNext(PublishBuildToolEventStreamResponse response) {
            ack.apply(response);
          }

          @Override
          public void onError(Throwable t) {
            streamReference.set(null);
            streamFinished.setException(t);
          }

          @Override
          public void onCompleted() {
            streamReference.set(null);
            streamFinished.set(Status.OK);
          }
        });
  }

  @Override
  public void sendOverStream(OrderedBuildEvent buildEvent) throws Exception {
    checkNotNull(streamReference.get(), "Attempting to send over a closed or unopened stream")
        .onNext(buildEvent);
  }

  @Override
  public void closeStream() {
    StreamObserver<OrderedBuildEvent> stream;
    if ((stream = streamReference.getAndSet(null)) != null) {
      stream.onCompleted();
    }
  }

  @Override
  public void abortStream(Status status) {
    StreamObserver<OrderedBuildEvent> stream;
    if ((stream = streamReference.getAndSet(null)) != null) {
      stream.onError(status.asException());
    }
  }

  @Override
  public boolean isStreamActive() {
    return streamReference.get() != null;
  }

  @Override
  public void shutdown() throws InterruptedException {
    this.channel.shutdown();
  }

  /**
   * Returns call credentials read from the specified file (if non-empty) or from
   * env(GOOGLE_APPLICATION_CREDENTIALS) otherwise.
   */
  @Nullable
  private static CallCredentials getCallCredentials(@Nullable String credentialsFile) {
    try {
      if (!isNullOrEmpty(credentialsFile)) {
        return MoreCallCredentials.from(
            GoogleCredentials.fromStream(newInputStream(Paths.get(credentialsFile)))
                .createScoped(ImmutableList.of(CREDENTIALS_SCOPE)));

      } else if (!isNullOrEmpty(getenv(DEFAULT_APP_CREDENTIALS_ENV_VAR))) {
        return MoreCallCredentials.from(
            GoogleCredentials.getApplicationDefault()
                .createScoped(ImmutableList.of(CREDENTIALS_SCOPE)));
      }
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to read credentials", e);
    }
    return null;
  }

  @Nullable
  private static Metadata getMetadata(@Nullable String apiKey) {
    Metadata metadata = new Metadata();
    if (!isNullOrEmpty(apiKey)) {
      metadata.put(API_KEY_HEADER, apiKey);
    }
    return metadata;
  }

  /**
   * Returns a ManagedChannel to the specified server with the specified API KEY injected on its
   * headers (https://cloud.google.com/endpoints/docs/restricting-api-access-with-api-keys-grpc).
   */
  private static ManagedChannel getChannel(String serverSpec) {
    return ManagedChannelBuilder.forTarget(serverSpec).build();
  }
}

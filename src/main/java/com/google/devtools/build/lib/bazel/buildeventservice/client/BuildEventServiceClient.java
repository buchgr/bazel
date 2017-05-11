package com.google.devtools.build.lib.bazel.buildeventservice.client;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.devtools.build.v1.OrderedBuildEvent;
import com.google.devtools.build.v1.PublishBuildToolEventStreamResponse;
import com.google.devtools.build.v1.PublishLifecycleEventRequest;
import io.grpc.Status;

/** Interface used to abstract both gRPC and Stubby BuildEventServiceBackend. */
public interface BuildEventServiceClient {

  /**
   * Makes a synchronous RPC that publishes the specified lifecycle event.
   *
   * @param lifecycleEvent Event to be published.
   * @return Status of the RPC.
   */
  Status publish(PublishLifecycleEventRequest lifecycleEvent) throws Exception;

  /**
   * Starts a new stream with the given ack callback. Throws an {@link IllegalStateException} if the
   * there is already opened stream. Callers should waiting on the returned Future in order to
   * guarantee that all callback calls have been received.
   *
   * @param ackCallback Consumer called every time a ack message is received.
   * @return Listenable future that blocks until the the onDone callback is called.
   * @throws Exception
   */
  ListenableFuture<Status> openStream(
      Function<PublishBuildToolEventStreamResponse, Void> ackCallback) throws Exception;

  /**
   * Sends an event to the most recently opened stream. This method may block due to flow control.
   *
   * @param buildEvent Event that should be sent.
   * @throws Exception
   */
  void sendOverStream(OrderedBuildEvent buildEvent) throws Exception;

  /**
   * Closes the currently opened opened stream. This method does not block. Callers should block on
   * the Future returned by {@link #openStream(Function)} if in order to make sure that all
   * ackCallback calls have been received.
   */
  void closeStream();

  /**
   * Closes the currently opened stream with error. This method does not block. Callers should block
   * on the Future returned by {@link #openStream(Function)} if in order to make sure that all
   * ackCallback calls have been received.
   */
  void abortStream(Status status);

  /**
   * Checks if there is a currently active stream.
   *
   * @return True if the current stream is active, false otherwise.
   */
  boolean isStreamActive();

  /**
   * Called once to dispose resources that this client might be holding (such as thread pools). This
   * should be the last method called on this object.
   *
   * @throws InterruptedException
   */
  void shutdown() throws InterruptedException;
}

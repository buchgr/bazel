package com.google.devtools.build.lib.bazel.buildeventservice;

import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.devtools.build.lib.events.EventKind.ERROR;
import static com.google.devtools.build.lib.events.EventKind.INFO;
import static com.google.devtools.build.lib.events.EventKind.WARNING;
import static com.google.devtools.build.v1.BuildEvent.EventCase.COMPONENT_STREAM_FINISHED;
import static com.google.devtools.build.v1.BuildStatus.Result.COMMAND_FAILED;
import static com.google.devtools.build.v1.BuildStatus.Result.COMMAND_SUCCEEDED;
import static com.google.devtools.build.v1.BuildStatus.Result.UNKNOWN_STATUS;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.devtools.build.lib.blaze.google.buildeventservice.client.BuildEventServiceClient;
import com.google.devtools.build.lib.buildeventstream.ArtifactGroupNamer;
import com.google.devtools.build.lib.buildeventstream.BuildEvent;
import com.google.devtools.build.lib.buildeventstream.BuildEventConverters;
import com.google.devtools.build.lib.buildeventstream.BuildEventStreamProtos;
import com.google.devtools.build.lib.buildeventstream.BuildEventStreamProtos.BuildEvent.PayloadCase;
import com.google.devtools.build.lib.buildeventstream.BuildEventStreamProtos.BuildFinished;
import com.google.devtools.build.lib.buildeventstream.BuildEventTransport;
import com.google.devtools.build.lib.buildeventstream.PathConverter;
import com.google.devtools.build.lib.events.Event;
import com.google.devtools.build.lib.events.EventHandler;
import com.google.devtools.build.lib.events.EventKind;
import com.google.devtools.build.lib.runtime.BlazeModule.ModuleEnvironment;
import com.google.devtools.build.lib.util.AbruptExitException;
import com.google.devtools.build.lib.util.Clock;
import com.google.devtools.build.lib.util.ExitCode;
import com.google.devtools.build.v1.BuildStatus.Result;
import com.google.devtools.build.v1.OrderedBuildEvent;
import com.google.devtools.build.v1.PublishBuildToolEventStreamResponse;
import com.google.devtools.build.v1.PublishLifecycleEventRequest;
import com.google.protobuf.Any;
import io.grpc.Status;
import java.util.Deque;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.Duration;

/** A {@link BuildEventTransport} that streams {@link BuildEvent}s to BuildEventService. */
public class BuildEventServiceTransport implements BuildEventTransport {

  private static final Logger logger = Logger.getLogger(BuildEventServiceTransport.class.getName());

  /** Max blocked wait time for new events from blaze. */
  private static final Duration NO_EVENTS_TIMEOUT = Duration.standardSeconds(120);

  private final ListeningExecutorService uploaderExecutorService;
  private final Duration uploadTimeout;
  private final boolean publishLifecycleEvents;
  private final boolean bestEffortUpload;
  private final BuildEventServiceClient besClient;
  private final BuildEventServiceProtoUtil besProtoUtil;
  private final ModuleEnvironment moduleEnvironment;
  private final EventHandler commandLineReporter;

  private final PathConverter pathConverter;
  // Contains all pendingAck events that might be retried in case of failures
  private ConcurrentLinkedDeque<OrderedBuildEvent> pendingAck;
  // Contains all events should be sent ordered by sequence number.
  private final BlockingDeque<OrderedBuildEvent> pendingSend;
  // Holds the result status of the BuildEventStreamProtos BuildFinished event.
  private Result invocationResult;
  // Used to block until all events have been uploaded
  private ListenableFuture<?> uploadComplete;
  // Used to ensure that the close logic is only invoked once.
  private SettableFuture<Void> shutdownFuture;

  public BuildEventServiceTransport(
      BuildEventServiceClient besClient,
      Duration uploadTimeout,
      boolean bestEffortUpload,
      boolean publishLifecycleEvents,
      String buildRequestId,
      String invocationId,
      ModuleEnvironment moduleEnvironment,
      Clock clock,
      PathConverter pathConverter,
      EventHandler commandLineReporter) {
    this(
        besClient,
        uploadTimeout,
        bestEffortUpload,
        publishLifecycleEvents,
        moduleEnvironment,
        new BuildEventServiceProtoUtil(buildRequestId, invocationId, clock),
        pathConverter,
        commandLineReporter);
  }

  @VisibleForTesting
  BuildEventServiceTransport(
      BuildEventServiceClient besClient,
      Duration uploadTimeout,
      boolean bestEffortUpload,
      boolean publishLifecycleEvents,
      ModuleEnvironment moduleEnvironment,
      BuildEventServiceProtoUtil besProtoUtil,
      PathConverter pathConverter,
      EventHandler commandLineReporter) {
    this.besClient = besClient;
    this.besProtoUtil = besProtoUtil;
    this.publishLifecycleEvents = publishLifecycleEvents;
    this.moduleEnvironment = moduleEnvironment;
    this.commandLineReporter = commandLineReporter;
    this.pendingAck = new ConcurrentLinkedDeque<>();
    this.pendingSend = new LinkedBlockingDeque<>();
    // Setting the thread count to 2 instead of 1 is a hack, but necessary as publishEventStream
    // blocks one thread permanently and thus we can't do any other work on the executor. A proper
    // fix would be to remove the spinning loop from publishEventStream and instead implement the
    // loop by publishEventStream re-submitting itself to the executor.
    // TODO(buchgr): Fix it.
    this.uploaderExecutorService = listeningDecorator(Executors.newFixedThreadPool(2));
    this.pathConverter = pathConverter;
    this.invocationResult = UNKNOWN_STATUS;
    this.uploadTimeout = uploadTimeout;
    this.bestEffortUpload = bestEffortUpload;
  }

  @Override
  public synchronized ListenableFuture<Void> close() {
    if (shutdownFuture != null) {
      return shutdownFuture;
    }
    // The future is completed once the close succeeded or failed.
    shutdownFuture = SettableFuture.create();

    uploaderExecutorService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          sendOrderedBuildEvent(besProtoUtil.streamFinished());
          if (uploadTimeout.isEqual(Duration.ZERO)) {
            report(INFO, "Async Build Event Protocol upload.");
          } else {
            report(INFO, "Waiting for Build Event Protocol upload to finish.");
            try {
              uploadComplete.get(uploadTimeout.getMillis(), MILLISECONDS);
            } catch (Exception e) {
              reportFailure(e);
            }
            report(INFO, "Build Event Protocol upload finished successfully.");
          }
        } finally {
          shutdownFuture.set(null);
          uploaderExecutorService.shutdown();
        }
      }
    });

    return shutdownFuture;
  }

  @Override
  public String name() {
    // TODO(buchgr): Also display the hostname / IP.
    return "Build Event Service";
  }

  @Override
  public synchronized void sendBuildEvent(BuildEvent event, final ArtifactGroupNamer namer) {
    BuildEventStreamProtos.BuildEvent eventProto = event.asStreamProto(
        new BuildEventConverters() {
          @Override
          public PathConverter pathConverter() {
            return pathConverter;
          }
          @Override
          public ArtifactGroupNamer artifactGroupNamer() {
            return namer;
          }
        });
    if (PayloadCase.FINISHED.equals(eventProto.getPayloadCase())) {
      BuildFinished finished = eventProto.getFinished();
      if (finished.hasExitCode() && finished.getExitCode().getCode() == 0) {
        invocationResult = COMMAND_SUCCEEDED;
      } else {
        invocationResult = COMMAND_FAILED;
      }
    }

    sendOrderedBuildEvent(besProtoUtil.bazelEvent(Any.pack(eventProto)));
  }

  private void reportFailure(Exception e) {
    if (bestEffortUpload) {
      String msg =
          format("Build Event Protocol upload failed in best-effort mode: %s", e.getMessage());
      logger.log(Level.WARNING, msg, e);
      report(WARNING, msg);
    } else {
      String msg = format("Build Event Protocol upload failed: %s", e.getMessage());
      logger.log(Level.SEVERE, msg, e);
      report(ERROR, msg);
      moduleEnvironment.exit(new AbruptExitException(ExitCode.BLAZE_INTERNAL_ERROR, e));
    }
  }

  private synchronized void sendOrderedBuildEvent(OrderedBuildEvent serialisedEvent) {
    pendingSend.add(serialisedEvent);
    if (uploadComplete == null) {
      uploadComplete = uploaderExecutorService.submit(new BuildEventServiceUpload());
    }
  }

  private synchronized Result getInvocationResult() {
    return invocationResult;
  }

  /**
   * Method responsible for sending all requests to BuildEventService.
   */
  private class BuildEventServiceUpload implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      try {
        publishBuildEnqueuedEvent();
        publishInvocationStartedEvent();
        try {
          publishEventStream0();
        } finally {
          Result result = getInvocationResult();
          publishInvocationFinishedEvent(result);
          publishBuildFinishedEvent(result);
        }
      } finally {
        besClient.shutdown();
      }
      return null;
    }

    private void publishBuildEnqueuedEvent() throws Exception {
      retryOnException(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          publishLifecycleEvent(besProtoUtil.buildEnqueued());
          return null;
        }
      });
    }

    private void publishInvocationStartedEvent() throws Exception {
      retryOnException(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          publishLifecycleEvent(besProtoUtil.invocationStarted());
          return null;
        }
      });
    }

    private void publishEventStream0() throws Exception {
      retryOnException(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          publishEventStream();
          return null;
        }
      });
    }

    private void publishInvocationFinishedEvent(final Result result) throws Exception {
      retryOnException(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          publishLifecycleEvent(besProtoUtil.invocationFinished(result));
          return null;
        }
      });
    }

    private void publishBuildFinishedEvent(final Result result) throws Exception {
      retryOnException(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          publishLifecycleEvent(besProtoUtil.buildFinished(result));
          return null;
        }
      });
    }
  }

  /** Used as method reference, responsible for publishing lifecycle evnts RPC. Safe to retry. */
  private Status publishLifecycleEvent(PublishLifecycleEventRequest request) throws Exception {
    if (publishLifecycleEvents) {
      // Change the status based on BEP data
      return besClient.publish(request);
    }
    return Status.OK;
  }

  /**
   * Used as method reference, responsible for the entire Streaming RPC. Safe to retry. This method
   * it carries states between consecutive calls (pendingAck messages will be added to the head of
   * of the pendingSend queue), but that is intended behavior.
   */
  private Status publishEventStream() throws Exception {
    // reschedule unacked messages if required, keeping its original order.
    OrderedBuildEvent unacked;
    while ((unacked = pendingAck.pollLast()) != null) {
      pendingSend.addFirst(unacked);
    }
    pendingAck = new ConcurrentLinkedDeque<>();

    return publishEventStream(pendingAck, pendingSend, besClient)
        .get(NO_EVENTS_TIMEOUT.getMillis(), MILLISECONDS);
  }

  /**
   * Method responsible for a single Streaming RPC.
   */
  private static ListenableFuture<Status> publishEventStream(
      final ConcurrentLinkedDeque<OrderedBuildEvent> pendingAck,
      final BlockingDeque<OrderedBuildEvent> pendingSend,
      final BuildEventServiceClient besClient)
      throws Exception {
    OrderedBuildEvent event;
    ListenableFuture<Status> streamDone = besClient.openStream(ackCallback(pendingAck, besClient));
    try {
      do {
        verify(
            (event = pendingSend.poll(NO_EVENTS_TIMEOUT.getMillis(), MILLISECONDS)) != null,
            "Timed out waiting for events to send (%s).",
            NO_EVENTS_TIMEOUT);
        pendingAck.add(event);
        besClient.sendOverStream(event);
      } while (!isLastEvent(event));
      besClient.closeStream();
    } catch (Exception e) {
      logger.log(Level.WARNING, "Aborting publishEventStream.", e);
      besClient.abortStream(Status.INTERNAL.augmentDescription(e.getMessage()));
    }
    return streamDone;
  }

  private static boolean isLastEvent(OrderedBuildEvent event) {
    return event != null && event.getEvent().getEventCase() == COMPONENT_STREAM_FINISHED;
  }

  private static Function<PublishBuildToolEventStreamResponse, Void> ackCallback(
      final Deque<OrderedBuildEvent> pendingAck, final BuildEventServiceClient besClient) {
    return new Function<PublishBuildToolEventStreamResponse, Void>() {
      @Override
      public Void apply(PublishBuildToolEventStreamResponse ack) {
        long pendingSeq =
            pendingAck.isEmpty() ? -1 : pendingAck.peekFirst().getSequenceNumber();
        long ackSeq = ack.getSequenceNumber();
        if (pendingSeq != ackSeq) {
          besClient.abortStream(Status.INTERNAL
            .augmentDescription(format("Expected ack %s but was %s.", pendingSeq, ackSeq)));
        } else {
          pendingAck.removeFirst();
        }
        return null;
      }
    };
  }

  private void retryOnException(Callable<?> c) throws Exception {
    retryOnException(c, 3, 100);
  }

  /**
   * Executes a {@link Callable} retrying on exception thrown.
   */
  // TODO(eduardocolaco): Implement transient/persistent failures
  private void retryOnException(Callable<?> c, final int maxRetries, final long initalDelayMillis)
      throws Exception {
    int tries = 0;
    Exception lastThrown = null;
    while (tries <= maxRetries) {
      try {
        c.call();
        return;
      } catch (Exception e) {
        tries++;
        lastThrown = e;
        /*
         * Exponential backoff:
         * Retry 1: initalDelayMillis * 2^0
         * Retry 2: initalDelayMillis * 2^1
         * Retry 3: initalDelayMillis * 2^2
         * ...
         */
        long sleepMillis = initalDelayMillis << (tries - 1);
        parkNanos(TimeUnit.MILLISECONDS.toNanos(sleepMillis));
      }
    }
    Preconditions.checkNotNull(lastThrown);
    throw lastThrown;
  }

  private void report(EventKind eventKind, String msg, Object... parameters) {
    commandLineReporter.handle(Event.of(eventKind, null, format(msg, parameters)));
  }
}

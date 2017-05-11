package com.google.devtools.build.lib.bazel.buildeventservice;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.devtools.build.lib.blaze.google.buildeventservice.client.BuildEventServiceClient;
import com.google.devtools.build.lib.buildeventstream.BuildEventTransport;
import com.google.devtools.build.lib.events.Event;
import com.google.devtools.build.lib.events.EventHandler;
import com.google.devtools.build.lib.runtime.BlazeModule;
import com.google.devtools.build.lib.runtime.BuildEventStreamer;
import com.google.devtools.build.lib.runtime.Command;
import com.google.devtools.build.lib.runtime.CommandEnvironment;
import com.google.devtools.build.lib.util.AbruptExitException;
import com.google.devtools.build.lib.util.Clock;
import com.google.devtools.build.lib.util.ExitCode;
import com.google.devtools.common.options.OptionsBase;
import com.google.devtools.common.options.OptionsProvider;
import java.util.UUID;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Module responsible for the {@link BuildEventTransport} and its {@link BuildEventStreamer}.
 *
 * Implementors of this class have to overwrite {@link #optionsClass()} and
 * {@link #createBesClient(BuildEventServiceOptions)}.
 */
public abstract class BuildEventServiceModule<T extends BuildEventServiceOptions>
    extends BlazeModule {

  private static final Logger logger = Logger.getLogger(BuildEventServiceModule.class.getName());

  private CommandEnvironment commandEnvironment;

  @Override
  public Iterable<Class<? extends OptionsBase>> getCommandOptions(Command command) {
    return ImmutableList.of(optionsClass());
  }

  @Override
  public void beforeCommand(Command command, CommandEnvironment commandEnvironment)
      throws AbruptExitException {
    this.commandEnvironment = commandEnvironment;
  }

  @Override
  public void handleOptions(OptionsProvider optionsProvider) {
    checkState(commandEnvironment != null, "Methods called out of order");
    BuildEventStreamer streamer =
        tryCreateStreamer(
            optionsProvider,
            commandEnvironment.getReporter(),
            commandEnvironment.getBlazeModuleEnvironment(),
            commandEnvironment.getRuntime().getClock(),
            commandEnvironment.getClientEnv().get("BAZEL_INTERNAL_BUILD_REQUEST_ID"),
            commandEnvironment.getCommandId().toString());
    if (streamer != null) {
      commandEnvironment.getReporter().addHandler(streamer);
      commandEnvironment.getEventBus().register(streamer);
      logger.fine("BuildEventStreamer created and registered successfully.");
    }
  }

  /**
   * Returns {@code null} if no stream could be created.
   *
   * @param buildRequestId  if {@code null} or {@code ""} a random UUID is used instead.
   */
  @Nullable
  private BuildEventStreamer tryCreateStreamer(
      OptionsProvider optionsProvider,
      EventHandler commandLineReporter,
      ModuleEnvironment moduleEnvironment,
      Clock clock,
      String buildRequestId,
      String invocationId) {
    try {
      T besOptions =
          checkNotNull(
              optionsProvider.getOptions(optionsClass()),
              "Could not get BuildEventServiceOptions");
      if (isNullOrEmpty(besOptions.besBackend)) {
        logger.fine("BuildEventServiceTransport is disabled.");
      } else {
        logger.fine(format("Will create BuildEventServiceTransport streaming to '%s'",
            besOptions.besBackend));

        buildRequestId = isNullOrEmpty(buildRequestId)
            ? UUID.randomUUID().toString()
            : buildRequestId;
        commandLineReporter.handle(
            Event.info(
                format(
                    "Streaming Build Event Protocol to %s build_request_id: %s invocation_id: %s",
                    besOptions.besBackend, buildRequestId, invocationId)));
        BuildEventServiceTransport besTransport =
            new BuildEventServiceTransport(
                createBesClient(besOptions),
                besOptions.besUploadTimeout,
                besOptions.besUploadBestEffort,
                besOptions.besPublishLifecycleEvents,
                buildRequestId,
                invocationId,
                moduleEnvironment,
                clock,
                commandEnvironment.getRuntime().getPathToUriConverter(),
                commandLineReporter);
        logger.fine("BuildEventServiceTransport was created successfully");
        return new BuildEventStreamer(ImmutableSet.of(besTransport),
            commandEnvironment.getReporter());
      }
    } catch (Exception e) {
      moduleEnvironment.exit(new AbruptExitException(ExitCode.BLAZE_INTERNAL_ERROR, e));
    }
    return null;
  }

  protected abstract Class<T> optionsClass();

  protected abstract BuildEventServiceClient createBesClient(T besOptions);
}

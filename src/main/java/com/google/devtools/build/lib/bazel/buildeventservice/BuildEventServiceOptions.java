package com.google.devtools.build.lib.bazel.buildeventservice;

import com.google.devtools.common.options.Converter;
import com.google.devtools.common.options.Option;
import com.google.devtools.common.options.OptionsBase;
import com.google.devtools.common.options.OptionsParser.OptionUsageRestrictions;
import com.google.devtools.common.options.OptionsParsingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.joda.time.Duration;

/** Options used by {@link BuildEventServiceModule}. */
public class BuildEventServiceOptions extends OptionsBase {

  @Option(
    name = "experimental_bes_backend",
    defaultValue = "",
    optionUsageRestrictions = OptionUsageRestrictions.HIDDEN,
    help =
        "If non-empty, sends the build event protocol to the specified BES server (defaults to '')."
  )
  public String besBackend;

  @Option(
    name = "experimental_bes_upload_timeout",
    defaultValue = "0s",
    optionUsageRestrictions = OptionUsageRestrictions.HIDDEN,
    converter = DurationConverter.class,
    help =
        "Specifies how long Blaze should wait for the BES upload to finish. A unit must be "
            + "specified: Days (d), hours (h), minutes (m), seconds (s), and "
            + "milliseconds (ms). A duration of '0' means the upload is done in the background "
            + "(defaults to '0s')."
  )
  public Duration besUploadTimeout;

  @Option(
    name = "experimental_bes_upload_best_effort",
    defaultValue = "true",
    optionUsageRestrictions = OptionUsageRestrictions.HIDDEN,
    help =
        "Specifies if the BES upload is treated as best-effort, or if failures should interrupt "
            + "the current invocation with GoogleExitCode.PUBLISH_ERROR. (defaults to 'true')"
  )
  public boolean besUploadBestEffort;

  @Option(
    name = "experimental_bes_upload_credentials",
    defaultValue = "",
    optionUsageRestrictions = OptionUsageRestrictions.HIDDEN,
    help =
        "Specifies the credentials file to be used to authenticate with BES backends. If empty, "
            + "it uses the default application credentials (see "
            + "https://developers.google.com/identity/protocols/OAuth2ServiceAccount and "
            + "https://developers.google.com/identity/protocols/application-default-credentials "
            + "for details). (defaults to '')"
  )
  public String besUploadCredentialsFile;

  @Option(
    name = "experimental_bes_upload_api_key",
    defaultValue = "",
    optionUsageRestrictions = OptionUsageRestrictions.HIDDEN,
    help =
        "Specifies the api key to be used when interacting with BES backends. See "
            + "https://cloud.google.com/endpoints/docs/restricting-api-access-with-api-keys-grpc "
            + "(defaults to '')"
  )
  public String besUploadApiKey;

  @Option(
    name = "experimental_bes_publish_lifecycle_events",
    defaultValue = "true",
    optionUsageRestrictions = OptionUsageRestrictions.HIDDEN,
    help = "Specifies whether to publish BES lifecyle events. (defaults to 'true')"
  )
  public boolean besPublishLifecycleEvents;

  /**
   * Simple String to Duration Converter.
   */
  public static class DurationConverter implements Converter<Duration> {

    private final Pattern durationRegex = Pattern.compile("^([0-9]+)(d|h|m|s|ms)$");

    @Override
    public Duration convert(String input) throws OptionsParsingException {
      // To be compatible with the previous parser, '0' doesn't need a unit.
      if ("0".equals(input)) {
        return Duration.ZERO;
      }
      Matcher m = durationRegex.matcher(input);
      if (!m.matches()) {
        throw new OptionsParsingException("Illegal duration '" + input + "'.");
      }
      long duration = Long.parseLong(m.group(1));
      String unit = m.group(2);
      switch(unit) {
        case "d":
          return Duration.standardDays(duration);
        case "h":
          return Duration.standardHours(duration);
        case "m":
          return Duration.standardMinutes(duration);
        case "s":
          return Duration.standardSeconds(duration);
        case "ms":
          return Duration.millis(duration);
        default:
          throw new IllegalStateException("This must not happen. Did you update the regex without "
              + "the switch case?");
      }
    }

    @Override
    public String getTypeDescription() {
      return "An immutable length of time.";
    }
  }

}

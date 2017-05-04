package com.google.devtools.build.lib.bazel.buildeventservice;

/**
 * Bazel's {@link BuildEventServiceModule}.
 */
public class BazelBuildEventServiceModule extends BuildEventServiceModule<BuildEventServiceOptions> {

  @Override
  protected Class<BuildEventServiceOptions> optionsClass() {
    return BuildEventServiceOptions.class;
  }
}

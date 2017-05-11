package com.google.devtools.build.lib.bazel.buildeventservice;

import com.google.devtools.build.lib.bazel.buildeventservice.client.BuildEventServiceClient;
import com.google.devtools.build.lib.bazel.buildeventservice.client.BuildEventServiceGrpcClient;

/**
 * Created by buchgr on 5/11/17.
 */
public class BazelBuildEventServiceModule extends BuildEventServiceModule<BuildEventServiceOptions> {

  @Override
  protected Class<BuildEventServiceOptions> optionsClass() {
    return BuildEventServiceOptions.class;
  }

  @Override
  protected BuildEventServiceClient createBesClient(BuildEventServiceOptions besOptions) {
    return new BuildEventServiceGrpcClient(
        besOptions.besBackend, besOptions.besUploadCredentialsFile, besOptions.besUploadApiKey);
  }
}

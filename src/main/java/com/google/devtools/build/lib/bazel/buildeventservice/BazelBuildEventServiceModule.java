package com.google.devtools.build.lib.bazel.buildeventservice;

import com.google.devtools.build.lib.bazel.buildeventservice.client.BuildEventServiceClient;
import com.google.devtools.build.lib.bazel.buildeventservice.client.BuildEventServiceGrpcClient;
import com.google.devtools.build.lib.runtime.AuthAndTLSOptions;

/**
 * Created by buchgr on 5/11/17.
 */
public class BazelBuildEventServiceModule extends BuildEventServiceModule<BuildEventServiceOptions> {

  @Override
  protected Class<BuildEventServiceOptions> optionsClass() {
    return BuildEventServiceOptions.class;
  }

  @Override
  protected BuildEventServiceClient createBesClient(BuildEventServiceOptions besOptions,
      AuthAndTLSOptions authTlsOptions) {
    return new BuildEventServiceGrpcClient(
        besOptions.besBackend, authTlsOptions.tlsEnabled, authTlsOptions.tlsCertificate,
        authTlsOptions.tlsAuthorityOverride, authTlsOptions.authCredentials,
        authTlsOptions.authScope);
  }
}

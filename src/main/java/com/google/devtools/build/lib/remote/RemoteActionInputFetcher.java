package com.google.devtools.build.lib.remote;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ActionInputPrefetcher;
import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.FileArtifactValue;
import com.google.devtools.build.lib.actions.MetadataProvider;
import com.google.devtools.build.lib.profiler.Profiler;
import com.google.devtools.build.lib.profiler.SilentCloseable;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.remote.util.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Stages files stored remotely on the local filesystem.
 *
 * <p>This is necessary for remote caching/execution when --experimental_remote_fetch_outputs
 * is set to none.
 */
public class RemoteActionInputFetcher implements ActionInputPrefetcher {

  private final AbstractRemoteActionCache remoteCache;

  public RemoteActionInputFetcher(AbstractRemoteActionCache remoteCache) {
    this.remoteCache = Preconditions.checkNotNull(remoteCache);
  }

  @Override
  public void prefetchFiles(Iterable<? extends ActionInput> inputs,
      MetadataProvider metadataProvider) throws IOException, InterruptedException {
    try (SilentCloseable c = Profiler.instance().profile("Remote.downloadInputs")) {
      List<ListenableFuture<Artifact>> downloads = new ArrayList<>();
      for (ActionInput input : inputs) {
        if (!(input instanceof Artifact)) {
          // If it's not an artifact it can't be stored remotely.
          continue;
        }
        Artifact artifact = (Artifact) input;
        FileArtifactValue metadata = Preconditions
            .checkNotNull(metadataProvider.getMetadata(input));
        // Download remote output files.
        if (metadata.isRemote() && artifact.getRoot().isOutputRoot()) {
          downloads.add(Futures.transform(remoteCache.downloadFile(artifact.getPath(),
              DigestUtil.buildDigest(metadata.getDigest(), metadata.getSize())), (none) -> artifact,
              MoreExecutors.directExecutor()));
        }
      }

      for (ListenableFuture<Artifact> download : downloads) {
        Utils.getFromFuture(download);
      }
    }
  }
}

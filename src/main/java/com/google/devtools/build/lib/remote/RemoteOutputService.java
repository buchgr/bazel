package com.google.devtools.build.lib.remote;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.devtools.build.lib.actions.Action;
import com.google.devtools.build.lib.actions.ActionInputMap;
import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.Artifact.SourceArtifact;
import com.google.devtools.build.lib.actions.ExecException;
import com.google.devtools.build.lib.actions.FilesetOutputSymlink;
import com.google.devtools.build.lib.actions.MetadataConsumer;
import com.google.devtools.build.lib.actions.UserExecException;
import com.google.devtools.build.lib.actions.cache.MetadataHandler;
import com.google.devtools.build.lib.events.EventHandler;
import com.google.devtools.build.lib.vfs.BatchStat;
import com.google.devtools.build.lib.vfs.FileSystem;
import com.google.devtools.build.lib.vfs.ModifiedFileSet;
import com.google.devtools.build.lib.vfs.OutputService;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.devtools.build.lib.vfs.Root;
import com.google.devtools.build.skyframe.SkyFunction.Environment;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Function;
import javax.annotation.Nullable;

public class RemoteOutputService implements OutputService {

  private RemoteActionInputFetcher actionInputFetcher;

  void setActionInputFetcher(RemoteActionInputFetcher actionInputFetcher) {
    this.actionInputFetcher = Preconditions.checkNotNull(actionInputFetcher);
  }

  @Override
  public ActionFileSystemSupport supportsActionFileSystem() {
    return ActionFileSystemSupport.STAGE_REMOTE_FIlES;
  }

  @Nullable
  @Override
  public FileSystem createActionFileSystem(FileSystem sourceDelegate, PathFragment execRootFragment,
      String relativeOutputPath, ImmutableList<Root> sourceRoots, ActionInputMap inputArtifactData,
      Iterable<Artifact> outputArtifacts,
      Function<PathFragment, SourceArtifact> sourceArtifactFactory) {
    Preconditions.checkNotNull(actionInputFetcher);
    return new RemoteActionFileSystem(sourceDelegate, execRootFragment, inputArtifactData,
        actionInputFetcher);
  }

  @Override
  public void updateActionFileSystemContext(FileSystem actionFileSystem, Environment env,
      MetadataConsumer consumer,
      ImmutableMap<Artifact, ImmutableList<FilesetOutputSymlink>> filesets) {
    ((RemoteActionFileSystem) actionFileSystem).updateActionFileSystemContext(consumer);
  }

  @Override
  public String getFilesSystemName() {
    return "remoteActionFS";
  }

  @Override
  public ModifiedFileSet startBuild(EventHandler eventHandler, UUID buildId,
      boolean finalizeActions) {
    return ModifiedFileSet.EVERYTHING_MODIFIED;
  }

  @Override
  public void finalizeBuild(boolean buildSuccessful) {
    // Intentionally left empty.
  }

  @Override
  public void finalizeAction(Action action, MetadataHandler metadataHandler) {
    // Intentionally left empty.
  }

  @Override
  public BatchStat getBatchStatter() {
    return null;
  }

  @Override
  public boolean canCreateSymlinkTree() {
    return false;
  }

  @Override
  public void createSymlinkTree(Path inputManifest, Path outputManifest, boolean filesetTree,
      PathFragment symlinkTreeRoot) throws ExecException  {
    try {
      // Just create the output marker to "complete" the symlink creation.
      outputManifest.createSymbolicLink(inputManifest);
    } catch (IOException e) {
      throw new UserExecException(e);
    }
  }

  @Override
  public void clean() {
    // Intentionally left empty.
  }

  @Override
  public boolean isRemoteFile(Artifact file) {
    return false;
  }
}

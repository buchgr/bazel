package com.google.devtools.build.lib.remote;

import com.google.common.base.Preconditions;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ActionInputMap;
import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.FileArtifactValue;
import com.google.devtools.build.lib.actions.MetadataConsumer;
import com.google.devtools.build.lib.vfs.DelegateFileSystem;
import com.google.devtools.build.lib.vfs.FileSystem;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;

public class RemoteActionFileSystem extends DelegateFileSystem {

  private final Path execRoot;
  private final ActionInputMap inputArtifactData;
  private final RemoteActionInputFetcher inputFetcher;
  private MetadataConsumer metadataInjector;

  public RemoteActionFileSystem(FileSystem localDelegate,
      PathFragment execRootFragment,
      ActionInputMap inputArtifactData,
      RemoteActionInputFetcher inputFetcher) {
    super(localDelegate);
    this.execRoot = getPath(Preconditions.checkNotNull(execRootFragment, "execRootFragment"));
    this.inputArtifactData = Preconditions.checkNotNull(inputArtifactData, "inputArtifactData");
    this.inputFetcher = Preconditions.checkNotNull(inputFetcher, "inputFetcher");
  }

  void updateActionFileSystemContext(MetadataConsumer metadataInjector) {
    this.metadataInjector = metadataInjector;
  }

   @Nullable
   private FileArtifactValue getInputMetadata(Path path) {
     PathFragment relativeToExecRoot = path.relativeTo(execRoot);
     return inputArtifactData.getMetadata(relativeToExecRoot.getPathString());
   }

   private ActionInput getInputOrFail(Path path) throws IOException {
     PathFragment relativeToExecRoot = path.relativeTo(execRoot);
     ActionInput input = inputArtifactData.getInput(relativeToExecRoot.getPathString());
     if (input == null) {
       throw new IOException(String.format("Unknown input '%s'", path));
     }
     return input;
   }

   @Override
   protected InputStream getInputStream(Path path) throws IOException {
     FileArtifactValue metadata = getInputMetadata(path);
     if (metadata != null && metadata.isRemote()) {
       System.err.println("Staging " + path);
       try {
         inputFetcher.downloadFile(delegatePath(path), metadata);
       } catch (InterruptedException e) {
         throw new IOException(String.format("Received interrupt while fetching file '%s'",
             path), e);
       }
     }
     return super.getInputStream(path);
   }

   @Override
   protected void createSymbolicLink(Path linkPath, PathFragment targetFragment) throws IOException {

     Path targetPath = execRoot.getRelative(targetFragment);
     FileArtifactValue targetMetadata = getInputMetadata(targetPath);
     if (targetMetadata != null && targetMetadata.isRemote()) {
       System.err.println("Injecting symbolic link from " + linkPath + " to " + targetFragment);
       // The target is a file that's stored remotely. Don't physically create the symlink on disk
       // but only inject the metadata into skyframe.
       metadataInjector.accept((Artifact) getInputOrFail(targetPath), targetMetadata);
       return;
     }
     // Invariant: targetFragment is a local file/directory
     super.createSymbolicLink(linkPath, targetFragment);
   }
}

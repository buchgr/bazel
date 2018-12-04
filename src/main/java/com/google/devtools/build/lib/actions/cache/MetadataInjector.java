package com.google.devtools.build.lib.actions.cache;

import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.Artifact.SpecialArtifact;
import com.google.devtools.build.lib.actions.FileArtifactValue.RemoteFileArtifactValue;
import com.google.devtools.build.lib.vfs.PathFragment;
import java.util.Map;

/**
 * Allows to inject metadata about an actions outputs into SkyFrame.
 */
public interface MetadataInjector {

  /**
   * Injects metadata of a file that is stored remotely.
   *
   * @param file  a regular output file.
   * @param digest  the digest of the file.
   * @param sizeBytes the size of the file in bytes.
   * @param locationIndex is only used in Blaze.
   */
  default void injectRemoteFile(Artifact file, byte[] digest, long sizeBytes, int locationIndex) {
    throw new UnsupportedOperationException();
  }

  /**
   * Inject the metadata of a tree artifact whose contents are stored remotely.
   *
   * @param treeArtifact  an output directory.
   * @param children  the metadata of the files stored in the directory.
   */
  default void injectRemoteDirectory(SpecialArtifact treeArtifact,
      Map<PathFragment, RemoteFileArtifactValue> children) {
    throw new UnsupportedOperationException();
  }
}

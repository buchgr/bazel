package com.google.devtools.build.lib.remote.merkletree;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.MetadataProvider;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import javax.annotation.Nullable;

public class MerkleTree {

  private final Map<Digest, Directory> digestDirectoryMap;
  private final Map<Digest, ActionInput> digestActionInputMap;
  private final Digest rootDigest;

  private MerkleTree(Map<Digest, Directory> digestDirectoryMap, Map<Digest, ActionInput>
      digestActionInputMap, Digest rootDigest) {
    this.digestDirectoryMap = digestDirectoryMap;
    this.digestActionInputMap = digestActionInputMap;
    this.rootDigest = rootDigest;
  }

  public Digest getRootDigest() {
    return rootDigest;
  }

  @Nullable
  public Directory getDirectoryByDigest(Digest digest) {
    return digestDirectoryMap.get(digest);
  }

  @Nullable
  public ActionInput getInputByDigest(Digest digest) {
    return digestActionInputMap.get(digest);
  }

  /**
   * Returns the hashes of all nodes and leafs of the merkle tree. That is, the hashes of the
   * {@link Directory} protobufs and {@link ActionInput} files.
   */
  public Iterable<Digest> getAllDigests() {
    return Iterables.concat(digestDirectoryMap.keySet(), digestActionInputMap.keySet());
  }

  public static MerkleTree build(SortedMap<PathFragment, ActionInput> inputs,
      MetadataProvider metadataProvider,
      Path execRoot,
      DigestUtil digestUtil) throws IOException {
    InputTree tree =
        InputTree.build(inputs, metadataProvider, execRoot, digestUtil);
    return MerkleTree.build(tree, digestUtil);
  }

  public static MerkleTree build(InputTree tree, DigestUtil digestUtil) {
    Preconditions.checkNotNull(tree);
    Map<Digest, Directory> digestDirectoryMap = new HashMap<>(tree.numDirectories());
    Map<Digest, ActionInput> digestActionInputMap = new HashMap<>(tree.numFiles());
    Map<PathFragment, Digest> m = new HashMap<>();
    tree.visit((dirname, files, dirs) -> {
      Directory.Builder b = Directory.newBuilder();
      for (InputTree.FileNode file : files) {
        b.addFiles(buildProto(file));
        digestActionInputMap.put(file.getDigest(), file.getActionInput());
      }
      for (InputTree.DirectoryNode dir : dirs) {
        PathFragment subDirname = dirname.getRelative(dir.getPathSegment());
        Digest protoDirDigest = Preconditions.checkNotNull(m.remove(subDirname), "protoDirDigest");
        b.addDirectories(buildProto(dir, protoDirDigest));
      }
      Directory protoDir = b.build();
      Digest protoDirDigest = digestUtil.compute(protoDir);
      digestDirectoryMap.put(protoDirDigest, protoDir);
      m.put(dirname, protoDirDigest);
    });
    return new MerkleTree(digestDirectoryMap, digestActionInputMap,
        m.get(PathFragment.EMPTY_FRAGMENT));
  }

  private static FileNode buildProto(InputTree.FileNode file) {
    return FileNode.newBuilder()
        .setName(file.getPathSegment()).setDigest(file.getDigest()).setIsExecutable(true).build();
  }

  private static DirectoryNode buildProto(InputTree.DirectoryNode dir, Digest protoDirDigest) {
    return DirectoryNode.newBuilder()
        .setName(dir.getPathSegment()).setDigest(protoDirDigest).build();
  }
}

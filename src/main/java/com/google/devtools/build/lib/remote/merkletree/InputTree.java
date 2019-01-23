package com.google.devtools.build.lib.remote.merkletree;

import build.bazel.remote.execution.v2.Digest;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ActionInputHelper;
import com.google.devtools.build.lib.actions.FileArtifactValue;
import com.google.devtools.build.lib.actions.MetadataProvider;
import com.google.devtools.build.lib.actions.cache.VirtualActionInput;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.vfs.Dirent;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.devtools.build.lib.vfs.Symlinks;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

class InputTree {

  public interface Visitor {
    void visitDirectory(PathFragment dirname, List<FileNode> files, List<DirectoryNode> dirs);
  }

  public abstract static class Node {
    private final String pathSegment;

    public Node(String pathSegment) {
      this.pathSegment = Preconditions.checkNotNull(pathSegment);
    }

    public String getPathSegment() {
      return pathSegment;
    }
  }

  public static class FileNode extends Node {
    private final ActionInput input;
    private final Digest digest;

    public FileNode(String pathSegment, ActionInput input, Digest digest) {
      super(pathSegment);
      this.input = Preconditions.checkNotNull(input, "input");
      this.digest = Preconditions.checkNotNull(digest, "digest");
    }

    public Digest getDigest() {
      return digest;
    }

    public ActionInput getActionInput() {
      return input;
    }
  }

  public static class DirectoryNode extends Node {
    private final List<Node> children = new ArrayList<>();

    public DirectoryNode(String pathSegment) {
      super(pathSegment);
    }

    public void addChild(Node child) {
      children.add(Preconditions.checkNotNull(child, "child"));
    }
  }

  private final Map<PathFragment, DirectoryNode> tree;
  private final int numFiles;

  private InputTree(Map<PathFragment, DirectoryNode> tree, int numFiles) {
    Preconditions.checkState(numFiles >= 0, "numFiles must gte 0");
    this.tree = Preconditions.checkNotNull(tree, "tree");
    this.numFiles = numFiles;
  }

  public int numDirectories() {
    return tree.size();
  }

  public int numFiles() {
    return numFiles;
  }

  /** Traverses the {@link InputTree} in a depth first search manner. */
  public void visit(Visitor visitor) {
    Preconditions.checkNotNull(visitor, "visitor");
    visit(visitor, PathFragment.EMPTY_FRAGMENT);
  }

  private void visit(Visitor visitor, PathFragment dirname) {
    DirectoryNode dir = tree.get(dirname);
    if (dir == null) {
      return;
    }

    List<FileNode> files = new ArrayList<>(dir.children.size());
    List<DirectoryNode> dirs = new ArrayList<>();
    for (Node child : dir.children) {
      if (child instanceof FileNode) {
        files.add((FileNode) child);
      } else if (child instanceof DirectoryNode) {
        dirs.add((DirectoryNode) child);
        visit(visitor, dirname.getRelative(child.pathSegment));
      }
    }
    visitor.visitDirectory(dirname, files, dirs);
  }

  @Override
  public String toString() {
    Map<PathFragment, StringBuilder> m = new HashMap<>();
    visit((dirname, files, dirs) -> {
          int depth = dirname.segmentCount() - 1;
          StringBuilder sb = new StringBuilder();

          if (!dirname.equals(PathFragment.EMPTY_FRAGMENT)) {
            sb.append(Strings.repeat("  ", depth));
            sb.append(dirname.getBaseName());
            sb.append("\n");
          }
          if (!files.isEmpty()) {
            for (FileNode file : files) {
              sb.append(Strings.repeat("  ", depth + 1));
              sb.append(formatFile(file));
              sb.append("\n");
            }
          }
          if (!dirs.isEmpty()) {
            for (DirectoryNode dir : dirs) {
              sb.append(m.remove(dirname.getRelative(dir.getPathSegment())));
            }
          }
          m.put(dirname, sb);
    });
    return m.get(PathFragment.EMPTY_FRAGMENT).toString();
  }

  private static String formatFile(FileNode file) {
    return String.format("%s (hash: %s, size: %d)", file.getPathSegment(), file.digest.getHash(),
        file.digest.getSizeBytes());
  }

  public static InputTree build(
      SortedMap<PathFragment, ActionInput> inputs,
      MetadataProvider metadataProvider,
      Path execRoot,
      DigestUtil digestUtil)
      throws IOException {
    Map<PathFragment, DirectoryNode> tree = new HashMap<>();
    int numFiles = build(inputs, metadataProvider, execRoot, digestUtil, tree);
    return new InputTree(tree, numFiles);
  }

  private static int build(
      SortedMap<PathFragment, ActionInput> inputs,
      MetadataProvider metadataProvider,
      Path execRoot,
      DigestUtil digestUtil,
      Map<PathFragment, DirectoryNode> tree)
      throws IOException {
    if (inputs.isEmpty()) {
      return 0;
    }

    PathFragment dirname = null;
    DirectoryNode dir = null;
    int numFiles = inputs.size();
    for (Map.Entry<PathFragment, ActionInput> e : inputs.entrySet()) {
      PathFragment path = e.getKey();
      ActionInput input = e.getValue();
      if (dirname == null || !path.getParentDirectory().equals(dirname)) {
        dirname = path.getParentDirectory();
        dir = tree.get(dirname);
        if (dir == null) {
          dir = new DirectoryNode(dirname.getBaseName());
          tree.put(dirname, dir);
          createParentDirectoriesIfNotExist(dirname, dir, tree);
        }
      }

      String basename = dirname.getBaseName();

      if (input instanceof VirtualActionInput) {
        Digest d = digestUtil.compute((VirtualActionInput) input);
        dir.addChild(new FileNode(basename, input, d));
        continue;
      }

      FileArtifactValue metadata =
          Preconditions.checkNotNull(metadataProvider.getMetadata(input), "metadata");
      if (metadata.getType().isDirectory()) {
        SortedMap<PathFragment, ActionInput> directoryInputs = explodeDirectory(path, execRoot);
        numFiles += build(directoryInputs, metadataProvider, execRoot, digestUtil, tree);
      } else {
        Digest d = DigestUtil.buildDigest(metadata.getDigest(), metadata.getSize());
        dir.addChild(new FileNode(path.getBaseName(), input, d));
      }
    }
    return numFiles;
  }

  private static SortedMap<PathFragment, ActionInput> explodeDirectory(PathFragment dirname,
      Path execRoot)
      throws IOException {
    SortedMap<PathFragment, ActionInput> inputs = new TreeMap<>();
    explodeDirectory(dirname, inputs, execRoot);
    return inputs;
  }

  private static void explodeDirectory(PathFragment dirname,
      SortedMap<PathFragment, ActionInput> inputs, Path execRoot) throws IOException {
    Collection<Dirent> entries = execRoot.getRelative(dirname).readdir(Symlinks.FOLLOW);
    for (Dirent entry : entries) {
      String basename = entry.getName();
      PathFragment path = dirname.getChild(basename);
      switch (entry.getType()) {
        case FILE:
          inputs.put(path, ActionInputHelper.fromPath(path));
          break;

        case DIRECTORY:
          explodeDirectory(path, inputs, execRoot);
          break;

        case SYMLINK:
          throw new IllegalStateException("this is a bug. all symlinks should have been resolved "
              + "by readdir");

        case UNKNOWN:
          throw new IOException(String.format("The file type of '%s' is not supported.", path));
      }
    }
  }

  private static void createParentDirectoriesIfNotExist(
      PathFragment dirname, DirectoryNode dir, Map<PathFragment, DirectoryNode> tree) {
    PathFragment parentDirname = dirname.getParentDirectory();
    DirectoryNode prevDir = dir;
    while (parentDirname != null) {
      DirectoryNode parentDir = tree.get(parentDirname);
      if (parentDir != null) {
        parentDir.addChild(prevDir);
        break;
      }

      parentDir = new DirectoryNode(parentDirname.getBaseName());
      parentDir.addChild(prevDir);
      tree.put(parentDirname, parentDir);

      parentDirname = parentDirname.getParentDirectory();
      prevDir = parentDir;
    }
  }
}

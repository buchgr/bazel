package com.google.devtools.build.lib.vfs;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;

public class DelegateFileSystem extends FileSystem {

  private final FileSystem delegate;

  public DelegateFileSystem(FileSystem delegate) {
    super(delegate.getDigestFunction());
    this.delegate = Preconditions.checkNotNull(delegate, "delegate");
  }

  @Override
  public boolean supportsModifications(Path path) {
    return delegate.supportsModifications(delegatePath(path));
  }

  @Override
  public boolean supportsSymbolicLinksNatively(Path path) {
    return delegate.supportsSymbolicLinksNatively(delegatePath(path));
  }

  @Override
  protected boolean supportsHardLinksNatively(Path path) {
    return delegate.supportsHardLinksNatively(delegatePath(path));
  }

  @Override
  public boolean isFilePathCaseSensitive() {
    return delegate.isFilePathCaseSensitive();
  }

  @Override
  public boolean createDirectory(Path path) throws IOException {
    return delegate.createDirectory(delegatePath(path));
  }

  @Override
  public void createDirectoryAndParents(Path path) throws IOException {
    delegate.createDirectoryAndParents(delegatePath(path));
  }

  @Override
  protected long getFileSize(Path path, boolean followSymlinks) throws IOException {
    return delegate.getFileSize(delegatePath(path), followSymlinks);
  }

  @Override
  public boolean delete(Path path) throws IOException {
    return delegate.delete(delegatePath(path));
  }

  @Override
  protected long getLastModifiedTime(Path path, boolean followSymlinks) throws IOException {
    return delegate.getLastModifiedTime(delegatePath(path), followSymlinks);
  }

  @Override
  public void setLastModifiedTime(Path path, long newTime) throws IOException {
    delegate.setLastModifiedTime(delegatePath(path), newTime);
  }

  @Override
  protected boolean isSymbolicLink(Path path) {
    return delegate.isSymbolicLink(delegatePath(path));
  }

  @Override
  protected boolean isDirectory(Path path, boolean followSymlinks) {
    return delegate.isDirectory(delegatePath(path), followSymlinks);
  }

  @Override
  protected boolean isFile(Path path, boolean followSymlinks) {
    return delegate.isFile(delegatePath(path), followSymlinks);
  }

  @Override
  protected boolean isSpecialFile(Path path, boolean followSymlinks) {
    return delegate.isSpecialFile(delegatePath(path), followSymlinks);
  }

  @Override
  protected void createSymbolicLink(Path linkPath, PathFragment targetFragment) throws IOException {
    delegate.createSymbolicLink(delegatePath(linkPath), targetFragment);
  }

  @Override
  protected PathFragment readSymbolicLink(Path path) throws IOException {
    return delegate.readSymbolicLink(delegatePath(path));
  }

  @Override
  protected boolean exists(Path path, boolean followSymlinks) {
    return delegate.exists(delegatePath(path), followSymlinks);
  }

  @Override
  protected Collection<String> getDirectoryEntries(Path path) throws IOException {
    return delegate.getDirectoryEntries(delegatePath(path));
  }

  @Override
  protected boolean isReadable(Path path) throws IOException {
    return delegate.isReadable(delegatePath(path));
  }

  @Override
  protected void setReadable(Path path, boolean readable) throws IOException {
    delegate.setReadable(delegatePath(path), readable);
  }

  @Override
  protected boolean isWritable(Path path) throws IOException {
    return delegate.isWritable(delegatePath(path));
  }

  @Override
  public void setWritable(Path path, boolean writable) throws IOException {
    delegate.setWritable(delegatePath(path), writable);
  }

  @Override
  protected boolean isExecutable(Path path) throws IOException {
    return delegate.isExecutable(delegatePath(path));
  }

  @Override
  protected void setExecutable(Path path, boolean executable) throws IOException {
    delegate.setExecutable(delegatePath(path), executable);
  }

  @Override
  protected InputStream getInputStream(Path path) throws IOException {
    return delegate.getInputStream(delegatePath(path));
  }

  @Override
  protected OutputStream getOutputStream(Path path, boolean append) throws IOException {
    return delegate.getOutputStream(delegatePath(path), append);
  }

  @Override
  public void renameTo(Path sourcePath, Path targetPath) throws IOException {
    delegate.renameTo(delegatePath(sourcePath), delegatePath(targetPath));
  }

  @Override
  protected void createFSDependentHardLink(Path linkPath, Path originalPath) throws IOException {
    delegate.createFSDependentHardLink(delegatePath(linkPath), delegatePath(originalPath));
  }

  @Override
  public String getFileSystemType(Path path) {
    return delegate.getFileSystemType(delegatePath(path));
  }

  @Override
  public void deleteTree(Path path) throws IOException {
    delegate.deleteTree(delegatePath(path));
  }

  @Override
  public void deleteTreesBelow(Path dir) throws IOException {
    delegate.deleteTreesBelow(delegatePath(dir));
  }

  @Override
  public byte[] getxattr(Path path, String name, boolean followSymlinks) throws IOException {
    return delegate.getxattr(delegatePath(path), name, followSymlinks);
  }

  @Override
  protected byte[] getFastDigest(Path path) throws IOException {
    return delegate.getFastDigest(delegatePath(path));
  }

  @Override
  protected byte[] getDigest(Path path) throws IOException {
    return delegate.getDigest(delegatePath(path));
  }

  @Override
  protected PathFragment resolveOneLink(Path path) throws IOException {
    return delegate.resolveOneLink(delegatePath(path));
  }

  @Override
  protected Path resolveSymbolicLinks(Path path) throws IOException {
    return delegate.resolveSymbolicLinks(delegatePath(path));
  }

  @Override
  protected FileStatus stat(Path path, boolean followSymlinks) throws IOException {
    return delegate.stat(delegatePath(path), followSymlinks);
  }

  @Override
  protected FileStatus statNullable(Path path, boolean followSymlinks) {
    return delegate.statNullable(delegatePath(path), followSymlinks);
  }

  @Override
  protected FileStatus statIfFound(Path path, boolean followSymlinks) throws IOException {
    return delegate.statIfFound(delegatePath(path), followSymlinks);
  }

  @Override
  protected PathFragment readSymbolicLinkUnchecked(Path path) throws IOException {
    return delegate.readSymbolicLink(delegatePath(path));
  }

  @Override
  public boolean exists(Path path) {
    return delegate.exists(delegatePath(path));
  }

  @Override
  protected Collection<Dirent> readdir(Path path, boolean followSymlinks) throws IOException {
    return delegate.readdir(delegatePath(path), followSymlinks);
  }

  @Override
  protected void chmod(Path path, int mode) throws IOException {
    delegate.chmod(delegatePath(path), mode);
  }

  @Override
  protected void createHardLink(Path linkPath, Path originalPath) throws IOException {
    delegate.createHardLink(delegatePath(linkPath), originalPath);
  }

  @Override
  protected void prefetchPackageAsync(Path path, int maxDirs) {
    delegate.prefetchPackageAsync(delegatePath(path), maxDirs);
  }

  protected Path delegatePath(Path path) {
    Preconditions.checkArgument(path.getFileSystem() == this);
    return Path.create(path.getPathString(), delegate);
  }
}

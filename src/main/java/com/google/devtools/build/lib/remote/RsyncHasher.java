package com.google.devtools.build.lib.remote;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class RsyncHasher {

  private static final MissingRange INITIAL_RANGE = new MissingRange(0, 0);
  private static long MOD_EXPONENT = 16;
  private static long MOD = ((2 << MOD_EXPONENT) - 1);

  private final HashFunction strongHash;
  private final byte[] tempData;
  private final int blockSize;

  public RsyncHasher(HashFunction strongHash, int blockSize) {
    Preconditions.checkNotNull(strongHash);
    Preconditions.checkArgument(blockSize > 0, "blockSize must be a positive number.");
    this.strongHash = strongHash;
    this.tempData = new byte[blockSize];
    this.blockSize = blockSize;
  }

  public List<BlockHash> computeBlockHashes(InputStream in) throws IOException {
    List<BlockHash> blocks = new ArrayList<>();
    Arrays.fill(tempData, (byte) 0);
    for(;;) {
      int bytesRead = ByteStreams.read(in, tempData, 0, blockSize);
      if (bytesRead == 0) {
        break;
      }
      long weakHash = computeWeakHash(tempData, bytesRead);
      HashCode strongHash = computeStrongHash(tempData, bytesRead);
      BlockHash blockHash = new BlockHash(weakHash, strongHash, bytesRead);
      blocks.add(blockHash);
    }
    in.close();
    return blocks;
  }

  public List<MissingRange> missingRanges(InputStream oldData, List<BlockHash> newBlocks) throws IOException {
    try (RollingHasher hasher = new RollingHasher(oldData, strongHash, blockSize)) {
      Multimap<Long, BlockHash> weakToBlockMap = ArrayListMultimap.create(newBlocks.size(), 1);
      for (BlockHash block : newBlocks) {
        weakToBlockMap.put(block.weakHash(), block);
      }
      Set<BlockHash> matchedBlocks = new HashSet<>(newBlocks.size());
      long weakHash;
      while ((weakHash = hasher.nextWeakHash()) != -1) {
        Collection<BlockHash> blocks = weakToBlockMap.get(weakHash);
        if (blocks.isEmpty()) {
          continue;
        }
        HashCode strongHash = hasher.currentStrongHash();
        for (BlockHash block : blocks) {
          if (strongHash.equals(block.strongHash())) {
            matchedBlocks.add(block);
            hasher.reset();
            break;
          }
        }
      }

      List<MissingRange> missing = new ArrayList<>();
      MissingRange previous = INITIAL_RANGE;
      int currentOffset = 0;
      for (BlockHash block : newBlocks) {
        if (!matchedBlocks.contains(block)) {
          if ((previous.offset + previous.len) == currentOffset) {
            previous = new MissingRange(previous.offset, previous.len + block.size());
          } else {
            if (previous != INITIAL_RANGE) {
              missing.add(previous);
            }
            previous = new MissingRange(currentOffset, block.size());
          }
        }
        currentOffset += block.size();
      }
      if (previous != INITIAL_RANGE) {
        missing.add(previous);
      }

      return missing;
    }
  }

  static long computeWeakHash(byte[] data, int len) {
    Preconditions.checkNotNull(data);
    int s1 = 0, s2 = 0;
    for (int i = 0; i < len; i++) {
      s1 += data[i];
      s2 += (len - i) * data[i];
    }
    s1 = s1 & (int) MOD;
    s2 = s2 & (int) MOD;
    return s1 | (s2 << MOD_EXPONENT);
  }

  private static HashCode computeStrongHash(byte[] data, int len) {
    return Hashing.md5().hashBytes(data, 0, len);
  }

  public static final class MissingRange {
    private final int offset;
    private final int len;

    private MissingRange(int offset, int len) {
      this.offset = offset;
      this.len = len;
    }

    public int offset() {
      return offset;
    }

    public int len() {
      return len;
    }
  }

  public static final class BlockHash {
    private final long weakHash;
    private final HashCode strongHash;
    private final int size;

    BlockHash(long weakHash, HashCode strongHash, int size) {
      this.weakHash = weakHash;
      this.strongHash = strongHash;
      this.size = size;
    }

    public long weakHash() {
      return weakHash;
    }

    public HashCode strongHash() {
      return strongHash;
    }

    public int size() {
      return size;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof BlockHash)) {
        return false;
      }
      return ((BlockHash) other).strongHash.equals(strongHash) &&
          ((BlockHash) other).weakHash == weakHash &&
          ((BlockHash) other).size == size;
    }

    @Override
    public int hashCode() {
      return strongHash.hashCode();
    }
  }

  static class RollingHasher implements AutoCloseable {

    private final InputStream in;
    private final int blockSize;
    private final HashFunction strongHashFn;

    private byte[] blockData;
    private int blockPos;
    private long rollingHash;
    private long s1;
    private long s2;

    private enum State {
      UNINITIALIZED,
      COMPUTE_ROLLING_HASH,
      FINISHED
    }

    private State state = State.UNINITIALIZED;

    public RollingHasher(InputStream in, HashFunction strongHashFn, int blockSize) {
      Preconditions.checkArgument(blockSize > 0, "blockSize must be a positive number.");
      Preconditions.checkArgument((blockSize & (blockSize - 1)) == 0, "blockSize must be a power of two");
      Preconditions.checkNotNull(in);
      this.in = new BufferedInputStream(in);
      this.strongHashFn = strongHashFn;
      this.blockSize = blockSize;
      this.blockData = new byte[blockSize];
    }

    /**
     * Returns the next weak hash or {@code -1} if EOS is reached.
     */
    public long nextWeakHash() throws IOException {
      if (state == State.FINISHED) {
        return -1;
      }
      if (state == State.UNINITIALIZED) {
        return initialize();
      }

      int val = in.read();
      if (val == -1) {
        in.close();
        state = State.FINISHED;
        return -1;
      }

      byte nextByte = (byte) val;
      s1 = (s1 - blockData[blockPos] + nextByte) & MOD;
      s2 = (s2 - blockSize * blockData[blockPos] + s1) & MOD;
      rollingHash = s1 | (s2 << MOD_EXPONENT);
      blockData[blockPos] = nextByte;
      blockPos = (blockPos + 1) & (blockData.length - 1);

      return rollingHash;
    }

    public HashCode currentStrongHash() {
      Hasher hasher = strongHashFn.newHasher();
      hasher.putBytes(blockData, blockPos, blockData.length - blockPos);
      hasher.putBytes(blockData, 0, blockPos);
      return hasher.hash();
    }

    public void reset() {
      state = State.UNINITIALIZED;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    private long initialize() throws IOException {
      int bytesRead = ByteStreams.read(in, blockData, 0, blockData.length);
      if (bytesRead <= 0) {
        state = State.FINISHED;
        return -1;
      } else if (bytesRead < blockSize) {
        Preconditions.checkState(blockData.length == blockSize);
        byte[] newData = new byte[bytesRead];
        System.arraycopy(blockData, 0, newData, 0, bytesRead);
        blockData = newData;
      }
      rollingHash = computeWeakHash(blockData, bytesRead);
      s1 = rollingHash & ((2 << 16) - 1);
      s2 = rollingHash >> 16;
      blockPos = 0;
      state = State.COMPUTE_ROLLING_HASH;
      return rollingHash;
    }
  }
}
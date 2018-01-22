// Copyright 2017 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.devtools.build.lib.remote;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.ExecException;
import com.google.devtools.build.lib.actions.ExecutionStrategy;
import com.google.devtools.build.lib.actions.Spawn;
import com.google.devtools.build.lib.actions.SpawnResult;
import com.google.devtools.build.lib.actions.SpawnResult.Status;
import com.google.devtools.build.lib.actions.Spawns;
import com.google.devtools.build.lib.actions.cache.Metadata;
import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import com.google.devtools.build.lib.events.Event;
import com.google.devtools.build.lib.events.Reporter;
import com.google.devtools.build.lib.exec.SpawnCache;
import com.google.devtools.build.lib.exec.SpawnRunner.SpawnExecutionPolicy;
import com.google.devtools.build.lib.remote.DigestUtil.ActionKey;
import com.google.devtools.build.lib.remote.RsyncHasher.BlockHash;
import com.google.devtools.build.lib.remote.RsyncHasher.MissingRange;
import com.google.devtools.build.lib.remote.TreeNodeRepository.TreeNode;
import com.google.devtools.build.lib.skyframe.FileArtifactValue;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.OutputFile;
import com.google.devtools.remoteexecution.v1test.RsyncBlock;
import io.grpc.Context;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/**
 * A remote {@link SpawnCache} implementation.
 */
@ThreadSafe // If the RemoteActionCache implementation is thread-safe.
@ExecutionStrategy(
  name = {"remote-cache"},
  contextType = SpawnCache.class
)
final class RemoteSpawnCache implements SpawnCache {
  private final Path execRoot;
  private final RemoteOptions options;

  private final AbstractRemoteActionCache remoteCache;
  private final String buildRequestId;
  private final String commandId;
  private final boolean verboseFailures;

  @Nullable private final Reporter cmdlineReporter;

  // Used to ensure that a warning is reported only once.
  private final AtomicBoolean warningReported = new AtomicBoolean();

  private final DigestUtil digestUtil;

  private AtomicLong totalBytes = new AtomicLong();
  private AtomicLong transferBytes = new AtomicLong();

  RemoteSpawnCache(
      Path execRoot,
      RemoteOptions options,
      AbstractRemoteActionCache remoteCache,
      String buildRequestId,
      String commandId,
      boolean verboseFailures,
      @Nullable Reporter cmdlineReporter,
      DigestUtil digestUtil) {
    this.execRoot = execRoot;
    this.options = options;
    this.remoteCache = remoteCache;
    this.verboseFailures = verboseFailures;
    this.cmdlineReporter = cmdlineReporter;
    this.buildRequestId = buildRequestId;
    this.commandId = commandId;
    this.digestUtil = digestUtil;
  }

  @Override
  public CacheHandle lookup(Spawn spawn, SpawnExecutionPolicy policy,
      Map<Artifact, Path> newToOldOutputs)
      throws InterruptedException, IOException, ExecException {
    // Temporary hack: the TreeNodeRepository should be created and maintained upstream!
    TreeNodeRepository repository =
        new TreeNodeRepository(execRoot, policy.getActionInputFileCache(), digestUtil);
    SortedMap<PathFragment, ActionInput> inputMap = policy.getInputMapping();
    TreeNode inputRoot = repository.buildFromActionInputs(inputMap);
    repository.computeMerkleDigests(inputRoot);
    Command command = RemoteSpawnRunner.buildCommand(spawn.getArguments(), spawn.getEnvironment());
    Action action =
        RemoteSpawnRunner.buildAction(
            execRoot,
            spawn.getOutputFiles(),
            digestUtil.compute(command),
            repository.getMerkleDigest(inputRoot),
            spawn.getExecutionPlatform(),
            policy.getTimeout(),
            Spawns.mayBeCached(spawn));

    // Look up action cache, and reuse the action output if it is found.
    final ActionKey actionKey = digestUtil.computeActionKey(action);
    Context withMetadata =
        TracingMetadataUtils.contextWithMetadata(buildRequestId, commandId, actionKey);
    // Metadata will be available in context.current() until we detach.
    // This is done via a thread-local variable.
    Context previous = withMetadata.attach();
    try {
      ActionResult result =
          this.options.remoteAcceptCached && Spawns.mayBeCached(spawn)
              ? remoteCache.getCachedActionResult(actionKey)
              : null;
      if (result != null) {
        for (OutputFile file : result.getOutputFilesList()) {
          long actualSize = file.getDigest().getSizeBytes();
          long total = totalBytes.addAndGet(actualSize);
          if (file.getBlocksCount() > 0) {
            for (Path existingFile : newToOldOutputs.values()) {
              String path = file.getPath();
              if (existingFile.getPathString().endsWith(path)) {
                if (path.endsWith("bazel")) {
                  report(Event.warn(path));
                }
                RsyncHasher hasher = new RsyncHasher(Hashing.md5(), 2048);
                List<BlockHash> blocks = new ArrayList<>();
                for (RsyncBlock block : file.getBlocksList()) {
                  BlockHash blockhash = new BlockHash(block.getWeakHash(), HashCode.fromBytes(block.getStrongHash().toByteArray()), block.getSize());
                  blocks.add(blockhash);
                }
                long deltaBytes = hasher.missingRanges(existingFile.getInputStream(), blocks).stream().mapToInt(
                    MissingRange::len).sum();
                long transfer = transferBytes.addAndGet(deltaBytes);
                report(Event.warn(String.format("transfer %d, total %d, ratio %f", transfer, total, transfer / (total * 1.0))));
              }
            }
          }
        }
        // We don't cache failed actions, so we know the outputs exist.
        // For now, download all outputs locally; in the future, we can reuse the digests to
        // just update the TreeNodeRepository and continue the build.
        remoteCache.download(result, execRoot, policy.getFileOutErr());
        SpawnResult spawnResult =
            new SpawnResult.Builder()
                .setStatus(Status.SUCCESS)
                .setExitCode(result.getExitCode())
                .build();
        return SpawnCache.success(spawnResult);
      }
    } catch (CacheNotFoundException e) {
      // There's a cache miss. Fall back to local execution.
    } catch (IOException e) {
      // There's an IO error. Fall back to local execution.
      reportOnce(
          Event.warn(
              "Some artifacts failed be downloaded from the remote cache: " + e.getMessage()));
    } finally {
      withMetadata.detach(previous);
    }
    if (options.remoteUploadLocalResults) {
      return new CacheHandle() {
        @Override
        public boolean hasResult() {
          return false;
        }

        @Override
        public SpawnResult getResult() {
          throw new NoSuchElementException();
        }

        @Override
        public boolean willStore() {
          return true;
        }

        @Override
        public void store(SpawnResult result, Collection<Path> files)
            throws InterruptedException, IOException {
          if (options.experimentalGuardAgainstConcurrentChanges) {
            try {
              checkForConcurrentModifications();
            } catch (IOException e) {
              report(Event.warn(e.getMessage()));
              return;
            }
          }
          boolean uploadAction =
              Spawns.mayBeCached(spawn)
                  && Status.SUCCESS.equals(result.status())
                  && result.exitCode() == 0;
          Context previous = withMetadata.attach();
          try {
            remoteCache.upload(actionKey, execRoot, files, policy.getFileOutErr(), uploadAction);
          } catch (IOException e) {
            if (verboseFailures) {
              report(Event.debug("Upload to remote cache failed: " + e.getMessage()));
            } else {
              reportOnce(
                  Event.warn(
                      "Some artifacts failed be uploaded to the remote cache: " + e.getMessage()));
            }
          } finally {
            withMetadata.detach(previous);
          }
        }

        @Override
        public void close() {}

        private void checkForConcurrentModifications() throws IOException {
          for (ActionInput input : inputMap.values()) {
            Metadata metadata = policy.getActionInputFileCache().getMetadata(input);
            if (metadata instanceof FileArtifactValue) {
              FileArtifactValue artifactValue = (FileArtifactValue) metadata;
              Path path = execRoot.getRelative(input.getExecPath());
              if (artifactValue.wasModifiedSinceDigest(path)) {
                throw new IOException(path + " was modified during execution");
              }
            }
          }
        }
      };
    } else {
      return SpawnCache.NO_RESULT_NO_STORE;
    }
  }

  private void reportOnce(Event evt) {
    if (warningReported.compareAndSet(false, true)) {
      report(evt);
    }
  }

  private void report(Event evt) {
    if (cmdlineReporter != null) {
      cmdlineReporter.handle(evt);
    }
  }
}

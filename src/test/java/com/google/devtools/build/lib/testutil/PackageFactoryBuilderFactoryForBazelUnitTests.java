// Copyright 2016 The Bazel Authors. All rights reserved.
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
package com.google.devtools.build.lib.testutil;

import com.google.devtools.build.lib.packages.Package;
import com.google.devtools.build.lib.packages.PackageFactory;
import com.google.devtools.build.lib.packages.RuleClassProvider;
import com.google.devtools.build.lib.skyframe.packages.PackageFactoryBuilderWithSkyframeForTesting;
import com.google.devtools.build.lib.vfs.FileSystem;

/**
 * A {@link PackageFactory.BuilderFactoryForTesting} implementation that injects a
 * {@link BazelPackageBuilderHelperForTesting}.
 */
class PackageFactoryBuilderFactoryForBazelUnitTests
    extends PackageFactory.BuilderFactoryForTesting {
  static final PackageFactoryBuilderFactoryForBazelUnitTests INSTANCE =
      new PackageFactoryBuilderFactoryForBazelUnitTests();

  private PackageFactoryBuilderFactoryForBazelUnitTests() {
  }

  @Override
  public PackageFactoryBuilderWithSkyframeForTesting builder() {
    return new PackageFactoryBuilderForBazelUnitTests();
  }

  private static class PackageFactoryBuilderForBazelUnitTests
      extends PackageFactoryBuilderWithSkyframeForTesting {
    @Override
    public PackageFactory build(RuleClassProvider ruleClassProvider, FileSystem fs) {
      Package.Builder.Helper packageBuilderHelperForTesting = doChecksForTesting
          ? new BazelPackageBuilderHelperForTesting(ruleClassProvider)
          : Package.Builder.DefaultHelper.INSTANCE;
      return new PackageFactory(
          ruleClassProvider,
          platformSetRegexps,
          attributeContainerFactory,
          environmentExtensions,
          version,
          packageBuilderHelperForTesting);
    }
  }
}


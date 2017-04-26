package(default_visibility = ["//src:__subpackages__"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

java_library(
    name = "common",
    srcs = glob(["*.java"]),
    deps = [
        "//src/tools/xcode-common/java/com/google/devtools/build/xcode/util",
        "@com_google_guava//:jar",
    ],
)

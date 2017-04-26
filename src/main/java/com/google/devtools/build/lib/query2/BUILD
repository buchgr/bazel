package(
    default_visibility = ["//src:__subpackages__"],
)

java_library(
    name = "query2",
    srcs = glob(["*.java"]),
    deps = [
        ":query-engine",
        "//src/main/java/com/google/devtools/build/lib:build-base",
        "//src/main/java/com/google/devtools/build/lib:concurrent",
        "//src/main/java/com/google/devtools/build/lib:events",
        "//src/main/java/com/google/devtools/build/lib:graph",
        "//src/main/java/com/google/devtools/build/lib:packages-internal",
        "//src/main/java/com/google/devtools/build/lib:util",
        "//src/main/java/com/google/devtools/build/lib:vfs",
        "//src/main/java/com/google/devtools/build/skyframe",
        "//src/main/java/com/google/devtools/build/skyframe:skyframe-objects",
        "@com_google_guava//:jar",
        "@jsr305//:jar",
    ],
)

java_library(
    name = "query-output",
    srcs = glob(["output/*.java"]),
    deps = [
        ":query-engine",
        ":query2",
        "//src/main/java/com/google/devtools/build/lib:collect",
        "//src/main/java/com/google/devtools/build/lib:events",
        "//src/main/java/com/google/devtools/build/lib:graph",
        "//src/main/java/com/google/devtools/build/lib:packages-internal",
        "//src/main/java/com/google/devtools/build/lib:util",
        "//src/main/java/com/google/devtools/common/options",
        "//src/main/protobuf:build_java_proto",
        "@com_google_guava//:jar",
        "@jsr305//:jar",
    ],
)

# Query library.
java_library(
    name = "query-engine",
    srcs = glob(["engine/*.java"]),
    deps = [
        "//src/main/java/com/google/devtools/build/lib:concurrent",
        "//src/main/java/com/google/devtools/build/lib:graph",
        "//src/main/java/com/google/devtools/build/lib:util",
        "@com_google_guava//:jar",
        "@jsr305//:jar",
    ],
)

filegroup(
    name = "srcs",
    testonly = 0,  # All srcs should be not test only, overwrite package default.
    srcs = glob(["**"]),
)

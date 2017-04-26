package(default_visibility = ["//src:__subpackages__"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

java_library(
    name = "util",
    srcs = glob(["*.java"]),
    deps = ["@com_google_guava//:jar"],
)

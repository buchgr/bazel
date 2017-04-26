package(default_visibility = ["//src:__subpackages__"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

java_proto_library(
    name = "build_event_stream_java_proto",
    deps = [":build_event_stream_proto"],
)

proto_library(
    name = "build_event_stream_proto",
    srcs = ["build_event_stream.proto"],
)

filegroup(
    name = "dist_jars",
    srcs = ["build_event_stream_java_proto_srcjar"],
)

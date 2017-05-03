licenses(["notice"])

load("@io_grpc//:build_defs.bzl", "java_grpc_library")

exports_files(["LICENSE"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//third_party:__pkg__"],
)

java_grpc_library(
    name = "google_devtools_build_v1_publish_build_event_java_grpc",
    srcs = [":google_devtools_build_v1_publish_build_event_proto"],
    deps = [":google_devtools_build_v1_publish_build_event_java_proto"],
    visibility = ["//visibility:public"],
)

java_proto_library(
    name = "google_devtools_build_v1_build_status_java_proto",
    deps = [":google_devtools_build_v1_build_status_proto"],
    visibility = ["//visibility:public"],
)

java_proto_library(
    name = "google_devtools_build_v1_build_events_java_proto",
    deps = [":google_devtools_build_v1_build_events_proto"],
    visibility = ["//visibility:public"],
)

java_proto_library(
    name = "google_devtools_build_v1_publish_build_event_java_proto",
    deps = [":google_devtools_build_v1_publish_build_event_proto"],
    visibility = ["//visibility:public"],
)

proto_library(
    name = "google_devtools_build_v1_build_status_proto",
    srcs = ["google/devtools/build/v1/build_status.proto"],
    deps = [
        "@com_google_protobuf//:well_known_types_any_proto",
        ":google_api_annotations_proto",
    ],
    visibility = ["//visibility:public"],
)

proto_library(
    name = "google_devtools_build_v1_build_events_proto",
    srcs = ["google/devtools/build/v1/build_events.proto"],
    deps = [
      "@com_google_protobuf//:well_known_types_any_proto",
      "@com_google_protobuf//:well_known_types_timestamp_proto",
      "@com_google_protobuf//:well_known_types_wrappers_proto",
      ":google_devtools_build_v1_build_status_proto",
      ":google_api_annotations_proto",
      ":google_rpc_status_proto",
    ],
    visibility = ["//visibility:public"],
)

proto_library(
    name = "google_devtools_build_v1_publish_build_event_proto",
    srcs = ["google/devtools/build/v1/publish_build_event.proto"],
    deps = [
        "@com_google_protobuf//:well_known_types_any_proto",
        "@com_google_protobuf//:well_known_types_duration_proto",
        "@com_google_protobuf//:well_known_types_empty_proto",
        ":google_devtools_build_v1_build_events_proto",
        ":google_api_annotations_proto",
        ":google_api_auth_proto",
    ],
    visibility = ["//visibility:public"],
)

proto_library(
    name = "google_api_annotations_proto",
    srcs = ["google/api/annotations.proto"],
    deps = [
        "@com_google_protobuf//:well_known_types_descriptor_proto",
        ":google_api_http_proto",
        ],
)

proto_library(
    name = "google_api_http_proto",
    srcs = ["google/api/http.proto"],
)

proto_library(
    name = "google_rpc_status_proto",
    srcs = ["google/rpc/status.proto"],
    deps = ["@com_google_protobuf//:well_known_types_any_proto"],
)

proto_library(
    name = "google_api_auth_proto",
    srcs = ["google/api/auth.proto"],
    deps = [":google_api_annotations_proto"],
)


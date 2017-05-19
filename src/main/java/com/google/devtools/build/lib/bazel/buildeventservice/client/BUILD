java_library(
    name = "client",
    srcs = glob(["*.java"]),
    visibility = [
        "//visibility:public",
    ],
    runtime_deps = [
        # This is required for client TLS.
        "//third_party:netty_tcnative",
    ],
    deps = [
        "//src/main/java/com/google/devtools/build/lib:util",
        "//third_party:apache_httpclient",
        "//third_party:apache_httpcore",
        "//third_party:auth",
        "//third_party:gson",
        "//third_party:joda_time",
        "@com_google_guava//:jar",
        "@googleapis//:google_devtools_build_v1_publish_build_event_java_grpc",
        "@googleapis//:google_devtools_build_v1_publish_build_event_java_proto",
        "@io_grpc//:grpc-jar",
        "@io_netty//:netty",
        "@jsr305//:jar",
    ],
)

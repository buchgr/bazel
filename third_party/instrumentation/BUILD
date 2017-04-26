licenses(["notice"])

exports_files(["LICENSE"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

java_import(
    name = "jar",
    jars = [
        "instrumentation-api-0.3.0.jar",
    ],
    visibility = ["//visibility:public"],
)


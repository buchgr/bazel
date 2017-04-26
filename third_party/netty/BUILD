licenses(["notice"])

exports_files(["LICENSE"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

java_import(
    name = "netty",
    jars = ["netty-all-4.1.0.CR6.jar"],
    visibility = ["//visibility:public"],
)

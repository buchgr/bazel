licenses(["notice"])

exports_files(["LICENSE"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

java_import(
    name = "netty",
    jars = ["netty-all-4.1.8.Final.jar"],
    visibility = ["//visibility:public"],
)

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
        "jcip-annotations-1.0-1.jar",
    ],
    visibility = ["//visibility:public"],
)

exports_files(glob(["*.jar"]))

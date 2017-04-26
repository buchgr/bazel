licenses(["notice"])

exports_files(["LICENSE"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

java_import(
    name = "jar",
    jars = [":jsr-305.jar"],
    visibility = ["//visibility:public"],
)

exports_files(glob(["*.jar"]))

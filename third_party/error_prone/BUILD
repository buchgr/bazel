licenses(["notice"])

exports_files(["LICENSE"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

java_import(
    name = "error_prone",
    jars = [
        "error_prone_check_api-2.0.19.jar",
        "error_prone_core-2.0.19.jar",
        "error_prone_annotations-2.0.19.jar",
        "error_prone_annotation-2.0.19.jar",
    ],
    visibility = ["//visibility:public"],
)

exports_files(glob(["*.jar"]))

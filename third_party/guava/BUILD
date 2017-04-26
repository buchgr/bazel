licenses(["notice"])

exports_files(["LICENSE"])

config_setting(
    name = "jdk7",
    values = {"define": "JAVA_VERSION=1.7"},
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

# As of Guava 20.0, Guava code uses Error Prone annotations. This isn't a
# problem when compiling with java 8, but is a problem when compiling Bazel
# with java 7 (the Error Prone jars need to be on the javac classpath). So,
# until we drop support for compiling Bazel with java 7, we unfortunately need
# to ensure that the Error Prone jars are always used together with the Guava
# jars.
java_import(
    name = "jar",
    jars = [
        "@jsr305//:jsr-305.jar",
        "@jcip_annotations//:jcip-annotations-1.0-1.jar",
        "@error_prone//:error_prone_annotations-2.0.19.jar",
    ]+ select({
        ":jdk7": ["guava-21.0-20161101.jar"],
        "//conditions:default": ["guava-21.0.jar"],
    }),
    visibility = ["//visibility:public"],
)

exports_files(glob(["*.jar"]))


package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//third_party:__pkg__"],
)

java_library(
    name = "buck-ios-support",
    srcs = glob(
        ["java/**/*.java"],
    ),
    deps = [
        "//third_party/java/dd_plist",
        "@com_google_guava//:jar",
        "@jsr305//:jar",
    ],
)

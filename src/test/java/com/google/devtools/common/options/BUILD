filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//src:__pkg__"],
)

java_test(
    name = "options_test",
    srcs = glob(["*.java"]),
    test_class = "com.google.devtools.common.options.AllTests",
    deps = [
        "//src/main/java/com/google/devtools/common/options",
        "//src/test/java/com/google/devtools/build/lib:testutil",
        "@com_google_guava//:jar",
        "@com_google_guava//:jar-testlib",
        "@jsr305//:jar",
        "//third_party:junit4",
        "//third_party:truth",
    ],
)

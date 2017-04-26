filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//src/test/java/com/google/devtools/build/android:__pkg__"],
)

java_test(
    name = "RClassGeneratorTest",
    size = "medium",
    srcs = glob(["*.java"]),
    deps = [
        "//src/tools/android/java/com/google/devtools/build/android/resources",
        "//third_party:android_common_25_0_0",
        "@com_google_guava//:jar",
        "//third_party:junit4",
        "//third_party:truth",
    ],
)

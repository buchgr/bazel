filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//src/test/java/com/google/devtools/build/android:__pkg__"],
)

java_test(
    name = "ziputils-tests",
    srcs = glob(["*.java"]),
    tags = ["ziputils"],
    test_class = "com.google.devtools.build.android.ziputils.AllTests",
    deps = [
        "//src/test/java/com/google/devtools/build/lib:testutil",
        "//src/tools/android/java/com/google/devtools/build/android/ziputils:splitter_lib",
        "//src/tools/android/java/com/google/devtools/build/android/ziputils:ziputils_lib",
        "@com_google_guava//:jar",
        "@jsr305//:jar",
        "//third_party:junit4",
        "//third_party:truth",
    ],
)

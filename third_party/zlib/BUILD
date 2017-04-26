licenses(["notice"])  #  BSD/MIT-like license

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

cc_library(
    name = "zlib",
    srcs = glob(["*.c"]),
    hdrs = glob(["*.h"]),
    copts = ["-w"],
    includes = ["."],
    visibility = ["//visibility:public"],
)

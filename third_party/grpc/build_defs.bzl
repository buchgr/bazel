"""BUILD rules to generate gRPC service interfaces.

You need to load the rules in your BUILD file for use, like:

load("@io_grpc//:build_defs.bzl", "java_grpc_library")
"""

def _path_ignoring_repository(f):
  if (len(f.owner.workspace_root) == 0):
    return f.short_path
  return f.path[len(f.owner.workspace_root)+1:]

def _gensource_impl(ctx):
  if len(ctx.attr.srcs) > 1:
    fail("Only one src value supported", "srcs")
  for s in ctx.attr.srcs:
    if s.label.package != ctx.label.package:
      print(("in srcs attribute of {0}: Proto source with label {1} should be in "
             + "same package as consuming rule").format(ctx.label, s.label))
  # Use .jar since .srcjar makes protoc think output will be a directory
  srcdotjar = ctx.new_file(ctx.label.name + "_src.jar")

  srcs = [f for dep in ctx.attr.srcs for f in dep.proto.direct_sources]
  includes = [f for dep in ctx.attr.srcs for f in dep.proto.transitive_imports]
  flavor = ctx.attr.flavor
  if flavor == "normal":
    flavor = ""

  ctx.action(
      inputs = [ctx.executable._java_plugin] + srcs + includes,
      outputs = [srcdotjar],
      executable = ctx.executable._protoc,
      arguments = [
          "--plugin=protoc-gen-grpc-java=" + ctx.executable._java_plugin.path,
          "--grpc-java_out={0},enable_deprecated={1}:{2}"
            .format(flavor, str(ctx.attr.enable_deprecated).lower(), srcdotjar.path)]
          + ["-I{0}={1}".format(_path_ignoring_repository(include), include.path) for include in includes]
          + [src.path for src in srcs])
  ctx.action(
      command = "cp $1 $2",
      inputs = [srcdotjar],
      outputs = [ctx.outputs.srcjar],
      arguments = [srcdotjar.path, ctx.outputs.srcjar.path])

_java_grpc_gensource = rule(
    attrs = {
        "srcs": attr.label_list(
            mandatory = True,
            non_empty = True,
            providers = ["proto"],
        ),
        "flavor": attr.string(
            values = [
                "normal",
                "lite",
            ],
            default = "normal",
        ),
        "enable_deprecated": attr.bool(
            default = False,
        ),
        "_protoc": attr.label(
            default = Label("@com_google_protobuf//:protoc"),
            executable = True,
            cfg = "host",
        ),
        "_java_plugin": attr.label(
            default = Label("@io_grpc//:grpc-java-plugin"),
            executable = True,
            cfg = "host",
        ),
    },
    outputs = {
        "srcjar": "%{name}.srcjar",
    },
    implementation = _gensource_impl,
)

def java_grpc_library(name, srcs, deps, flavor=None,
                      enable_deprecated=None, visibility=None,
                      constraints=None, **kwargs):
  """Generates and compiles gRPC Java sources for services defined in a proto
  file. This rule is compatible with proto_library with java_api_version,
  java_proto_library, and java_lite_proto_library.

  Do note that this rule only scans through the proto file for RPC services. It
  does not generate Java classes for proto messages. You will need a separate
  proto_library with java_api_version, java_proto_library, or
  java_lite_proto_library rule.

  Args:
    name: (str) A unique name for this rule. Required.
    srcs: (list) a single proto_library target that contains the schema of the
        service. Required.
    deps: (list) a single java_proto_library target for the proto_library in
        srcs.  Required.
    flavor: (str) "normal" (default) for normal proto runtime. "lite"
        for the lite runtime.
    visibility: (list) the visibility list
    constraints: (list) the constraints list (not available in Bazel)
    **kwargs: Passed through to generated targets
  """
  if flavor == None:
    flavor = "normal"

  # Multiple deps temporarily permitted because of strict_deps. Eventually we
  # hope to disable strict_deps for our java_library compilation, at which
  # point this could be re-introduced and even become an fail().
  #if len(deps) > 1:
  #  print("Multiple values in 'deps' is deprecated in " + name)

  gensource_name = name + "__do_not_reference__srcjar"
  _java_grpc_gensource(
      name = gensource_name,
      srcs = srcs,
      flavor = flavor,
      enable_deprecated = enable_deprecated,
      visibility = ["//visibility:private"],
      tags = [
          "avoid_dep",
      ],
      **kwargs
  )

  added_deps = [
      "@io_grpc//:grpc-jar",
      "@com_google_guava//:jar",
      "@com_google_protobuf//:protobuf_java",
  ]
  if flavor != "normal":
    fail("Unknown flavor type", "flavor")

  native.java_library(
      name = name,
      srcs = [gensource_name],
      visibility = visibility,
      deps = [
          "@jsr305//:jar",
      ] + deps + added_deps,
      **kwargs
  )

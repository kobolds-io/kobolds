const std = @import("std");

const version = std.SemanticVersion{ .major = 0, .minor = 1, .patch = 0 };

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    // This is shamelessly taken from the `zbench` library's `build.zig` file.
    // see [here](https://github.com/hendriknielaender/zBench/blob/b69a438f5a1a96d4dd0ea69e1dbcb73a209f76cd/build.zig)
    setupExecutable(b, target, optimize);

    setupTests(b, target, optimize);

    setupCICDTests(b, target, optimize);

    setupBenchmarks(b, target, optimize);
}

fn setupExecutable(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) void {
    const kobolds_exe = b.addExecutable(.{
        .name = "kobolds",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .version = version,
    });

    const stdx_dep = b.dependency("stdx", .{
        .target = target,
        .optimize = optimize,
    });
    const stdx_mod = stdx_dep.module("stdx");

    const kid_dep = b.dependency("kid", .{
        .target = target,
        .optimize = optimize,
    });
    const kid_mod = kid_dep.module("kid");

    const clap_dep = b.dependency("clap", .{
        .target = target,
        .optimize = optimize,
    });
    const clap_mod = clap_dep.module("clap");

    const uuid_dep = b.dependency("uuid", .{
        .target = target,
        .optimize = optimize,
    });
    const uuid_mod = uuid_dep.module("uuid");

    kobolds_exe.root_module.addImport("uuid", uuid_mod);
    kobolds_exe.root_module.addImport("stdx", stdx_mod);
    kobolds_exe.root_module.addImport("kid", kid_mod);
    kobolds_exe.root_module.addImport("clap", clap_mod);

    b.installArtifact(kobolds_exe);

    const run_kobolds_exe = b.addRunArtifact(kobolds_exe);
    const run_step = b.step("run", "Run kobolds");
    run_step.dependOn(&run_kobolds_exe.step);

    if (b.args) |args| {
        run_kobolds_exe.addArgs(args);
    }
}

fn setupTests(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) void {
    const kobolds_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/test.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const uuid_dep = b.dependency("uuid", .{
        .target = target,
        .optimize = optimize,
    });
    const uuid_mod = uuid_dep.module("uuid");

    const stdx_dep = b.dependency("stdx", .{
        .target = target,
        .optimize = optimize,
    });
    const stdx_mod = stdx_dep.module("stdx");

    const kid_dep = b.dependency("kid", .{
        .target = target,
        .optimize = optimize,
    });
    const kid_mod = kid_dep.module("kid");

    kobolds_unit_tests.root_module.addImport("uuid", uuid_mod);
    kobolds_unit_tests.root_module.addImport("stdx", stdx_mod);
    kobolds_unit_tests.root_module.addImport("kid", kid_mod);

    const run_unit_tests = b.addRunArtifact(kobolds_unit_tests);
    const test_step = b.step("test", "Run unit tests");

    test_step.dependOn(&run_unit_tests.step);
}

fn setupCICDTests(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) void {
    const kobolds_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/test.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .test_runner = .{ .path = b.path("src/cicd_test_runner.zig"), .mode = .simple },
    });

    const uuid_dep = b.dependency("uuid", .{
        .target = target,
        .optimize = optimize,
    });
    const uuid_mod = uuid_dep.module("uuid");

    const stdx_dep = b.dependency("stdx", .{
        .target = target,
        .optimize = optimize,
    });
    const stdx_mod = stdx_dep.module("stdx");

    const kid_dep = b.dependency("kid", .{
        .target = target,
        .optimize = optimize,
    });
    const kid_mod = kid_dep.module("kid");

    kobolds_unit_tests.root_module.addImport("uuid", uuid_mod);
    kobolds_unit_tests.root_module.addImport("stdx", stdx_mod);
    kobolds_unit_tests.root_module.addImport("kid", kid_mod);

    const run_unit_tests = b.addRunArtifact(kobolds_unit_tests);
    const test_step = b.step("cicd:test", "Run unit tests");

    test_step.dependOn(&run_unit_tests.step);
}

fn setupBenchmarks(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) void {
    const bench_lib = b.addTest(.{
        .name = "bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("./src/bench.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const zbench_dep = b.dependency("zbench", .{
        .target = target,
        .optimize = optimize,
    });
    const zbench_mod = zbench_dep.module("zbench");

    const stdx_dep = b.dependency("stdx", .{
        .target = target,
        .optimize = optimize,
    });
    const stdx_mod = stdx_dep.module("stdx");

    const kid_dep = b.dependency("kid", .{
        .target = target,
        .optimize = optimize,
    });
    const kid_mod = kid_dep.module("kid");

    const uuid_dep = b.dependency("uuid", .{
        .target = target,
        .optimize = optimize,
    });
    const uuid_mod = uuid_dep.module("uuid");

    bench_lib.root_module.addImport("kid", kid_mod);
    bench_lib.root_module.addImport("zbench", zbench_mod);
    bench_lib.root_module.addImport("stdx", stdx_mod);
    bench_lib.root_module.addImport("uuid", uuid_mod);

    const run_bench_tests = b.addRunArtifact(bench_lib);
    const bench_test_step = b.step("bench", "Run benchmark tests");
    bench_test_step.dependOn(&run_bench_tests.step);
}

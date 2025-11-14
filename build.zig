const std = @import("std");

const version = std.SemanticVersion{ .major = 0, .minor = 0, .patch = 0 };

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    setupExecutable(b, target, optimize);
    setupTests(b, target, optimize);
    setupBenchmarks(b, target, optimize);
}

fn setupExecutable(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
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

fn setupTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
    const kobolds_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/test.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const runner = b.addExecutable(.{
        .name = "runner",
        .root_source_file = b.path("src/test_runner.zig"),
        .target = target,
        .optimize = optimize,
    });

    runner.addAllPackageTests();

    const run_verbose_test = b.addRunArtifact(runner);
    run_verbose.addArg("--verbose");
    b.step("test:verbose", "Run tests with verbose output")
        .dependOn(&run_verbose_test.step);

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

    kobolds_unit_tests.root_module.addImport("stdx", stdx_mod);
    kobolds_unit_tests.root_module.addImport("kid", kid_mod);

    const run_unit_tests = b.addRunArtifact(kobolds_unit_tests);
    const test_step = b.step("test", "Run unit tests");

    test_step.dependOn(&run_unit_tests.step);
}

fn setupBenchmarks(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
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

    bench_lib.root_module.addImport("kid", kid_mod);
    bench_lib.root_module.addImport("zbench", zbench_mod);
    bench_lib.root_module.addImport("stdx", stdx_mod);

    const run_bench_tests = b.addRunArtifact(bench_lib);
    const bench_test_step = b.step("bench", "Run benchmark tests");
    bench_test_step.dependOn(&run_bench_tests.step);
}

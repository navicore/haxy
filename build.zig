const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // main
    const install_main_exe = blk: {
        const exe = b.addExecutable(.{
            .name = "haxy",
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/main.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        exe.root_module.addImport("xit", b.dependency("xit", .{}).module("xit"));

        const install_exe = b.addInstallArtifact(exe, .{});
        b.getInstallStep().dependOn(&install_exe.step);

        const run_cmd = b.addRunArtifact(exe);

        run_cmd.step.dependOn(b.getInstallStep());

        if (b.args) |args| {
            run_cmd.addArgs(args);
        }

        const run_step = b.step("run", "Run the app");
        run_step.dependOn(&run_cmd.step);

        break :blk install_exe;
    };

    // module for using haxy as a library
    // (the commands below consume haxy this way)
    const haxy = b.addModule("haxy", .{
        .root_source_file = b.path("src/lib.zig"),
    });
    haxy.addImport("xit", b.dependency("xit", .{}).module("xit"));

    // test
    {
        const unit_tests = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/test.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        unit_tests.root_module.addImport("haxy", haxy);

        const run_unit_tests = b.addRunArtifact(unit_tests);
        run_unit_tests.has_side_effects = true;
        const test_step = b.step("test", "Run unit tests");
        test_step.dependOn(&install_main_exe.step);
        test_step.dependOn(&run_unit_tests.step);
    }

    // testnet
    {
        const unit_tests = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/testnet.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        unit_tests.root_module.addImport("haxy", haxy);

        const run_unit_tests = b.addRunArtifact(unit_tests);
        run_unit_tests.has_side_effects = true;
        const test_step = b.step("testnet", "Run network unit tests");
        test_step.dependOn(&install_main_exe.step);
        test_step.dependOn(&run_unit_tests.step);
    }
}

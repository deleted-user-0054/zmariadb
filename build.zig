const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const myzql = b.addModule("myzql", .{
        .root_source_file = b.path("./src/myzql.zig"),
        .target = target,
        .optimize = optimize,
    });
    linkPlatformLibraries(myzql, target);

    // -Dtest-filter="..."
    const test_filter = b.option([]const []const u8, "test-filter", "Filter for tests to run");

    // zig build unit_test
    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("./src/myzql.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    linkPlatformLibraries(unit_tests.root_module, target);
    if (test_filter) |t| unit_tests.filters = t;

    // zig build [install]
    b.installArtifact(unit_tests);

    // zig build -Dtest-filter="..." run_unit_test
    const run_unit_tests = b.addRunArtifact(unit_tests);
    const unit_test_step = b.step("unit_test", "Run unit tests");
    unit_test_step.dependOn(&run_unit_tests.step);

    // zig build -Dtest-filter="..." integration_test
    const integration_test_db_port = b.option(u16, "test-db-port", "Port for integration test database") orelse 3306;
    const integration_test_db_user = b.option([]const u8, "test-db-user", "Username for integration test database") orelse "root";
    const integration_test_db_password = b.option([]const u8, "test-db-password", "Password for integration test database") orelse "password";
    const integration_test_db_name = b.option([]const u8, "test-db-name", "Database name for integration tests") orelse "mysql";

    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("./integration_tests/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    linkPlatformLibraries(integration_tests.root_module, target);
    const integration_test_options = b.addOptions();
    integration_test_options.addOption(u16, "db_port", integration_test_db_port);
    integration_test_options.addOption([]const u8, "db_user", integration_test_db_user);
    integration_test_options.addOption([]const u8, "db_password", integration_test_db_password);
    integration_test_options.addOption([]const u8, "db_name", integration_test_db_name);
    integration_tests.root_module.addImport("myzql", myzql);
    integration_tests.root_module.addOptions("integration_test_options", integration_test_options);
    if (test_filter) |t| integration_tests.filters = t;

    // zig build [install]
    b.installArtifact(integration_tests);

    // zig build integration_test
    const run_integration_tests = b.addRunArtifact(integration_tests);
    const integration_test_step = b.step("integration_test", "Run integration tests");
    integration_test_step.dependOn(&run_integration_tests.step);
}

fn linkPlatformLibraries(module: *std.Build.Module, target: std.Build.ResolvedTarget) void {
    if (target.result.os.tag == .windows) {
        module.link_libc = true;
        module.linkSystemLibrary("secur32", .{});
    }
}

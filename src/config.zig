const std = @import("std");
const constants = @import("./constants.zig");

pub const Address = union(enum) {
    ip: std.Io.net.IpAddress,
    unix: std.Io.net.UnixAddress,

    pub fn localhost(port: u16) Address {
        return .{ .ip = .{ .ip4 = .loopback(port) } };
    }
};

pub const ReconnectPolicy = struct {
    enabled: bool = false,
    max_attempts: u8 = 1,
};

/// Configuration for a MySQL/MariaDB connection.
pub const Config = struct {
    /// MySQL username. Default: "root"
    username: [:0]const u8 = "root",
    /// Server address. Default: 127.0.0.1:3306
    address: Address = Address.localhost(3306),
    /// MySQL password. Default: ""
    password: []const u8 = "",
    /// Default database to use. Default: ""
    database: [:0]const u8 = "",
    collation: u8 = constants.utf8mb4_general_ci,

    /// Return number of matching rows instead of rows changed. Default: false
    client_found_rows: bool = false,
    /// Allow multiple statements in a single query. Default: false
    multi_statements: bool = false,
    /// Reconnect dead sessions after a communication failure, before a later command. Default: disabled.
    reconnect: ReconnectPolicy = .{},

    pub fn capability_flags(config: *const Config) u32 {
        // zig fmt: off
        var flags: u32 = constants.CLIENT_PROTOCOL_41
                       | constants.CLIENT_PLUGIN_AUTH
                       | constants.CLIENT_SECURE_CONNECTION
                       | constants.CLIENT_TRANSACTIONS
                       | constants.CLIENT_DEPRECATE_EOF
                       // TODO: Support more
                       ;
        // zig fmt: on
        if (config.client_found_rows) {
            flags |= constants.CLIENT_FOUND_ROWS;
        }
        if (config.multi_statements) {
            flags |= constants.CLIENT_MULTI_STATEMENTS;
        }
        if (config.database.len > 0) {
            flags |= constants.CLIENT_CONNECT_WITH_DB;
        }
        return flags;
    }
};

pub const OwnedConfig = struct {
    username: [:0]u8,
    address: Address,
    password: []u8,
    database: [:0]u8,
    collation: u8,
    client_found_rows: bool,
    multi_statements: bool,
    reconnect: ReconnectPolicy,

    pub fn init(allocator: std.mem.Allocator, config: *const Config) !OwnedConfig {
        const username = try allocator.dupeZ(u8, config.username);
        errdefer allocator.free(username);

        const password = try allocator.dupe(u8, config.password);
        errdefer allocator.free(password);

        const database = try allocator.dupeZ(u8, config.database);
        errdefer allocator.free(database);

        return .{
            .username = username,
            .address = config.address,
            .password = password,
            .database = database,
            .collation = config.collation,
            .client_found_rows = config.client_found_rows,
            .multi_statements = config.multi_statements,
            .reconnect = config.reconnect,
        };
    }

    pub fn deinit(config: *OwnedConfig, allocator: std.mem.Allocator) void {
        allocator.free(config.username);
        allocator.free(config.password);
        allocator.free(config.database);
    }

    pub fn view(config: *const OwnedConfig) Config {
        return .{
            .username = config.username,
            .address = config.address,
            .password = config.password,
            .database = config.database,
            .collation = config.collation,
            .client_found_rows = config.client_found_rows,
            .multi_statements = config.multi_statements,
            .reconnect = config.reconnect,
        };
    }
};

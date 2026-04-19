const std = @import("std");

const config_mod = @import("./config.zig");
const Conn = @import("./conn.zig").Conn;
const Tx = @import("./conn.zig").Tx;
const result = @import("./result.zig");
const PreparedStatement = result.PreparedStatement;
const QueryResult = result.QueryResult;
const QueryResultRows = result.QueryResultRows;
const TextResultRow = result.TextResultRow;
const BinaryResultRow = result.BinaryResultRow;

pub const PoolOptions = struct {
    min_connections: u16 = 0,
    max_connections: u16 = 8,
    ping_on_acquire: bool = false,
    reset_on_release: bool = true,
};

const Entry = struct {
    conn: ?*Conn = null,
    in_use: bool = false,
};

pub const Pool = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    config: config_mod.OwnedConfig,
    options: PoolOptions,
    entries: std.ArrayList(Entry),

    pub fn init(io: std.Io, allocator: std.mem.Allocator, config: *const config_mod.Config, options: PoolOptions) !Pool {
        var pool = Pool{
            .io = io,
            .allocator = allocator,
            .config = try config_mod.OwnedConfig.init(allocator, config),
            .options = options,
            .entries = .empty,
        };
        errdefer pool.deinit();

        if (pool.options.max_connections == 0) {
            return error.InvalidPoolSize;
        }

        const warmup = @min(pool.options.min_connections, pool.options.max_connections);
        var i: u16 = 0;
        while (i < warmup) : (i += 1) {
            const conn = try pool.createConn();
            errdefer {
                conn.deinit(allocator);
                allocator.destroy(conn);
            }
            try pool.entries.append(allocator, .{ .conn = conn, .in_use = false });
        }

        return pool;
    }

    pub fn deinit(pool: *Pool) void {
        for (pool.entries.items) |entry| {
            if (entry.conn) |conn| {
                conn.deinit(pool.allocator);
                pool.allocator.destroy(conn);
            }
        }
        pool.entries.deinit(pool.allocator);
        pool.config.deinit(pool.allocator);
    }

    pub fn acquire(pool: *Pool) !Lease {
        for (pool.entries.items, 0..) |*entry, index| {
            if (entry.in_use) continue;
            entry.in_use = true;
            errdefer entry.in_use = false;
            const conn = try pool.prepareEntryConn(entry);
            return .{ .pool = pool, .entry_index = index, .conn = conn, .released = false };
        }

        if (pool.entries.items.len >= pool.options.max_connections) {
            return error.PoolExhausted;
        }

        const conn = try pool.createConn();
        errdefer {
            conn.deinit(pool.allocator);
            pool.allocator.destroy(conn);
        }
        try pool.entries.append(pool.allocator, .{ .conn = conn, .in_use = true });
        return .{ .pool = pool, .entry_index = pool.entries.items.len - 1, .conn = conn, .released = false };
    }

    fn createConn(pool: *Pool) !*Conn {
        const cfg = pool.config.view();
        const conn = try pool.allocator.create(Conn);
        errdefer pool.allocator.destroy(conn);
        conn.* = try Conn.init(pool.io, pool.allocator, &cfg);
        return conn;
    }

    fn prepareEntryConn(pool: *Pool, entry: *Entry) !*Conn {
        if (entry.conn == null) {
            entry.conn = try pool.createConn();
        }
        const conn = entry.conn.?;

        if (!conn.connected) {
            conn.ensureConnected() catch |err| {
                pool.discardEntryConn(entry);
                return err;
            };
        }

        if (pool.options.ping_on_acquire) {
            conn.ping() catch |err| {
                if (!conn.connected) {
                    pool.discardEntryConn(entry);
                }
                return err;
            };
        }

        return conn;
    }

    fn discardEntryConn(pool: *Pool, entry: *Entry) void {
        if (entry.conn) |conn| {
            conn.deinit(pool.allocator);
            pool.allocator.destroy(conn);
            entry.conn = null;
        }
    }

    fn release(pool: *Pool, lease: *Lease) void {
        const entry = &pool.entries.items[lease.entry_index];
        defer entry.in_use = false;

        if (entry.conn == null) return;
        const conn = entry.conn.?;

        var discard = !conn.isReusable();
        if (!discard and conn.inTransaction()) {
            conn.rollback() catch {
                discard = true;
            };
        }
        if (!discard and pool.options.reset_on_release and conn.connected) {
            conn.resetSession() catch {
                discard = true;
            };
        }

        if (discard) pool.discardEntryConn(entry);
    }
};

pub const Lease = struct {
    pool: *Pool,
    entry_index: usize,
    conn: *Conn,
    released: bool,

    pub fn deinit(lease: *Lease) void {
        if (lease.released) return;
        lease.pool.release(lease);
        lease.released = true;
    }

    pub fn ping(lease: *Lease) !void {
        return lease.conn.ping();
    }

    pub fn query(lease: *Lease, sql: []const u8) !QueryResult {
        return lease.conn.query(sql);
    }

    pub fn queryRows(lease: *Lease, allocator: std.mem.Allocator, sql: []const u8) !QueryResultRows(TextResultRow) {
        return lease.conn.queryRows(allocator, sql);
    }

    pub fn prepare(lease: *Lease, allocator: std.mem.Allocator, sql: []const u8) !result.PrepareResult {
        return lease.conn.prepare(allocator, sql);
    }

    pub fn execute(lease: *Lease, stmt: *const PreparedStatement, params: anytype) !QueryResult {
        return lease.conn.execute(stmt, params);
    }

    pub fn executeRows(lease: *Lease, allocator: std.mem.Allocator, stmt: *const PreparedStatement, params: anytype) !QueryResultRows(BinaryResultRow) {
        return lease.conn.executeRows(allocator, stmt, params);
    }

    pub fn begin(lease: *Lease) !Tx {
        return lease.conn.begin();
    }
};

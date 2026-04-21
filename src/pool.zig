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
const Counter = std.atomic.Value(u64);

pub const PoolOptions = struct {
    min_connections: u16 = 0,
    max_connections: u16 = 8,
    ping_on_acquire: bool = false,
    /// Optional SQL executed on acquire to validate liveness.
    /// The query may return either OK or rows; rows are fully drained.
    health_check_query: ?[]const u8 = null,
    reset_on_release: bool = true,
    /// Max time to wait for a free lease when pool is exhausted. 0 means no wait.
    acquire_timeout_ms: u32 = 0,
    /// Poll interval while waiting for a free lease.
    acquire_retry_interval_ms: u16 = 10,
    /// Retire a connection once it has existed for this long. 0 disables.
    max_lifetime_ms: u32 = 0,
    /// Retire a connection that stayed idle for this long. 0 disables.
    max_idle_ms: u32 = 0,
};

pub const PoolStats = struct {
    acquire_requests: u64,
    acquire_success: u64,
    acquire_waits: u64,
    acquire_timeouts: u64,
    acquire_exhausted_immediate: u64,
    connections_created: u64,
    connections_discarded: u64,
    discarded_idle_expired: u64,
    discarded_lifetime_expired: u64,
    ping_failures: u64,
    health_check_failures: u64,
    rollback_failures: u64,
    reset_failures: u64,
};

const Entry = struct {
    conn: ?*Conn = null,
    in_use: bool = false,
    created_at_ns: i96 = 0,
    last_released_at_ns: i96 = 0,
};

const ExpirationReason = enum {
    none,
    lifetime,
    idle,
};

const StatsState = struct {
    acquire_requests: Counter = .init(0),
    acquire_success: Counter = .init(0),
    acquire_waits: Counter = .init(0),
    acquire_timeouts: Counter = .init(0),
    acquire_exhausted_immediate: Counter = .init(0),
    connections_created: Counter = .init(0),
    connections_discarded: Counter = .init(0),
    discarded_idle_expired: Counter = .init(0),
    discarded_lifetime_expired: Counter = .init(0),
    ping_failures: Counter = .init(0),
    health_check_failures: Counter = .init(0),
    rollback_failures: Counter = .init(0),
    reset_failures: Counter = .init(0),
};

pub const Pool = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    config: config_mod.OwnedConfig,
    options: PoolOptions,
    entries: std.ArrayList(Entry),
    health_check_query: ?[]u8,
    stats: StatsState,
    mutex: std.Io.Mutex,
    available_event: std.Io.Event,

    pub fn init(io: std.Io, allocator: std.mem.Allocator, config: *const config_mod.Config, options: PoolOptions) !Pool {
        const health_check_query = if (options.health_check_query) |query|
            if (query.len == 0) null else try allocator.dupe(u8, query)
        else
            null;
        errdefer if (health_check_query) |query| allocator.free(query);

        var pool = Pool{
            .io = io,
            .allocator = allocator,
            .config = try config_mod.OwnedConfig.init(allocator, config),
            .options = options,
            .entries = .empty,
            .health_check_query = health_check_query,
            .stats = .{},
            .mutex = .init,
            .available_event = .unset,
        };
        errdefer pool.deinit();

        if (pool.options.max_connections == 0) {
            return error.InvalidPoolSize;
        }
        if (pool.options.min_connections > pool.options.max_connections) {
            return error.InvalidPoolSize;
        }

        try pool.entries.ensureTotalCapacity(allocator, pool.options.max_connections);

        const warmup = @min(pool.options.min_connections, pool.options.max_connections);
        const warmed_at_ns = pool.nowNs();
        var i: u16 = 0;
        while (i < warmup) : (i += 1) {
            const conn = try pool.createConn();
            errdefer {
                conn.deinit(allocator);
                allocator.destroy(conn);
            }
            try pool.entries.append(allocator, .{
                .conn = conn,
                .in_use = false,
                .created_at_ns = warmed_at_ns,
                .last_released_at_ns = warmed_at_ns,
            });
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
        if (pool.health_check_query) |query| pool.allocator.free(query);
        pool.entries.deinit(pool.allocator);
        pool.config.deinit(pool.allocator);
    }

    pub fn acquire(pool: *Pool) !Lease {
        inc(&pool.stats.acquire_requests);

        if (pool.options.acquire_timeout_ms == 0) {
            const entry_index = try pool.reserveLeaseIndex() orelse {
                inc(&pool.stats.acquire_exhausted_immediate);
                return error.PoolExhausted;
            };
            const lease = try pool.activateLease(entry_index);
            inc(&pool.stats.acquire_success);
            return lease;
        }

        const deadline = std.Io.Clock.Timestamp.fromNow(pool.io, .{
            .clock = .awake,
            .raw = std.Io.Duration.fromMilliseconds(@intCast(pool.options.acquire_timeout_ms)),
        });
        const retry_interval_ns = std.Io.Duration.fromMilliseconds(@intCast(@as(u32, @max(pool.options.acquire_retry_interval_ms, 1)))).nanoseconds;

        while (true) {
            if (try pool.reserveLeaseIndex()) |entry_index| {
                const lease = try pool.activateLease(entry_index);
                inc(&pool.stats.acquire_success);
                return lease;
            }

            const remaining = deadline.durationFromNow(pool.io);
            if (remaining.raw.nanoseconds <= 0) {
                inc(&pool.stats.acquire_timeouts);
                return error.AcquireTimeout;
            }

            const wait_ns: i96 = @max(@as(i96, 1), @min(remaining.raw.nanoseconds, retry_interval_ns));
            inc(&pool.stats.acquire_waits);
            pool.available_event.waitTimeout(pool.io, .{
                .duration = .{
                    .clock = .awake,
                    .raw = std.Io.Duration.fromNanoseconds(wait_ns),
                },
            }) catch |err| switch (err) {
                error.Timeout => continue,
                else => return err,
            };
        }
    }

    fn reserveLeaseIndex(pool: *Pool) !?usize {
        pool.mutex.lockUncancelable(pool.io);
        defer pool.mutex.unlock(pool.io);

        for (pool.entries.items, 0..) |*entry, index| {
            if (entry.in_use) continue;
            entry.in_use = true;
            return index;
        }

        if (pool.entries.items.len >= pool.options.max_connections) {
            pool.available_event.reset();
            return null;
        }

        try pool.entries.append(pool.allocator, .{ .conn = null, .in_use = true });
        return pool.entries.items.len - 1;
    }

    fn activateLease(pool: *Pool, entry_index: usize) !Lease {
        const entry = &pool.entries.items[entry_index];
        const conn = pool.prepareEntryConn(entry) catch |err| {
            pool.releaseReservation(entry_index);
            return err;
        };
        return .{ .pool = pool, .entry_index = entry_index, .conn = conn, .released = false };
    }

    fn releaseReservation(pool: *Pool, entry_index: usize) void {
        pool.mutex.lockUncancelable(pool.io);
        defer pool.mutex.unlock(pool.io);
        pool.entries.items[entry_index].in_use = false;
        pool.available_event.set(pool.io);
    }

    fn createConn(pool: *Pool) !*Conn {
        const cfg = pool.config.view();
        const conn = try pool.allocator.create(Conn);
        errdefer pool.allocator.destroy(conn);
        conn.* = try Conn.init(pool.io, pool.allocator, &cfg);
        inc(&pool.stats.connections_created);
        return conn;
    }

    fn prepareEntryConn(pool: *Pool, entry: *Entry) !*Conn {
        const now_ns = pool.nowNs();
        const expiration_reason = pool.connectionExpirationReason(entry, now_ns);
        if (entry.conn != null and expiration_reason != .none) {
            switch (expiration_reason) {
                .lifetime => inc(&pool.stats.discarded_lifetime_expired),
                .idle => inc(&pool.stats.discarded_idle_expired),
                .none => {},
            }
            pool.discardEntryConn(entry);
        }

        if (entry.conn == null) {
            entry.conn = try pool.createConn();
            entry.created_at_ns = now_ns;
            entry.last_released_at_ns = now_ns;
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
                inc(&pool.stats.ping_failures);
                if (!conn.connected) {
                    pool.discardEntryConn(entry);
                }
                return err;
            };
        }
        if (pool.health_check_query) |query| {
            conn.healthCheckQuery(pool.allocator, query) catch |err| {
                inc(&pool.stats.health_check_failures);
                pool.discardEntryConn(entry);
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
            inc(&pool.stats.connections_discarded);
        }
        entry.created_at_ns = 0;
        entry.last_released_at_ns = 0;
    }

    fn connectionExpirationReason(pool: *const Pool, entry: *const Entry, now_ns: i96) ExpirationReason {
        if (pool.options.max_lifetime_ms > 0 and entry.created_at_ns > 0) {
            const lifetime_ns = @as(i96, pool.options.max_lifetime_ms) * std.time.ns_per_ms;
            if (now_ns - entry.created_at_ns >= lifetime_ns) return .lifetime;
        }
        if (pool.options.max_idle_ms > 0 and entry.last_released_at_ns > 0) {
            const idle_ns = @as(i96, pool.options.max_idle_ms) * std.time.ns_per_ms;
            if (now_ns - entry.last_released_at_ns >= idle_ns) return .idle;
        }
        return .none;
    }

    fn nowNs(pool: *const Pool) i96 {
        return std.Io.Clock.Timestamp.now(pool.io, .awake).raw.nanoseconds;
    }

    fn release(pool: *Pool, lease: *Lease) void {
        const entry = &pool.entries.items[lease.entry_index];
        if (entry.conn) |conn| {
            var discard = !conn.isReusable();
            if (!discard and conn.inTransaction()) {
                conn.rollback() catch {
                    inc(&pool.stats.rollback_failures);
                    discard = true;
                };
            }
            if (!discard and pool.options.reset_on_release and conn.connected) {
                conn.resetSession() catch {
                    inc(&pool.stats.reset_failures);
                    discard = true;
                };
            }

            if (discard) {
                pool.discardEntryConn(entry);
            } else {
                entry.last_released_at_ns = pool.nowNs();
            }
        }

        pool.releaseReservation(lease.entry_index);
    }

    pub fn statsSnapshot(pool: *const Pool) PoolStats {
        return .{
            .acquire_requests = load(&pool.stats.acquire_requests),
            .acquire_success = load(&pool.stats.acquire_success),
            .acquire_waits = load(&pool.stats.acquire_waits),
            .acquire_timeouts = load(&pool.stats.acquire_timeouts),
            .acquire_exhausted_immediate = load(&pool.stats.acquire_exhausted_immediate),
            .connections_created = load(&pool.stats.connections_created),
            .connections_discarded = load(&pool.stats.connections_discarded),
            .discarded_idle_expired = load(&pool.stats.discarded_idle_expired),
            .discarded_lifetime_expired = load(&pool.stats.discarded_lifetime_expired),
            .ping_failures = load(&pool.stats.ping_failures),
            .health_check_failures = load(&pool.stats.health_check_failures),
            .rollback_failures = load(&pool.stats.rollback_failures),
            .reset_failures = load(&pool.stats.reset_failures),
        };
    }
};

fn inc(counter: *Counter) void {
    _ = counter.fetchAdd(1, .monotonic);
}

fn load(counter: *const Counter) u64 {
    return counter.load(.monotonic);
}

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

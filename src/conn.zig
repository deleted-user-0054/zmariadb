const std = @import("std");

const auth = @import("./auth.zig");
const auth_gssapi = @import("./auth_gssapi.zig");
const config_mod = @import("./config.zig");
const Config = config_mod.Config;
const OwnedConfig = config_mod.OwnedConfig;
const ReconnectPolicy = config_mod.ReconnectPolicy;
const constants = @import("./constants.zig");
const protocol = @import("./protocol.zig");
const HandshakeV10 = protocol.handshake_v10.HandshakeV10;
const ErrorPacket = protocol.generic_response.ErrorPacket;
const OkPacket = protocol.generic_response.OkPacket;
const HandshakeResponse41 = protocol.handshake_response.HandshakeResponse41;
const AuthSwitchRequest = protocol.auth_switch_request.AuthSwitchRequest;
const QueryRequest = protocol.text_command.QueryRequest;
const prepared_statements = protocol.prepared_statements;
const PrepareRequest = prepared_statements.PrepareRequest;
const ExecuteRequest = prepared_statements.ExecuteRequest;
const Packet = protocol.packet.Packet;
const PacketReader = protocol.packet_reader.PacketReader;
const PacketWriter = protocol.packet_writer.PacketWriter;
const result = @import("./result.zig");
const QueryResultRows = result.QueryResultRows;
const QueryResult = result.QueryResult;
const PrepareResult = result.PrepareResult;
const PreparedStatement = result.PreparedStatement;
const TextResultRow = result.TextResultRow;
const BinaryResultRow = result.BinaryResultRow;
const ResultMeta = @import("./result_meta.zig").ResultMeta;

/// A MySQL/MariaDB connection.
/// Use `init` to establish a connection, and `deinit` to close it.
/// A single `Conn` must not be used concurrently from multiple threads.
pub const Conn = struct {
    io: std.Io,
    connected: bool,
    stream: ?std.Io.net.Stream,
    reader: ?PacketReader,
    writer: ?PacketWriter,
    capabilities: u32,
    status_flags: u16,
    sequence_id: u8,
    generation: u64,
    active_result_set: bool,
    transaction_state_lost: bool,
    allocator: std.mem.Allocator,
    owned_config: OwnedConfig,

    // Buffer to store metadata of the result set
    result_meta: ResultMeta,

    /// Establish a connection to a MySQL/MariaDB server.
    /// Performs the TCP connection and authentication handshake.
    /// Caller must call `deinit` when done.
    pub fn init(io: std.Io, allocator: std.mem.Allocator, config: *const Config) !Conn {
        var conn = Conn{
            .io = io,
            .connected = false,
            .stream = null,
            .reader = null,
            .writer = null,
            .capabilities = 0,
            .status_flags = constants.SERVER_STATUS_AUTOCOMMIT,
            .sequence_id = 0,
            .generation = 0,
            .active_result_set = false,
            .transaction_state_lost = false,
            .allocator = allocator,
            .owned_config = try OwnedConfig.init(allocator, config),
            .result_meta = ResultMeta.init(),
        };
        errdefer conn.deinit(allocator);

        try conn.connect();
        return conn;
    }

    /// Close the connection and free resources.
    /// Sends a COM_QUIT packet to the server before closing.
    pub fn deinit(c: *Conn, allocator: std.mem.Allocator) void {
        _ = allocator;
        if (c.connected) {
            c.quit() catch |err| {
                std.log.err("Failed to quit: {any}\n", .{err});
            };
        }
        c.cleanupTransport();
        c.result_meta.deinit(c.allocator);
        c.owned_config.deinit(c.allocator);
    }

    /// Reconnect a session explicitly, invalidating prepared statements and active results.
    pub fn reconnect(c: *Conn) !void {
        c.cleanupTransport();
        try c.connect();
    }

    /// Reconnect a dead session if reconnect is enabled.
    pub fn ensureConnected(c: *Conn) !void {
        if (c.connected) return;
        if (!c.owned_config.reconnect.enabled) return error.ConnectionClosed;
        if (c.transaction_state_lost) return error.TransactionLost;

        const max_attempts: u8 = @max(@as(u8, 1), c.owned_config.reconnect.max_attempts);
        var retry_delay_ms = clampReconnectDelayMs(c.owned_config.reconnect.retry_delay_ms, &c.owned_config.reconnect);
        var attempt: u8 = 0;
        while (attempt < max_attempts) : (attempt += 1) {
            c.connect() catch |err| {
                if (attempt + 1 >= max_attempts) return err;
                const sleep_ms = try reconnectSleepDelayMs(c.io, retry_delay_ms, &c.owned_config.reconnect);
                if (sleep_ms > 0) {
                    try std.Io.sleep(c.io, std.Io.Duration.fromMilliseconds(@intCast(sleep_ms)), .awake);
                }
                retry_delay_ms = nextReconnectDelayMs(retry_delay_ms, &c.owned_config.reconnect);
                continue;
            };
            return;
        }
    }

    fn reconnectSleepDelayMs(io: std.Io, base_delay_ms: u32, policy: *const ReconnectPolicy) !u32 {
        if (policy.jitter_ms == 0) return base_delay_ms;

        var random_bytes: [2]u8 = undefined;
        io.random(&random_bytes);
        const random = std.mem.readInt(u16, &random_bytes, .little);
        const jitter = @as(u32, random % (policy.jitter_ms + 1));
        return base_delay_ms +% jitter;
    }

    fn clampReconnectDelayMs(delay_ms: u32, policy: *const ReconnectPolicy) u32 {
        if (policy.max_retry_delay_ms > 0 and delay_ms > policy.max_retry_delay_ms) {
            return policy.max_retry_delay_ms;
        }
        return delay_ms;
    }

    fn nextReconnectDelayMs(current_delay_ms: u32, policy: *const ReconnectPolicy) u32 {
        const multiplier: u32 = @max(@as(u32, 1), policy.retry_backoff_multiplier);
        const scaled = std.math.mul(u32, current_delay_ms, multiplier) catch std.math.maxInt(u32);
        return clampReconnectDelayMs(scaled, policy);
    }

    /// Return true when the server session is currently in a transaction.
    pub fn inTransaction(c: *const Conn) bool {
        return c.status_flags & constants.SERVER_STATUS_IN_TRANS != 0;
    }

    pub fn hasActiveResultSet(c: *const Conn) bool {
        return c.active_result_set;
    }

    pub fn currentGeneration(c: *const Conn) u64 {
        return c.generation;
    }

    pub fn isReusable(c: *const Conn) bool {
        if (!c.connected) return false;
        if (c.active_result_set) return false;
        if (c.writer == null or c.reader == null) return false;
        return c.writer.?.pos == 0 and c.reader.?.reader.interface.bufferedLen() == 0;
    }

    pub fn rememberOkPacket(c: *Conn, ok: OkPacket) void {
        if (ok.status_flags) |status_flags| {
            c.status_flags = status_flags;
            if (status_flags & constants.SERVER_STATUS_IN_TRANS == 0) {
                c.transaction_state_lost = false;
            }
        }
    }

    pub fn setActiveResultSet(c: *Conn, active: bool) void {
        c.active_result_set = active;
    }

    /// Reset the server session before reusing the connection.
    pub fn resetSession(c: *Conn) !void {
        try c.ensureReady();
        try c.writeBytesAsPacket(&[_]u8{constants.COM_RESET_CONNECTION});
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
        const packet = c.readPacket() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
        switch (packet.payload[0]) {
            constants.OK => {
                c.rememberOkPacket(OkPacket.init(&packet, c.capabilities));
                c.generation +%= 1;
                c.transaction_state_lost = false;
            },
            else => return packet.asError(),
        }
    }

    /// Start a transaction.
    pub fn begin(c: *Conn) !Tx {
        if (c.inTransaction()) return error.TransactionAlreadyActive;
        const query_res = try c.query("START TRANSACTION");
        _ = try query_res.expect(.ok);
        return .{ .conn = c, .generation = c.currentGeneration(), .active = true };
    }

    /// Commit the current transaction.
    pub fn commit(c: *Conn) !void {
        const query_res = try c.query("COMMIT");
        _ = try query_res.expect(.ok);
    }

    /// Roll back the current transaction.
    pub fn rollback(c: *Conn) !void {
        const query_res = try c.query("ROLLBACK");
        _ = try query_res.expect(.ok);
    }

    /// Send COM_STMT_CLOSE for a live prepared statement.
    pub fn closePreparedStatement(c: *Conn, statement_id: u32) !void {
        try c.ensureReady();
        var payload: [5]u8 = undefined;
        payload[0] = constants.COM_STMT_CLOSE;
        std.mem.writeInt(u32, payload[1..5], statement_id, .little);
        try c.writeBytesAsPacket(payload[0..]);
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
    }

    /// Send a ping to the server to verify the connection is alive.
    pub fn ping(c: *Conn) !void {
        try c.ensureReady();
        try c.writeBytesAsPacket(&[_]u8{constants.COM_PING});
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
        const packet = c.readPacket() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };

        switch (packet.payload[0]) {
            constants.OK => c.rememberOkPacket(OkPacket.init(&packet, c.capabilities)),
            else => return packet.asError(),
        }
    }

    /// Execute a text query that does not return rows (e.g. CREATE, INSERT, UPDATE, DELETE).
    pub fn query(c: *Conn, query_string: []const u8) !QueryResult {
        try c.ensureReady();
        const query_req: QueryRequest = .{ .query = query_string };
        try c.writePacket(query_req);
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
        const packet = c.readPacket() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
        return c.queryResult(&packet);
    }

    /// Execute a text query that returns rows (e.g. SELECT).
    pub fn queryRows(c: *Conn, allocator: std.mem.Allocator, query_string: []const u8) !QueryResultRows(TextResultRow) {
        try c.ensureReady();
        const query_req: QueryRequest = .{ .query = query_string };
        try c.writePacket(query_req);
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
        return c.queryRowsResult(TextResultRow, allocator);
    }

    /// Run a health-check query and fully consume any result set it returns.
    /// Accepts both statement-style responses (`OK`) and row-producing responses.
    pub fn healthCheckQuery(c: *Conn, allocator: std.mem.Allocator, query_string: []const u8) !void {
        try c.ensureReady();
        const query_req: QueryRequest = .{ .query = query_string };
        try c.writePacket(query_req);
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
        const packet = c.readPacket() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };

        switch (packet.payload[0]) {
            constants.OK => {
                c.rememberOkPacket(OkPacket.init(&packet, c.capabilities));
                c.active_result_set = false;
            },
            constants.ERR => return packet.asError(),
            constants.LOCAL_INFILE_REQUEST => {
                c.closeDueToFatalProtocolError();
                return error.UnsupportedLocalInfileRequest;
            },
            else => {
                var rows = result.ResultSet(TextResultRow).init(c, allocator, &packet) catch |err| {
                    c.closeDueToCommunicationFailure();
                    return err;
                };
                c.active_result_set = true;
                rows.drain() catch |err| {
                    c.closeDueToCommunicationFailure();
                    return err;
                };
                c.active_result_set = false;
            },
        }
    }

    /// Prepare a SQL statement for execution.
    pub fn prepare(c: *Conn, allocator: std.mem.Allocator, query_string: []const u8) !PrepareResult {
        try c.ensureReady();
        const prepare_request: PrepareRequest = .{ .query = query_string };
        try c.writePacket(prepare_request);
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
        return PrepareResult.init(c, allocator) catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
    }

    /// Execute a prepared statement that does not return rows.
    pub fn execute(c: *Conn, prep_stmt: *const PreparedStatement, params: anytype) !QueryResult {
        try c.ensureReady();
        if (!prep_stmt.isValidFor(c)) return error.StalePreparedStatement;
        std.debug.assert(prep_stmt.res_cols.len == 0);
        c.sequence_id = 0;
        const execute_request: ExecuteRequest = .{
            .prep_stmt = prep_stmt,
        };
        try c.writePacketWithParam(execute_request, params);
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
        const packet = c.readPacket() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
        return c.queryResult(&packet);
    }

    /// Execute a prepared statement that returns rows.
    pub fn executeRows(c: *Conn, allocator: std.mem.Allocator, prep_stmt: *const PreparedStatement, params: anytype) !QueryResultRows(BinaryResultRow) {
        try c.ensureReady();
        if (!prep_stmt.isValidFor(c)) return error.StalePreparedStatement;
        std.debug.assert(prep_stmt.res_cols.len > 0);
        c.sequence_id = 0;
        const execute_request: ExecuteRequest = .{
            .prep_stmt = prep_stmt,
        };
        try c.writePacketWithParam(execute_request, params);
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
        return c.queryRowsResult(BinaryResultRow, allocator);
    }

    fn connect(c: *Conn) !void {
        std.debug.assert(c.stream == null and c.reader == null and c.writer == null);

        const stream = switch (c.owned_config.address) {
            .ip => |address| try address.connect(c.io, .{ .mode = .stream }),
            .unix => |address| try address.connect(c.io),
        };

        c.stream = stream;
        errdefer c.cleanupTransport();

        c.reader = try PacketReader.init(stream, c.io, c.allocator);
        c.writer = try PacketWriter.init(stream, c.io, c.allocator);
        c.connected = true;
        c.capabilities = 0;
        c.sequence_id = 0;
        c.status_flags = constants.SERVER_STATUS_AUTOCOMMIT;
        c.active_result_set = false;
        c.transaction_state_lost = false;

        const config = c.owned_config.view();
        const auth_plugin, const auth_data = blk: {
            const packet = try c.readPacket();
            const handshake_v10 = switch (packet.payload[0]) {
                constants.HANDSHAKE_V10 => HandshakeV10.init(&packet),
                constants.ERR => return ErrorPacket.initFirst(&packet).asError(),
                else => return packet.asError(),
            };
            c.capabilities = handshake_v10.capability_flags() & config.capability_flags();
            c.status_flags = handshake_v10.status_flags;

            if (c.capabilities & constants.CLIENT_PROTOCOL_41 == 0) {
                std.log.err("protocol older than 4.1 is not supported\n", .{});
                return error.UnsupportedProtocol;
            }

            break :blk .{ handshake_v10.get_auth_plugin(), handshake_v10.get_auth_data() };
        };

        switch (auth_plugin) {
            .caching_sha2_password => try c.auth_caching_sha2_password(c.allocator, &auth_data, &config),
            .mysql_native_password => try c.auth_mysql_native_password(c.allocator, &auth_data, &config),
            .sha256_password => try c.auth_sha256_password(c.allocator, &auth_data, &config),
            else => {
                std.log.warn("Unsupported auth plugin: {any}\n", .{auth_plugin});
                return error.UnsupportedAuthPlugin;
            },
        }

        c.generation +%= 1;
    }

    fn cleanupTransport(c: *Conn) void {
        const tx_was_active = c.inTransaction();
        if (c.stream) |stream| {
            stream.close(c.io);
        }
        if (c.reader) |*reader| {
            reader.deinit();
        }
        if (c.writer) |*writer| {
            writer.deinit();
        }

        c.stream = null;
        c.reader = null;
        c.writer = null;
        c.connected = false;
        c.capabilities = 0;
        c.sequence_id = 0;
        c.status_flags = constants.SERVER_STATUS_AUTOCOMMIT;
        c.active_result_set = false;
        c.transaction_state_lost = c.transaction_state_lost or tx_was_active;
    }

    fn ensureReady(c: *Conn) !void {
        try c.ensureConnected();
        if (c.writer == null or c.reader == null) return error.ConnectionClosed;
        if (c.writer.?.pos != 0 or c.reader.?.reader.interface.bufferedLen() != 0 or c.active_result_set) {
            return error.ConnectionBusy;
        }
        c.sequence_id = 0;
    }

    fn quit(c: *Conn) !void {
        if (!c.connected) return;
        if (c.writer == null or c.reader == null) return;
        if (c.writer.?.pos != 0 or c.reader.?.reader.interface.bufferedLen() != 0 or c.active_result_set) {
            return error.ConnectionBusy;
        }

        c.sequence_id = 0;
        try c.writeBytesAsPacket(&[_]u8{constants.COM_QUIT});
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
        const packet = c.readPacket() catch |err| switch (err) {
            error.EndOfStream => {
                c.cleanupTransport();
                return;
            },
            else => {
                c.closeDueToCommunicationFailure();
                return err;
            },
        };
        return packet.asError();
    }

    fn authPluginData20(plugin_data: []const u8) ![20]u8 {
        const trimmed_len = std.mem.indexOfScalar(u8, plugin_data, 0) orelse plugin_data.len;
        if (trimmed_len > 20) {
            return error.InvalidAuthPluginData;
        }

        var auth_data = [_]u8{0} ** 20;
        @memcpy(auth_data[0..trimmed_len], plugin_data[0..trimmed_len]);
        return auth_data;
    }

    fn clearPasswordBytes(allocator: std.mem.Allocator, password: []const u8) ![]u8 {
        const bytes = try allocator.alloc(u8, password.len + 1);
        @memcpy(bytes[0..password.len], password);
        bytes[password.len] = 0;
        return bytes;
    }

    fn flushAndReadPacket(c: *Conn) !Packet {
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
        return c.readPacket() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
    }

    fn writeAuthPacketAndRead(c: *Conn, payload: []const u8) !Packet {
        try c.writeBytesAsPacket(payload);
        return c.flushAndReadPacket();
    }

    fn readNextAuthPacket(c: *Conn, payload: []const u8) !Packet {
        if (payload.len != 0) {
            return c.writeAuthPacketAndRead(payload);
        }
        return c.readPacket() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };
    }

    fn finishAuthPacket(c: *Conn, allocator: std.mem.Allocator, packet: *const Packet, config: *const Config) anyerror!void {
        return switch (packet.payload[0]) {
            constants.OK => c.rememberOkPacket(OkPacket.init(packet, c.capabilities)),
            constants.AUTH_SWITCH => c.authSwitch(allocator, AuthSwitchRequest.initFromPacket(packet), config),
            else => packet.asError(),
        };
    }

    fn continueSha256Password(c: *Conn, allocator: std.mem.Allocator, auth_data: *const [20]u8, packet: *const Packet, config: *const Config) anyerror!void {
        switch (packet.payload[0]) {
            constants.OK, constants.ERR, constants.AUTH_SWITCH => return c.finishAuthPacket(allocator, packet, config),
            else => {},
        }

        const decoded_pk = try auth.decodePublicKey(packet.payload, allocator);
        defer decoded_pk.deinit(allocator);

        const enc_pw = try auth.encryptPassword(c.io, allocator, config.password, auth_data, &decoded_pk.value);
        defer allocator.free(enc_pw);

        const resp_packet = try c.writeAuthPacketAndRead(enc_pw);
        return c.finishAuthPacket(allocator, &resp_packet, config);
    }

    fn continueCachingSha2Password(c: *Conn, allocator: std.mem.Allocator, auth_data: *const [20]u8, packet: *const Packet, config: *const Config) anyerror!void {
        switch (packet.payload[0]) {
            constants.OK, constants.ERR, constants.AUTH_SWITCH => return c.finishAuthPacket(allocator, packet, config),
            constants.AUTH_MORE_DATA => {
                const more_data = packet.payload[1..];
                if (more_data.len == 0) return error.UnsupportedCachingSha2PasswordMoreData;

                switch (more_data[0]) {
                    auth.caching_sha2_password_fast_auth_success => {
                        const ok_packet = try c.readPacket();
                        return c.finishAuthPacket(allocator, &ok_packet, config);
                    },
                    auth.caching_sha2_password_full_authentication_start => {
                        const pk_packet = try c.writeAuthPacketAndRead(&[_]u8{auth.caching_sha2_password_public_key_request});
                        const decoded_pk = try auth.decodePublicKey(pk_packet.payload, allocator);
                        defer decoded_pk.deinit(allocator);

                        const enc_pw = try auth.encryptPassword(c.io, allocator, config.password, auth_data, &decoded_pk.value);
                        defer allocator.free(enc_pw);

                        const resp_packet = try c.writeAuthPacketAndRead(enc_pw);
                        return c.finishAuthPacket(allocator, &resp_packet, config);
                    },
                    else => return error.UnsupportedCachingSha2PasswordMoreData,
                }
            },
            else => return packet.asError(),
        }
    }

    fn auth_gssapi_client(c: *Conn, allocator: std.mem.Allocator, plugin_data: []const u8, config: *const Config) anyerror!void {
        var session = try auth_gssapi.Session.init(allocator, plugin_data);
        defer session.deinit();

        var step = try session.nextToken(null);
        var packet = try c.readNextAuthPacket(step.token);

        while (true) {
            const server_token = switch (packet.payload[0]) {
                constants.OK, constants.ERR, constants.AUTH_SWITCH => return c.finishAuthPacket(allocator, &packet, config),
                else => try gssapiServerToken(packet.payload),
            };

            if (!step.continue_needed) {
                std.log.err("auth_gssapi_client received trailing token after SSPI completion", .{});
                return error.UnexpectedGssapiServerToken;
            }
            if (server_token.len == 0) return error.InvalidAuthPluginData;

            step = try session.nextToken(server_token);
            packet = try c.readNextAuthPacket(step.token);
        }
    }

    fn gssapiServerToken(payload: []const u8) ![]const u8 {
        if (payload.len == 0) return error.InvalidAuthPluginData;
        return switch (payload[0]) {
            constants.AUTH_MORE_DATA => if (payload.len > 1) payload[1..] else error.InvalidAuthPluginData,
            else => payload,
        };
    }

    fn authSwitch(c: *Conn, allocator: std.mem.Allocator, request: AuthSwitchRequest, config: *const Config) anyerror!void {
        const plugin = auth.AuthPlugin.fromName(request.plugin_name);
        switch (plugin) {
            .mysql_native_password => {
                const auth_data = try authPluginData20(request.plugin_data);
                const auth_resp = auth.scramblePassword(&auth_data, config.password);
                const packet = try c.writeAuthPacketAndRead(if (config.password.len > 0) &auth_resp else &[_]u8{});
                return c.finishAuthPacket(allocator, &packet, config);
            },
            .mysql_clear_password => {
                const clear_pw = try clearPasswordBytes(allocator, config.password);
                defer allocator.free(clear_pw);

                const packet = try c.writeAuthPacketAndRead(clear_pw);
                return c.finishAuthPacket(allocator, &packet, config);
            },
            .sha256_password => {
                const auth_data = try authPluginData20(request.plugin_data);
                if (config.password.len == 0) {
                    const packet = try c.writeAuthPacketAndRead(&[_]u8{0});
                    return c.finishAuthPacket(allocator, &packet, config);
                }

                const pk_packet = try c.writeAuthPacketAndRead(&[_]u8{auth.sha256_password_public_key_request});
                return c.continueSha256Password(allocator, &auth_data, &pk_packet, config);
            },
            .caching_sha2_password => {
                const auth_data = try authPluginData20(request.plugin_data);
                const packet = blk: {
                    if (config.password.len == 0) {
                        break :blk try c.writeAuthPacketAndRead(&[_]u8{});
                    }

                    const auth_resp = auth.scrambleSHA256Password(&auth_data, config.password);
                    break :blk try c.writeAuthPacketAndRead(&auth_resp);
                };
                return c.continueCachingSha2Password(allocator, &auth_data, &packet, config);
            },
            .auth_gssapi_client => return c.auth_gssapi_client(allocator, request.plugin_data, config),
            else => {
                std.log.warn("Unsupported auth switch plugin: {s}\n", .{request.plugin_name});
                return error.UnsupportedAuthPlugin;
            },
        }
    }

    fn auth_mysql_native_password(c: *Conn, allocator: std.mem.Allocator, auth_data: *const [20]u8, config: *const Config) !void {
        const auth_resp = auth.scramblePassword(auth_data, config.password);
        const response = HandshakeResponse41.init(.mysql_native_password, config, if (config.password.len > 0) &auth_resp else &[_]u8{});
        try c.writePacket(response);
        const packet = try c.flushAndReadPacket();
        return c.finishAuthPacket(allocator, &packet, config);
    }

    fn auth_sha256_password(c: *Conn, allocator: std.mem.Allocator, auth_data: *const [20]u8, config: *const Config) !void {
        const initial_response = if (config.password.len == 0)
            HandshakeResponse41.init(.sha256_password, config, &[_]u8{})
        else
            HandshakeResponse41.init(.sha256_password, config, &[_]u8{auth.sha256_password_public_key_request});
        try c.writePacket(initial_response);
        const packet = try c.flushAndReadPacket();

        if (config.password.len == 0) {
            return c.finishAuthPacket(allocator, &packet, config);
        }
        return c.continueSha256Password(allocator, auth_data, &packet, config);
    }

    fn auth_caching_sha2_password(c: *Conn, allocator: std.mem.Allocator, auth_data: *const [20]u8, config: *const Config) !void {
        const response = blk: {
            if (config.password.len == 0) {
                break :blk HandshakeResponse41.init(.caching_sha2_password, config, &[_]u8{});
            }

            const auth_resp = auth.scrambleSHA256Password(auth_data, config.password);
            break :blk HandshakeResponse41.init(.caching_sha2_password, config, &auth_resp);
        };
        try c.writePacket(&response);
        const packet = try c.flushAndReadPacket();
        return c.continueCachingSha2Password(allocator, auth_data, &packet, config);
    }

    pub inline fn readPacket(c: *Conn) !Packet {
        const packet = try c.reader.?.readPacket();
        c.sequence_id = packet.sequence_id +% 1;
        return packet;
    }

    pub inline fn readPutResultColumns(c: *Conn, allocator: std.mem.Allocator, n: usize) !void {
        try c.result_meta.readPutResultColumns(allocator, c, n);
    }

    inline fn writePacket(c: *Conn, packet: anytype) !void {
        try c.writer.?.writePacket(c.generateSequenceId(), packet);
    }

    inline fn writePacketWithParam(c: *Conn, packet: anytype, params: anytype) !void {
        try c.writer.?.writePacketWithParams(c.generateSequenceId(), packet, params);
    }

    inline fn writeBytesAsPacket(c: *Conn, packet: anytype) !void {
        try c.writer.?.writeBytesAsPacket(c.generateSequenceId(), packet);
    }

    inline fn generateSequenceId(c: *Conn) u8 {
        const sequence_id = c.sequence_id;
        c.sequence_id +%= 1;
        return sequence_id;
    }

    inline fn queryResult(c: *Conn, packet: *const Packet) !QueryResult {
        const res = QueryResult.init(packet, c.capabilities) catch |err| {
            switch (err) {
                error.UnrecoverableError, error.UnsupportedLocalInfileRequest => {
                    c.closeDueToFatalProtocolError();
                    return err;
                },
            }
        };
        if (res == .ok) {
            c.rememberOkPacket(res.ok);
        }
        c.active_result_set = false;
        return res;
    }

    inline fn queryRowsResult(c: *Conn, comptime T: type, allocator: std.mem.Allocator) !QueryResultRows(T) {
        const rows = QueryResultRows(T).init(c, allocator) catch |err| switch (err) {
            error.UnsupportedLocalInfileRequest => {
                c.closeDueToFatalProtocolError();
                return err;
            },
            else => {
                c.closeDueToCommunicationFailure();
                return err;
            },
        };

        switch (rows) {
            .rows => c.active_result_set = true,
            .err => c.active_result_set = false,
        }
        return rows;
    }

    inline fn closeDueToFatalProtocolError(c: *Conn) void {
        c.cleanupTransport();
    }

    inline fn closeDueToCommunicationFailure(c: *Conn) void {
        c.cleanupTransport();
    }
};

pub const Tx = struct {
    conn: *Conn,
    generation: u64,
    active: bool,

    fn validate(tx: *const Tx) !void {
        if (!tx.active) return error.TransactionNotActive;
        if (!tx.conn.connected) return error.TransactionLost;
        if (tx.conn.transaction_state_lost) return error.TransactionLost;
        if (tx.generation != tx.conn.currentGeneration()) return error.TransactionLost;
        if (!tx.conn.inTransaction()) return error.TransactionLost;
    }

    pub fn deinit(tx: *Tx) void {
        if (!tx.active) return;
        tx.validate() catch {
            tx.active = false;
            return;
        };
        tx.rollback() catch {
            tx.conn.cleanupTransport();
        };
    }

    pub fn commit(tx: *Tx) !void {
        try tx.validate();
        try tx.conn.commit();
        tx.active = false;
    }

    pub fn rollback(tx: *Tx) !void {
        try tx.validate();
        try tx.conn.rollback();
        tx.active = false;
    }

    /// Create a savepoint inside the current transaction.
    pub fn savepoint(tx: *Tx, name: []const u8) !void {
        try tx.validate();
        try validateSavepointName(name);
        const sql = try std.fmt.allocPrint(tx.conn.allocator, "SAVEPOINT {s}", .{name});
        defer tx.conn.allocator.free(sql);
        const query_res = try tx.conn.query(sql);
        _ = try query_res.expect(.ok);
    }

    /// Roll back to a previously created savepoint.
    pub fn rollbackToSavepoint(tx: *Tx, name: []const u8) !void {
        try tx.validate();
        try validateSavepointName(name);
        const sql = try std.fmt.allocPrint(tx.conn.allocator, "ROLLBACK TO SAVEPOINT {s}", .{name});
        defer tx.conn.allocator.free(sql);
        const query_res = try tx.conn.query(sql);
        _ = try query_res.expect(.ok);
    }

    /// Release a previously created savepoint.
    pub fn releaseSavepoint(tx: *Tx, name: []const u8) !void {
        try tx.validate();
        try validateSavepointName(name);
        const sql = try std.fmt.allocPrint(tx.conn.allocator, "RELEASE SAVEPOINT {s}", .{name});
        defer tx.conn.allocator.free(sql);
        const query_res = try tx.conn.query(sql);
        _ = try query_res.expect(.ok);
    }

    fn validateSavepointName(name: []const u8) !void {
        if (name.len == 0) return error.InvalidSavepointName;
        if (!isIdentifierStart(name[0])) return error.InvalidSavepointName;
        for (name[1..]) |ch| {
            if (!isIdentifierPart(ch)) return error.InvalidSavepointName;
        }
    }

    fn isIdentifierStart(ch: u8) bool {
        return (ch >= 'A' and ch <= 'Z') or (ch >= 'a' and ch <= 'z') or ch == '_';
    }

    fn isIdentifierPart(ch: u8) bool {
        return isIdentifierStart(ch) or (ch >= '0' and ch <= '9') or ch == '$';
    }

    pub fn query(tx: *Tx, sql: []const u8) !QueryResult {
        try tx.validate();
        return tx.conn.query(sql);
    }

    pub fn queryRows(tx: *Tx, allocator: std.mem.Allocator, sql: []const u8) !QueryResultRows(TextResultRow) {
        try tx.validate();
        return tx.conn.queryRows(allocator, sql);
    }

    pub fn prepare(tx: *Tx, allocator: std.mem.Allocator, sql: []const u8) !PrepareResult {
        try tx.validate();
        return tx.conn.prepare(allocator, sql);
    }

    pub fn execute(tx: *Tx, prep_stmt: *const PreparedStatement, params: anytype) !QueryResult {
        try tx.validate();
        return tx.conn.execute(prep_stmt, params);
    }

    pub fn executeRows(tx: *Tx, allocator: std.mem.Allocator, prep_stmt: *const PreparedStatement, params: anytype) !QueryResultRows(BinaryResultRow) {
        try tx.validate();
        return tx.conn.executeRows(allocator, prep_stmt, params);
    }
};

test "authPluginData20 trims switch terminator" {
    const auth_data = try Conn.authPluginData20("12345678901234567890\x00");
    try std.testing.expectEqualDeep("12345678901234567890".*, auth_data);
}

test "authPluginData20 rejects oversized seed" {
    try std.testing.expectError(error.InvalidAuthPluginData, Conn.authPluginData20("123456789012345678901"));
}

test "gssapiServerToken strips auth more data marker" {
    const token = try Conn.gssapiServerToken(&[_]u8{ constants.AUTH_MORE_DATA, 0x4e, 0x54 });
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x4e, 0x54 }, token);
}

test "gssapiServerToken rejects empty auth more data payload" {
    try std.testing.expectError(error.InvalidAuthPluginData, Conn.gssapiServerToken(&[_]u8{constants.AUTH_MORE_DATA}));
}

test "reconnect delay scaling and clamp" {
    const policy: ReconnectPolicy = .{
        .enabled = true,
        .retry_backoff_multiplier = 2,
        .max_retry_delay_ms = 250,
    };
    try std.testing.expectEqual(@as(u32, 200), Conn.nextReconnectDelayMs(100, &policy));
    try std.testing.expectEqual(@as(u32, 250), Conn.nextReconnectDelayMs(200, &policy));
}

test "reconnect delay uses multiplier 1 when zero is configured" {
    const policy: ReconnectPolicy = .{
        .enabled = true,
        .retry_backoff_multiplier = 0,
    };
    try std.testing.expectEqual(@as(u32, 123), Conn.nextReconnectDelayMs(123, &policy));
}

test "savepoint name validation" {
    try Tx.validateSavepointName("sp1");
    try Tx.validateSavepointName("_sp$1");
    try std.testing.expectError(error.InvalidSavepointName, Tx.validateSavepointName(""));
    try std.testing.expectError(error.InvalidSavepointName, Tx.validateSavepointName("1sp"));
    try std.testing.expectError(error.InvalidSavepointName, Tx.validateSavepointName("sp-name"));
}

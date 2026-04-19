const std = @import("std");

const auth = @import("./auth.zig");
const config_mod = @import("./config.zig");
const Config = config_mod.Config;
const OwnedConfig = config_mod.OwnedConfig;
const constants = @import("./constants.zig");
const protocol = @import("./protocol.zig");
const HandshakeV10 = protocol.handshake_v10.HandshakeV10;
const ErrorPacket = protocol.generic_response.ErrorPacket;
const OkPacket = protocol.generic_response.OkPacket;
const HandshakeResponse41 = protocol.handshake_response.HandshakeResponse41;
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
    pub fn init(allocator: std.mem.Allocator, config: *const Config) !Conn {
        var conn = Conn{
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
        var attempt: u8 = 0;
        while (attempt < max_attempts) : (attempt += 1) {
            c.connect() catch |err| {
                if (attempt + 1 >= max_attempts) return err;
                continue;
            };
            return;
        }
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
            .capabilities = c.capabilities,
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
            .capabilities = c.capabilities,
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

        const io = std.Io.Threaded.global_single_threaded.io();
        const stream = switch (c.owned_config.address) {
            .ip => |address| try address.connect(io, .{ .mode = .stream }),
            .unix => |address| try address.connect(io),
        };

        c.stream = stream;
        errdefer c.cleanupTransport();

        c.reader = try PacketReader.init(stream, io, c.allocator);
        c.writer = try PacketWriter.init(stream, io, c.allocator);
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
            .mysql_native_password => try c.auth_mysql_native_password(&auth_data, &config),
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
            stream.close(std.Io.Threaded.global_single_threaded.io());
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

    fn auth_mysql_native_password(c: *Conn, auth_data: *const [20]u8, config: *const Config) !void {
        const auth_resp = auth.scramblePassword(auth_data, config.password);
        const response = HandshakeResponse41.init(.mysql_native_password, config, if (config.password.len > 0) &auth_resp else &[_]u8{});
        try c.writePacket(response);
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };

        const packet = try c.readPacket();
        return switch (packet.payload[0]) {
            constants.OK => c.rememberOkPacket(OkPacket.init(&packet, c.capabilities)),
            else => packet.asError(),
        };
    }

    fn auth_sha256_password(c: *Conn, allocator: std.mem.Allocator, auth_data: *const [20]u8, config: *const Config) !void {
        const response = HandshakeResponse41.init(.sha256_password, config, &[_]u8{auth.sha256_password_public_key_request});
        try c.writePacket(response);
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };

        const pk_packet = try c.readPacket();

        const decoded_pk = try auth.decodePublicKey(pk_packet.payload, allocator);
        defer decoded_pk.deinit(allocator);

        const enc_pw = try auth.encryptPassword(allocator, config.password, auth_data, &decoded_pk.value);
        defer allocator.free(enc_pw);

        try c.writeBytesAsPacket(enc_pw);
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };

        const resp_packet = try c.readPacket();
        return switch (resp_packet.payload[0]) {
            constants.OK => c.rememberOkPacket(OkPacket.init(&resp_packet, c.capabilities)),
            else => resp_packet.asError(),
        };
    }

    fn auth_caching_sha2_password(c: *Conn, allocator: std.mem.Allocator, auth_data: *const [20]u8, config: *const Config) !void {
        const auth_resp = auth.scrambleSHA256Password(auth_data, config.password);
        const response = HandshakeResponse41.init(.caching_sha2_password, config, &auth_resp);
        try c.writePacket(&response);
        c.writer.?.flush() catch |err| {
            c.closeDueToCommunicationFailure();
            return err;
        };

        while (true) {
            const packet = try c.readPacket();
            switch (packet.payload[0]) {
                constants.OK => {
                    c.rememberOkPacket(OkPacket.init(&packet, c.capabilities));
                    return;
                },
                constants.AUTH_MORE_DATA => {
                    const more_data = packet.payload[1..];
                    switch (more_data[0]) {
                        auth.caching_sha2_password_fast_auth_success => {},
                        auth.caching_sha2_password_full_authentication_start => {
                            try c.writeBytesAsPacket(&[_]u8{auth.caching_sha2_password_public_key_request});
                            c.writer.?.flush() catch |err| {
                                c.closeDueToCommunicationFailure();
                                return err;
                            };
                            const pk_packet = try c.readPacket();

                            const decoded_pk = try auth.decodePublicKey(pk_packet.payload, allocator);
                            defer decoded_pk.deinit(allocator);

                            const enc_pw = try auth.encryptPassword(allocator, config.password, auth_data, &decoded_pk.value);
                            defer allocator.free(enc_pw);

                            try c.writeBytesAsPacket(enc_pw);
                            c.writer.?.flush() catch |err| {
                                c.closeDueToCommunicationFailure();
                                return err;
                            };
                        },
                        else => return error.UnsupportedCachingSha2PasswordMoreData,
                    }
                },
                else => return packet.asError(),
            }
        }
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

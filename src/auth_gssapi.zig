const std = @import("std");
const builtin = @import("builtin");

const is_windows = builtin.os.tag == .windows;
const c = if (is_windows)
    @cImport({
        @cDefine("SECURITY_WIN32", "1");
        @cInclude("windows.h");
        @cInclude("sspi.h");
    })
else
    struct {
        pub const CredHandle = usize;
        pub const CtxtHandle = usize;
        pub const TimeStamp = usize;
    };

pub const max_token_size = 50_000;

pub const Step = struct {
    token: []const u8,
    continue_needed: bool,
};

pub const Challenge = struct {
    principal_name: []const u8,
    mechanism: []const u8,
};

pub fn parseChallenge(plugin_data: []const u8) !Challenge {
    const principal_end = std.mem.indexOfScalar(u8, plugin_data, 0) orelse return error.InvalidAuthPluginData;
    if (principal_end == 0) return error.InvalidAuthPluginData;

    const mechanism_bytes = plugin_data[principal_end + 1 ..];
    const mechanism_end = std.mem.indexOfScalar(u8, mechanism_bytes, 0) orelse mechanism_bytes.len;
    return .{
        .principal_name = plugin_data[0..principal_end],
        .mechanism = mechanism_bytes[0..mechanism_end],
    };
}

pub fn normalizeMechanism(mechanism: []const u8) []const u8 {
    return if (std.mem.eql(u8, mechanism, "Negotiate")) "Negotiate" else "Kerberos";
}

pub const Session = struct {
    allocator: std.mem.Allocator,
    principal_name: [:0]u8,
    mechanism: [:0]u8,
    output_buffer: [max_token_size]u8,
    credential: c.CredHandle,
    context: c.CtxtHandle,
    credential_initialized: bool,
    context_initialized: bool,

    pub fn init(allocator: std.mem.Allocator, plugin_data: []const u8) !Session {
        if (!is_windows) return error.UnsupportedAuthPlugin;

        const challenge = try parseChallenge(plugin_data);
        const mechanism_name = normalizeMechanism(challenge.mechanism);
        const principal_name = try allocator.dupeZ(u8, challenge.principal_name);
        errdefer allocator.free(principal_name);
        const mechanism = try allocator.dupeZ(u8, mechanism_name);
        errdefer allocator.free(mechanism);

        var session = Session{
            .allocator = allocator,
            .principal_name = principal_name,
            .mechanism = mechanism,
            .output_buffer = undefined,
            .credential = undefined,
            .context = undefined,
            .credential_initialized = false,
            .context_initialized = false,
        };
        errdefer session.deinit();

        var lifetime: c.TimeStamp = undefined;
        const status = c.AcquireCredentialsHandleA(
            null,
            session.mechanism.ptr,
            c.SECPKG_CRED_OUTBOUND,
            null,
            null,
            null,
            null,
            &session.credential,
            &lifetime,
        );
        if (securityError(status)) {
            std.log.err("AcquireCredentialsHandleA failed for mechanism '{s}' with status {d}", .{ session.mechanism, status });
            return error.GssapiCredentialAcquisitionFailed;
        }
        session.credential_initialized = true;
        return session;
    }

    pub fn deinit(session: *Session) void {
        if (!is_windows) return;

        if (session.context_initialized) {
            _ = c.DeleteSecurityContext(&session.context);
        }
        if (session.credential_initialized) {
            _ = c.FreeCredentialsHandle(&session.credential);
        }
        session.allocator.free(session.mechanism);
        session.allocator.free(session.principal_name);
    }

    pub fn nextToken(session: *Session, incoming: ?[]const u8) !Step {
        if (!is_windows) return error.UnsupportedAuthPlugin;

        var input_buffer = c.SecBuffer{
            .cbBuffer = if (incoming) |bytes| @intCast(bytes.len) else 0,
            .BufferType = c.SECBUFFER_TOKEN,
            .pvBuffer = if (incoming) |bytes| @ptrCast(@constCast(bytes.ptr)) else null,
        };
        var input_desc = c.SecBufferDesc{
            .ulVersion = c.SECBUFFER_VERSION,
            .cBuffers = 1,
            .pBuffers = &input_buffer,
        };

        var output_buffer = c.SecBuffer{
            .cbBuffer = max_token_size,
            .BufferType = c.SECBUFFER_TOKEN,
            .pvBuffer = @ptrCast(session.output_buffer[0..].ptr),
        };
        var output_desc = c.SecBufferDesc{
            .ulVersion = c.SECBUFFER_VERSION,
            .cBuffers = 1,
            .pBuffers = &output_buffer,
        };

        var attributes: c.ULONG = 0;
        var lifetime: c.TimeStamp = undefined;
        const status = c.InitializeSecurityContextA(
            &session.credential,
            if (session.context_initialized) &session.context else null,
            session.principal_name.ptr,
            0,
            0,
            c.SECURITY_NATIVE_DREP,
            if (incoming != null) &input_desc else null,
            0,
            &session.context,
            &output_desc,
            &attributes,
            &lifetime,
        );
        if (securityError(status)) {
            const token_prefix = if (incoming) |bytes| bytes[0..@min(bytes.len, 16)] else &[_]u8{};
            std.log.err(
                "InitializeSecurityContextA failed for principal '{s}' with mechanism '{s}', token_len={}, token_prefix={x}, status {d}",
                .{ session.principal_name, session.mechanism, if (incoming) |bytes| bytes.len else @as(usize, 0), token_prefix, status },
            );
            return error.GssapiContextInitializationFailed;
        }
        session.context_initialized = true;

        return switch (status) {
            c.SEC_E_OK => .{
                .token = session.output_buffer[0..@intCast(output_buffer.cbBuffer)],
                .continue_needed = false,
            },
            c.SEC_I_CONTINUE_NEEDED => .{
                .token = session.output_buffer[0..@intCast(output_buffer.cbBuffer)],
                .continue_needed = true,
            },
            else => {
                std.log.err("Unexpected SSPI status {d}", .{status});
                return error.UnsupportedGssapiSspiStatus;
            },
        };
    }
};

fn securityError(status: c.SECURITY_STATUS) bool {
    return status < 0;
}

test "parse gssapi challenge" {
    const challenge = try parseChallenge("localhost\x00Negotiate\x00");
    try std.testing.expectEqualStrings("localhost", challenge.principal_name);
    try std.testing.expectEqualStrings("Negotiate", challenge.mechanism);
}

test "parse gssapi challenge without mechanism" {
    const challenge = try parseChallenge("db/localhost\x00\x00");
    try std.testing.expectEqualStrings("db/localhost", challenge.principal_name);
    try std.testing.expectEqualStrings("", challenge.mechanism);
}

test "normalize gssapi mechanism" {
    try std.testing.expectEqualStrings("Negotiate", normalizeMechanism("Negotiate"));
    try std.testing.expectEqualStrings("Kerberos", normalizeMechanism(""));
    try std.testing.expectEqualStrings("Kerberos", normalizeMechanism("Kerberos"));
}

const std = @import("std");
const Packet = @import("./packet.zig");

pub const PacketReader = struct {
    reader: std.Io.net.Stream.Reader,
    allocator: std.mem.Allocator,

    const buffer_size = 4096;

    pub fn init(stream: std.Io.net.Stream, io: std.Io, allocator: std.mem.Allocator) !PacketReader {
        const buffer = try allocator.alloc(u8, buffer_size);
        return .{
            .reader = stream.reader(io, buffer),
            .allocator = allocator,
        };
    }

    pub fn deinit(p: *const PacketReader) void {
        p.allocator.free(p.reader.interface.buffer);
    }

    // invalidates the last packet returned
    pub fn readPacket(p: *PacketReader) !Packet.Packet {
        const header = try p.reader.interface.takeArray(4);
        const payload_length = std.mem.readInt(u24, header[0..3], .little);
        const payload = try p.reader.interface.take(payload_length);

        return .{
            .sequence_id = header[3],
            .payload = payload,
        };
    }
};

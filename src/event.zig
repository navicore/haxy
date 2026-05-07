const std = @import("std");

pub const id_size: usize = 32;

pub const EventData = union(enum) {
    issue: struct {
        title: []const u8,
        description: []const u8,
        tags: []const []const u8,
    },
};

pub const Event = struct {
    id: [id_size * 2]u8,
    data: EventData,
};

pub fn randomId(random: std.Random) [id_size]u8 {
    var id_bytes: [id_size]u8 = undefined;
    random.bytes(&id_bytes);
    return id_bytes;
}

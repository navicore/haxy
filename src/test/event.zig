const std = @import("std");
const builtin = @import("builtin");
const xit = @import("xit");
const rp = xit.repo;

test "simple" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;
    const temp_dir_name = "temp-event-simple";

    // create the temp dir
    const cwd = std.Io.Dir.cwd();
    var temp_dir_or_err = cwd.openDir(io, temp_dir_name, .{});
    if (temp_dir_or_err) |*temp_dir| {
        temp_dir.close(io);
        try cwd.deleteTree(io, temp_dir_name);
    } else |_| {}
    var temp_dir = try cwd.createDirPathOpen(io, temp_dir_name, .{});
    defer cwd.deleteTree(io, temp_dir_name) catch {};
    defer temp_dir.close(io);

    const cwd_path = try std.process.currentPathAlloc(io, allocator);
    defer allocator.free(cwd_path);

    const work_path = try std.fs.path.join(allocator, &.{ cwd_path, temp_dir_name });
    defer allocator.free(work_path);

    var repo = try rp.Repo(.xit, .{ .is_test = true }).init(io, allocator, .{ .path = work_path });
    defer repo.deinit(io, allocator);

    const id_size: usize = 20;
    const AddIssueEvent = struct {
        id: [2 * id_size]u8,
        kind: []const u8,
        data: struct {
            title: []const u8,
            description: []const u8,
            tags: []const []const u8,
        },
    };

    var json: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer json.deinit();

    // create event as json
    {
        var prng = std.Random.DefaultPrng.init(std.testing.random_seed);
        var entropy: [id_size]u8 = undefined;
        prng.random().bytes(&entropy);

        const event = AddIssueEvent{
            .id = std.fmt.bytesToHex(entropy, .lower),
            .kind = "add-issue",
            .data = .{
                .title = "Login form clears password on validation error",
                .description = "Submitting an invalid email address resets the password field. Preserve the field value and show an inline validation message.",
                .tags = &[_][]const u8{ "bug", "priority-high", "ui" },
            },
        };

        try std.json.Stringify.value(event, .{}, &json.writer);
    }

    _ = try repo.commitAtRef(io, allocator, .{ .message = json.written() }, null, .{ .kind = .head, .name = "haxy/meta" });
}

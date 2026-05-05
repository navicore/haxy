const std = @import("std");
const builtin = @import("builtin");
const xit = @import("xit");
const rp = xit.repo;
const hash = xit.hash;

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

    const repo_opts: rp.RepoOpts(.xit) = .{ .is_test = true };
    const Repo = rp.Repo(.xit, repo_opts);
    var repo = try Repo.init(io, allocator, .{ .path = work_path });
    defer repo.deinit(io, allocator);

    // define test events

    const AddIssueData = struct {
        title: []const u8,
        description: []const u8,
        tags: []const []const u8,
    };

    const id_size: usize = 20;
    const AddIssueEvent = struct {
        id: [2 * id_size]u8,
        kind: []const u8,
        data: AddIssueData,
    };

    const issues_data = [_]AddIssueData{
        .{
            .title = "Login form clears password on validation error",
            .description = "Submitting an invalid email address resets the password field. Preserve the field value and show an inline validation message.",
            .tags = &[_][]const u8{ "bug", "priority-high", "ui" },
        },
        .{
            .title = "Search results ignore archived project filter",
            .description = "Filtering search results to active projects still returns issues from archived projects. Apply the archived flag before ranking results.",
            .tags = &[_][]const u8{ "bug", "search", "backend" },
        },
        .{
            .title = "Issue list does not persist selected sort order",
            .description = "Changing the issue list sort order is lost after refresh. Store the selected sort field and direction with the user's view preferences.",
            .tags = &[_][]const u8{ "enhancement", "frontend", "preferences" },
        },
        .{
            .title = "Webhook retries stop after transient timeout",
            .description = "A single gateway timeout marks webhook delivery as failed permanently. Retry transient network errors with exponential backoff.",
            .tags = &[_][]const u8{ "bug", "webhooks", "reliability" },
        },
        .{
            .title = "CSV export omits labels with commas",
            .description = "Exported issue rows drop labels that contain commas instead of escaping them. Quote CSV fields according to RFC 4180.",
            .tags = &[_][]const u8{ "bug", "export", "data-integrity" },
        },
    };

    // insert issues as commits in the repo

    var prng = std.Random.DefaultPrng.init(std.testing.random_seed);
    var json: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer json.deinit();

    for (issues_data) |issue_data| {
        json.clearRetainingCapacity();

        var entropy: [id_size]u8 = undefined;
        prng.random().bytes(&entropy);

        const event = AddIssueEvent{
            .id = std.fmt.bytesToHex(entropy, .lower),
            .kind = "add-issue",
            .data = issue_data,
        };

        try std.json.Stringify.value(event, .{}, &json.writer);
        _ = try repo.commitAtRef(io, allocator, .{ .message = json.written() }, null, .{ .kind = .head, .name = "haxy/meta" });
    }

    // read and parse all of the events from the repo

    var events: std.ArrayList(AddIssueEvent) = .empty;
    defer events.deinit(allocator);

    var event_strs: std.ArrayList([]const u8) = .empty;
    defer event_strs.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const ref_haxy_meta = try repo.readRef(io, .{ .kind = .head, .name = "haxy/meta" }) orelse return error.RefNotFound;
    var commit_iter = try repo.log(io, allocator, &.{ref_haxy_meta});
    defer commit_iter.deinit();

    while (try commit_iter.next()) |commit_object| {
        defer commit_object.deinit();

        try commit_object.object_reader.seekTo(commit_object.content.commit.message_position);
        const message = try commit_object.object_reader.interface.allocRemaining(arena.allocator(), .unlimited);

        try event_strs.append(allocator, message);
    }

    for (0..event_strs.items.len) |i| {
        const event_str = event_strs.items[event_strs.items.len - i - 1];
        const event = try std.json.parseFromSliceLeaky(AddIssueEvent, arena.allocator(), event_str, .{});
        try events.append(allocator, event);
    }

    // process events into the database

    for (events.items) |event| {
        const Ctx = struct {
            event: AddIssueEvent,

            pub fn run(ctx: @This(), cursor: *Repo.DB.Cursor(.read_write)) !void {
                const moment = try Repo.DB.HashMap(.read_write).init(cursor.*);

                const haxy_views_cursor = try moment.putCursor(hash.hashInt(repo_opts.hash, "haxy-views"));
                const haxy_views = try Repo.DB.HashMap(.read_write).init(haxy_views_cursor);

                const issues_cursor = try haxy_views.putCursor(hash.hashInt(repo_opts.hash, "issues"));
                const issues = try Repo.DB.ArrayList(.read_write).init(issues_cursor);

                const issue_cursor = try issues.appendCursor();
                const issue = try Repo.DB.HashMap(.read_write).init(issue_cursor);

                try issue.put(hash.hashInt(repo_opts.hash, "id"), .{ .bytes = &ctx.event.id });
                try issue.put(hash.hashInt(repo_opts.hash, "title"), .{ .bytes = ctx.event.data.title });
                try issue.put(hash.hashInt(repo_opts.hash, "description"), .{ .bytes = ctx.event.data.description });
            }
        };

        try repo.core.db_file.lock(io, .exclusive);
        defer repo.core.db_file.unlock(io);

        const history = try Repo.DB.ArrayList(.read_write).init(repo.core.db.rootCursor());
        try history.appendContext(
            .{ .slot = try history.getSlot(-1) },
            Ctx{ .event = event },
        );
    }
}

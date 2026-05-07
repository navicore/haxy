const std = @import("std");
const builtin = @import("builtin");
const xit = @import("xit");
const rp = xit.repo;
const hash = xit.hash;

const event_id_size: usize = 32;

const EventData = union(enum) {
    issue: struct {
        title: []const u8,
        description: []const u8,
        tags: []const []const u8,
    },
};

const Event = struct {
    id: [event_id_size * 2]u8,
    data: EventData,
};

fn randomId(random: std.Random) [event_id_size]u8 {
    var id_bytes: [event_id_size]u8 = undefined;
    random.bytes(&id_bytes);
    return id_bytes;
}

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

    //
    // define test events
    //

    var prng = std.Random.DefaultPrng.init(std.testing.random_seed);

    const first_event_id = randomId(prng.random());

    const events_to_process = [_]Event{
        .{
            .id = std.fmt.bytesToHex(first_event_id, .lower),
            .data = .{
                .issue = .{
                    .title = "Login form clears password on validation error",
                    .description = "Submitting an invalid email address resets the password field. Preserve the field value and show an inline validation message.",
                    .tags = &[_][]const u8{ "bug", "priority-high", "ui" },
                },
            },
        },
        // this event edits the previous one because it has the same id
        .{
            .id = std.fmt.bytesToHex(first_event_id, .lower),
            .data = .{
                .issue = .{
                    .title = "Login form clears password on validation error",
                    .description = "Submitting an invalid email address resets the password field and removes typed input. Preserve the field value and show an inline validation message.",
                    .tags = &[_][]const u8{ "bug", "priority-high", "ui" },
                },
            },
        },
        .{
            .id = std.fmt.bytesToHex(randomId(prng.random()), .lower),
            .data = .{
                .issue = .{
                    .title = "Search results ignore archived project filter",
                    .description = "Filtering search results to active projects still returns issues from archived projects. Apply the archived flag before ranking results.",
                    .tags = &[_][]const u8{ "bug", "search", "backend" },
                },
            },
        },
        .{
            .id = std.fmt.bytesToHex(randomId(prng.random()), .lower),
            .data = .{
                .issue = .{
                    .title = "Issue list does not persist selected sort order",
                    .description = "Changing the issue list sort order is lost after refresh. Store the selected sort field and direction with the user's view preferences.",
                    .tags = &[_][]const u8{ "enhancement", "frontend", "preferences" },
                },
            },
        },
    };

    //
    // insert issues as commits in the repo
    //

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    {
        var json: std.Io.Writer.Allocating = .init(std.testing.allocator);
        defer json.deinit();

        for (events_to_process) |event| {
            json.clearRetainingCapacity();

            try std.json.Stringify.value(event, .{}, &json.writer);

            // commit the event into a special branch
            _ = try repo.commitAtRef(io, allocator, .{ .message = json.written() }, null, .{ .kind = .head, .name = "haxy/meta" });
        }
    }

    //
    // read and parse all of the events from the repo
    //

    var events: std.ArrayList(Event) = .empty;
    defer events.deinit(allocator);

    {
        var event_strs: std.ArrayList([]const u8) = .empty;
        defer event_strs.deinit(allocator);

        const ref_haxy_meta = try repo.readRef(io, .{ .kind = .head, .name = "haxy/meta" }) orelse return error.RefNotFound;
        var commit_iter = try repo.log(io, allocator, &.{ref_haxy_meta});
        defer commit_iter.deinit();

        // read the message from each commit
        while (try commit_iter.next()) |commit_object| {
            defer commit_object.deinit();

            try commit_object.object_reader.seekTo(commit_object.content.commit.message_position);
            const message = try commit_object.object_reader.interface.allocRemaining(arena.allocator(), .unlimited);

            try event_strs.append(allocator, message);
        }

        // parse commit messages as JSON into event values.
        // add events in reverse order so the earliest event is first.
        for (0..event_strs.items.len) |i| {
            const event_str = event_strs.items[event_strs.items.len - i - 1];
            const event = try std.json.parseFromSliceLeaky(Event, arena.allocator(), event_str, .{});
            try events.append(allocator, event);
        }
    }

    //
    // process events into the database
    //

    const Ctx = struct {
        events: []Event,

        pub fn run(ctx: @This(), cursor: *Repo.DB.Cursor(.read_write)) !void {
            const moment = try Repo.DB.HashMap(.read_write).init(cursor.*);

            // the map with all of haxy's state and materialized views
            const haxy_cursor = try moment.putCursor(hash.hashInt(repo_opts.hash, "haxy"));
            const haxy = try Repo.DB.HashMap(.read_write).init(haxy_cursor);

            // try reading the last event id that was processed in the db
            var last_event_id_maybe: ?[event_id_size]u8 = null;
            if (try haxy.getCursor(hash.hashInt(repo_opts.hash, "last-event-id"))) |last_event_id_cursor| {
                var last_event_id_buffer: [event_id_size]u8 = undefined;
                _ = try last_event_id_cursor.readBytes(&last_event_id_buffer);
                last_event_id_maybe = last_event_id_buffer;
            }

            // for each event we want to process...
            for (ctx.events) |event| {
                // get the id of the current event as bytes
                var current_event_id_buffer: [event_id_size]u8 = undefined;
                _ = try std.fmt.hexToBytes(&current_event_id_buffer, &event.id);

                // map with the views as they appeared when each event was processed.
                // we can use this to see (and revert) the views to any previous state.
                const event_id_to_views_cursor = try haxy.putCursor(hash.hashInt(repo_opts.hash, "event-id->views"));
                const event_id_to_views = try Repo.DB.HashMap(.read_write).init(event_id_to_views_cursor);

                // create a new views map for the current event we are processing
                var views_cursor = try event_id_to_views.putCursor(hash.hashInt(repo_opts.hash, &current_event_id_buffer));

                // if there was a previous event, set the views map to have the same value as it.
                // this efficiently "clones" the map so we make further modifications based on it.
                if (last_event_id_maybe) |*last_event_id| {
                    if (try event_id_to_views.getCursor(hash.hashInt(repo_opts.hash, last_event_id))) |last_view_cursor| {
                        try views_cursor.write(.{ .slot = last_view_cursor.slot() });
                    }
                }

                // process the event into the views map
                {
                    const views = try Repo.DB.HashMap(.read_write).init(views_cursor);

                    switch (event.data) {
                        .issue => |data| {
                            const event_id_to_issue_cursor = try views.putCursor(hash.hashInt(repo_opts.hash, "event-id->issue"));
                            const event_id_to_issue = try Repo.DB.HashMap(.read_write).init(event_id_to_issue_cursor);

                            const issue_cursor = try event_id_to_issue.putCursor(hash.hashInt(repo_opts.hash, &current_event_id_buffer));
                            const issue = try Repo.DB.HashMap(.read_write).init(issue_cursor);

                            try putIfDifferent(issue, hash.hashInt(repo_opts.hash, "id"), &current_event_id_buffer);
                            try putIfDifferent(issue, hash.hashInt(repo_opts.hash, "title"), data.title);
                            try putIfDifferent(issue, hash.hashInt(repo_opts.hash, "description"), data.description);
                        },
                    }
                }

                // the current event id is now the last one
                last_event_id_maybe = current_event_id_buffer;

                // prevent any of the data created above from being mutated by future iterations of this loop
                try cursor.db.freeze();
            }

            if (last_event_id_maybe) |*last_event_id| {
                try haxy.put(hash.hashInt(repo_opts.hash, "last-event-id"), .{ .bytes = last_event_id });
            }
        }

        fn putIfDifferent(map: Repo.DB.HashMap(.read_write), key: hash.HashInt(repo_opts.hash), value: []const u8) !void {
            if (try map.getCursor(key)) |value_cursor| {
                var buffer: [4096]u8 = undefined;
                const existing_value = try value_cursor.readBytes(&buffer);
                if (std.mem.eql(u8, existing_value, value)) {
                    return;
                }
            }

            try map.put(key, .{ .bytes = value });
        }
    };

    try repo.core.db_file.lock(io, .exclusive);
    defer repo.core.db_file.unlock(io);

    // create a new transaction in the database that runs the above-defined Ctx function
    const history = try Repo.DB.ArrayList(.read_write).init(repo.core.db.rootCursor());
    try history.appendContext(.{ .slot = try history.getSlot(-1) }, Ctx{ .events = events.items });

    // read the moment we just created
    const moment_cursor = try history.getCursor(-1) orelse return error.NotFound;
    const moment = try Repo.DB.HashMap(.read_only).init(moment_cursor);

    const haxy_cursor = try moment.getCursor(hash.hashInt(repo_opts.hash, "haxy")) orelse return error.NotFound;
    const haxy = try Repo.DB.HashMap(.read_only).init(haxy_cursor);

    const event_id_to_views_cursor = try haxy.getCursor(hash.hashInt(repo_opts.hash, "event-id->views")) orelse return error.NotFound;
    const event_id_to_views = try Repo.DB.HashMap(.read_only).init(event_id_to_views_cursor);

    // make sure the issue from the first event was correctly edited
    {
        // get the last event id
        const last_event_id_cursor = try haxy.getCursor(hash.hashInt(repo_opts.hash, "last-event-id")) orelse return error.NotFound;
        var last_event_id_buffer: [event_id_size]u8 = undefined;
        _ = try last_event_id_cursor.readBytes(&last_event_id_buffer);

        // get the latest views (the views generated by the last event id)
        const views_cursor = try event_id_to_views.getCursor(hash.hashInt(repo_opts.hash, &last_event_id_buffer)) orelse return error.NotFound;
        const views = try Repo.DB.HashMap(.read_only).init(views_cursor);

        // get the map of issues
        const event_id_to_issue_cursor = try views.getCursor(hash.hashInt(repo_opts.hash, "event-id->issue")) orelse return error.NotFound;
        const event_id_to_issue = try Repo.DB.HashMap(.read_only).init(event_id_to_issue_cursor);

        // get the issue out of the map that was edited
        const first_issue_cursor = try event_id_to_issue.getCursor(hash.hashInt(repo_opts.hash, &first_event_id)) orelse return error.NotFound;
        const first_issue = try Repo.DB.HashMap(.read_only).init(first_issue_cursor);

        // make sure the issue's description was correctly edited
        const first_issue_description_cursor = try first_issue.getCursor(hash.hashInt(repo_opts.hash, "description")) orelse return error.NotFound;
        const first_issue_description_value = try first_issue_description_cursor.readBytesAlloc(allocator, null);
        defer allocator.free(first_issue_description_value);
        try std.testing.expectEqualStrings(events_to_process[1].data.issue.description, first_issue_description_value);
    }
}

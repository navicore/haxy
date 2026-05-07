const std = @import("std");
const xit = @import("xit");
const rp = xit.repo;
const hash = xit.hash;

const event_id_size: usize = 32;

pub const EventData = union(enum) {
    issue: struct {
        title: []const u8,
        description: []const u8,
        tags: []const []const u8,
    },
};

pub const Event = struct {
    id: [event_id_size * 2]u8,
    data: EventData,
};

/// associates an Event with a git object id.
/// we save this oid in the database so we can reliably re-consume
/// events if their oid changed. this can happen if the branch
/// is rebased and force-pushed.
pub fn RepoEvent(comptime hash_kind: hash.HashKind) type {
    return struct {
        oid: [hash.byteLen(hash_kind)]u8,
        event: Event,
    };
}

pub fn randomId(random: std.Random) [event_id_size]u8 {
    var id_bytes: [event_id_size]u8 = undefined;
    random.bytes(&id_bytes);
    return id_bytes;
}

pub fn consume(
    comptime DB: type,
    comptime hash_kind: hash.HashKind,
    io: std.Io,
    db: *DB,
    db_file: std.Io.File,
    repo_events: []RepoEvent(hash_kind),
) !void {
    const Ctx = struct {
        repo_events: []RepoEvent(hash_kind),

        pub fn run(ctx: @This(), cursor: *DB.Cursor(.read_write)) !void {
            const moment = try DB.HashMap(.read_write).init(cursor.*);

            // the map with all of haxy's state and materialized views
            const haxy_cursor = try moment.putCursor(hash.hashInt(hash_kind, "haxy"));
            const haxy = try DB.HashMap(.read_write).init(haxy_cursor);

            // try reading the last object id that was consumed
            var last_object_id_maybe: ?[hash.byteLen(hash_kind)]u8 = null;
            if (try haxy.getCursor(hash.hashInt(hash_kind, "last-object-id"))) |last_object_id_cursor| {
                var last_object_id_buffer: [hash.byteLen(hash_kind)]u8 = undefined;
                _ = try last_object_id_cursor.readBytes(&last_object_id_buffer);
                last_object_id_maybe = last_object_id_buffer;
            }

            // for each event we want to consume...
            for (ctx.repo_events) |repo_event| {
                // map with the views as they appeared when each event was consumed.
                // we can use this to see (and revert) the views to any previous state.
                const object_id_to_views_cursor = try haxy.putCursor(hash.hashInt(hash_kind, "object-id->views"));
                const object_id_to_views = try DB.HashMap(.read_write).init(object_id_to_views_cursor);

                // create a new views map for the current event we are consuming
                var views_cursor = try object_id_to_views.putCursor(hash.bytesToInt(hash_kind, &repo_event.oid));

                // if there was a previous event, set the views map to have the same value as it.
                // this efficiently "clones" the map so we make further modifications based on it.
                if (last_object_id_maybe) |*last_object_id| {
                    if (try object_id_to_views.getCursor(hash.bytesToInt(hash_kind, last_object_id))) |last_view_cursor| {
                        try views_cursor.write(.{ .slot = last_view_cursor.slot() });
                    }
                }

                // consume the event into the views map
                {
                    const event = repo_event.event;

                    // get the id of the current event as bytes
                    var current_event_id: [event_id_size]u8 = undefined;
                    _ = try std.fmt.hexToBytes(&current_event_id, &event.id);

                    const views = try DB.HashMap(.read_write).init(views_cursor);

                    switch (event.data) {
                        .issue => |data| {
                            const event_id_to_issue_cursor = try views.putCursor(hash.hashInt(hash_kind, "event-id->issue"));
                            const event_id_to_issue = try DB.HashMap(.read_write).init(event_id_to_issue_cursor);

                            const issue_cursor = try event_id_to_issue.putCursor(hash.hashInt(hash_kind, &current_event_id));
                            const issue = try DB.HashMap(.read_write).init(issue_cursor);

                            try putIfDifferent(issue, hash.hashInt(hash_kind, "title"), data.title);
                            try putIfDifferent(issue, hash.hashInt(hash_kind, "description"), data.description);
                        },
                    }
                }

                // the current object id is now the last one
                last_object_id_maybe = repo_event.oid;

                // prevent any of the data created above from being mutated by future iterations of this loop
                try cursor.db.freeze();
            }

            if (last_object_id_maybe) |*last_object_id| {
                try haxy.put(hash.hashInt(hash_kind, "last-object-id"), .{ .bytes = last_object_id });
            }
        }

        fn putIfDifferent(map: DB.HashMap(.read_write), key: hash.HashInt(hash_kind), value: []const u8) !void {
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

    try db_file.lock(io, .exclusive);
    defer db_file.unlock(io);

    // create a new transaction in the database that runs the above-defined Ctx function
    const history = try DB.ArrayList(.read_write).init(db.rootCursor());
    try history.appendContext(.{ .slot = try history.getSlot(-1) }, Ctx{ .repo_events = repo_events });
}

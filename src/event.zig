const std = @import("std");
const xit = @import("xit");
const rp = xit.repo;
const hash = xit.hash;

const event_id_size: usize = 32;

pub const EventKind = enum {
    issue,
};

pub const EventData = union(EventKind) {
    issue: struct {
        title: []const u8,
        description: []const u8,
        tags: []const u8,
    },

    pub fn read(
        comptime DB: type,
        comptime hash_kind: hash.HashKind,
        allocator: std.mem.Allocator,
        map: DB.HashMap(.read_only),
        kind: EventKind,
    ) !EventData {
        return switch (kind) {
            .issue => .{
                .issue = .{
                    .title = try readBytes(DB, hash_kind, allocator, map, "title"),
                    .description = try readBytes(DB, hash_kind, allocator, map, "description"),
                    .tags = try readBytes(DB, hash_kind, allocator, map, "tags"),
                },
            },
        };
    }

    fn readBytes(
        comptime DB: type,
        comptime hash_kind: hash.HashKind,
        allocator: std.mem.Allocator,
        map: DB.HashMap(.read_only),
        field_name: []const u8,
    ) ![]const u8 {
        const cursor = try map.getCursor(hash.hashInt(hash_kind, field_name)) orelse return error.NotFound;
        return try cursor.readBytesAlloc(allocator, null);
    }
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
        parent_oid: ?[hash.byteLen(hash_kind)]u8,
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
        history_count: u64,

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
                // map that associates this oid with the current transaction id.
                // this will be important for reverting things when a rebase occurrs.
                var object_id_to_tx_id_cursor = try haxy.putCursor(hash.hashInt(hash_kind, "object-id->tx-id"));

                // map with the views as they appeared when each event was consumed.
                // we can use this to see (and revert) the views to any previous state.
                var object_id_to_views_cursor = try haxy.putCursor(hash.hashInt(hash_kind, "object-id->views"));

                // if this object id has already been consumed, skip it
                {
                    const object_id_to_views = try DB.HashMap(.read_only).init(object_id_to_views_cursor.readOnly());
                    if (null != try object_id_to_views.getCursor(hash.bytesToInt(hash_kind, &repo_event.oid))) {
                        continue;
                    }
                }

                // compare last-object-id to the event's parent. this is important
                // in situations where the branch was rebased, because in that case
                // the last-object-id may no longer be valid.
                if (last_object_id_maybe) |*last_object_id| {
                    if (repo_event.parent_oid) |*parent_oid| {
                        if (!std.mem.eql(u8, last_object_id, parent_oid)) {
                            // the last-object-id does not match the current event's parent id.
                            // this means that a rebase occurred. what we need to do is just
                            // revert the two object-id maps to the state they were in when
                            // parent-oid was consumed.

                            const object_id_to_tx_id = try DB.HashMap(.read_only).init(object_id_to_tx_id_cursor.readOnly());
                            const tx_id_cursor = try object_id_to_tx_id.getCursor(hash.bytesToInt(hash_kind, parent_oid)) orelse return error.ObjectNotFound;
                            const tx_id = try tx_id_cursor.readUint();
                            const history = try DB.ArrayList(.read_only).init(cursor.db.rootCursor().readOnly());

                            const old_moment_cursor = try history.getCursor(tx_id) orelse return error.TransactionNotFound;
                            const old_moment = try DB.HashMap(.read_only).init(old_moment_cursor);

                            const old_haxy_cursor = try old_moment.getCursor(hash.hashInt(hash_kind, "haxy")) orelse return error.CursorNotFound;
                            const old_haxy = try DB.HashMap(.read_only).init(old_haxy_cursor);

                            const old_object_id_to_tx_id_cursor = try old_haxy.getCursor(hash.hashInt(hash_kind, "object-id->tx-id")) orelse return error.CursorNotFound;
                            const old_object_id_to_views_cursor = try old_haxy.getCursor(hash.hashInt(hash_kind, "object-id->views")) orelse return error.CursorNotFound;

                            try object_id_to_tx_id_cursor.write(.{ .slot = old_object_id_to_tx_id_cursor.slot() });
                            try object_id_to_views_cursor.write(.{ .slot = old_object_id_to_views_cursor.slot() });

                            last_object_id_maybe = parent_oid.*;
                        }
                    } else {
                        // the branch was rebased all the way to the very beginning.
                        // we have a repo event with no parent, which means it is now
                        // the very first event. all we need to do is set the
                        // object-id->views map to be empty so we can rebuild it.
                        try object_id_to_views_cursor.write(.{ .slot = null });
                    }
                } else {
                    if (repo_event.parent_oid) |_| {
                        // there is no last-object-id, but this event has a parent.
                        // this is an invalid state. if the event has a parent, that
                        // implies that an event has already been processed, but
                        // if that's true then there would be a last-object-id.
                        return error.UnexpectedParent;
                    }
                }

                // associate this object id with this transaction id
                const object_id_to_tx_id = try DB.HashMap(.read_write).init(object_id_to_tx_id_cursor);
                try object_id_to_tx_id.put(hash.bytesToInt(hash_kind, &repo_event.oid), .{ .uint = ctx.history_count });

                // init the object-id->views map
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

                            try upsert(DB, hash_kind, issue, @TypeOf(data), data);
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
    };

    try db_file.lock(io, .exclusive);
    defer db_file.unlock(io);

    // create a new transaction in the database that runs the above-defined Ctx function
    const history = try DB.ArrayList(.read_write).init(db.rootCursor());
    try history.appendContext(.{ .slot = try history.getSlot(-1) }, Ctx{
        .repo_events = repo_events,
        .history_count = try history.count(),
    });
}

fn upsert(
    comptime DB: type,
    comptime hash_kind: hash.HashKind,
    map: DB.HashMap(.read_write),
    comptime Data: type,
    data: Data,
) !void {
    switch (@typeInfo(Data)) {
        .@"struct" => |struct_info| {
            inline for (struct_info.fields) |field| {
                try upsertField(DB, hash_kind, map, field.name, field.type, @field(data, field.name));
            }
        },
        else => @compileError("upsert expects a struct"),
    }
}

fn upsertField(
    comptime DB: type,
    comptime hash_kind: hash.HashKind,
    map: DB.HashMap(.read_write),
    comptime field_name: []const u8,
    comptime Field: type,
    value: Field,
) !void {
    const key = hash.hashInt(hash_kind, field_name);

    switch (@typeInfo(Field)) {
        .pointer => |pointer_info| {
            if (pointer_info.size == .slice and pointer_info.child == u8) {
                try upsertBytes(DB, hash_kind, map, key, value);
            } else {
                @compileError("unsupported upsert field type: " ++ @typeName(Field));
            }
        },
        .array => |array_info| {
            if (array_info.child == u8) {
                try upsertBytes(DB, hash_kind, map, key, &value);
            } else {
                @compileError("unsupported upsert field type: " ++ @typeName(Field));
            }
        },
        .int => |int_info| switch (int_info.signedness) {
            .unsigned => {
                if (try map.getCursor(key)) |value_cursor| {
                    if (try value_cursor.readUint() == value) {
                        return;
                    }
                }

                try map.put(key, .{ .uint = value });
            },
            .signed => {
                if (try map.getCursor(key)) |value_cursor| {
                    if (try value_cursor.readInt() == value) {
                        return;
                    }
                }

                try map.put(key, .{ .int = value });
            },
        },
        else => @compileError("unsupported upsert field type: " ++ @typeName(Field)),
    }
}

fn upsertBytes(
    comptime DB: type,
    comptime hash_kind: hash.HashKind,
    map: DB.HashMap(.read_write),
    key: hash.HashInt(hash_kind),
    value: []const u8,
) !void {
    var existing_cursor_maybe = try map.getCursor(key);
    if (existing_cursor_maybe) |*existing_cursor| {
        if (try bytesEqual(DB, existing_cursor, value)) {
            return;
        }
    }

    var value_cursor = try map.putCursor(key);
    var write_buffer: [1024]u8 = undefined;
    var writer = try value_cursor.writer(&write_buffer);
    try writer.interface.writeAll(value);
    try writer.finish();
}

fn bytesEqual(
    comptime DB: type,
    cursor: *DB.Cursor(.read_only),
    value: []const u8,
) !bool {
    var read_buffer: [1024]u8 = undefined;
    var reader = try cursor.reader(&read_buffer);
    if (reader.size != value.len) {
        return false;
    }

    var chunk_buffer: [1024]u8 = undefined;
    var offset: usize = 0;
    while (offset < value.len) {
        const chunk_len = @min(chunk_buffer.len, value.len - offset);
        try reader.interface.readSliceAll(chunk_buffer[0..chunk_len]);
        if (!std.mem.eql(u8, chunk_buffer[0..chunk_len], value[offset .. offset + chunk_len])) {
            return false;
        }
        offset += chunk_len;
    }

    return true;
}

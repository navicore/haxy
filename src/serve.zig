const std = @import("std");
const xit = @import("xit");
const rp = xit.repo;
const hash = xit.hash;

pub const Options = struct {
    http_listen: []const u8,
    project_root: []const u8,
};

const ListenAddress = struct {
    host: []const u8,
    port: u16,
};

pub fn run(
    comptime repo_kind: rp.RepoKind,
    comptime any_repo_opts: rp.AnyRepoOpts(repo_kind),
    io: std.Io,
    allocator: std.mem.Allocator,
    cwd_path: []const u8,
    options: Options,
    err: *std.Io.Writer,
) !void {
    const listen_address = try parseListenAddress(options.http_listen);
    const address = try std.Io.net.IpAddress.parseIp4(listen_address.host, listen_address.port);
    var net_server = try address.listen(io, .{ .reuse_address = true });
    defer net_server.deinit(io);

    const project_root = try std.fs.path.resolve(allocator, &.{ cwd_path, options.project_root });
    defer allocator.free(project_root);

    try err.print("serving HTTP on {s}, project root {s}\n", .{ options.http_listen, project_root });
    try err.flush();

    var send_buffer = [_]u8{0} ** any_repo_opts.net_buffer_size;
    var recv_buffer = [_]u8{0} ** any_repo_opts.net_buffer_size;

    while (true) {
        const stream = net_server.accept(io) catch |accept_err| {
            try err.print("accept failed: {s}\n", .{@errorName(accept_err)});
            continue;
        };
        defer stream.close(io);

        var conn_br = stream.reader(io, &recv_buffer);
        var conn_bw = stream.writer(io, &send_buffer);
        var http_server = std.http.Server.init(&conn_br.interface, &conn_bw.interface);

        while (http_server.reader.state == .ready) {
            var request = http_server.receiveHead() catch |receive_err| switch (receive_err) {
                error.HttpConnectionClosing => break,
                error.ReadFailed => break,
                else => |e| return e,
            };

            handleRequest(repo_kind, any_repo_opts, io, allocator, project_root, &http_server, &request) catch |request_err| {
                try err.print("request failed: {s}\n", .{@errorName(request_err)});
                try err.flush();
                if (http_server.reader.state == .received_head) {
                    http_server.reader.state = .ready;
                }
                try writeSimpleResponse(&http_server, 500, "Internal Server Error", "text/plain", @errorName(request_err));
            };
            try http_server.out.flush();
            break;
        }
    }
}

fn handleRequest(
    comptime repo_kind: rp.RepoKind,
    comptime any_repo_opts: rp.AnyRepoOpts(repo_kind),
    io: std.Io,
    allocator: std.mem.Allocator,
    project_root: []const u8,
    http_server: *std.http.Server,
    request: *std.http.Server.Request,
) !void {
    const uri = try std.Uri.parseAfterScheme("", request.head.target);
    const path = uri.path.percent_encoded;
    if (path.len == 0 or path[0] != '/') {
        try writeSimpleResponse(http_server, 400, "Bad Request", "text/plain", "bad path");
        return;
    }

    const handler, const suffix = findRoute(path) orelse {
        if (http_server.reader.state == .received_head) {
            http_server.reader.state = .ready;
        }
        try writeSimpleResponse(http_server, 404, "Not Found", "text/plain", "not found");
        return;
    };

    const repo_rel_encoded = path[1 .. path.len - suffix.len];
    const repo_rel = try decodeAndValidateRepoPath(allocator, repo_rel_encoded);
    defer allocator.free(repo_rel);

    const repo_path = try std.fs.path.resolve(allocator, &.{ project_root, repo_rel });
    defer allocator.free(repo_path);

    if (!isSubPath(project_root, repo_path)) {
        if (http_server.reader.state == .received_head) {
            http_server.reader.state = .ready;
        }
        try writeSimpleResponse(http_server, 403, "Forbidden", "text/plain", "forbidden");
        return;
    }

    const request_method = normalizeMethod(request.head.method);
    const content_type = try allocator.dupe(u8, findHeader(request, "content-type") orelse "");
    defer allocator.free(content_type);
    const has_remote_user = findHeader(request, "authorization") != null;
    const protocol_version = protocolVersionFromHeader(findHeader(request, "git-protocol"));

    const body = if (request.head.method == .POST) blk: {
        const reader = try request.readerExpectContinue(&.{});
        break :blk try reader.allocRemaining(allocator, .unlimited);
    } else try allocator.dupe(u8, "");
    defer allocator.free(body);

    if (http_server.reader.state == .received_head) {
        http_server.reader.state = .ready;
    }

    var body_reader = std.Io.Reader.fixed(body);
    if (any_repo_opts.hash) |hash_kind| {
        var repo = try rp.Repo(repo_kind, any_repo_opts.toRepoOptsWithHash(hash_kind)).open(io, allocator, .{ .path = repo_path });
        defer repo.deinit(io, allocator);
        try runHttpBackend(repo_kind, any_repo_opts.toRepoOptsWithHash(hash_kind), &repo, io, allocator, &body_reader, http_server.out, request_method, handler, suffix, uri.query, content_type, has_remote_user, protocol_version);
    } else {
        var any_repo = try rp.AnyRepo(repo_kind, any_repo_opts).open(io, allocator, .{ .path = repo_path });
        defer any_repo.deinit(io, allocator);
        switch (any_repo) {
            inline else => |*repo| {
                try runHttpBackend(repo.self_repo_kind, repo.self_repo_opts, repo, io, allocator, &body_reader, http_server.out, request_method, handler, suffix, uri.query, content_type, has_remote_user, protocol_version);
            },
        }
    }
}

fn runHttpBackend(
    comptime repo_kind: rp.RepoKind,
    comptime repo_opts: rp.RepoOpts(repo_kind),
    repo: *rp.Repo(repo_kind, repo_opts),
    io: std.Io,
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    request_method: std.http.Method,
    handler: xit.net_server_http_backend.HandlerKind,
    suffix: []const u8,
    query: ?std.Uri.Component,
    content_type: []const u8,
    has_remote_user: bool,
    protocol_version: xit.net_server_common.ProtocolVersion,
) !void {
    try repo.httpBackend(io, allocator, reader, writer, .http, .{
        .request_method = request_method,
        .handler = handler,
        .suffix = suffix,
        .query_string = if (query) |q| q.percent_encoded else "",
        .content_type = content_type,
        .has_remote_user = has_remote_user,
        .protocol_version = protocol_version,
    });
}

fn findRoute(path: []const u8) ?struct { xit.net_server_http_backend.HandlerKind, []const u8 } {
    for (&xit.net_server_http_backend.routes) |*route| {
        if (std.mem.endsWith(u8, path, route.suffix)) {
            return .{ route.handler, route.suffix };
        }
    }
    return null;
}

fn parseListenAddress(value: []const u8) !ListenAddress {
    const colon = std.mem.lastIndexOfScalar(u8, value, ':') orelse return error.InvalidListenAddress;
    if (colon == 0 or colon + 1 >= value.len) return error.InvalidListenAddress;
    const port = try std.fmt.parseInt(u16, value[colon + 1 ..], 10);
    return .{ .host = value[0..colon], .port = port };
}

fn decodeAndValidateRepoPath(allocator: std.mem.Allocator, encoded: []const u8) ![]const u8 {
    if (encoded.len == 0) return error.InvalidRepoPath;

    const mutable = try allocator.dupe(u8, encoded);
    errdefer allocator.free(mutable);
    const decoded = std.Uri.percentDecodeInPlace(mutable);

    var iter = std.mem.splitScalar(u8, decoded, '/');
    while (iter.next()) |segment| {
        if (segment.len == 0 or std.mem.eql(u8, segment, ".") or std.mem.eql(u8, segment, "..")) {
            return error.InvalidRepoPath;
        }
    }

    return try allocator.realloc(mutable, decoded.len);
}

fn isSubPath(parent: []const u8, child: []const u8) bool {
    if (std.mem.eql(u8, parent, std.fs.path.sep_str)) return std.fs.path.isAbsolute(child);
    if (!std.mem.startsWith(u8, child, parent)) return false;
    return child.len == parent.len or child[parent.len] == std.fs.path.sep;
}

fn normalizeMethod(method: std.http.Method) std.http.Method {
    return if (method == .HEAD) .GET else method;
}

fn protocolVersionFromHeader(header: ?[]const u8) xit.net_server_common.ProtocolVersion {
    const git_protocol = header orelse return .v0;
    var version: xit.net_server_common.ProtocolVersion = .v0;
    var iter = std.mem.splitScalar(u8, git_protocol, ':');
    while (iter.next()) |entry| {
        const value = std.mem.trimStart(u8, entry, " ");
        if (std.mem.startsWith(u8, value, "version=")) {
            const v = value["version=".len..];
            if (std.mem.eql(u8, v, "2")) {
                version = .v2;
            } else if (std.mem.eql(u8, v, "1") and version != .v2) {
                version = .v1;
            }
        }
    }
    return version;
}

fn findHeader(request: *std.http.Server.Request, name: []const u8) ?[]const u8 {
    var it = request.iterateHeaders();
    while (it.next()) |header| {
        if (std.ascii.eqlIgnoreCase(header.name, name)) return header.value;
    }
    return null;
}

fn writeSimpleResponse(
    http_server: *std.http.Server,
    code: u16,
    message: []const u8,
    content_type: []const u8,
    body: []const u8,
) !void {
    try http_server.out.print(
        "HTTP/1.1 {d} {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\n\r\n{s}",
        .{ code, message, content_type, body.len, body },
    );
}

const std = @import("std");
const xit = @import("xit");
const rp = xit.repo;
const hash = xit.hash;

pub const Options = struct {
    http_listen: []const u8,
    ssh_listen: ?[]const u8,
    data_dir: []const u8,
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
    const data_dir = try std.fs.path.resolve(allocator, &.{ cwd_path, options.data_dir });
    defer allocator.free(data_dir);
    const repo_root = try std.fs.path.resolve(allocator, &.{ data_dir, "repos" });
    defer allocator.free(repo_root);

    try std.Io.Dir.cwd().createDirPath(io, repo_root);

    if (options.ssh_listen) |ssh_listen| {
        const HttpContext = struct {
            io: std.Io,
            allocator: std.mem.Allocator,
            repo_root: []const u8,
            http_listen: []const u8,
            err: *std.Io.Writer,

            fn run(ctx: @This()) !void {
                try runHttpListener(repo_kind, any_repo_opts, ctx.io, ctx.allocator, ctx.repo_root, ctx.http_listen, ctx.err);
            }
        };

        const http_thread = try std.Thread.spawn(.{}, HttpContext.run, .{HttpContext{
            .io = io,
            .allocator = allocator,
            .repo_root = repo_root,
            .http_listen = options.http_listen,
            .err = err,
        }});
        defer http_thread.join();

        try runSshListener(repo_kind, any_repo_opts, io, allocator, repo_root, ssh_listen, err);
    } else {
        try runHttpListener(repo_kind, any_repo_opts, io, allocator, repo_root, options.http_listen, err);
    }
}

fn runHttpListener(
    comptime repo_kind: rp.RepoKind,
    comptime any_repo_opts: rp.AnyRepoOpts(repo_kind),
    io: std.Io,
    allocator: std.mem.Allocator,
    repo_root: []const u8,
    http_listen: []const u8,
    err: *std.Io.Writer,
) !void {
    const listen_address = try parseListenAddress(http_listen);
    const address = try std.Io.net.IpAddress.parseIp4(listen_address.host, listen_address.port);
    var net_server = try address.listen(io, .{ .reuse_address = true });
    defer net_server.deinit(io);

    try err.print("serving HTTP on {s}, repo root {s}\n", .{ http_listen, repo_root });
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

            handleRequest(repo_kind, any_repo_opts, io, allocator, repo_root, &http_server, &request) catch |request_err| {
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
    repo_root: []const u8,
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

    const repo_path = try std.fs.path.resolve(allocator, &.{ repo_root, repo_rel });
    defer allocator.free(repo_path);

    if (!isSubPath(repo_root, repo_path)) {
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
    const create_if_missing = isReceivePack(handler, suffix, uri.query);
    const http_backend_options = xit.net_server_http_backend.Options{
        .request_method = request_method,
        .handler = handler,
        .suffix = suffix,
        .query_string = if (uri.query) |q| q.percent_encoded else "",
        .content_type = content_type,
        .has_remote_user = has_remote_user,
        .protocol_version = protocol_version,
    };

    if (any_repo_opts.hash) |hash_kind| {
        var repo = try openRepo(repo_kind, any_repo_opts.toRepoOptsWithHash(hash_kind), io, allocator, repo_path, create_if_missing);
        defer repo.deinit(io, allocator);
        try repo.httpBackend(io, allocator, &body_reader, http_server.out, .http, http_backend_options);
    } else {
        var any_repo = rp.AnyRepo(repo_kind, any_repo_opts).open(io, allocator, .{ .path = repo_path }) catch |err| switch (err) {
            error.RepoNotFound => {
                if (!create_if_missing) return err;

                var repo = try openRepo(repo_kind, any_repo_opts.toRepoOpts(), io, allocator, repo_path, true);
                defer repo.deinit(io, allocator);

                try repo.httpBackend(io, allocator, &body_reader, http_server.out, .http, http_backend_options);
                return;
            },
            else => |e| return e,
        };
        defer any_repo.deinit(io, allocator);

        switch (any_repo) {
            inline else => |*repo| {
                try repo.httpBackend(io, allocator, &body_reader, http_server.out, .http, http_backend_options);
            },
        }
    }
}

fn findRoute(path: []const u8) ?struct { xit.net_server_http_backend.HandlerKind, []const u8 } {
    for (&xit.net_server_http_backend.routes) |*route| {
        if (std.mem.endsWith(u8, path, route.suffix)) {
            return .{ route.handler, route.suffix };
        }
    }
    return null;
}

const SshService = enum {
    upload_pack,
    receive_pack,
};

const SshRequest = struct {
    service: SshService,
    protocol_version: xit.net_server_common.ProtocolVersion,
    repo: []const u8,

    fn deinit(self: SshRequest, allocator: std.mem.Allocator) void {
        allocator.free(self.repo);
    }
};

fn runSshListener(
    comptime repo_kind: rp.RepoKind,
    comptime any_repo_opts: rp.AnyRepoOpts(repo_kind),
    io: std.Io,
    allocator: std.mem.Allocator,
    repo_root: []const u8,
    ssh_listen: []const u8,
    err: *std.Io.Writer,
) !void {
    const listen_address = try parseListenAddress(ssh_listen);
    const address = try std.Io.net.IpAddress.parseIp4(listen_address.host, listen_address.port);
    var net_server = try address.listen(io, .{ .reuse_address = true });
    defer net_server.deinit(io);

    try err.print("serving SSH helper connections on {s}, repo root {s}\n", .{ ssh_listen, repo_root });
    try err.flush();

    while (true) {
        const stream = net_server.accept(io) catch |accept_err| {
            try err.print("ssh accept failed: {s}\n", .{@errorName(accept_err)});
            continue;
        };
        defer stream.close(io);

        handleSshConnection(repo_kind, any_repo_opts, io, allocator, repo_root, stream) catch |request_err| {
            try err.print("ssh request failed: {s}\n", .{@errorName(request_err)});
            try err.flush();
        };
    }
}

fn handleSshConnection(
    comptime repo_kind: rp.RepoKind,
    comptime any_repo_opts: rp.AnyRepoOpts(repo_kind),
    io: std.Io,
    allocator: std.mem.Allocator,
    repo_root: []const u8,
    stream: std.Io.net.Stream,
) !void {
    var send_buffer = [_]u8{0} ** any_repo_opts.net_buffer_size;
    var recv_buffer = [_]u8{0} ** any_repo_opts.net_buffer_size;
    var reader = stream.reader(io, &recv_buffer);
    var writer = stream.writer(io, &send_buffer);

    const request = try readSshRequest(allocator, &reader.interface);
    defer request.deinit(allocator);

    const repo_path = try resolveSshRepoPath(allocator, repo_root, request.repo);
    defer allocator.free(repo_path);

    if (!isSubPath(repo_root, repo_path)) return error.Forbidden;

    if (any_repo_opts.hash) |hash_kind| {
        var repo = try openRepo(repo_kind, any_repo_opts.toRepoOptsWithHash(hash_kind), io, allocator, repo_path, request.service == .receive_pack);
        defer repo.deinit(io, allocator);
        try runSshService(repo_kind, any_repo_opts.toRepoOptsWithHash(hash_kind), &repo, io, allocator, &reader.interface, &writer.interface, request);
    } else {
        var any_repo = rp.AnyRepo(repo_kind, any_repo_opts).open(io, allocator, .{ .path = repo_path }) catch |open_err| switch (open_err) {
            error.RepoNotFound => {
                if (request.service != .receive_pack) return open_err;

                var repo = try openRepo(repo_kind, any_repo_opts.toRepoOpts(), io, allocator, repo_path, true);
                defer repo.deinit(io, allocator);
                try runSshService(repo_kind, any_repo_opts.toRepoOpts(), &repo, io, allocator, &reader.interface, &writer.interface, request);
                return;
            },
            else => |e| return e,
        };
        defer any_repo.deinit(io, allocator);
        switch (any_repo) {
            inline else => |*repo| {
                try runSshService(repo.self_repo_kind, repo.self_repo_opts, repo, io, allocator, &reader.interface, &writer.interface, request);
            },
        }
    }
}

fn runSshService(
    comptime repo_kind: rp.RepoKind,
    comptime repo_opts: rp.RepoOpts(repo_kind),
    repo: *rp.Repo(repo_kind, repo_opts),
    io: std.Io,
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    request: SshRequest,
) !void {
    switch (request.service) {
        .upload_pack => try repo.uploadPack(io, allocator, reader, writer, .{
            .protocol_version = request.protocol_version,
        }),
        .receive_pack => try repo.receivePack(io, allocator, reader, writer, .{
            .protocol_version = request.protocol_version,
        }),
    }
}

fn openRepo(
    comptime repo_kind: rp.RepoKind,
    comptime repo_opts: rp.RepoOpts(repo_kind),
    io: std.Io,
    allocator: std.mem.Allocator,
    repo_path: []const u8,
    create_if_missing: bool,
) !rp.Repo(repo_kind, repo_opts) {
    return rp.Repo(repo_kind, repo_opts).open(io, allocator, .{ .path = repo_path }) catch |open_err| switch (open_err) {
        error.RepoNotFound => {
            if (!create_if_missing) return open_err;

            var repo = try rp.Repo(repo_kind, repo_opts).init(io, allocator, .{ .path = repo_path });
            errdefer repo.deinit(io, allocator);
            try repo.addConfig(io, allocator, .{ .name = "http.receivepack", .value = "true" });
            try repo.addConfig(io, allocator, .{ .name = "receive.denycurrentbranch", .value = "updateinstead" });
            return repo;
        },
        else => |e| return e,
    };
}

fn isReceivePack(
    handler: xit.net_server_http_backend.HandlerKind,
    suffix: []const u8,
    query: ?std.Uri.Component,
) bool {
    return switch (handler) {
        .run_service => std.mem.eql(u8, suffix, "/git-receive-pack"),
        .get_info_refs => if (query) |q|
            std.mem.startsWith(u8, q.percent_encoded, "service=git-receive-pack")
        else
            false,
    };
}

fn readSshRequest(allocator: std.mem.Allocator, reader: *std.Io.Reader) !SshRequest {
    const magic = try readSshPreludeLine(reader);
    if (!std.mem.eql(u8, magic, "haxy-ssh-helper-v1")) return error.InvalidSshMagic;

    const service_line = try readSshPreludeLine(reader);
    const service = parseSshService(stripPrefix(service_line, "service=") orelse return error.InvalidSshServiceLine) orelse return error.InvalidSshService;

    const protocol_line = try readSshPreludeLine(reader);
    const protocol_version = std.meta.stringToEnum(
        xit.net_server_common.ProtocolVersion,
        stripPrefix(protocol_line, "protocol=") orelse return error.InvalidSshProtocolLine,
    ) orelse return error.InvalidSshProtocol;

    const repo_length_line = try readSshPreludeLine(reader);
    const repo_len = try std.fmt.parseInt(usize, stripPrefix(repo_length_line, "repo-length=") orelse return error.InvalidSshRepoLengthLine, 10);

    const empty_line = try readSshPreludeLine(reader);
    if (empty_line.len != 0) return error.InvalidSshPreludeTerminator;

    const repo = try reader.readAlloc(allocator, repo_len);
    errdefer allocator.free(repo);

    return .{
        .service = service,
        .protocol_version = protocol_version,
        .repo = repo,
    };
}

fn readSshPreludeLine(reader: *std.Io.Reader) ![]const u8 {
    return (try reader.takeDelimiter('\n')) orelse error.InvalidSshHelperRequest;
}

fn parseSshService(value: []const u8) ?SshService {
    if (std.mem.eql(u8, value, "upload-pack")) return .upload_pack;
    if (std.mem.eql(u8, value, "receive-pack")) return .receive_pack;
    return null;
}

fn stripPrefix(value: []const u8, prefix: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, value, prefix)) return null;
    return value[prefix.len..];
}

fn resolveSshRepoPath(
    allocator: std.mem.Allocator,
    repo_root: []const u8,
    repo: []const u8,
) ![]const u8 {
    if (std.fs.path.isAbsolute(repo)) {
        return try std.fs.path.resolve(allocator, &.{repo});
    }
    return try std.fs.path.resolve(allocator, &.{ repo_root, repo });
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

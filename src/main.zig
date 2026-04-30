const std = @import("std");
const builtin = @import("builtin");
const xit = @import("xit");
const rp = xit.repo;
const cmd = @import("./command.zig");
const serve = @import("./serve.zig");

pub const RunOpts = struct {
    out: *std.Io.Writer,
    err: *std.Io.Writer,
    environ_map: *std.process.Environ.Map,
};

pub fn main(init: std.process.Init) !u8 {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = if (builtin.mode == .Debug) debug_allocator.allocator() else std.heap.smp_allocator;
    defer if (builtin.mode == .Debug) {
        _ = debug_allocator.deinit();
    };

    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var args: std.ArrayList([]const u8) = .empty;
    defer args.deinit(allocator);

    var arg_it = try init.minimal.args.iterateAllocator(allocator);
    defer arg_it.deinit();
    _ = arg_it.skip();
    while (arg_it.next()) |arg| {
        try args.append(allocator, arg);
    }

    var stdout_writer = std.Io.File.stdout().writer(io, &.{});
    var stderr_writer = std.Io.File.stderr().writer(io, &.{});
    const run_opts = RunOpts{ .out = &stdout_writer.interface, .err = &stderr_writer.interface, .environ_map = init.environ_map };

    const cwd_path = try std.process.currentPathAlloc(io, allocator);
    defer allocator.free(cwd_path);

    run(.xit, .{}, io, allocator, args.items, cwd_path, run_opts) catch |err| switch (err) {
        error.HandledError => return 1,
        else => |e| return e,
    };

    return 0;
}

pub fn run(
    comptime repo_kind: rp.RepoKind,
    comptime any_repo_opts: rp.AnyRepoOpts(repo_kind),
    io: std.Io,
    allocator: std.mem.Allocator,
    args: []const []const u8,
    cwd_path: []const u8,
    run_opts: RunOpts,
) !void {
    var cmd_args = try cmd.CommandArgs.init(allocator, args);
    defer cmd_args.deinit();

    switch (try cmd.CommandDispatch(repo_kind, any_repo_opts.toRepoOpts().hash).init(&cmd_args)) {
        .invalid => |invalid| switch (invalid) {
            .command => |command| {
                try run_opts.err.print("\"{s}\" is not a valid command\n\n", .{command});
                try cmd.printHelp(null, run_opts.err);
                return error.HandledError;
            },
            .argument => |argument| {
                try run_opts.err.print("\"{s}\" is not a valid argument\n\n", .{argument.value});
                try cmd.printHelp(argument.command, run_opts.err);
                return error.HandledError;
            },
        },
        .help => |cmd_kind_maybe| try cmd.printHelp(cmd_kind_maybe, run_opts.out),
        .cli => |cli_cmd| {
            if (cli_cmd == .serve) {
                try serve.run(repo_kind, any_repo_opts, io, allocator, cwd_path, .{
                    .http_listen = cli_cmd.serve.http_listen,
                    .project_root = cli_cmd.serve.project_root,
                }, run_opts.err);
                return;
            }

            // some commands allow the path to be specified. for all others, just use the cwd path.
            const work_path = switch (cli_cmd) {
                .upload_pack => |upload_pack| try std.fs.path.resolve(allocator, &.{ cwd_path, upload_pack.dir }),
                .receive_pack => |receive_pack| try std.fs.path.resolve(allocator, &.{ cwd_path, receive_pack.dir }),
                .http_backend => xit.net_server_http_backend.resolveDir(allocator, cwd_path, run_opts.environ_map) catch {
                    var http_stdout_buf: [any_repo_opts.buffer_size]u8 = undefined;
                    var http_stdout_writer = std.Io.File.stdout().writer(io, &http_stdout_buf);
                    try xit.net_server_http_backend.sendNotFound(&http_stdout_writer.interface);
                    return;
                },
                .serve => unreachable,
            };
            defer allocator.free(work_path);

            if (any_repo_opts.hash) |hash_kind| {
                var repo = try rp.Repo(repo_kind, any_repo_opts.toRepoOptsWithHash(hash_kind)).open(io, allocator, .{ .path = work_path });
                defer repo.deinit(io, allocator);
                try runCommand(repo_kind, any_repo_opts.toRepoOptsWithHash(hash_kind), &repo, io, allocator, cli_cmd, run_opts);
            } else {
                // if no hash was specified, use AnyRepo to detect the hash being used
                var any_repo = try rp.AnyRepo(repo_kind, any_repo_opts).open(io, allocator, .{ .path = work_path });
                defer any_repo.deinit(io, allocator);
                switch (any_repo) {
                    inline else => |*repo| {
                        const cmd_maybe = try cmd.Command(repo.self_repo_kind, repo.self_repo_opts.hash).initMaybe(&cmd_args);
                        try runCommand(repo.self_repo_kind, repo.self_repo_opts, repo, io, allocator, cmd_maybe orelse return error.InvalidCommand, run_opts);
                    },
                }
            }
        },
    }
}

fn runCommand(
    comptime repo_kind: rp.RepoKind,
    comptime repo_opts: rp.RepoOpts(repo_kind),
    repo: *rp.Repo(repo_kind, repo_opts),
    io: std.Io,
    allocator: std.mem.Allocator,
    command: cmd.Command(repo_kind, repo_opts.hash),
    run_opts: RunOpts,
) !void {
    switch (command) {
        .upload_pack => |upload_pack_cmd| {
            var options = upload_pack_cmd.options;
            options.protocol_version = xit.net_server_common.detectProtocolVersion(run_opts.environ_map);
            var stdin_buf: [repo_opts.net_buffer_size]u8 = undefined;
            var stdin_reader = std.Io.File.stdin().reader(io, &stdin_buf);
            var stdout_buf: [repo_opts.net_buffer_size]u8 = undefined;
            var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buf);
            try repo.uploadPack(io, allocator, &stdin_reader.interface, &stdout_writer.interface, options);
        },
        .receive_pack => |receive_pack_cmd| {
            var options = receive_pack_cmd.options;
            options.protocol_version = xit.net_server_common.detectProtocolVersion(run_opts.environ_map);
            var stdin_buf: [repo_opts.net_buffer_size]u8 = undefined;
            var stdin_reader = std.Io.File.stdin().reader(io, &stdin_buf);
            var stdout_buf: [repo_opts.net_buffer_size]u8 = undefined;
            var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buf);
            try repo.receivePack(io, allocator, &stdin_reader.interface, &stdout_writer.interface, options);
        },
        .http_backend => {
            const environ_map = run_opts.environ_map;
            var path_buf: [std.Io.Dir.max_path_bytes]u8 = undefined;
            const path = try xit.net_server_http_backend.resolveRepoPath(environ_map, &path_buf);

            var stdout_buf: [repo_opts.net_buffer_size]u8 = undefined;
            var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buf);

            const handler, const suffix = for (&xit.net_server_http_backend.routes) |*svc| {
                if (std.mem.endsWith(u8, path, svc.suffix))
                    break .{ svc.handler, svc.suffix };
            } else {
                try xit.net_server_http_backend.sendNotFound(&stdout_writer.interface);
                return;
            };

            const request_method: std.http.Method = blk: {
                const method_str = environ_map.get("REQUEST_METHOD") orelse break :blk .GET;
                const method = std.meta.stringToEnum(std.http.Method, method_str) orelse break :blk .GET;
                break :blk if (method == .HEAD) .GET else method;
            };

            var stdin_buf: [repo_opts.net_buffer_size]u8 = undefined;
            var stdin_reader = std.Io.File.stdin().reader(io, &stdin_buf);
            try repo.httpBackend(io, allocator, &stdin_reader.interface, &stdout_writer.interface, .cgi, .{
                .request_method = request_method,
                .handler = handler,
                .suffix = suffix,
                .query_string = environ_map.get("QUERY_STRING") orelse "",
                .content_type = environ_map.get("CONTENT_TYPE") orelse "",
                .has_remote_user = environ_map.get("REMOTE_USER") != null,
                .protocol_version = xit.net_server_common.detectProtocolVersion(environ_map),
            });
        },
        .serve => unreachable,
    }
}

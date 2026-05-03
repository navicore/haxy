const std = @import("std");
const builtin = @import("builtin");
const xit = @import("xit");

pub const Options = struct {
    ssh_connect: []const u8,
    service: ?[]const u8,
    dir: ?[]const u8,
};

const Service = enum {
    upload_pack,
    receive_pack,
};

const ConnectAddress = struct {
    host: []const u8,
    port: u16,
};

pub fn run(
    io: std.Io,
    allocator: std.mem.Allocator,
    options: Options,
    environ_map: *std.process.Environ.Map,
) !void {
    const command = try resolveCommand(allocator, options, environ_map);
    defer command.deinit(allocator);

    const connect_address = try parseConnectAddress(options.ssh_connect);
    const address = try std.Io.net.IpAddress.parseIp4(connect_address.host, connect_address.port);
    const stream = try address.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    var send_buffer = [_]u8{0} ** 1024;
    var recv_buffer = [_]u8{0} ** 1024;
    var stream_writer = stream.writer(io, &send_buffer);
    var stream_reader = stream.reader(io, &recv_buffer);

    try stream_writer.interface.print(
        "haxy-ssh-helper-v1\nservice={s}\nprotocol={s}\nrepo-length={d}\n\n",
        .{ serviceName(command.service), @tagName(xit.net_server_common.detectProtocolVersion(environ_map)), command.dir.len },
    );
    try stream_writer.interface.writeAll(command.dir);
    try stream_writer.interface.flush();

    if (builtin.os.tag == .windows) {
        var stdin_buffer = [_]u8{0} ** 1024;
        var stdin_reader = std.Io.File.stdin().reader(io, &stdin_buffer);

        const CopyIn = struct {
            io: std.Io,
            stream: std.Io.net.Stream,
            reader: *std.Io.Reader,
            writer: *std.Io.Writer,

            fn run(ctx: @This()) void {
                copy(ctx.reader, ctx.writer) catch {};
                ctx.writer.flush() catch {};
                ctx.stream.shutdown(ctx.io, .send) catch {};
            }
        };
        const stdin_thread = try std.Thread.spawn(.{}, CopyIn.run, .{CopyIn{
            .io = io,
            .stream = stream,
            .reader = &stdin_reader.interface,
            .writer = &stream_writer.interface,
        }});
        stdin_thread.detach();

        var stdout_buffer = [_]u8{0} ** 1024;
        var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buffer);
        try copy(&stream_reader.interface, &stdout_writer.interface);
        try stdout_writer.interface.flush();
    } else {
        const CopyIn = struct {
            io: std.Io,
            stream: std.Io.net.Stream,

            fn run(ctx: @This()) void {
                copyFd(std.posix.STDIN_FILENO, ctx.stream.socket.handle) catch {};
                ctx.stream.shutdown(ctx.io, .send) catch {};
            }
        };
        const stdin_thread = try std.Thread.spawn(.{}, CopyIn.run, .{CopyIn{ .io = io, .stream = stream }});
        stdin_thread.detach();

        try copyFd(stream.socket.handle, std.posix.STDOUT_FILENO);
    }
}

const Command = struct {
    service: Service,
    dir: []const u8,

    fn deinit(self: Command, allocator: std.mem.Allocator) void {
        allocator.free(self.dir);
    }
};

fn resolveCommand(
    allocator: std.mem.Allocator,
    options: Options,
    environ_map: *std.process.Environ.Map,
) !Command {
    if (options.service) |service_name| {
        const dir = options.dir orelse return error.InvalidSshCommand;
        return .{
            .service = try parseService(service_name),
            .dir = try allocator.dupe(u8, dir),
        };
    }

    const original_command = environ_map.get("SSH_ORIGINAL_COMMAND") orelse return error.InvalidSshCommand;
    var tokens = try parseShellWords(allocator, original_command);
    defer {
        for (tokens.items) |token| allocator.free(token);
        tokens.deinit(allocator);
    }
    if (tokens.items.len < 2) return error.InvalidSshCommand;

    return .{
        .service = try parseService(tokens.items[0]),
        .dir = try allocator.dupe(u8, tokens.items[1]),
    };
}

fn parseShellWords(allocator: std.mem.Allocator, command: []const u8) !std.ArrayList([]const u8) {
    var tokens: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (tokens.items) |token| allocator.free(token);
        tokens.deinit(allocator);
    }

    var current: std.ArrayList(u8) = .empty;
    defer current.deinit(allocator);

    var quote: ?u8 = null;
    var escaping = false;
    var in_token = false;

    for (command) |c| {
        if (escaping) {
            try current.append(allocator, c);
            escaping = false;
            in_token = true;
            continue;
        }

        if (c == '\\' and quote != '\'') {
            escaping = true;
            in_token = true;
            continue;
        }

        if (quote) |q| {
            if (c == q) {
                quote = null;
            } else {
                try current.append(allocator, c);
            }
            in_token = true;
            continue;
        }

        if (c == '\'' or c == '"') {
            quote = c;
            in_token = true;
            continue;
        }

        if (std.ascii.isWhitespace(c)) {
            if (in_token) {
                try tokens.append(allocator, try current.toOwnedSlice(allocator));
                current.clearRetainingCapacity();
                in_token = false;
            }
            continue;
        }

        try current.append(allocator, c);
        in_token = true;
    }

    if (escaping or quote != null) return error.InvalidSshCommand;
    if (in_token) {
        try tokens.append(allocator, try current.toOwnedSlice(allocator));
    }

    return tokens;
}

fn parseService(value: []const u8) !Service {
    if (std.mem.eql(u8, value, "upload-pack") or std.mem.eql(u8, value, "git-upload-pack")) return .upload_pack;
    if (std.mem.eql(u8, value, "receive-pack") or std.mem.eql(u8, value, "git-receive-pack")) return .receive_pack;
    return error.InvalidSshCommand;
}

fn serviceName(service: Service) []const u8 {
    return switch (service) {
        .upload_pack => "upload-pack",
        .receive_pack => "receive-pack",
    };
}

fn parseConnectAddress(value: []const u8) !ConnectAddress {
    const colon = std.mem.lastIndexOfScalar(u8, value, ':') orelse return error.InvalidConnectAddress;
    if (colon == 0 or colon + 1 >= value.len) return error.InvalidConnectAddress;
    const port = try std.fmt.parseInt(u16, value[colon + 1 ..], 10);
    return .{ .host = value[0..colon], .port = port };
}

fn copy(reader: *std.Io.Reader, writer: *std.Io.Writer) !void {
    var buffer: [4096]u8 = undefined;
    while (true) {
        const len = try reader.readSliceShort(&buffer);
        if (len == 0) break;
        try writer.writeAll(buffer[0..len]);
        try writer.flush();
    }
}

fn copyFd(src: std.posix.fd_t, dst: std.posix.fd_t) !void {
    var buffer: [4096]u8 = undefined;
    while (true) {
        const len = try std.posix.read(src, &buffer);
        if (len == 0) break;
        try writeAllFd(dst, buffer[0..len]);
    }
}

fn writeAllFd(fd: std.posix.fd_t, bytes: []const u8) !void {
    var written: usize = 0;
    while (written < bytes.len) {
        const rc = std.posix.system.write(fd, bytes[written..].ptr, bytes.len - written);
        switch (std.posix.errno(rc)) {
            .SUCCESS => written += @intCast(rc),
            .INTR => continue,
            .PIPE => return error.BrokenPipe,
            else => return error.WriteFailed,
        }
    }
}

"""Connection, server and introspection commands."""

import asyncio
import os
import sys
import time

from .. import REDIS_VERSION, __version__
from ..errors import RedisError
from ..protocol import NO_REPLY, Map, SimpleString
from ..utils import glob_match, parse_int, syntax_error
from . import COMMANDS, command


def _config_get(server, pattern):
    result = []
    for name, value in server.config.items():
        if glob_match(name.encode(), pattern):
            result.extend((name, value))
    return result


@command("ping", 0, 1)
async def ping(client, args):
    message = args[0] if args else b""
    if client.subscriptions or client.patterns:
        return [b"pong", message]
    if args:
        return args[0]
    return SimpleString("PONG")


@command("echo", 1, 1)
async def echo(client, args):
    return args[0]


@command("quit", 0, 0)
async def quit_(client, args):
    client.close_after_reply = True
    return SimpleString("OK")


@command("select", 1, 1)
async def select(client, args):
    index = parse_int(args[0])
    if not 0 <= index < len(client.server.databases):
        raise RedisError("ERR DB index is out of range")
    client.db_index = index
    return SimpleString("OK")


@command("dbsize", 0, 0)
async def dbsize(client, args):
    return client.db.dbsize()


@command("flushdb", 0, 1)
async def flushdb(client, args):
    _check_flush_mode(args)
    db = client.db
    db.flush()
    client.server.notifier.notify_db(db.index)
    return SimpleString("OK")


@command("flushall", 0, 1)
async def flushall(client, args):
    _check_flush_mode(args)
    for db in client.server.databases:
        db.flush()
        client.server.notifier.notify_db(db.index)
    return SimpleString("OK")


def _check_flush_mode(args):
    if args and args[0].upper() not in (b"ASYNC", b"SYNC"):
        raise syntax_error()


@command("swapdb", 2, 2)
async def swapdb(client, args):
    first = parse_int(args[0])
    second = parse_int(args[1])
    databases = client.server.databases
    if not (0 <= first < len(databases) and 0 <= second < len(databases)):
        raise RedisError("ERR DB index is out of range")
    if first != second:
        a, b = databases[first], databases[second]
        a.data, b.data = b.data, a.data
        a.expires, b.expires = b.expires, a.expires
        a.versions, b.versions = b.versions, a.versions
        a.epoch += 1
        b.epoch += 1
        client.server.notifier.notify_db(first)
        client.server.notifier.notify_db(second)
    return SimpleString("OK")


@command("time", 0, 0)
async def time_(client, args):
    now = time.time()
    seconds = int(now)
    microseconds = int((now - seconds) * 1_000_000)
    return [str(seconds).encode(), str(microseconds).encode()]


@command("config", 2)
async def config(client, args):
    server = client.server
    subcommand = args[0].upper()
    if subcommand == b"GET":
        if len(args) != 2:
            raise RedisError("ERR wrong number of arguments for 'config|get' command")
        values = _config_get(server, args[1])
        if client.protocol == 3:
            return Map(
                [(values[i], values[i + 1]) for i in range(0, len(values), 2)]
            )
        return values
    if subcommand == b"SET":
        if len(args) < 3 or len(args) % 2 == 0:
            raise RedisError("ERR wrong number of arguments for 'config|set' command")
        for i in range(1, len(args), 2):
            name = args[i].decode("utf-8", "replace").lower()
            value = args[i + 1].decode("utf-8", "replace")
            if name not in server.config:
                raise RedisError(
                    f"ERR Unknown option or number of arguments for CONFIG SET - '{name}'"
                )
            if name == "requirepass":
                server.config[name] = value
                if not value:
                    for connected in server.clients.values():
                        connected.authenticated = True
            else:
                server.config[name] = value
        return SimpleString("OK")
    if subcommand == b"RESETSTAT":
        return SimpleString("OK")
    if subcommand == b"REWRITE":
        raise RedisError("ERR The server is running without a config file")
    raise RedisError(
        f"ERR Unknown subcommand or wrong number of arguments for '{args[0].decode('utf-8', 'replace')}'. Try CONFIG HELP."
    )


@command("info", 0, 1)
async def info(client, args):
    server = client.server
    section = args[0].decode("utf-8", "replace").lower() if args else "all"
    lines = []

    def add(name, value):
        lines.append(f"{name}:{value}")

    def header(name):
        lines.append("")
        lines.append(f"# {name}")

    if section in ("all", "default", "server"):
        header("Server")
        add("redis_version", REDIS_VERSION)
        add("miniredis_version", __version__)
        add("redis_mode", "standalone")
        add("os", sys.platform)
        add("arch_bits", 64)
        add("process_id", os.getpid())
        add("run_id", f"{id(server):x}")
        add("tcp_port", server.port)
        add("uptime_in_seconds", int(time.time() - server.start_time))
        add("uptime_in_days", int((time.time() - server.start_time) / 86400))

    if section in ("all", "default", "clients"):
        header("Clients")
        add("connected_clients", len(server.clients))
        add("blocked_clients", 0)

    if section in ("all", "default", "memory"):
        header("Memory")
        used = 0
        for db in server.databases:
            used += len(db.data) * 64
        add("used_memory", used)
        add("used_memory_human", f"{used / 1024:.2f}K")
        add("maxmemory", server.config.get("maxmemory", "0"))
        add("maxmemory_policy", "noeviction")

    if section in ("all", "default", "persistence"):
        header("Persistence")
        enabled = server.persistence is not None
        add("loading", 0)
        add("rdb_changes_since_last_save", 1 if server.dirty else 0)
        add("rdb_bgsave_in_progress", 0)
        add("rdb_last_save_time", int(server.last_save))
        add("rdb_last_bgsave_status", "ok")
        add("aof_enabled", 0)
        if not enabled:
            add("persistence_disabled", 1)

    if section in ("all", "default", "stats"):
        header("Stats")
        add("total_connections_received", server.total_connections)
        add("total_commands_processed", server.total_commands)
        add("instantaneous_ops_per_sec", 0)
        add("total_net_input_bytes", 0)
        add("total_net_output_bytes", 0)
        add("keyspace_hits", 0)
        add("keyspace_misses", 0)

    if section in ("all", "default", "replication"):
        header("Replication")
        add("role", "master")
        add("connected_slaves", 0)

    if section in ("all", "default", "cpu"):
        header("CPU")
        add("used_cpu_sys", 0)
        add("used_cpu_user", 0)

    if section in ("all", "default", "keyspace"):
        header("Keyspace")
        for db in server.databases:
            size = db.dbsize()
            if size:
                add(f"db{db.index}", f"keys={size},expires={len(db.expires)},avg_ttl=0")

    lines.append("")
    return "\r\n".join(lines).encode()


@command("command", 0)
async def command_(client, args):
    if not args:
        entries = []
        for entry in sorted(COMMANDS.values(), key=lambda item: item.name):
            details = [entry.name.lower().encode(), entry.arity, [], 0, 0, 0]
            if client.protocol == 3:
                details.append([])  # ACL categories, required by RESP3 clients
            entries.append(details)
        return entries
    subcommand = args[0].upper()
    if subcommand == b"COUNT":
        return len(COMMANDS)
    if subcommand == b"DOCS":
        return Map([]) if client.protocol == 3 else []
    if subcommand == b"INFO":
        result = []
        for raw_name in args[1:]:
            name = raw_name.decode("utf-8", "replace").upper()
            entry = COMMANDS.get(name)
            if entry is None:
                result.append(None)
            else:
                details = [entry.name.lower().encode(), entry.arity, [], 0, 0, 0]
                if client.protocol == 3:
                    details.append([])
                result.append(details)
        return result
    if subcommand == b"GETKEYS":
        return []
    if subcommand == b"LIST":
        return [entry.name.lower().encode() for entry in COMMANDS.values()]
    raise RedisError(
        f"ERR Unknown subcommand or wrong number of arguments for '{args[0].decode('utf-8', 'replace')}'. Try COMMAND HELP."
    )


@command("client", 1)
async def client_cmd(client, args):
    subcommand = args[0].upper()
    server = client.server
    if subcommand == b"ID":
        return client.id
    if subcommand == b"GETNAME":
        return client.name or None
    if subcommand == b"SETNAME":
        if len(args) != 2:
            raise RedisError("ERR wrong number of arguments for 'client|setname' command")
        name = args[1]
        if any(byte in name for byte in b" \n\r\t"):
            raise RedisError("ERR Client names cannot contain spaces, newlines or special characters.")
        client.name = name
        return SimpleString("OK")
    if subcommand == b"SETINFO":
        return SimpleString("OK")
    if subcommand == b"LIST":
        lines = []
        now = time.time()
        for connected in server.clients.values():
            lines.append(
                "id={} addr={} name={} db={} sub={} psub={} age={} idle=0 flags=N".format(
                    connected.id,
                    connected.address,
                    connected.name.decode("utf-8", "replace") or "",
                    connected.db_index,
                    len(connected.subscriptions),
                    len(connected.patterns),
                    int(now - connected.created_at),
                )
            )
        return "\n".join(lines).encode()
    if subcommand == b"INFO":
        return f"id={client.id} db={client.db_index} name={client.name.decode('utf-8', 'replace')}".encode()
    if subcommand in (b"NO-EVICT", b"NO-TOUCH", b"UNPAUSE", b"PAUSE", b"REPLY"):
        return SimpleString("OK")
    if subcommand == b"KILL":
        raise RedisError("ERR CLIENT KILL is not supported")
    raise RedisError(
        f"ERR Unknown subcommand or wrong number of arguments for '{args[0].decode('utf-8', 'replace')}'. Try CLIENT HELP."
    )


@command("hello", 0)
async def hello(client, args):
    protocol = client.protocol
    if args:
        protocol = parse_int(args[0])
        if protocol not in (2, 3):
            raise RedisError("NOPROTO unsupported protocol version")
        client.protocol = protocol
        i = 1
        while i < len(args):
            option = args[i].upper()
            if option == b"AUTH" and i + 2 < len(args):
                await auth(client, [args[i + 1], args[i + 2]])
                i += 3
            elif option == b"SETNAME" and i + 1 < len(args):
                client.name = args[i + 1]
                i += 2
            else:
                raise syntax_error()
    fields = [
        (b"server", b"redis"),
        (b"version", REDIS_VERSION.encode()),
        (b"proto", protocol),
        (b"id", client.id),
        (b"mode", b"standalone"),
        (b"role", b"master"),
        (b"modules", []),
    ]
    if client.protocol == 3:
        return Map(fields)
    result = []
    for key, value in fields:
        result.extend((key, value))
    return result


@command("auth", 1, 2)
async def auth(client, args):
    server = client.server
    password = args[-1]
    if not server.config.get("requirepass"):
        raise RedisError(
            "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?"
        )
    if password.decode("utf-8", "replace") != server.config["requirepass"]:
        raise RedisError("WRONGPASS invalid username-password pair or user is disabled.")
    client.authenticated = True
    return SimpleString("OK")


@command("reset", 0, 0)
async def reset(client, args):
    client.reset()
    return SimpleString("RESET")


@command("role", 0, 0)
async def role(client, args):
    return [b"master", 0, []]


@command("save", 0, 0)
async def save(client, args):
    server = client.server
    if server.persistence is None:
        raise RedisError("ERR persistence is disabled")
    try:
        await server.persistence.save()
    except RuntimeError:
        raise RedisError("ERR Background save already in progress") from None
    return SimpleString("OK")


@command("bgsave", 0, 1)
async def bgsave(client, args):
    server = client.server
    if server.persistence is None:
        raise RedisError("ERR persistence is disabled")
    if server.saving:
        raise RedisError("ERR Background save already in progress")
    server.saving = True
    asyncio.create_task(server.persistence.save(background=True))
    return SimpleString("Background saving started")


@command("lastsave", 0, 0)
async def lastsave(client, args):
    return int(client.server.last_save)


@command("shutdown", 0, 1)
async def shutdown(client, args):
    server = client.server
    mode = args[0].upper() if args else None
    if mode not in (None, b"SAVE", b"NOSAVE"):
        raise syntax_error()
    server.save_on_shutdown = mode != b"NOSAVE"
    client.close_after_reply = True
    server.request_shutdown()
    return NO_REPLY


@command("lolwut", 0)
async def lolwut(client, args):
    return (
        f"miniredis {__version__} (Redis compatible)\r\n"
        "A tiny keyspace, written in Python.\r\n"
    ).encode()

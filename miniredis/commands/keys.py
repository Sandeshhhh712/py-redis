"""Generic key commands: existence, expiry, rename, SCAN, ..."""

import copy
import random
import time

from ..errors import RedisError
from ..protocol import SimpleString
from ..types import type_name
from ..utils import glob_match, parse_int, syntax_error
from . import command


@command("del", 1)
async def del_(client, args):
    return sum(1 for key in args if client.db.delete(key))


@command("unlink", 1)
async def unlink(client, args):
    return sum(1 for key in args if client.db.delete(key))


@command("exists", 1)
async def exists(client, args):
    return sum(1 for key in args if client.db.exists(key))


@command("type", 1, 1)
async def type_(client, args):
    return SimpleString(client.db.type_of(args[0]))


@command("keys", 1, 1)
async def keys(client, args):
    pattern = args[0]
    if pattern == b"*":
        return client.db.keys()
    return [key for key in client.db.keys() if glob_match(key, pattern)]


@command("randomkey", 0, 0)
async def randomkey(client, args):
    all_keys = client.db.keys()
    if not all_keys:
        return None
    return random.choice(all_keys)


@command("rename", 2, 2)
async def rename(client, args):
    source, destination = args
    db = client.db
    value = db.get(source)
    if value is None:
        raise RedisError("ERR no such key")
    if source == destination:
        return SimpleString("OK")
    ttl = db.expires.get(source)
    db.delete(destination)
    db.delete(source)
    db.set(destination, value, expires_at=ttl)
    return SimpleString("OK")


@command("renamenx", 2, 2)
async def renamenx(client, args):
    source, destination = args
    db = client.db
    value = db.get(source)
    if value is None:
        raise RedisError("ERR no such key")
    if source == destination:
        return 0
    if db.get(destination) is not None:
        return 0
    ttl = db.expires.get(source)
    db.delete(source)
    db.set(destination, value, expires_at=ttl)
    return 1


@command("copy", 2)
async def copy_cmd(client, args):
    source, destination = args[0], args[1]
    replace = False
    destination_db_index = client.db_index
    i = 2
    while i < len(args):
        option = args[i].upper()
        if option == b"REPLACE":
            replace = True
            i += 1
        elif option == b"DB":
            if i + 1 >= len(args):
                raise syntax_error()
            destination_db_index = parse_int(args[i + 1])
            if not 0 <= destination_db_index < len(client.server.databases):
                raise RedisError("ERR DB index is out of range")
            i += 2
        else:
            raise syntax_error()

    db = client.db
    value = db.get(source)
    if value is None:
        return 0

    target = client.server.databases[destination_db_index]
    if target is client.db and source == destination:
        raise RedisError("ERR source and destination objects are the same")
    if target.get(destination) is not None and not replace:
        return 0

    ttl = db.expires.get(source)
    target.set(destination, copy.deepcopy(value), expires_at=ttl)
    return 1


@command("move", 2, 2)
async def move(client, args):
    key = args[0]
    target_index = parse_int(args[1])
    if not 0 <= target_index < len(client.server.databases):
        raise RedisError("ERR DB index is out of range")
    source = client.db
    target = client.server.databases[target_index]
    if source is target:
        raise RedisError("ERR source and destination objects are the same")
    value = source.get(key)
    if value is None or target.get(key) is not None:
        return 0
    ttl = source.expires.get(key)
    source.delete(key)
    target.set(key, value, expires_at=ttl)
    return 1


def _expire_command(client, args, unit_seconds, absolute, command_name):
    key, raw_amount = args[0], args[1]
    amount = parse_int(raw_amount)
    options = [option.upper() for option in args[2:]]
    nx = b"NX" in options
    xx = b"XX" in options
    gt = b"GT" in options
    lt = b"LT" in options
    if any(option not in (b"NX", b"XX", b"GT", b"LT") for option in options):
        raise syntax_error()
    if nx and (xx or gt or lt):
        raise RedisError(
            "ERR NX and XX, GT or LT options at the same time are not compatible"
        )
    if gt and lt:
        raise RedisError(
            "ERR GT and LT options at the same time are not compatible"
        )

    db = client.db
    if db.get(key) is None:
        return 0

    current = db.expires.get(key)
    if nx and current is not None:
        return 0
    if xx and current is None:
        return 0

    now = time.time()
    if absolute:
        deadline = float(amount) if unit_seconds else amount / 1000.0
    else:
        deadline = now + (amount if unit_seconds else amount / 1000.0)

    if gt and (current is None or deadline <= current):
        return 0
    if lt and current is not None and deadline >= current:
        return 0

    if deadline <= now:
        db.delete(key)
        return 1

    db.set_expire(key, deadline)
    return 1


@command("expire", 2)
async def expire(client, args):
    return _expire_command(client, args, True, False, "expire")


@command("pexpire", 2)
async def pexpire(client, args):
    return _expire_command(client, args, False, False, "pexpire")


@command("expireat", 2)
async def expireat(client, args):
    return _expire_command(client, args, True, True, "expireat")


@command("pexpireat", 2)
async def pexpireat(client, args):
    return _expire_command(client, args, False, True, "pexpireat")


@command("ttl", 1, 1)
async def ttl(client, args):
    ttl_ms = client.db.ttl_ms(args[0])
    if ttl_ms < 0:
        return ttl_ms
    return (ttl_ms + 500) // 1000


@command("pttl", 1, 1)
async def pttl(client, args):
    return client.db.ttl_ms(args[0])


@command("expiretime", 1, 1)
async def expiretime(client, args):
    value = client.db.expire_time(args[0])
    return value if value < 0 else value


@command("pexpiretime", 1, 1)
async def pexpiretime(client, args):
    value = client.db.expire_time(args[0])
    return value if value < 0 else value * 1000


@command("persist", 1, 1)
async def persist(client, args):
    return 1 if client.db.persist(args[0]) else 0


@command("touch", 1)
async def touch(client, args):
    return sum(1 for key in args if client.db.exists(key))


@command("object", 2, 2)
async def object_(client, args):
    subcommand = args[0].upper()
    if subcommand == b"ENCODING":
        value = client.db.get(args[1])
        if value is None:
            return None
        name = type_name(value)
        if name == "string":
            return SimpleString("embstr")
        if name == "list":
            return SimpleString("listpack")
        if name == "hash":
            return SimpleString("listpack")
        if name == "set":
            return SimpleString("listpack")
        return SimpleString("listpack")
    if subcommand == b"REFCOUNT":
        return 1 if client.db.exists(args[1]) else None
    if subcommand == b"IDLETIME":
        return 0 if client.db.exists(args[1]) else None
    if subcommand == b"FREQ":
        return 0 if client.db.exists(args[1]) else None
    raise RedisError(
        f"ERR Unknown subcommand or wrong number of arguments for '{args[0].decode('utf-8', 'replace')}'. Try OBJECT HELP."
    )


def paginate(names, cursor_raw, match, count):
    """Cursor pagination over a stable, sorted snapshot of ``names``."""
    cursor = parse_int(cursor_raw, "ERR invalid cursor")
    if cursor < 0:
        raise RedisError("ERR invalid cursor")
    if cursor > len(names):
        cursor = len(names)
    selected = []
    position = cursor
    scanned = 0
    while position < len(names) and scanned < count:
        name = names[position]
        position += 1
        scanned += 1
        if glob_match(name, match):
            selected.append(name)
    next_cursor = position if position < len(names) else 0
    return next_cursor, selected


def scan_options(args):
    """Parse MATCH/COUNT/TYPE options; returns (match, count, type_filter)."""
    match = b"*"
    count = 10
    type_filter = None
    i = 0
    while i < len(args):
        option = args[i].upper()
        if option in (b"MATCH", b"COUNT", b"TYPE"):
            if i + 1 >= len(args):
                raise syntax_error()
            if option == b"MATCH":
                match = args[i + 1]
            elif option == b"COUNT":
                count = parse_int(args[i + 1])
                if count < 1:
                    count = 1
            else:
                type_filter = args[i + 1].decode("utf-8", "replace").lower()
            i += 2
        else:
            raise syntax_error()
    return match, count, type_filter


@command("scan", 1)
async def scan(client, args):
    match, count, type_filter = scan_options(args[1:])
    all_keys = sorted(client.db.keys())
    if type_filter is not None:
        all_keys = [
            key for key in all_keys if client.db.type_of(key) == type_filter
        ]
    next_cursor, selected = paginate(all_keys, args[0], match, count)
    return [str(next_cursor).encode(), selected]

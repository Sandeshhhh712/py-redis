"""String commands."""

import time

from ..errors import WRONGTYPE, RedisError
from ..protocol import Double, SimpleString
from ..utils import (
    INT64_MAX,
    INT64_MIN,
    format_float,
    parse_float,
    parse_int,
    syntax_error,
    wrong_args,
)
from . import command


def _string(db, key):
    value = db.get(key)
    if value is None:
        return None
    if not isinstance(value, bytes):
        raise RedisError(WRONGTYPE)
    return value


@command("get", 1, 1)
async def get(client, args):
    return _string(client.db, args[0])


@command("set", 2)
async def set_(client, args):
    key, value = args[0], args[1]
    expires_at = None
    keep_ttl = False
    nx = xx = get_old = False
    expire_option = False
    i = 2
    while i < len(args):
        option = args[i].upper()
        if option in (b"EX", b"PX", b"EXAT", b"PXAT"):
            if i + 1 >= len(args):
                raise syntax_error()
            amount = parse_int(
                args[i + 1], "ERR invalid expire time in 'set' command"
            )
            if amount <= 0:
                raise RedisError("ERR invalid expire time in 'set' command")
            now = time.time()
            if option == b"EX":
                expires_at = now + amount
            elif option == b"PX":
                expires_at = now + amount / 1000.0
            elif option == b"EXAT":
                expires_at = float(amount)
            else:  # PXAT
                expires_at = amount / 1000.0
            expire_option = True
            i += 2
        elif option == b"KEEPTTL":
            keep_ttl = True
            i += 1
        elif option == b"NX":
            nx = True
            i += 1
        elif option == b"XX":
            xx = True
            i += 1
        elif option == b"GET":
            get_old = True
            i += 1
        else:
            raise syntax_error()

    if nx and xx:
        raise syntax_error()
    if keep_ttl and expire_option:
        raise syntax_error()

    db = client.db
    old = db.get(key)
    if get_old and old is not None and not isinstance(old, bytes):
        raise RedisError(WRONGTYPE)

    if (nx and old is not None) or (xx and old is None):
        return old if get_old else None

    db.set(key, value, expires_at=expires_at, keep_ttl=keep_ttl)
    return old if get_old else SimpleString("OK")


@command("setnx", 2, 2)
async def setnx(client, args):
    db = client.db
    if db.get(args[0]) is not None:
        return 0
    db.set(args[0], args[1])
    return 1


@command("getset", 2, 2)
async def getset(client, args):
    old = _string(client.db, args[0])
    client.db.set(args[0], args[1])
    return old


@command("getdel", 1, 1)
async def getdel(client, args):
    old = _string(client.db, args[0])
    if old is not None:
        client.db.delete(args[0])
    return old


@command("getex", 1)
async def getex(client, args):
    key = args[0]
    old = _string(client.db, key)
    if old is None:
        return None

    expires_at = None
    persist = False
    i = 1
    while i < len(args):
        option = args[i].upper()
        if option in (b"EX", b"PX", b"EXAT", b"PXAT"):
            if i + 1 >= len(args):
                raise syntax_error()
            amount = parse_int(
                args[i + 1], "ERR invalid expire time in 'getex' command"
            )
            if amount <= 0:
                raise RedisError("ERR invalid expire time in 'getex' command")
            now = time.time()
            if option == b"EX":
                expires_at = now + amount
            elif option == b"PX":
                expires_at = now + amount / 1000.0
            elif option == b"EXAT":
                expires_at = float(amount)
            else:
                expires_at = amount / 1000.0
            i += 2
        elif option == b"PERSIST":
            persist = True
            i += 1
        else:
            raise syntax_error()

    db = client.db
    if persist:
        db.persist(key)
    elif expires_at is not None:
        if expires_at <= time.time():
            db.delete(key)
        else:
            db.set_expire(key, expires_at)
    return old


@command("mget", 1)
async def mget(client, args):
    return [_string(client.db, key) for key in args]


@command("mset", 2)
async def mset(client, args):
    if len(args) % 2:
        raise wrong_args("mset")
    db = client.db
    for i in range(0, len(args), 2):
        db.set(args[i], args[i + 1])
    return SimpleString("OK")


@command("msetnx", 2)
async def msetnx(client, args):
    if len(args) % 2:
        raise wrong_args("msetnx")
    db = client.db
    for i in range(0, len(args), 2):
        if db.get(args[i]) is not None:
            return 0
    for i in range(0, len(args), 2):
        db.set(args[i], args[i + 1])
    return 1


@command("append", 2, 2)
async def append(client, args):
    db = client.db
    current = _string(db, args[0])
    value = args[1] if current is None else current + args[1]
    db.set(args[0], value)
    return len(value)


@command("strlen", 1, 1)
async def strlen(client, args):
    value = _string(client.db, args[0])
    return 0 if value is None else len(value)


def _incr(db, key, delta):
    current = _string(db, key)
    base = 0 if current is None else parse_int(current)
    result = base + delta
    if not INT64_MIN <= result <= INT64_MAX:
        raise RedisError("ERR increment or decrement would overflow")
    db.set(key, str(result).encode())
    return result


@command("incr", 1, 1)
async def incr(client, args):
    return _incr(client.db, args[0], 1)


@command("decr", 1, 1)
async def decr(client, args):
    return _incr(client.db, args[0], -1)


@command("incrby", 2, 2)
async def incrby(client, args):
    return _incr(client.db, args[0], parse_int(args[1]))


@command("decrby", 2, 2)
async def decrby(client, args):
    return _incr(client.db, args[0], -parse_int(args[1]))


@command("incrbyfloat", 2, 2)
async def incrbyfloat(client, args):
    db = client.db
    current = _string(db, args[0])
    try:
        base = 0.0 if current is None else parse_float(current)
        delta = parse_float(args[1])
    except RedisError:
        raise RedisError("ERR value is not a valid float") from None
    result = base + delta
    if result != result or result in (float("inf"), float("-inf")):
        raise RedisError("ERR increment would produce NaN or Infinity")
    value = format_float(result)
    db.set(args[0], value)
    if client.protocol == 3:
        return Double(result)
    return value


@command("getrange", 3, 3)
async def getrange(client, args):
    value = _string(client.db, args[0])
    if value is None:
        return b""
    start = parse_int(args[1])
    end = parse_int(args[2])
    length = len(value)
    if start < 0:
        start += length
    if end < 0:
        end += length
    start = max(0, start)
    end = min(length - 1, end)
    if start > end or start >= length:
        return b""
    return value[start : end + 1]


@command("setrange", 3, 3)
async def setrange(client, args):
    key, offset_raw, patch = args[0], args[1], args[2]
    offset = parse_int(offset_raw, "ERR offset is out of range")
    if offset < 0:
        raise RedisError("ERR offset is out of range")
    if len(patch) == 0:
        value = _string(client.db, key)
        return 0 if value is None else len(value)
    if offset + len(patch) > 512 * 1024 * 1024:
        raise RedisError("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
    db = client.db
    value = _string(db, key) or b""
    if len(value) < offset:
        value += b"\x00" * (offset - len(value))
    value = value[:offset] + patch + value[offset + len(patch) :]
    db.set(key, value)
    return len(value)


@command("substr", 3, 3)
async def substr(client, args):
    return await getrange(client, args)

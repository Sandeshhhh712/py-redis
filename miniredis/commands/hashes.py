"""Hash commands."""

import random

from ..errors import WRONGTYPE, RedisError
from ..protocol import Double, Map, SimpleString
from ..types import Hash
from ..utils import format_float, parse_float, parse_int, syntax_error, wrong_args
from . import command
from .keys import paginate, scan_options


def _hash(db, key, create=False):
    value = db.get(key)
    if value is None:
        if not create:
            return None
        value = Hash()
        db.set(key, value)
    elif not isinstance(value, Hash):
        raise RedisError(WRONGTYPE)
    return value


@command("hset", 3)
async def hset(client, args):
    if len(args) % 2 == 0:
        raise wrong_args("hset")
    return _hset(client, args)


@command("hmset", 3)
async def hmset(client, args):
    if len(args) % 2 == 0:
        raise wrong_args("hmset")
    _hset(client, args)
    return SimpleString("OK")


def _hset(client, args):
    db = client.db
    key = args[0]
    value = _hash(db, key, create=True)
    added = 0
    for i in range(1, len(args), 2):
        field = args[i]
        if field not in value:
            added += 1
        value[field] = args[i + 1]
    db.touch(key)
    return added


@command("hsetnx", 3, 3)
async def hsetnx(client, args):
    db = client.db
    key, field, item = args
    value = _hash(db, key, create=True)
    if field in value:
        return 0
    value[field] = item
    db.touch(key)
    return 1


@command("hget", 2, 2)
async def hget(client, args):
    value = _hash(client.db, args[0])
    if value is None:
        return None
    return value.get(args[1])


@command("hmget", 2)
async def hmget(client, args):
    value = _hash(client.db, args[0])
    if value is None:
        return [None for _ in args[1:]]
    return [value.get(field) for field in args[1:]]


@command("hgetall", 1, 1)
async def hgetall(client, args):
    value = _hash(client.db, args[0])
    if value is None:
        return Map([]) if client.protocol == 3 else []
    if client.protocol == 3:
        return Map(value.items())
    result = []
    for field, item in value.items():
        result.extend((field, item))
    return result


@command("hdel", 2)
async def hdel(client, args):
    db = client.db
    key = args[0]
    value = _hash(db, key)
    if value is None:
        return 0
    removed = 0
    for field in args[1:]:
        if field in value:
            del value[field]
            removed += 1
    if removed:
        if value:
            db.touch(key)
        else:
            db.delete(key)
    return removed


@command("hlen", 1, 1)
async def hlen(client, args):
    value = _hash(client.db, args[0])
    return 0 if value is None else len(value)


@command("hexists", 2, 2)
async def hexists(client, args):
    value = _hash(client.db, args[0])
    return int(value is not None and args[1] in value)


@command("hkeys", 1, 1)
async def hkeys(client, args):
    value = _hash(client.db, args[0])
    return [] if value is None else list(value.keys())


@command("hvals", 1, 1)
async def hvals(client, args):
    value = _hash(client.db, args[0])
    return [] if value is None else list(value.values())


@command("hstrlen", 2, 2)
async def hstrlen(client, args):
    value = _hash(client.db, args[0])
    if value is None:
        return 0
    return len(value.get(args[1], b""))


@command("hincrby", 3, 3)
async def hincrby(client, args):
    db = client.db
    key, field = args[0], args[1]
    delta = parse_int(args[2], "ERR hash value is not an integer")
    value = _hash(db, key, create=True)
    current = value.get(field, b"0")
    base = parse_int(current, "ERR hash value is not an integer")
    result = base + delta
    if not -(2**63) <= result <= 2**63 - 1:
        raise RedisError("ERR increment or decrement would overflow")
    value[field] = str(result).encode()
    db.touch(key)
    return result


@command("hincrbyfloat", 3, 3)
async def hincrbyfloat(client, args):
    db = client.db
    key, field = args[0], args[1]
    try:
        delta = parse_float(args[2])
    except RedisError:
        raise RedisError("ERR hash value is not a float") from None
    value = _hash(db, key, create=True)
    current = value.get(field, b"0")
    try:
        base = parse_float(current)
    except RedisError:
        raise RedisError("ERR hash value is not a float") from None
    result = base + delta
    if result != result or result in (float("inf"), float("-inf")):
        raise RedisError("ERR increment would produce NaN or Infinity")
    encoded = format_float(result)
    value[field] = encoded
    db.touch(key)
    if client.protocol == 3:
        return Double(result)
    return encoded


@command("hrandfield", 1)
async def hrandfield(client, args):
    value = _hash(client.db, args[0])
    with_values = False
    if args[1:]:
        if args[-1].upper() == b"WITHVALUES":
            with_values = True
            args = args[:-1]
    if len(args) == 1:
        if not value:
            return None
        return random.choice(list(value.keys()))
    if len(args) != 2:
        raise syntax_error()
    count = parse_int(args[1])
    if not value:
        return []
    fields = list(value.keys())
    if count >= 0:
        chosen = random.sample(fields, min(count, len(fields)))
    else:
        chosen = [random.choice(fields) for _ in range(-count)]
    if with_values:
        if client.protocol == 3:
            return [[field, value[field]] for field in chosen]
        result = []
        for field in chosen:
            result.extend((field, value[field]))
        return result
    return chosen


@command("hscan", 2)
async def hscan(client, args):
    value = _hash(client.db, args[0])
    match, count, _ = scan_options(args[2:])
    fields = [] if value is None else sorted(value.keys())
    next_cursor, selected = paginate(fields, args[1], match, count)
    result = []
    for field in selected:
        result.extend((field, value[field]))
    return [str(next_cursor).encode(), result]

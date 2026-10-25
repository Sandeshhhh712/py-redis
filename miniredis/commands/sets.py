"""Set commands."""

import random

from ..errors import WRONGTYPE, RedisError
from ..utils import parse_int
from . import command
from .keys import paginate, scan_options


def _set(db, key, create=False):
    value = db.get(key)
    if value is None:
        if not create:
            return None
        value = set()
        db.set(key, value)
    elif not isinstance(value, set):
        raise RedisError(WRONGTYPE)
    return value


def _store_result(client, destination, members):
    db = client.db
    members = set(members)
    if members:
        db.set(destination, members)
    else:
        db.delete(destination)
    return len(members)


@command("sadd", 2)
async def sadd(client, args):
    db = client.db
    key = args[0]
    value = _set(db, key, create=True)
    added = 0
    for member in args[1:]:
        if member not in value:
            value.add(member)
            added += 1
    if added:
        db.touch(key)
    return added


@command("srem", 2)
async def srem(client, args):
    db = client.db
    key = args[0]
    value = _set(db, key)
    if value is None:
        return 0
    removed = 0
    for member in args[1:]:
        if member in value:
            value.remove(member)
            removed += 1
    if removed:
        if value:
            db.touch(key)
        else:
            db.delete(key)
    return removed


@command("sismember", 2, 2)
async def sismember(client, args):
    value = _set(client.db, args[0])
    return int(value is not None and args[1] in value)


@command("smismember", 2)
async def smismember(client, args):
    value = _set(client.db, args[0])
    if value is None:
        return [0 for _ in args[1:]]
    return [int(member in value) for member in args[1:]]


@command("scard", 1, 1)
async def scard(client, args):
    value = _set(client.db, args[0])
    return 0 if value is None else len(value)


@command("smembers", 1, 1)
async def smembers(client, args):
    value = _set(client.db, args[0])
    return [] if value is None else list(value)


@command("spop", 1, 2)
async def spop(client, args):
    db = client.db
    key = args[0]
    value = _set(db, key)
    if not value:
        return None if len(args) == 1 else []
    count = 1 if len(args) == 1 else parse_int(args[1])
    if count < 0:
        raise RedisError("ERR value is out of range, must be positive")
    count = min(count, len(value))
    chosen = random.sample(list(value), count)
    for member in chosen:
        value.remove(member)
    if value:
        db.touch(key)
    else:
        db.delete(key)
    return chosen[0] if len(args) == 1 else chosen


@command("srandmember", 1, 2)
async def srandmember(client, args):
    value = _set(client.db, args[0])
    if not value:
        return None if len(args) == 1 else []
    if len(args) == 1:
        return random.choice(list(value))
    count = parse_int(args[1])
    members = list(value)
    if count >= 0:
        return random.sample(members, min(count, len(members)))
    return [random.choice(members) for _ in range(-count)]


@command("smove", 3, 3)
async def smove(client, args):
    db = client.db
    source, destination, member = args
    src = _set(db, source)
    if src is None or member not in src:
        return 0
    dst = _set(db, destination, create=True)
    src.remove(member)
    dst.add(member)
    if src:
        db.touch(source)
    else:
        db.delete(source)
    db.touch(destination)
    return 1


def _members(db, keys):
    sets = []
    for key in keys:
        value = _set(db, key)
        if value is not None:
            sets.append(value)
    return sets


@command("sinter", 1)
async def sinter(client, args):
    sets = _members(client.db, args)
    if not sets:
        return []
    result = set(sets[0])
    for value in sets[1:]:
        result &= value
    return list(result)


@command("sunion", 1)
async def sunion(client, args):
    result = set()
    for value in _members(client.db, args):
        result |= value
    return list(result)


@command("sdiff", 1)
async def sdiff(client, args):
    if not args:
        return []
    first = _set(client.db, args[0])
    result = set() if first is None else set(first)
    for value in _members(client.db, args[1:]):
        result -= value
    return list(result)


@command("sinterstore", 2)
async def sinterstore(client, args):
    return _store_result(client, args[0], await sinter(client, args[1:]))


@command("sunionstore", 2)
async def sunionstore(client, args):
    return _store_result(client, args[0], await sunion(client, args[1:]))


@command("sdiffstore", 2)
async def sdiffstore(client, args):
    return _store_result(client, args[0], await sdiff(client, args[1:]))


@command("sintercard", 2)
async def sintercard(client, args):
    numkeys = parse_int(args[0])
    if numkeys <= 0:
        raise RedisError("ERR numkeys should be greater than 0")
    if len(args) < 1 + numkeys:
        raise RedisError("ERR Number of keys can't be greater than number of args")
    keys = args[1 : 1 + numkeys]
    limit = 0
    rest = args[1 + numkeys :]
    if rest:
        if rest[0].upper() != b"LIMIT" or len(rest) != 2:
            raise RedisError("ERR syntax error")
        limit = parse_int(rest[1])
        if limit < 0:
            raise RedisError("ERR LIMIT can't be negative")
    result = await sinter(client, keys)
    count = len(result)
    return min(count, limit) if limit else count


@command("sscan", 2)
async def sscan(client, args):
    value = _set(client.db, args[0])
    match, count, _ = scan_options(args[2:])
    members = [] if value is None else sorted(value)
    next_cursor, selected = paginate(members, args[1], match, count)
    return [str(next_cursor).encode(), selected]

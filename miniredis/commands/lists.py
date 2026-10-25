"""List commands, including blocking pops."""

import time

from ..errors import WRONGTYPE, RedisError
from ..utils import parse_float, parse_int, syntax_error
from . import command


def _list(db, key, create=False):
    value = db.get(key)
    if value is None:
        if not create:
            return None
        value = []
        db.set(key, value)
    elif not isinstance(value, list):
        raise RedisError(WRONGTYPE)
    return value


def _finish_pop(db, key, value):
    if value:
        db.touch(key)
    else:
        db.delete(key)


def _push(client, args, left, only_existing):
    key = args[0]
    db = client.db
    value = db.get(key)
    if value is None:
        if only_existing:
            return 0
        value = []
        db.set(key, value)
    elif not isinstance(value, list):
        raise RedisError(WRONGTYPE)
    if left:
        for item in args[1:]:
            value.insert(0, item)
    else:
        value.extend(args[1:])
    db.touch(key)
    return len(value)


@command("lpush", 2)
async def lpush(client, args):
    return _push(client, args, left=True, only_existing=False)


@command("rpush", 2)
async def rpush(client, args):
    return _push(client, args, left=False, only_existing=False)


@command("lpushx", 2)
async def lpushx(client, args):
    return _push(client, args, left=True, only_existing=True)


@command("rpushx", 2)
async def rpushx(client, args):
    return _push(client, args, left=False, only_existing=True)


@command("llen", 1, 1)
async def llen(client, args):
    value = _list(client.db, args[0])
    return 0 if value is None else len(value)


@command("lrange", 3, 3)
async def lrange(client, args):
    value = _list(client.db, args[0])
    if value is None:
        return []
    start = parse_int(args[1])
    stop = parse_int(args[2])
    length = len(value)
    if start < 0:
        start += length
    if stop < 0:
        stop += length
    start = max(0, start)
    stop = min(length - 1, stop)
    if start > stop or start >= length:
        return []
    return value[start : stop + 1]


@command("lindex", 2, 2)
async def lindex(client, args):
    value = _list(client.db, args[0])
    if value is None:
        return None
    index = parse_int(args[1])
    if index < 0:
        index += len(value)
    if not 0 <= index < len(value):
        return None
    return value[index]


@command("lset", 3, 3)
async def lset(client, args):
    db = client.db
    value = _list(db, args[0])
    if value is None:
        raise RedisError("ERR no such key")
    index = parse_int(args[1])
    if index < 0:
        index += len(value)
    if not 0 <= index < len(value):
        raise RedisError("ERR index out of range")
    value[index] = args[2]
    db.touch(args[0])
    return client.server.ok()


@command("lrem", 3, 3)
async def lrem(client, args):
    db = client.db
    key = args[0]
    value = _list(db, key)
    if value is None:
        return 0
    count = parse_int(args[1])
    target = args[2]
    removed = 0
    if count >= 0:
        limit = count or len(value)
        kept = []
        for item in value:
            if item == target and removed < limit:
                removed += 1
            else:
                kept.append(item)
        value[:] = kept
    else:
        limit = -count
        kept = []
        for item in reversed(value):
            if item == target and removed < limit:
                removed += 1
            else:
                kept.append(item)
        value[:] = list(reversed(kept))
    if removed:
        _finish_pop(db, key, value)
    return removed


@command("linsert", 4, 4)
async def linsert(client, args):
    db = client.db
    key = args[0]
    where = args[1].upper()
    if where not in (b"BEFORE", b"AFTER"):
        raise syntax_error()
    value = _list(db, key)
    if value is None:
        return 0
    pivot = args[2]
    try:
        position = value.index(pivot)
    except ValueError:
        return -1
    if where == b"AFTER":
        position += 1
    value.insert(position, args[3])
    db.touch(key)
    return len(value)


@command("ltrim", 3, 3)
async def ltrim(client, args):
    db = client.db
    key = args[0]
    value = _list(db, key)
    if value is None:
        return client.server.ok()
    start = parse_int(args[1])
    stop = parse_int(args[2])
    length = len(value)
    if start < 0:
        start += length
    if stop < 0:
        stop += length
    start = max(0, start)
    stop = min(length - 1, stop)
    if start > stop or start >= length:
        db.delete(key)
    else:
        value[:] = value[start : stop + 1]
        db.touch(key)
    return client.server.ok()


@command("lpop", 1, 2)
async def lpop(client, args):
    return _pop(client, args, left=True)


@command("rpop", 1, 2)
async def rpop(client, args):
    return _pop(client, args, left=False)


def _pop(client, args, left):
    db = client.db
    key = args[0]
    value = _list(db, key)
    count = 1 if len(args) == 1 else parse_int(args[1])
    if count < 0:
        raise RedisError("ERR value is out of range, must be positive")
    if len(args) == 1:
        if not value:
            return None
        item = value.pop(0 if left else -1)
        _finish_pop(db, key, value)
        return item
    if not value:
        return None
    popped = []
    for _ in range(min(count, len(value))):
        popped.append(value.pop(0 if left else -1))
    _finish_pop(db, key, value)
    return popped


@command("lpos", 2)
async def lpos(client, args):
    value = _list(client.db, args[0])
    if value is None:
        return None if len(args) == 2 else []
    target = args[1]
    rank = 1
    count = None
    maxlen = 0
    i = 2
    while i < len(args):
        option = args[i].upper()
        if option in (b"RANK", b"COUNT", b"MAXLEN"):
            if i + 1 >= len(args):
                raise syntax_error()
            amount = parse_int(args[i + 1])
            if option == b"RANK":
                if amount == 0:
                    raise RedisError("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second, ...")
                rank = amount
            elif option == b"COUNT":
                if amount < 0:
                    raise RedisError("ERR COUNT can't be negative")
                count = amount
            else:
                if amount < 0:
                    raise RedisError("ERR MAXLEN can't be negative")
                maxlen = amount
            i += 2
        else:
            raise syntax_error()

    scanned = 0
    matches = []
    if rank > 0:
        skip = rank - 1
        for index, item in enumerate(value):
            scanned += 1
            if maxlen and scanned > maxlen:
                break
            if item == target:
                if skip:
                    skip -= 1
                    continue
                matches.append(index)
                if count is None or (count and len(matches) >= count):
                    break
    else:
        skip = -rank - 1
        for index in range(len(value) - 1, -1, -1):
            scanned += 1
            if maxlen and scanned > maxlen:
                break
            if value[index] == target:
                if skip:
                    skip -= 1
                    continue
                matches.append(index)
                if count is None or (count and len(matches) >= count):
                    break

    if count is None:
        return matches[0] if matches else None
    return matches


@command("rpoplpush", 2, 2)
async def rpoplpush(client, args):
    return await _move(client, args[0], args[1], b"RIGHT", b"LEFT")


@command("lmove", 4, 4)
async def lmove(client, args):
    source, destination = args[0], args[1]
    where_from, where_to = args[2].upper(), args[3].upper()
    if where_from not in (b"LEFT", b"RIGHT") or where_to not in (b"LEFT", b"RIGHT"):
        raise syntax_error()
    return await _move(client, source, destination, where_from, where_to)


async def _move(client, source, destination, where_from, where_to):
    db = client.db
    src = _list(db, source)
    if src is None or not src:
        return None
    dst = _list(db, destination)
    if source == destination:
        item = src.pop(0 if where_from == b"LEFT" else -1)
        if where_to == b"LEFT":
            src.insert(0, item)
        else:
            src.append(item)
        db.touch(source)
        return item
    item = src.pop(0 if where_from == b"LEFT" else -1)
    _finish_pop(db, source, src)
    if dst is None:
        dst = []
        db.set(destination, dst)
    if where_to == b"LEFT":
        dst.insert(0, item)
    else:
        dst.append(item)
    db.touch(destination)
    return item


@command("blpop", 2)
async def blpop(client, args):
    return await _blocking_pop(client, args, left=True)


@command("brpop", 2)
async def brpop(client, args):
    return await _blocking_pop(client, args, left=False)


async def _blocking_pop(client, args, left):
    timeout = parse_float(
        args[-1], "ERR timeout is not a float or out of range"
    )
    if timeout < 0:
        raise RedisError("ERR timeout is negative")
    keys = args[:-1]
    db = client.db
    deadline = None if timeout == 0 else time.monotonic() + timeout

    while True:
        for key in keys:
            value = _list(db, key)
            if value is None:
                continue
            if value:
                item = value.pop(0 if left else -1)
                _finish_pop(db, key, value)
                return [key, item]
        if client.in_exec:
            return None
        if deadline is None:
            wait = None
        else:
            wait = deadline - time.monotonic()
            if wait <= 0:
                return None
        await client.server.notifier.wait(db.index, keys, wait)


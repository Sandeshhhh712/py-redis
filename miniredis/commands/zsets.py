"""Sorted-set commands."""

import random

from ..errors import WRONGTYPE, RedisError
from ..protocol import Double
from ..types import ZSet
from ..utils import (
    format_float,
    parse_int,
    parse_score,
    syntax_error,
    wrong_args,
)
from . import command
from .keys import paginate, scan_options


def _zset(db, key, create=False):
    value = db.get(key)
    if value is None:
        if not create:
            return None
        value = ZSet()
        db.set(key, value)
    elif not isinstance(value, ZSet):
        raise RedisError(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )
    return value


def _sorted_items(value, reverse=False):
    items = sorted(value.items(), key=lambda item: (item[1], item[0]))
    if reverse:
        items.reverse()
    return items


# -- score / lex range helpers ------------------------------------------------


def _score_bound(raw, message="ERR min or max is not a float"):
    exclusive = raw.startswith(b"(")
    number = raw[1:] if exclusive else raw
    return parse_score(number, message), exclusive


def _score_range(start, stop, reverse):
    lo, lo_exclusive = _score_bound(start)
    hi, hi_exclusive = _score_bound(stop)
    if reverse:
        lo, hi = hi, lo
        lo_exclusive, hi_exclusive = hi_exclusive, lo_exclusive
    return lo, lo_exclusive, hi, hi_exclusive


def _in_score_range(score, bounds):
    lo, lo_exclusive, hi, hi_exclusive = bounds
    if lo_exclusive:
        if not score > lo:
            return False
    elif not score >= lo:
        return False
    if hi_exclusive:
        if not score < hi:
            return False
    elif not score <= hi:
        return False
    return True


def _lex_ge(member, bound):
    if bound == b"-":
        return True
    if bound == b"+":
        return False
    if bound.startswith(b"["):
        return member >= bound[1:]
    if bound.startswith(b"("):
        return member > bound[1:]
    raise RedisError("ERR min or max not valid string range item")


def _lex_le(member, bound):
    if bound == b"+":
        return True
    if bound == b"-":
        return False
    if bound.startswith(b"["):
        return member <= bound[1:]
    if bound.startswith(b"("):
        return member < bound[1:]
    raise RedisError("ERR min or max not valid string range item")


def _in_lex_range(member, start, stop, reverse):
    lo, hi = (stop, start) if reverse else (start, stop)
    return _lex_ge(member, lo) and _lex_le(member, hi)


def _normalize_range(start, stop, length):
    if start < 0:
        start += length
    if stop < 0:
        stop += length
    start = max(0, start)
    stop = min(length - 1, stop)
    return start, stop


# -- core range implementation ------------------------------------------------


def _range(
    value,
    start,
    stop,
    *,
    byscore=False,
    bylex=False,
    reverse=False,
    withscores=False,
    limit=None,
    protocol=2,
):
    if byscore:
        bounds = _score_range(start, stop, reverse)
        items = [
            item for item in _sorted_items(value, reverse) if _in_score_range(item[1], bounds)
        ]
    elif bylex:
        items = [
            item
            for item in _sorted_items(value, reverse=False)
            if _in_lex_range(item[0], start, stop, reverse)
        ]
        if reverse:
            items.reverse()
    else:
        index_start = parse_int(start)
        index_stop = parse_int(stop)
        items = _sorted_items(value, reverse)
        first, last = _normalize_range(index_start, index_stop, len(items))
        items = [] if first > last else items[first : last + 1]

    if limit is not None:
        offset, count = limit
        items = items[offset:] if count < 0 else items[offset : offset + count]

    if withscores:
        if protocol == 3:
            return [[member, Double(score)] for member, score in items]
        result = []
        for member, score in items:
            result.extend((member, format_float(score)))
        return result
    return [member for member, _score in items]


def _parse_range_options(args, *, allow_limit=True):
    byscore = bylex = reverse = withscores = False
    limit = None
    i = 0
    while i < len(args):
        option = args[i].upper()
        if option == b"BYSCORE":
            byscore = True
            i += 1
        elif option == b"BYLEX":
            bylex = True
            i += 1
        elif option == b"REV":
            reverse = True
            i += 1
        elif option == b"WITHSCORES":
            withscores = True
            i += 1
        elif option == b"LIMIT" and allow_limit:
            if i + 2 >= len(args):
                raise syntax_error()
            offset = parse_int(args[i + 1])
            count = parse_int(args[i + 2])
            limit = (max(0, offset), count)
            i += 3
        else:
            raise syntax_error()
    if byscore and bylex:
        raise syntax_error()
    if limit is not None and not (byscore or bylex):
        raise syntax_error()
    return byscore, bylex, reverse, withscores, limit


def _maybe_store(db, key, value, changed, existed):
    if not changed:
        return
    if not existed:
        db.set(key, value)
    elif value:
        db.touch(key)
    else:
        db.delete(key)


# -- write commands -----------------------------------------------------------


@command("zadd", 3)
async def zadd(client, args):
    db = client.db
    key = args[0]
    nx = xx = gt = lt = ch = incr = False
    i = 1
    while i < len(args):
        option = args[i].upper()
        if option == b"NX":
            nx = True
        elif option == b"XX":
            xx = True
        elif option == b"GT":
            gt = True
        elif option == b"LT":
            lt = True
        elif option == b"CH":
            ch = True
        elif option == b"INCR":
            incr = True
        else:
            break
        i += 1

    if nx and (xx or gt or lt):
        raise RedisError(
            "ERR GT, LT, and/or NX options at the same time are not compatible"
        )
    if gt and lt:
        raise RedisError(
            "ERR GT, LT, and/or NX options at the same time are not compatible"
        )

    pairs = args[i:]
    if not pairs or len(pairs) % 2:
        raise wrong_args("zadd")
    if incr and len(pairs) != 2:
        raise RedisError("ERR INCR option supports a single increment-element pair")

    parsed = []
    for j in range(0, len(pairs), 2):
        parsed.append((parse_score(pairs[j]), pairs[j + 1]))

    value = db.get(key)
    if value is None:
        value = ZSet()
        existed = False
    elif not isinstance(value, ZSet):
        raise RedisError(WRONGTYPE)
    else:
        existed = True

    added = changed = 0
    increment_result = None
    for score, member in parsed:
        current = value.get(member)
        if incr:
            if nx and current is not None:
                increment_result = None
                break
            if xx and current is None:
                increment_result = None
                break
            if current is None:
                new_score = score
            else:
                if gt and not score > 0:
                    increment_result = None
                    break
                if lt and not score < 0:
                    increment_result = None
                    break
                new_score = current + score
            if gt and current is not None and not new_score > current:
                increment_result = None
                break
            if lt and current is not None and not new_score < current:
                increment_result = None
                break
            if current is None:
                added += 1
            else:
                changed += 1
            value[member] = new_score
            increment_result = format_float(new_score)
            break

        if current is None:
            if xx:
                continue
            value[member] = score
            added += 1
            changed += 1
        else:
            if nx:
                continue
            if gt and score <= current:
                continue
            if lt and score >= current:
                continue
            if score != current:
                value[member] = score
                changed += 1

    if incr:
        if increment_result is None:
            _maybe_store(db, key, value, changed and not existed, existed)
            return None
        if not existed:
            db.set(key, value)
        else:
            db.touch(key)
        if client.protocol == 3:
            return Double(increment_result)
        return increment_result

    _maybe_store(db, key, value, changed > 0, existed)
    return added + changed if ch else added


@command("zincrby", 3, 3)
async def zincrby(client, args):
    key, increment, member = args
    db = client.db
    score = parse_score(increment)
    value = db.get(key)
    if value is None:
        value = ZSet()
        existed = False
    elif not isinstance(value, ZSet):
        raise RedisError(WRONGTYPE)
    else:
        existed = True
    new_score = value.get(member, 0.0) + score
    value[member] = new_score
    if existed:
        db.touch(key)
    else:
        db.set(key, value)
    if client.protocol == 3:
        return Double(new_score)
    return format_float(new_score)


@command("zrem", 2)
async def zrem(client, args):
    db = client.db
    key = args[0]
    value = _zset(db, key)
    if value is None:
        return 0
    removed = 0
    for member in args[1:]:
        if member in value:
            del value[member]
            removed += 1
    if removed:
        if value:
            db.touch(key)
        else:
            db.delete(key)
    return removed


@command("zcard", 1, 1)
async def zcard(client, args):
    value = _zset(client.db, args[0])
    return 0 if value is None else len(value)


@command("zscore", 2, 2)
async def zscore(client, args):
    value = _zset(client.db, args[0])
    if value is None or args[1] not in value:
        return None
    score = value[args[1]]
    return Double(score) if client.protocol == 3 else format_float(score)


@command("zmscore", 2)
async def zmscore(client, args):
    value = _zset(client.db, args[0])
    if value is None:
        return [None for _ in args[1:]]
    resp3 = client.protocol == 3
    return [
        (Double(value[member]) if resp3 else format_float(value[member]))
        if member in value
        else None
        for member in args[1:]
    ]


@command("zcount", 3, 3)
async def zcount(client, args):
    value = _zset(client.db, args[0])
    if value is None:
        return 0
    bounds = _score_range(args[1], args[2], False)
    return sum(1 for score in value.values() if _in_score_range(score, bounds))


@command("zlexcount", 3, 3)
async def zlexcount(client, args):
    value = _zset(client.db, args[0])
    if value is None:
        return 0
    return sum(
        1
        for member in value
        if _in_lex_range(member, args[1], args[2], reverse=False)
    )


@command("zrange", 3)
async def zrange(client, args):
    value = _zset(client.db, args[0])
    if value is None:
        return []
    byscore, bylex, reverse, withscores, limit = _parse_range_options(args[3:])
    return _range(
        value,
        args[1],
        args[2],
        byscore=byscore,
        bylex=bylex,
        reverse=reverse,
        withscores=withscores,
        limit=limit,
        protocol=client.protocol,
    )


@command("zrevrange", 3, 4)
async def zrevrange(client, args):
    value = _zset(client.db, args[0])
    if value is None:
        return []
    withscores = len(args) == 4 and args[3].upper() == b"WITHSCORES"
    if len(args) == 4 and not withscores:
        raise syntax_error()
    return _range(
        value,
        args[1],
        args[2],
        reverse=True,
        withscores=withscores,
        protocol=client.protocol,
    )


@command("zrangebyscore", 3)
async def zrangebyscore(client, args):
    value = _zset(client.db, args[0])
    if value is None:
        return []
    withscores = False
    limit = None
    i = 3
    while i < len(args):
        option = args[i].upper()
        if option == b"WITHSCORES":
            withscores = True
            i += 1
        elif option == b"LIMIT":
            if i + 2 >= len(args):
                raise syntax_error()
            offset = parse_int(args[i + 1])
            count = parse_int(args[i + 2])
            limit = (max(0, offset), count)
            i += 3
        else:
            raise syntax_error()
    return _range(
        value,
        args[1],
        args[2],
        byscore=True,
        withscores=withscores,
        limit=limit,
        protocol=client.protocol,
    )


@command("zrevrangebyscore", 3)
async def zrevrangebyscore(client, args):
    value = _zset(client.db, args[0])
    if value is None:
        return []
    withscores = False
    limit = None
    i = 3
    while i < len(args):
        option = args[i].upper()
        if option == b"WITHSCORES":
            withscores = True
            i += 1
        elif option == b"LIMIT":
            if i + 2 >= len(args):
                raise syntax_error()
            offset = parse_int(args[i + 1])
            count = parse_int(args[i + 2])
            limit = (max(0, offset), count)
            i += 3
        else:
            raise syntax_error()
    return _range(
        value,
        args[1],
        args[2],
        byscore=True,
        reverse=True,
        withscores=withscores,
        limit=limit,
        protocol=client.protocol,
    )


@command("zrangebylex", 3)
async def zrangebylex(client, args):
    value = _zset(client.db, args[0])
    if value is None:
        return []
    limit = _parse_lex_limit(args[3:])
    return _range(value, args[1], args[2], bylex=True, limit=limit)


@command("zrevrangebylex", 3)
async def zrevrangebylex(client, args):
    value = _zset(client.db, args[0])
    if value is None:
        return []
    limit = _parse_lex_limit(args[3:])
    return _range(value, args[1], args[2], bylex=True, reverse=True, limit=limit)


def _parse_lex_limit(args):
    if not args:
        return None
    if len(args) != 3 or args[0].upper() != b"LIMIT":
        raise syntax_error()
    offset = parse_int(args[1])
    count = parse_int(args[2])
    return max(0, offset), count


@command("zrank", 2, 3)
async def zrank(client, args):
    return _rank(client, args, reverse=False)


@command("zrevrank", 2, 3)
async def zrevrank(client, args):
    return _rank(client, args, reverse=True)


def _rank(client, args, reverse):
    value = _zset(client.db, args[0])
    withscore = False
    if len(args) == 3:
        if args[2].upper() != b"WITHSCORE":
            raise syntax_error()
        withscore = True
    if value is None:
        return None
    member = args[1]
    for index, (candidate, score) in enumerate(_sorted_items(value, reverse)):
        if candidate == member:
            if withscore:
                if client.protocol == 3:
                    return [index, Double(score)]
                return [index, format_float(score)]
            return index
    return None


@command("zremrangebyrank", 3, 3)
async def zremrangebyrank(client, args):
    db = client.db
    key = args[0]
    value = _zset(db, key)
    if value is None:
        return 0
    items = _sorted_items(value)
    first, last = _normalize_range(
        parse_int(args[1]), parse_int(args[2]), len(items)
    )
    if first > last:
        return 0
    victims = [member for member, _ in items[first : last + 1]]
    for member in victims:
        del value[member]
    if value:
        db.touch(key)
    else:
        db.delete(key)
    return len(victims)


@command("zremrangebyscore", 3, 3)
async def zremrangebyscore(client, args):
    db = client.db
    key = args[0]
    value = _zset(db, key)
    if value is None:
        return 0
    bounds = _score_range(args[1], args[2], False)
    victims = [member for member, score in value.items() if _in_score_range(score, bounds)]
    for member in victims:
        del value[member]
    if victims:
        if value:
            db.touch(key)
        else:
            db.delete(key)
    return len(victims)


@command("zremrangebylex", 3, 3)
async def zremrangebylex(client, args):
    db = client.db
    key = args[0]
    value = _zset(db, key)
    if value is None:
        return 0
    victims = [
        member
        for member in value
        if _in_lex_range(member, args[1], args[2], reverse=False)
    ]
    for member in victims:
        del value[member]
    if victims:
        if value:
            db.touch(key)
        else:
            db.delete(key)
    return len(victims)


@command("zpopmin", 1, 2)
async def zpopmin(client, args):
    return _zpop(client, args, reverse=False)


@command("zpopmax", 1, 2)
async def zpopmax(client, args):
    return _zpop(client, args, reverse=True)


def _zpop(client, args, reverse):
    db = client.db
    key = args[0]
    value = _zset(db, key)
    if value is None:
        return []
    count = 1 if len(args) == 1 else parse_int(args[1])
    if count < 0:
        raise RedisError("ERR value is out of range, must be positive")
    items = _sorted_items(value, reverse)[:count]
    result = []
    resp3 = client.protocol == 3
    for member, score in items:
        del value[member]
        if resp3:
            # RESP3 uses a flat [member, score] reply when no COUNT was
            # given and nested pairs when COUNT was given.
            if len(args) == 1:
                result = [member, Double(score)]
            else:
                result.append([member, Double(score)])
        else:
            result.extend((member, format_float(score)))
    if items:
        if value:
            db.touch(key)
        else:
            db.delete(key)
    return result


@command("zrandmember", 1)
async def zrandmember(client, args):
    value = _zset(client.db, args[0])
    withscores = False
    if args[1:] and args[-1].upper() == b"WITHSCORES":
        withscores = True
        args = args[:-1]
    if not value:
        return None if len(args) == 1 else []
    if len(args) == 1:
        return random.choice(list(value.keys()))
    if len(args) != 2:
        raise syntax_error()
    count = parse_int(args[1])
    members = list(value.keys())
    if count >= 0:
        chosen = random.sample(members, min(count, len(members)))
    else:
        chosen = [random.choice(members) for _ in range(-count)]
    if withscores:
        if client.protocol == 3:
            return [[member, Double(value[member])] for member in chosen]
        result = []
        for member in chosen:
            result.extend((member, format_float(value[member])))
        return result
    return chosen


@command("zscan", 2)
async def zscan(client, args):
    value = _zset(client.db, args[0])
    match, count, _ = scan_options(args[2:])
    members = [] if value is None else sorted(value.keys())
    next_cursor, selected = paginate(members, args[1], match, count)
    result = []
    for member in selected:
        if client.protocol == 3:
            result.extend((member, Double(value[member])))
        else:
            result.extend((member, format_float(value[member])))
    return [str(next_cursor).encode(), result]

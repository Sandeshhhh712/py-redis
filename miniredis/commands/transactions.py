"""MULTI/EXEC/DISCARD and WATCH/UNWATCH transactions."""

from ..errors import RedisError
from ..protocol import SimpleString
from . import command


@command("multi", 0, 0)
async def multi(client, args):
    if client.in_multi:
        raise RedisError("ERR MULTI calls can not be nested")
    client.in_multi = True
    client.queue = []
    client.multi_error = False
    return SimpleString("OK")


@command("discard", 0, 0)
async def discard(client, args):
    if not client.in_multi:
        raise RedisError("ERR DISCARD without MULTI")
    client.reset_multi()
    return SimpleString("OK")


@command("exec", 0, 0)
async def exec_(client, args):
    if not client.in_multi:
        raise RedisError("ERR EXEC without MULTI")
    queued = client.queue
    aborted = client.multi_error
    watched = client.watched
    client.in_multi = False
    client.queue = []
    client.multi_error = False
    client.watched = {}

    if aborted:
        raise RedisError(
            "EXECABORT Transaction discarded because of previous errors."
        )

    for (db_index, key), (epoch, version) in watched.items():
        db = client.server.databases[db_index]
        current_epoch, current_version = db.watch_version(key)
        if current_epoch != epoch or current_version != version:
            return None

    results = []
    client.in_exec = True
    try:
        for queued_command in queued:
            results.append(await client.server.execute(client, queued_command))
    finally:
        client.in_exec = False
    return results


@command("watch", 1)
async def watch(client, args):
    if client.in_multi:
        raise RedisError("ERR WATCH inside MULTI is not allowed")
    db = client.db
    for key in args:
        client.watched[(client.db_index, key)] = db.watch_version(key)
    return SimpleString("OK")


@command("unwatch", 0, 0)
async def unwatch(client, args):
    client.watched = {}
    return SimpleString("OK")

"""Pub/Sub commands."""

import asyncio

from ..errors import RedisError
from ..protocol import NO_REPLY, Push
from ..utils import glob_match, syntax_error
from . import command


def _subscription_count(client):
    return len(client.subscriptions) + len(client.patterns)


def _frame(client, parts):
    """Pub/Sub frames are RESP3 pushes, plain arrays on RESP2."""
    return Push(parts) if client.protocol == 3 else parts


@command("subscribe", 1)
async def subscribe(client, args):
    for channel in args:
        client.subscriptions.add(channel)
        await client.send(_frame(client, [b"subscribe", channel, _subscription_count(client)]))
    return NO_REPLY


@command("unsubscribe", 0)
async def unsubscribe(client, args):
    channels = list(args) if args else list(client.subscriptions)
    if not channels:
        await client.send(_frame(client, [b"unsubscribe", None, _subscription_count(client)]))
        return NO_REPLY
    for channel in channels:
        client.subscriptions.discard(channel)
        await client.send(_frame(client, [b"unsubscribe", channel, _subscription_count(client)]))
    return NO_REPLY


@command("psubscribe", 1)
async def psubscribe(client, args):
    for pattern in args:
        client.patterns.add(pattern)
        await client.send(_frame(client, [b"psubscribe", pattern, _subscription_count(client)]))
    return NO_REPLY


@command("punsubscribe", 0)
async def punsubscribe(client, args):
    patterns = list(args) if args else list(client.patterns)
    if not patterns:
        await client.send(_frame(client, [b"punsubscribe", None, _subscription_count(client)]))
        return NO_REPLY
    for pattern in patterns:
        client.patterns.discard(pattern)
        await client.send(_frame(client, [b"punsubscribe", pattern, _subscription_count(client)]))
    return NO_REPLY


@command("publish", 2, 2)
async def publish(client, args):
    channel, message = args
    return await client.server.publish(channel, message)


@command("pubsub", 1)
async def pubsub(client, args):
    server = client.server
    subcommand = args[0].upper()
    if subcommand == b"CHANNELS":
        if len(args) > 2:
            raise syntax_error()
        pattern = args[1] if len(args) == 2 else b"*"
        channels = set()
        for connected in server.clients.values():
            for channel in connected.subscriptions:
                if glob_match(channel, pattern):
                    channels.add(channel)
        return list(channels)
    if subcommand == b"NUMSUB":
        result = []
        for channel in args[1:]:
            count = sum(
                1
                for connected in server.clients.values()
                if channel in connected.subscriptions
            )
            result.extend((channel, count))
        return result
    if subcommand == b"NUMPAT":
        return sum(len(connected.patterns) for connected in server.clients.values())
    if subcommand in (b"SHARDCHANNELS", b"SHARDNUMSUB"):
        return []
    raise RedisError(
        f"ERR Unknown PUBSUB subcommand or wrong number of arguments for '{args[0].decode('utf-8', 'replace')}'"
    )


async def publish_message(server, channel, message):
    """Deliver a message to every matching subscriber; return receiver count."""
    deliveries = []
    for connected in list(server.clients.values()):
        if channel in connected.subscriptions:
            deliveries.append(
                connected.send(
                    _frame(connected, [b"message", channel, message])
                )
            )
        for pattern in connected.patterns:
            if glob_match(channel, pattern):
                deliveries.append(
                    connected.send(
                        _frame(connected, [b"pmessage", pattern, channel, message])
                    )
                )
    if deliveries:
        await asyncio.gather(*deliveries, return_exceptions=True)
    return len(deliveries)

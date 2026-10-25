"""Integration tests for miniredis.

Run with:
    python -m unittest discover -s tests -v
"""

import asyncio
import os
import tempfile
import unittest

from miniredis.protocol import (
    Double,
    Map,
    Push,
    RespError,
    SimpleString,
    encode_command,
    read_reply,
)
from miniredis.server import Server


class Connection:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    async def cmd(self, *args):
        self.writer.write(encode_command(args))
        await self.writer.drain()
        return await read_reply(self.reader)

    async def read(self):
        return await read_reply(self.reader)

    async def close(self):
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except ConnectionError:
            pass


class Assertions:
    def assertOK(self, reply):
        self.assertEqual(reply, SimpleString("OK"))

    def assertError(self, reply, prefix):
        self.assertIsInstance(reply, RespError)
        self.assertTrue(
            reply.message.startswith(prefix),
            f"{reply.message!r} does not start with {prefix!r}",
        )


class ServerTestCase(Assertions, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = Server(port=0, persistence=False)
        await self.server.start()
        self.connections = []

    async def asyncTearDown(self):
        for connection in self.connections:
            await connection.close()
        await self.server.stop()
        await asyncio.sleep(0)

    async def connect(self, host="127.0.0.1", port=None):
        reader, writer = await asyncio.open_connection(
            host, port or self.server.port
        )
        connection = Connection(reader, writer)
        self.connections.append(connection)
        return connection

class TestStrings(ServerTestCase):
    async def test_ping_echo_and_set_get(self):
        connection = await self.connect()
        self.assertEqual(await connection.cmd("PING"), SimpleString("PONG"))
        self.assertEqual(await connection.cmd("PING", "hi"), b"hi")
        self.assertEqual(await connection.cmd("ECHO", "hello"), b"hello")

        self.assertOK(await connection.cmd("SET", "name", "sandesh"))
        self.assertEqual(await connection.cmd("GET", "name"), b"sandesh")
        self.assertIsNone(await connection.cmd("GET", "missing"))
        self.assertEqual(await connection.cmd("EXISTS", "name"), 1)
        self.assertEqual(await connection.cmd("STRLEN", "name"), 7)
        self.assertEqual(await connection.cmd("APPEND", "name", "!"), 8)
        self.assertEqual(await connection.cmd("GET", "name"), b"sandesh!")

        self.assertOK(await connection.cmd("MSET", "a", "1", "b", "2"))
        self.assertEqual(
            await connection.cmd("MGET", "a", "b", "missing"),
            [b"1", b"2", None],
        )
        self.assertEqual(await connection.cmd("DEL", "a", "b", "nope"), 2)

    async def test_set_options(self):
        connection = await self.connect()
        self.assertOK(await connection.cmd("SET", "key", "one"))
        self.assertIsNone(await connection.cmd("SET", "key", "two", "NX"))
        self.assertEqual(await connection.cmd("GET", "key"), b"one")
        self.assertOK(await connection.cmd("SET", "key", "two", "XX"))
        self.assertEqual(await connection.cmd("GET", "key"), b"two")

        self.assertIsNone(await connection.cmd("SET", "new", "x", "XX"))
        self.assertEqual(await connection.cmd("SET", "new", "x", "NX"), SimpleString("OK"))
        self.assertEqual(
            await connection.cmd("SET", "new", "y", "NX", "GET"), b"x"
        )
        self.assertEqual(await connection.cmd("GET", "new"), b"x")

        self.assertOK(await connection.cmd("SET", "ttl", "v", "PX", "5000"))
        ttl = await connection.cmd("PTTL", "ttl")
        self.assertGreater(ttl, 1000)
        self.assertOK(await connection.cmd("SET", "ttl", "v2", "KEEPTTL"))
        self.assertGreater(await connection.cmd("PTTL", "ttl"), 1000)
        self.assertError(
            await connection.cmd("SET", "ttl", "v3", "KEEPTTL", "EX", "10"),
            "ERR syntax error",
        )

    async def test_expiration(self):
        connection = await self.connect()
        self.assertOK(await connection.cmd("SET", "temp", "v", "PX", "60"))
        self.assertEqual(await connection.cmd("GET", "temp"), b"v")
        self.assertGreater(await connection.cmd("PTTL", "temp"), 0)
        await asyncio.sleep(0.12)
        self.assertIsNone(await connection.cmd("GET", "temp"))
        self.assertEqual(await connection.cmd("PTTL", "temp"), -2)

        self.assertOK(await connection.cmd("SET", "key", "v"))
        self.assertEqual(await connection.cmd("TTL", "key"), -1)
        self.assertEqual(await connection.cmd("EXPIRE", "key", "10"), 1)
        self.assertEqual(await connection.cmd("TTL", "key"), 10)
        self.assertEqual(await connection.cmd("PERSIST", "key"), 1)
        self.assertEqual(await connection.cmd("TTL", "key"), -1)
        self.assertEqual(await connection.cmd("EXPIRE", "missing", "10"), 0)

        self.assertOK(await connection.cmd("SET", "past", "v"))
        self.assertEqual(await connection.cmd("EXPIRE", "past", "-1"), 1)
        self.assertIsNone(await connection.cmd("GET", "past"))

    async def test_counters_and_ranges(self):
        connection = await self.connect()
        self.assertEqual(await connection.cmd("INCR", "counter"), 1)
        self.assertEqual(await connection.cmd("INCRBY", "counter", "10"), 11)
        self.assertEqual(await connection.cmd("DECR", "counter"), 10)
        self.assertEqual(await connection.cmd("DECRBY", "counter", "4"), 6)

        self.assertOK(await connection.cmd("SET", "bad", "abc"))
        self.assertError(await connection.cmd("INCR", "bad"), "ERR value is not an integer")

        self.assertEqual(await connection.cmd("INCRBYFLOAT", "float", "10.5"), b"10.5")
        self.assertEqual(await connection.cmd("INCRBYFLOAT", "float", "0.1"), b"10.6")

        self.assertOK(await connection.cmd("SET", "text", "Hello World"))
        self.assertEqual(await connection.cmd("GETRANGE", "text", "0", "4"), b"Hello")
        self.assertEqual(await connection.cmd("GETRANGE", "text", "-5", "-1"), b"World")
        self.assertEqual(await connection.cmd("SETRANGE", "text", "6", "Redis"), 11)
        self.assertEqual(await connection.cmd("GET", "text"), b"Hello Redis")

    async def test_wrong_type(self):
        connection = await self.connect()
        await connection.cmd("RPUSH", "list", "a")
        self.assertError(await connection.cmd("GET", "list"), "WRONGTYPE")
        self.assertError(await connection.cmd("SET", "list", "x", "GET"), "WRONGTYPE")
        self.assertError(await connection.cmd("INCR", "list"), "WRONGTYPE")
        self.assertEqual(await connection.cmd("TYPE", "list"), SimpleString("list"))


class TestKeys(ServerTestCase):
    async def test_keys_scan_and_rename(self):
        connection = await self.connect()
        for index in range(30):
            await connection.cmd("SET", f"user:{index}", str(index))
        await connection.cmd("SET", "other", "x")

        self.assertEqual(await connection.cmd("DBSIZE"), 31)
        keys = await connection.cmd("KEYS", "user:*")
        self.assertEqual(len(keys), 30)

        cursor = b"0"
        found = set()
        while True:
            cursor, batch = await connection.cmd("SCAN", cursor, "MATCH", "user:*", "COUNT", "7")
            found.update(batch)
            if cursor == b"0":
                break
        self.assertEqual(found, {f"user:{i}".encode() for i in range(30)})

        cursor, batch = await connection.cmd("SCAN", "0", "TYPE", "string", "COUNT", "100")
        self.assertEqual(len(batch), 31)

        self.assertOK(await connection.cmd("RENAME", "other", "renamed"))
        self.assertEqual(await connection.cmd("GET", "renamed"), b"x")
        self.assertError(await connection.cmd("RENAME", "nope", "x"), "ERR no such key")

        self.assertEqual(await connection.cmd("COPY", "renamed", "copy"), 1)
        self.assertEqual(await connection.cmd("GET", "copy"), b"x")
        self.assertEqual(await connection.cmd("COPY", "renamed", "copy"), 0)
        self.assertEqual(await connection.cmd("COPY", "renamed", "copy", "REPLACE"), 1)

    async def test_select_and_flush(self):
        connection = await self.connect()
        self.assertOK(await connection.cmd("SELECT", "1"))
        self.assertOK(await connection.cmd("SET", "key", "db1"))
        self.assertEqual(await connection.cmd("DBSIZE"), 1)
        self.assertOK(await connection.cmd("SELECT", "0"))
        self.assertIsNone(await connection.cmd("GET", "key"))
        self.assertEqual(await connection.cmd("DBSIZE"), 0)
        self.assertError(await connection.cmd("SELECT", "99"), "ERR DB index is out of range")

        await connection.cmd("SET", "key", "v")
        self.assertOK(await connection.cmd("FLUSHDB"))
        self.assertEqual(await connection.cmd("DBSIZE"), 0)

    async def test_randomkey(self):
        connection = await self.connect()
        self.assertIsNone(await connection.cmd("RANDOMKEY"))
        await connection.cmd("SET", "only", "1")
        self.assertEqual(await connection.cmd("RANDOMKEY"), b"only")


class TestLists(ServerTestCase):
    async def test_push_pop_and_ranges(self):
        connection = await self.connect()
        self.assertEqual(await connection.cmd("RPUSH", "list", "a", "b", "c"), 3)
        self.assertEqual(await connection.cmd("LPUSH", "list", "z"), 4)
        self.assertEqual(await connection.cmd("LRANGE", "list", "0", "-1"), [b"z", b"a", b"b", b"c"])
        self.assertEqual(await connection.cmd("LINDEX", "list", "1"), b"a")
        self.assertEqual(await connection.cmd("LINDEX", "list", "-1"), b"c")

        self.assertOK(await connection.cmd("LSET", "list", "0", "y"))
        self.assertEqual(await connection.cmd("LPOP", "list"), b"y")
        self.assertEqual(await connection.cmd("RPOP", "list"), b"c")
        self.assertEqual(await connection.cmd("LLEN", "list"), 2)
        self.assertEqual(await connection.cmd("LPOP", "list", "2"), [b"a", b"b"])
        self.assertIsNone(await connection.cmd("LPOP", "list"))
        self.assertEqual(await connection.cmd("EXISTS", "list"), 0)

    async def test_insert_remove_trim_move(self):
        connection = await self.connect()
        await connection.cmd("RPUSH", "list", "a", "b", "c", "b", "d")
        self.assertEqual(await connection.cmd("LREM", "list", "1", "b"), 1)
        self.assertEqual(await connection.cmd("LRANGE", "list", "0", "-1"), [b"a", b"c", b"b", b"d"])
        self.assertEqual(await connection.cmd("LINSERT", "list", "BEFORE", "c", "x"), 5)
        self.assertEqual(await connection.cmd("LRANGE", "list", "0", "-1"), [b"a", b"x", b"c", b"b", b"d"])
        self.assertEqual(await connection.cmd("LINSERT", "list", "AFTER", "nope", "z"), -1)

        self.assertOK(await connection.cmd("LTRIM", "list", "1", "3"))
        self.assertEqual(await connection.cmd("LRANGE", "list", "0", "-1"), [b"x", b"c", b"b"])

        self.assertEqual(await connection.cmd("LMOVE", "list", "dest", "RIGHT", "LEFT"), b"b")
        self.assertEqual(await connection.cmd("LRANGE", "dest", "0", "-1"), [b"b"])
        self.assertEqual(await connection.cmd("RPOPLPUSH", "list", "dest"), b"c")
        self.assertEqual(await connection.cmd("LRANGE", "dest", "0", "-1"), [b"c", b"b"])

    async def test_lpos(self):
        connection = await self.connect()
        await connection.cmd("RPUSH", "list", "a", "b", "c", "1", "2", "3", "c", "c")
        self.assertEqual(await connection.cmd("LPOS", "list", "c"), 2)
        self.assertEqual(await connection.cmd("LPOS", "list", "c", "RANK", "-1"), 7)
        self.assertEqual(await connection.cmd("LPOS", "list", "c", "COUNT", "0"), [2, 6, 7])
        self.assertIsNone(await connection.cmd("LPOS", "list", "missing"))

    async def test_blocking_pop_timeout(self):
        connection = await self.connect()
        start = asyncio.get_running_loop().time()
        self.assertIsNone(await connection.cmd("BLPOP", "queue", "0.1"))
        elapsed = asyncio.get_running_loop().time() - start
        self.assertGreaterEqual(elapsed, 0.09)

    async def test_blocking_pop_wakeup(self):
        first = await self.connect()
        second = await self.connect()
        task = asyncio.create_task(first.cmd("BLPOP", "queue", "2"))
        await asyncio.sleep(0.05)
        await second.cmd("RPUSH", "queue", "job")
        self.assertEqual(await task, [b"queue", b"job"])
        self.assertIsNone(await first.cmd("BLPOP", "queue", "0.05"))


class TestHashes(ServerTestCase):
    async def test_hash_commands(self):
        connection = await self.connect()
        self.assertEqual(await connection.cmd("HSET", "h", "a", "1", "b", "2"), 2)
        self.assertEqual(await connection.cmd("HSET", "h", "a", "3"), 0)
        self.assertEqual(await connection.cmd("HGET", "h", "a"), b"3")
        self.assertIsNone(await connection.cmd("HGET", "h", "missing"))
        self.assertEqual(await connection.cmd("HMGET", "h", "a", "b", "x"), [b"3", b"2", None])
        self.assertEqual(await connection.cmd("HLEN", "h"), 2)
        self.assertEqual(await connection.cmd("HEXISTS", "h", "a"), 1)
        self.assertEqual(await connection.cmd("HKEYS", "h"), [b"a", b"b"])
        self.assertEqual(await connection.cmd("HVALS", "h"), [b"3", b"2"])
        self.assertEqual(await connection.cmd("HGETALL", "h"), [b"a", b"3", b"b", b"2"])
        self.assertEqual(await connection.cmd("HINCRBY", "h", "a", "2"), 5)
        self.assertEqual(await connection.cmd("HINCRBYFLOAT", "h", "f", "1.5"), b"1.5")
        self.assertEqual(await connection.cmd("HDEL", "h", "a", "nope"), 1)
        self.assertEqual(await connection.cmd("HLEN", "h"), 2)

        self.assertEqual(await connection.cmd("HSETNX", "h", "b", "x"), 0)
        self.assertEqual(await connection.cmd("HSETNX", "h", "new", "x"), 1)
        await connection.cmd("HSET", "h", "words", "abc")
        self.assertError(await connection.cmd("HINCRBY", "h", "words", "1"), "ERR hash value is not an integer")
        self.assertError(await connection.cmd("GET", "h"), "WRONGTYPE")

    async def test_hscan_and_hrandfield(self):
        connection = await self.connect()
        for index in range(20):
            await connection.cmd("HSET", "h", f"f{index}", str(index))
        cursor, batch = await connection.cmd("HSCAN", "h", "0", "COUNT", "100")
        self.assertEqual(cursor, b"0")
        self.assertEqual(len(batch), 40)
        field = await connection.cmd("HRANDFIELD", "h")
        self.assertIn(field, {f"f{i}".encode() for i in range(20)})
        fields = await connection.cmd("HRANDFIELD", "h", "5")
        self.assertEqual(len(fields), 5)


class TestSets(ServerTestCase):
    async def test_set_commands(self):
        connection = await self.connect()
        self.assertEqual(await connection.cmd("SADD", "s", "a", "b", "c", "a"), 3)
        self.assertEqual(await connection.cmd("SCARD", "s"), 3)
        self.assertEqual(await connection.cmd("SISMEMBER", "s", "a"), 1)
        self.assertEqual(await connection.cmd("SISMEMBER", "s", "x"), 0)
        self.assertEqual(await connection.cmd("SMISMEMBER", "s", "a", "x"), [1, 0])
        self.assertEqual(set(await connection.cmd("SMEMBERS", "s")), {b"a", b"b", b"c"})
        self.assertEqual(await connection.cmd("SREM", "s", "a", "x"), 1)

        await connection.cmd("SADD", "s2", "b", "c", "d")
        self.assertEqual(set(await connection.cmd("SINTER", "s", "s2")), {b"b", b"c"})
        self.assertEqual(
            set(await connection.cmd("SUNION", "s", "s2")), {b"b", b"c", b"d"}
        )
        self.assertEqual(set(await connection.cmd("SDIFF", "s", "s2")), set())
        self.assertEqual(await connection.cmd("SINTERSTORE", "dest", "s", "s2"), 2)
        self.assertEqual(set(await connection.cmd("SMEMBERS", "dest")), {b"b", b"c"})

        self.assertEqual(await connection.cmd("SMOVE", "s2", "s3", "d"), 1)
        self.assertEqual(await connection.cmd("SISMEMBER", "s3", "d"), 1)
        self.assertIn(await connection.cmd("SPOP", "s3"), {b"d"})
        self.assertEqual(await connection.cmd("SCARD", "s3"), 0)
        self.assertEqual(await connection.cmd("SADD", "q", "a", "b", "c"), 3)
        self.assertEqual(len(await connection.cmd("SRANDMEMBER", "q", "2")), 2)
        self.assertEqual(len(await connection.cmd("SRANDMEMBER", "q", "-5")), 5)

    async def test_sscan(self):
        connection = await self.connect()
        for index in range(15):
            await connection.cmd("SADD", "s", f"m{index}")
        cursor, batch = await connection.cmd("SSCAN", "s", "0", "COUNT", "100")
        self.assertEqual(cursor, b"0")
        self.assertEqual(len(batch), 15)
        cursor, batch = await connection.cmd("SSCAN", "s", "0", "MATCH", "m1*", "COUNT", "100")
        self.assertEqual(set(batch), {b"m1", b"m10", b"m11", b"m12", b"m13", b"m14"})


class TestSortedSets(ServerTestCase):
    async def test_zadd_and_ranges(self):
        connection = await self.connect()
        self.assertEqual(
            await connection.cmd("ZADD", "z", "1", "a", "2", "b", "3", "c"), 3
        )
        self.assertEqual(await connection.cmd("ZCARD", "z"), 3)
        self.assertEqual(await connection.cmd("ZSCORE", "z", "b"), b"2")
        self.assertEqual(
            await connection.cmd("ZRANGE", "z", "0", "-1", "WITHSCORES"),
            [b"a", b"1", b"b", b"2", b"c", b"3"],
        )
        self.assertEqual(
            await connection.cmd("ZREVRANGE", "z", "0", "-1"), [b"c", b"b", b"a"]
        )
        self.assertEqual(await connection.cmd("ZRANK", "z", "b"), 1)
        self.assertEqual(await connection.cmd("ZREVRANK", "z", "b"), 1)
        self.assertEqual(await connection.cmd("ZRANK", "z", "b", "WITHSCORE"), [1, b"2"])

        self.assertEqual(
            await connection.cmd("ZRANGEBYSCORE", "z", "2", "3"), [b"b", b"c"]
        )
        self.assertEqual(
            await connection.cmd("ZRANGEBYSCORE", "z", "(1", "3"), [b"b", b"c"]
        )
        self.assertEqual(
            await connection.cmd("ZREVRANGEBYSCORE", "z", "3", "2"), [b"c", b"b"]
        )
        self.assertEqual(await connection.cmd("ZCOUNT", "z", "1", "2"), 2)
        self.assertEqual(
            await connection.cmd("ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "1", "1"),
            [b"b"],
        )

    async def test_zadd_options_and_incr(self):
        connection = await self.connect()
        self.assertEqual(await connection.cmd("ZADD", "z", "NX", "1", "a"), 1)
        self.assertEqual(await connection.cmd("ZADD", "z", "NX", "5", "a"), 0)
        self.assertEqual(await connection.cmd("ZSCORE", "z", "a"), b"1")
        self.assertEqual(await connection.cmd("ZADD", "z", "XX", "5", "a"), 0)
        self.assertEqual(await connection.cmd("ZADD", "z", "XX", "CH", "5", "a"), 0)
        self.assertEqual(await connection.cmd("ZADD", "z", "XX", "CH", "6", "a"), 1)
        self.assertEqual(await connection.cmd("ZADD", "z", "GT", "3", "a"), 0)
        self.assertEqual(await connection.cmd("ZADD", "z", "GT", "7", "a"), 0)
        self.assertEqual(await connection.cmd("ZSCORE", "z", "a"), b"7")
        self.assertEqual(await connection.cmd("ZINCRBY", "z", "3", "a"), b"10")
        self.assertEqual(await connection.cmd("ZADD", "z", "INCR", "2", "a"), b"12")

    async def test_zpop_remrange_and_lex(self):
        connection = await self.connect()
        await connection.cmd("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d")
        self.assertEqual(await connection.cmd("ZPOPMIN", "z"), [b"a", b"1"])
        self.assertEqual(await connection.cmd("ZPOPMAX", "z", "2"), [b"d", b"4", b"c", b"3"])
        self.assertEqual(await connection.cmd("ZREMRANGEBYSCORE", "z", "1", "2"), 1)
        self.assertEqual(await connection.cmd("ZCARD", "z"), 0)

        await connection.cmd("ZADD", "lex", "0", "a", "0", "b", "0", "c", "0", "d")
        self.assertEqual(
            await connection.cmd("ZRANGEBYLEX", "lex", "[b", "[c"), [b"b", b"c"]
        )
        self.assertEqual(
            await connection.cmd("ZRANGEBYLEX", "lex", "-", "+", "LIMIT", "1", "2"),
            [b"b", b"c"],
        )
        self.assertEqual(await connection.cmd("ZLEXCOUNT", "lex", "-", "+"), 4)
        self.assertEqual(await connection.cmd("ZREMRANGEBYLEX", "lex", "[a", "[b"), 2)

    async def test_zscan_and_randmember(self):
        connection = await self.connect()
        for index in range(10):
            await connection.cmd("ZADD", "z", str(index), f"m{index}")
        cursor, batch = await connection.cmd("ZSCAN", "z", "0", "COUNT", "100")
        self.assertEqual(cursor, b"0")
        self.assertEqual(len(batch), 20)
        self.assertIn(
            await connection.cmd("ZRANDMEMBER", "z"), {f"m{i}".encode() for i in range(10)}
        )
        self.assertEqual(len(await connection.cmd("ZRANDMEMBER", "z", "3")), 3)
        self.assertEqual(len(await connection.cmd("ZRANDMEMBER", "z", "-6")), 6)


class TestTransactions(ServerTestCase):
    async def test_multi_exec(self):
        connection = await self.connect()
        self.assertOK(await connection.cmd("MULTI"))
        self.assertEqual(await connection.cmd("SET", "a", "1"), SimpleString("QUEUED"))
        self.assertEqual(await connection.cmd("INCR", "a"), SimpleString("QUEUED"))
        result = await connection.cmd("EXEC")
        self.assertEqual(result[0], SimpleString("OK"))
        self.assertEqual(result[1], 2)
        self.assertEqual(await connection.cmd("GET", "a"), b"2")

        self.assertOK(await connection.cmd("MULTI"))
        await connection.cmd("GET", "a")
        self.assertOK(await connection.cmd("DISCARD"))
        self.assertError(await connection.cmd("EXEC"), "ERR EXEC without MULTI")

    async def test_multi_errors(self):
        connection = await self.connect()
        await connection.cmd("MULTI")
        self.assertError(await connection.cmd("NOTACOMMAND"), "ERR unknown command")
        self.assertError(await connection.cmd("EXEC"), "EXECABORT")

        await connection.cmd("MULTI")
        self.assertError(await connection.cmd("MULTI"), "ERR MULTI calls can not be nested")
        self.assertOK(await connection.cmd("DISCARD"))

    async def test_pubsub_not_allowed_in_transactions(self):
        connection = await self.connect()
        self.assertOK(await connection.cmd("MULTI"))
        self.assertError(
            await connection.cmd("SUBSCRIBE", "news"),
            "ERR SUBSCRIBE is not allowed in transactions",
        )
        self.assertError(await connection.cmd("EXEC"), "EXECABORT")

    async def test_watch_aborts_on_change(self):
        first = await self.connect()
        second = await self.connect()
        await first.cmd("SET", "watched", "1")
        self.assertOK(await first.cmd("WATCH", "watched"))
        await second.cmd("SET", "watched", "2")
        self.assertOK(await first.cmd("MULTI"))
        await first.cmd("SET", "watched", "3")
        self.assertIsNone(await first.cmd("EXEC"))
        self.assertEqual(await first.cmd("GET", "watched"), b"2")

    async def test_watch_unmodified_commits(self):
        connection = await self.connect()
        await connection.cmd("SET", "watched", "1")
        await connection.cmd("WATCH", "watched")
        await connection.cmd("MULTI")
        await connection.cmd("INCR", "watched")
        self.assertEqual(await connection.cmd("EXEC"), [2])


class TestPubSub(ServerTestCase):
    async def test_publish_subscribe(self):
        subscriber = await self.connect()
        publisher = await self.connect()
        self.assertEqual(
            await subscriber.cmd("SUBSCRIBE", "news"),
            [b"subscribe", b"news", 1],
        )
        self.assertEqual(await publisher.cmd("PUBLISH", "news", "hello"), 1)
        self.assertEqual(
            await subscriber.read(), [b"message", b"news", b"hello"]
        )
        self.assertEqual(await publisher.cmd("PUBLISH", "other", "x"), 0)

        self.assertError(await subscriber.cmd("GET", "key"), "ERR Can't execute")

        self.assertEqual(
            await subscriber.cmd("UNSUBSCRIBE", "news"),
            [b"unsubscribe", b"news", 0],
        )
        self.assertOK(await subscriber.cmd("SET", "key", "v"))

    async def test_pattern_subscribe(self):
        subscriber = await self.connect()
        publisher = await self.connect()
        await subscriber.cmd("PSUBSCRIBE", "news.*")
        self.assertEqual(await publisher.cmd("PUBLISH", "news.tech", "hi"), 1)
        self.assertEqual(
            await subscriber.read(),
            [b"pmessage", b"news.*", b"news.tech", b"hi"],
        )
        self.assertEqual(await publisher.cmd("PUBSUB", "NUMPAT"), 1)
        self.assertEqual(await publisher.cmd("PUBSUB", "CHANNELS", "news.*"), [])


class TestAuthAndServer(ServerTestCase):
    async def test_info_and_command(self):
        connection = await self.connect()
        info = await connection.cmd("INFO", "server")
        self.assertIn(b"redis_version", info)
        self.assertGreater(await connection.cmd("COMMAND", "COUNT"), 50)
        self.assertGreater(len(await connection.cmd("COMMAND")), 50)
        self.assertEqual(len(await connection.cmd("TIME")), 2)
        self.assertEqual((await connection.cmd("ROLE"))[0], b"master")
        client_id = await connection.cmd("CLIENT", "ID")
        self.assertIsInstance(client_id, int)
        self.assertOK(await connection.cmd("CLIENT", "SETNAME", "tester"))
        self.assertEqual(await connection.cmd("CLIENT", "GETNAME"), b"tester")

    async def test_hello(self):
        connection = await self.connect()
        reply = await connection.cmd("HELLO", "2")
        data = dict(zip(reply[0::2], reply[1::2]))
        self.assertEqual(data[b"server"], b"redis")
        self.assertEqual(data[b"proto"], 2)

        from miniredis.protocol import Map

        reply = await connection.cmd("HELLO", "3")
        self.assertIsInstance(reply, Map)
        data = dict(reply.items)
        self.assertEqual(data[b"proto"], 3)
        self.assertError(await connection.cmd("HELLO", "4"), "NOPROTO")


class TestRESP3(ServerTestCase):
    async def hello(self, connection):
        reply = await connection.cmd("HELLO", "3")
        self.assertIsInstance(reply, Map)
        self.assertEqual(dict(reply.items)[b"proto"], 3)

    async def test_nulls_maps_and_doubles(self):
        connection = await self.connect()
        await self.hello(connection)

        self.assertIsNone(await connection.cmd("GET", "missing"))
        self.assertEqual(
            await connection.cmd("MGET", "missing", "also-missing"),
            [None, None],
        )

        self.assertOK(await connection.cmd("SET", "float", "1.5"))
        self.assertEqual(
            await connection.cmd("INCRBYFLOAT", "float", "0.25"),
            Double(1.75),
        )

        self.assertEqual(
            await connection.cmd("HSET", "hash", "a", "1", "b", "2"),
            2,
        )
        reply = await connection.cmd("HGETALL", "hash")
        self.assertIsInstance(reply, Map)
        self.assertEqual(dict(reply.items), {b"a": b"1", b"b": b"2"})
        self.assertEqual(
            await connection.cmd("HINCRBYFLOAT", "hash", "a", "0.5"),
            Double(1.5),
        )

        empty = await connection.cmd("HGETALL", "absent")
        self.assertIsInstance(empty, Map)
        self.assertEqual(empty.items, [])

        reply = await connection.cmd("CONFIG", "GET", "maxmemory")
        self.assertIsInstance(reply, Map)
        self.assertEqual(dict(reply.items), {b"maxmemory": b"0"})

    async def test_sorted_set_doubles_and_pairs(self):
        connection = await self.connect()
        await self.hello(connection)

        self.assertEqual(
            await connection.cmd("ZADD", "z", "1", "a", "2", "b"),
            2,
        )
        self.assertEqual(await connection.cmd("ZSCORE", "z", "b"), Double(2.0))
        self.assertEqual(
            await connection.cmd("ZMSCORE", "z", "a", "missing"),
            [Double(1.0), None],
        )
        self.assertEqual(
            await connection.cmd("ZINCRBY", "z", "1.5", "b"),
            Double(3.5),
        )
        self.assertEqual(
            await connection.cmd("ZRANGE", "z", "0", "-1", "WITHSCORES"),
            [[b"a", Double(1.0)], [b"b", Double(3.5)]],
        )
        self.assertEqual(
            await connection.cmd("ZRANK", "z", "b", "WITHSCORE"),
            [1, Double(3.5)],
        )

        # RESP3 ZPOPMIN without COUNT is a flat pair; with COUNT it is nested.
        self.assertEqual(
            await connection.cmd("ZPOPMIN", "z"),
            [b"a", Double(1.0)],
        )
        self.assertEqual(
            await connection.cmd("ZPOPMIN", "z", "1"),
            [[b"b", Double(3.5)]],
        )

        await connection.cmd("ZADD", "scan", "1", "a", "2", "b")
        cursor, entries = await connection.cmd("ZSCAN", "scan", "0")
        self.assertEqual(cursor, b"0")
        self.assertEqual(entries, [b"a", Double(1.0), b"b", Double(2.0)])

    async def test_pubsub_push_frames(self):
        subscriber = await self.connect()
        publisher = await self.connect()
        await self.hello(subscriber)
        await self.hello(publisher)

        reply = await subscriber.cmd("SUBSCRIBE", "news")
        self.assertIsInstance(reply, Push)
        self.assertEqual(reply.values, [b"subscribe", b"news", 1])

        self.assertEqual(await publisher.cmd("PUBLISH", "news", "hello"), 1)
        message = await subscriber.read()
        self.assertIsInstance(message, Push)
        self.assertEqual(
            message.values, [b"message", b"news", b"hello"]
        )

        reply = await subscriber.cmd("UNSUBSCRIBE", "news")
        self.assertIsInstance(reply, Push)
        self.assertEqual(reply.values, [b"unsubscribe", b"news", 0])


class TestPersistence(Assertions, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    async def test_save_and_reload(self):
        server = Server(
            port=0,
            data_dir=self.directory.name,
            dbfilename="dump.mrd",
            save_interval=0,
            persistence=True,
        )
        await server.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        connection = Connection(reader, writer)
        try:
            await connection.cmd("SET", "persisted", "yes")
            await connection.cmd("RPUSH", "list", "a", "b")
            await connection.cmd("HSET", "hash", "f", "v")
            await connection.cmd("SET", "volatile", "x", "PX", "5000")
            self.assertOK(await connection.cmd("SAVE"))
            path = os.path.join(self.directory.name, "dump.mrd")
            self.assertTrue(os.path.exists(path))
        finally:
            await connection.close()
            await server.stop()

        second = Server(
            port=0,
            data_dir=self.directory.name,
            dbfilename="dump.mrd",
            save_interval=0,
            persistence=True,
        )
        await second.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", second.port)
        connection = Connection(reader, writer)
        try:
            self.assertEqual(await connection.cmd("GET", "persisted"), b"yes")
            self.assertEqual(await connection.cmd("LRANGE", "list", "0", "-1"), [b"a", b"b"])
            self.assertEqual(await connection.cmd("HGET", "hash", "f"), b"v")
            self.assertGreater(await connection.cmd("PTTL", "volatile"), 0)
        finally:
            await connection.close()
            await second.stop()


class TestShutdownSave(Assertions, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    async def test_shutdown_writes_dirty_snapshot(self):
        path = os.path.join(self.directory.name, "shutdown.mrd")
        server = Server(
            port=0,
            data_dir=self.directory.name,
            dbfilename="shutdown.mrd",
            save_interval=0,
            persistence=True,
        )
        await server.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        connection = Connection(reader, writer)
        try:
            self.assertOK(await connection.cmd("SET", "shutdown", "yes"))
        finally:
            await connection.close()
            await server.stop()

        self.assertTrue(os.path.exists(path))

        second = Server(
            port=0,
            data_dir=self.directory.name,
            dbfilename="shutdown.mrd",
            save_interval=0,
            persistence=True,
        )
        await second.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", second.port)
        connection = Connection(reader, writer)
        try:
            self.assertEqual(await connection.cmd("GET", "shutdown"), b"yes")
        finally:
            await connection.close()
            await second.stop()


class TestAuth(Assertions, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = Server(port=0, persistence=False, requirepass="secret")
        await self.server.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        self.connection = Connection(reader, writer)

    async def asyncTearDown(self):
        await self.connection.close()
        await self.server.stop()
        await asyncio.sleep(0)

    async def test_auth_flow(self):
        self.assertError(await self.connection.cmd("GET", "x"), "NOAUTH")
        self.assertError(await self.connection.cmd("AUTH", "wrong"), "WRONGPASS")
        self.assertOK(await self.connection.cmd("AUTH", "secret"))
        self.assertOK(await self.connection.cmd("SET", "x", "1"))
        self.assertEqual(await self.connection.cmd("GET", "x"), b"1")


if __name__ == "__main__":
    unittest.main()

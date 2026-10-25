"""Unit tests for RESP parsing and encoding."""

import asyncio
import unittest

from miniredis.protocol import (
    Double,
    Map,
    ProtocolError,
    Push,
    RespError,
    SimpleString,
    encode,
    encode_command,
    read_command,
    read_reply,
)


class TestEncode(unittest.TestCase):
    def test_encoding(self):
        self.assertEqual(encode(SimpleString("OK")), b"+OK\r\n")
        self.assertEqual(encode(b"hi"), b"$2\r\nhi\r\n")
        self.assertEqual(encode(None), b"$-1\r\n")
        self.assertEqual(encode(42), b":42\r\n")
        self.assertEqual(
            encode([b"a", 1, None]), b"*3\r\n$1\r\na\r\n:1\r\n$-1\r\n"
        )
        self.assertEqual(encode(RespError("ERR nope")), b"-ERR nope\r\n")
        self.assertEqual(encode(True), b":1\r\n")

    def test_resp3_encoding(self):
        self.assertEqual(encode(None, resp3=True), b"_\r\n")
        self.assertEqual(encode(Double(1.5), resp3=True), b",1.5\r\n")
        self.assertEqual(encode(Double(2.0), resp3=True), b",2\r\n")
        self.assertEqual(
            encode(Map([(b"a", 1), (b"b", 2)]), resp3=True),
            b"%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n",
        )
        self.assertEqual(
            encode(Push([b"message", b"news"]), resp3=True),
            b">2\r\n$7\r\nmessage\r\n$4\r\nnews\r\n",
        )

    def test_encode_command(self):
        self.assertEqual(
            encode_command([b"SET", b"key", b"value"]),
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
        )


class TestRead(unittest.IsolatedAsyncioTestCase):
    async def parse(self, data):
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()
        return await read_command(reader)

    async def test_array_command(self):
        args = await self.parse(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n")
        self.assertEqual(args, [b"SET", b"a", b"b"])

    async def test_binary_safe(self):
        args = await self.parse(b"*2\r\n$3\r\nGET\r\n$3\r\na\x00b\r\n")
        self.assertEqual(args, [b"GET", b"a\x00b"])

    async def test_empty_array(self):
        self.assertEqual(await self.parse(b"*0\r\n"), [])

    async def test_inline(self):
        self.assertEqual(await self.parse(b"PING\r\n"), [b"PING"])
        self.assertEqual(
            await self.parse(b'SET greeting "hello world"\r\n'),
            [b"SET", b"greeting", b"hello world"],
        )
        self.assertEqual(await self.parse(b"  GET   key  \r\n"), [b"GET", b"key"])

    async def test_eof(self):
        self.assertIsNone(await self.parse(b""))

    async def test_bad_array_length(self):
        with self.assertRaises(ProtocolError):
            await self.parse(b"*abc\r\n")

    async def test_bad_bulk_header(self):
        with self.assertRaises(ProtocolError):
            await self.parse(b"*1\r\n+OK\r\n")

    async def test_truncated(self):
        with self.assertRaises(ProtocolError):
            await self.parse(b"*1\r\n$5\r\nabc")

    async def test_reply_parsing(self):
        reader = asyncio.StreamReader()
        reader.feed_data(
            b"+OK\r\n$3\r\nfoo\r\n:7\r\n$-1\r\n*2\r\n$1\r\na\r\n:1\r\n"
        )
        reader.feed_eof()
        self.assertEqual(await read_reply(reader), SimpleString("OK"))
        self.assertEqual(await read_reply(reader), b"foo")
        self.assertEqual(await read_reply(reader), 7)
        self.assertIsNone(await read_reply(reader))
        self.assertEqual(await read_reply(reader), [b"a", 1])

    async def test_error_reply_parsing(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b"-ERR something\r\n")
        reader.feed_eof()
        reply = await read_reply(reader)
        self.assertIsInstance(reply, RespError)
        self.assertEqual(reply.message, "ERR something")

    async def test_resp3_reply_parsing(self):
        reader = asyncio.StreamReader()
        reader.feed_data(
            b"_\r\n"
            b",1.5\r\n"
            b">2\r\n$7\r\nmessage\r\n$4\r\nnews\r\n"
            b"%1\r\n$1\r\nk\r\n$1\r\nv\r\n"
            b"#t\r\n"
        )
        reader.feed_eof()
        self.assertIsNone(await read_reply(reader))
        self.assertEqual(await read_reply(reader), Double(1.5))
        push = await read_reply(reader)
        self.assertIsInstance(push, Push)
        self.assertEqual(push.values, [b"message", b"news"])
        mapping = await read_reply(reader)
        self.assertIsInstance(mapping, Map)
        self.assertEqual(mapping.items, [(b"k", b"v")])
        self.assertIs(await read_reply(reader), True)


if __name__ == "__main__":
    unittest.main()

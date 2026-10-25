"""Unit tests for the bundled client's reply rendering."""

import unittest

from miniredis.client import render
from miniredis.protocol import Double, Map, Push, RespError, SimpleString


class TestRender(unittest.TestCase):
    def test_scalars(self):
        self.assertEqual(render(SimpleString("OK")), "OK")
        self.assertEqual(render(b"hello"), '"hello"')
        self.assertEqual(render(b'line\nquote"'), '"line\\nquote\\""')
        self.assertEqual(render(42), "(integer) 42")
        self.assertEqual(render(None), "(nil)")
        self.assertEqual(render(Double(1.5)), "(double) 1.5")
        self.assertEqual(render(Double(2.0)), "(double) 2")
        self.assertEqual(render(True), "(true)")
        self.assertEqual(render(RespError("ERR nope")), "(error) ERR nope")

    def test_arrays_and_nesting(self):
        self.assertEqual(render([]), "(empty array)")
        self.assertEqual(
            render([b"a", 1, None]),
            '1) "a"\n2) (integer) 1\n3) (nil)',
        )
        self.assertEqual(
            render([b"a", [b"b", Double(2.0)]]),
            '1) "a"\n2) 1) "b"\n   2) (double) 2',
        )

    def test_map(self):
        self.assertEqual(render(Map([])), "(empty map)")
        self.assertEqual(
            render(Map([(b"server", b"redis"), (b"proto", 3)])),
            '1# "server" => "redis"\n2# "proto" => (integer) 3',
        )
        self.assertEqual(
            render(Map([(b"nested", [b"a"])])),
            '1# "nested" =>\n   1) "a"',
        )

    def test_push(self):
        self.assertEqual(render(Push([])), "(empty array)")
        self.assertEqual(
            render(Push([b"message", b"news", b"hello"])),
            '1) "message"\n2) "news"\n3) "hello"',
        )


if __name__ == "__main__":
    unittest.main()

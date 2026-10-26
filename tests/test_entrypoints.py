"""Smoke tests for the command-line entry points."""

import unittest

import redis_client
import redis_server

from miniredis import cli, client


class TestEntryPoints(unittest.TestCase):
    def test_main_functions_exist(self):
        self.assertTrue(callable(redis_server.main))
        self.assertTrue(callable(redis_client.main))
        self.assertTrue(callable(cli.main))
        self.assertTrue(callable(client.main))


if __name__ == "__main__":
    unittest.main()

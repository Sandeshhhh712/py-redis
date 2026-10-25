"""Keyspace implementation: values, expirations, versions and snapshots."""

import copy
import time

from .errors import RedisError
from .types import type_name


class Database:
    """One Redis logical database."""

    def __init__(self, index):
        self.index = index
        self.data = {}
        self.expires = {}  # key -> absolute unix timestamp (float seconds)
        self.versions = {}  # key -> monotonic version, used by WATCH
        self.epoch = 0  # bumped by FLUSHDB, invalidates all WATCHes
        # Replaced by the server; called whenever a key changes.
        self.on_change = lambda key: None

    # -- versions / dirty tracking ---------------------------------------

    def touch(self, key):
        self.versions[key] = self.versions.get(key, 0) + 1
        self.on_change(key)

    def watch_version(self, key):
        """Return (epoch, version) for WATCH, expiring the key lazily first."""
        self._purge_if_expired(key)
        return self.epoch, self.versions.get(key, 0)

    # -- expiration -------------------------------------------------------

    def _expired(self, key):
        deadline = self.expires.get(key)
        return deadline is not None and deadline <= time.time()

    def _purge_if_expired(self, key):
        if key in self.data and self._expired(key):
            del self.data[key]
            self.expires.pop(key, None)
            self.touch(key)
            return True
        return False

    def purge_expired(self, limit=100):
        """Expire up to ``limit`` stale keys.  Returns how many were removed."""
        if not self.expires:
            return 0
        now = time.time()
        removed = 0
        for key in list(self.expires):
            if removed >= limit:
                break
            if self.expires.get(key, now + 1) <= now:
                if self._purge_if_expired(key):
                    removed += 1
        return removed

    def purge_all(self):
        now = time.time()
        for key in list(self.expires):
            if self.expires.get(key, now + 1) <= now:
                self._purge_if_expired(key)

    # -- basic access -----------------------------------------------------

    def get(self, key):
        self._purge_if_expired(key)
        return self.data.get(key)

    def exists(self, key):
        return self.get(key) is not None

    def set(self, key, value, expires_at=None, keep_ttl=False):
        self.data[key] = value
        if expires_at is not None:
            self.expires[key] = expires_at
        elif not keep_ttl:
            self.expires.pop(key, None)
        self.touch(key)
        return value

    def delete(self, key):
        self._purge_if_expired(key)
        if key not in self.data:
            return False
        del self.data[key]
        self.expires.pop(key, None)
        self.touch(key)
        return True

    def get_typed(self, key, expected, command="operation"):
        """Return the value or raise WRONGTYPE if it is another type."""
        value = self.get(key)
        if value is not None and not isinstance(value, expected):
            raise RedisError(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        return value

    def set_expire(self, key, deadline):
        if self.get(key) is None:
            return False
        self.expires[key] = deadline
        self.touch(key)
        return True

    def persist(self, key):
        if self.get(key) is None or key not in self.expires:
            return False
        del self.expires[key]
        self.touch(key)
        return True

    def ttl_ms(self, key):
        if self.get(key) is None:
            return -2
        deadline = self.expires.get(key)
        if deadline is None:
            return -1
        return max(0, int((deadline - time.time()) * 1000))

    def expire_time(self, key):
        if self.get(key) is None:
            return -2
        deadline = self.expires.get(key)
        if deadline is None:
            return -1
        return int(deadline)

    def keys(self):
        self.purge_all()
        return list(self.data)

    def dbsize(self):
        self.purge_all()
        return len(self.data)

    def type_of(self, key):
        return type_name(self.get(key))

    def flush(self):
        self.data.clear()
        self.expires.clear()
        self.versions.clear()
        self.epoch += 1

    # -- persistence ------------------------------------------------------

    def export(self):
        """Deep copy of the database, safe to serialize off the event loop."""
        self.purge_all()
        return {
            "data": copy.deepcopy(self.data),
            "expires": dict(self.expires),
        }

    def load(self, payload):
        self.data = payload.get("data", {})
        self.expires = payload.get("expires", {})
        self.versions.clear()
        self.epoch += 1

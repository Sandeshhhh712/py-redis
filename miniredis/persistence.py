"""Snapshot persistence (a tiny stand-in for Redis' RDB files).

Snapshots are pickled dictionaries.  They are written atomically (temp file +
``os.replace``) so a crash cannot leave a half-written dump behind.

Note: pickles can execute code when loaded.  Only load dump files you trust.
"""

import asyncio
import os
import pickle
import time
from pathlib import Path


class SnapshotPersistence:
    def __init__(self, server):
        self.server = server
        self.path = Path(server.data_dir) / server.dbfilename
        self._lock = asyncio.Lock()

    # -- writing ----------------------------------------------------------

    def _serialize(self):
        payload = {
            "magic": "miniredis",
            "version": 1,
            "created_at": time.time(),
            "db_count": len(self.server.databases),
            "databases": [db.export() for db in self.server.databases],
        }
        counter = self.server.change_counter
        return payload, counter

    def _write(self, payload, counter):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(self.path.name + ".tmp")
        with open(temporary, "wb") as handle:
            pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)
        self.server.last_save = time.time()
        # Changes made while a background save was running are not in the
        # snapshot, so keep ``dirty`` set in that case.
        if self.server.change_counter == counter:
            self.server.dirty = False

    async def save(self, background=False):
        if self._lock.locked():
            raise RuntimeError("save already in progress")
        async with self._lock:
            self.server.saving = True
            try:
                # Deep copy on the event loop, then serialize off the loop.
                payload, counter = self._serialize()
                if background:
                    await asyncio.to_thread(self._write, payload, counter)
                else:
                    self._write(payload, counter)
            finally:
                self.server.saving = False

    # -- loading ----------------------------------------------------------

    def load(self):
        if not self.path.exists():
            return False
        with open(self.path, "rb") as handle:
            payload = pickle.load(handle)
        if not isinstance(payload, dict) or payload.get("magic") != "miniredis":
            raise ValueError(f"{self.path} is not a miniredis snapshot")
        databases = payload.get("databases") or []
        for index, db in enumerate(self.server.databases):
            if index < len(databases):
                db.load(databases[index])
        self.server.last_save = payload.get("created_at", time.time())
        self.server.dirty = False
        return True

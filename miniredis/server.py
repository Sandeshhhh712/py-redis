"""Async RESP server: connection lifecycle, dispatch and background tasks."""

import asyncio
import logging
import time

from . import protocol
from .commands import COMMANDS
from .commands.pubsub import publish_message
from .database import Database
from .errors import ProtocolError, RedisError
from .notifier import Notifier
from .persistence import SnapshotPersistence
from .protocol import NO_REPLY, RespError, SimpleString

log = logging.getLogger("miniredis")

SUBSCRIBE_ALLOWED = {
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "PING",
    "QUIT",
    "RESET",
}
AUTH_ALLOWED = {"AUTH", "HELLO", "QUIT", "RESET"}
TRANSACTION_DISALLOWED = {
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
}


class Client:
    def __init__(self, server, reader, writer, address):
        self.server = server
        self.reader = reader
        self.writer = writer
        self.address = address
        self.id = server.next_client_id()
        self.name = b""
        self.db_index = 0
        self.protocol = 2
        self.authenticated = not bool(server.config.get("requirepass"))
        self.in_multi = False
        self.in_exec = False
        self.multi_error = False
        self.queue = []
        self.watched = {}
        self.subscriptions = set()
        self.patterns = set()
        self.close_after_reply = False
        self.closed = False
        self.created_at = time.time()
        self.lock = asyncio.Lock()

    @property
    def db(self):
        return self.server.databases[self.db_index]

    def reset_multi(self):
        self.in_multi = False
        self.in_exec = False
        self.multi_error = False
        self.queue = []
        self.watched = {}

    def reset(self):
        self.reset_multi()
        self.subscriptions.clear()
        self.patterns.clear()
        self.db_index = 0
        self.name = b""
        self.authenticated = not bool(self.server.config.get("requirepass"))

    async def send(self, value):
        if self.closed:
            return
        data = protocol.encode(value, resp3=self.protocol == 3)
        async with self.lock:
            if self.closed:
                return
            try:
                self.writer.write(data)
                await self.writer.drain()
            except (ConnectionError, OSError):
                self.closed = True

    async def run(self):
        try:
            while not self.closed:
                args = await protocol.read_command(self.reader)
                if args is None:
                    break
                if not args:
                    continue
                reply = await self.server.execute(self, args)
                if reply is not NO_REPLY:
                    await self.send(reply)
                if self.close_after_reply:
                    break
        except ProtocolError as exc:
            await self.send(RespError(f"ERR Protocol error: {exc}"))
        except (ConnectionError, BrokenPipeError, asyncio.IncompleteReadError):
            pass
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("unexpected error on connection %s", self.address)
        finally:
            await self.close()

    async def close(self):
        if self.closed:
            return
        self.closed = True
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except (ConnectionError, OSError):
            pass


class Server:
    def __init__(
        self,
        host="127.0.0.1",
        port=6767,
        databases=16,
        data_dir=".",
        dbfilename="dump.mrd",
        persistence=True,
        save_interval=60,
        requirepass=None,
    ):
        self.host = host
        self.port = port
        self.databases = [Database(index) for index in range(databases)]
        self.notifier = Notifier()
        self.clients = {}
        self._next_client_id = 1
        self.config = {
            "dbfilename": dbfilename,
            "dir": str(data_dir),
            "save": f"{save_interval} 1" if persistence and save_interval else "",
            "appendonly": "no",
            "maxmemory": "0",
            "requirepass": requirepass or "",
            "timeout": "0",
            "databases": str(databases),
        }
        self.requirepass = requirepass
        self.data_dir = str(data_dir)
        self.dbfilename = dbfilename
        self.persistence = (
            SnapshotPersistence(self) if persistence else None
        )
        self.save_interval = save_interval
        self.save_on_shutdown = True
        self.dirty = False
        self.saving = False
        self.last_save = time.time()
        self.start_time = time.time()
        self.total_connections = 0
        self.total_commands = 0
        self.change_counter = 0
        self._server = None
        self._tasks = []
        self._shutdown = asyncio.Event()
        for db in self.databases:
            db.on_change = self._make_change_handler(db.index)

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def ok():
        return SimpleString("OK")

    def _make_change_handler(self, db_index):
        def changed(key):
            self.dirty = True
            self.change_counter += 1
            self.notifier.notify(db_index, key)

        return changed

    def next_client_id(self):
        client_id = self._next_client_id
        self._next_client_id += 1
        return client_id

    async def publish(self, channel, message):
        return await publish_message(self, channel, message)

    # -- dispatch ---------------------------------------------------------

    async def execute(self, client, args):
        raw_name = args[0]
        try:
            name = raw_name.decode("utf-8").upper()
        except UnicodeDecodeError:
            return RespError("ERR unknown command")
        command = COMMANDS.get(name)

        if command is None:
            if client.in_multi:
                client.multi_error = True
            preview = ", ".join(
                f"'{arg.decode('utf-8', 'replace')}'" for arg in args[1:3]
            )
            return RespError(
                f"ERR unknown command '{raw_name.decode('utf-8', 'replace')}', "
                f"with args beginning with: {preview}"
            )

        if (
            self.config.get("requirepass")
            and not client.authenticated
            and name not in AUTH_ALLOWED
        ):
            return RespError("NOAUTH Authentication required.")

        if (
            (client.subscriptions or client.patterns)
            and name not in SUBSCRIBE_ALLOWED
            and not client.in_multi
        ):
            return RespError(
                f"ERR Can't execute '{name.lower()}': only (P|S)SUBSCRIBE / "
                "(P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
            )

        arg_count = len(args) - 1
        if not command.accepts(arg_count):
            if client.in_multi and name not in (
                "EXEC",
                "DISCARD",
                "MULTI",
                "WATCH",
                "RESET",
            ):
                client.multi_error = True
            return RespError(
                f"ERR wrong number of arguments for '{name.lower()}' command"
            )

        if client.in_multi and name not in (
            "EXEC",
            "DISCARD",
            "MULTI",
            "WATCH",
            "RESET",
        ):
            if name in TRANSACTION_DISALLOWED:
                client.multi_error = True
                return RespError(f"ERR {name} is not allowed in transactions")
            client.queue.append(args)
            return SimpleString("QUEUED")

        client.server.total_commands += 1
        try:
            return await command.handler(client, args[1:])
        except RedisError as exc:
            return RespError(str(exc))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("command %s failed", name)
            return RespError("ERR internal error")

    # -- lifecycle --------------------------------------------------------

    async def start(self):
        if self.persistence is not None:
            try:
                if self.persistence.load():
                    log.info("loaded snapshot from %s", self.persistence.path)
            except Exception:
                log.exception("failed to load snapshot %s", self.persistence.path)
        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        sockets = self._server.sockets
        if sockets:
            self.port = sockets[0].getsockname()[1]
        self.start_time = time.time()
        self._tasks = [
            asyncio.create_task(self._expiry_loop(), name="miniredis-expiry")
        ]
        if self.persistence is not None and self.save_interval:
            self._tasks.append(
                asyncio.create_task(self._autosave_loop(), name="miniredis-autosave")
            )
        log.info("ready to accept connections on %s:%s", self.host, self.port)
        return self

    async def serve(self):
        await self.start()
        await self.wait_shutdown()
        await self.stop()

    async def wait_shutdown(self):
        await self._shutdown.wait()

    def request_shutdown(self):
        self._shutdown.set()

    async def stop(self):
        log.debug("stopping server")
        listener = self._server
        self._server = None
        if listener is not None:
            listener.close()
        # Close client connections before waiting for the listener: since
        # Python 3.12 ``wait_closed()`` also waits for connection handlers to
        # finish, and an idle client would otherwise keep us here forever.
        for client in list(self.clients.values()):
            await client.close()
        self.clients.clear()
        if listener is not None:
            try:
                await listener.wait_closed()
            except Exception:
                pass
        log.debug("listener closed")
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []
        log.debug("background tasks stopped")
        if (
            self.persistence is not None
            and self.save_on_shutdown
            and self.dirty
        ):
            try:
                await self.persistence.save()
                log.info("saved snapshot to %s", self.persistence.path)
            except Exception:
                log.exception("failed to save snapshot on shutdown")

    async def _handle_client(self, reader, writer):
        peer = writer.get_extra_info("peername")
        address = f"{peer[0]}:{peer[1]}" if peer else "unknown"
        client = Client(self, reader, writer, address)
        self.clients[client.id] = client
        self.total_connections += 1
        try:
            await client.run()
        finally:
            self.clients.pop(client.id, None)

    async def _expiry_loop(self):
        while True:
            try:
                for db in self.databases:
                    db.purge_expired(100)
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("expiry cycle failed")

    async def _autosave_loop(self):
        while True:
            try:
                await asyncio.sleep(self.save_interval)
                if self.dirty and not self.saving:
                    await self.persistence.save(background=True)
                    log.debug("autosaved %s", self.persistence.path)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("autosave failed")

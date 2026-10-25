# miniredis

A small Redis-compatible server written in Python, built on `asyncio`. It
speaks RESP2 and RESP3, so real clients such as `redis-cli` and `redis-py`
can connect to it, and it implements 148 commands across strings, lists,
hashes, sets, sorted sets, pub/sub, transactions and server introspection.

Default port: **6767**. The default snapshot file is `dump.mrd` in the
current directory. Data lives in memory; snapshots are written to disk with
`SAVE` / `BGSAVE`, on a periodic autosave, and on a clean shutdown.

## Requirements

- Python 3.9 or newer
- No third-party runtime dependencies

## Quick start

Start the server:

```bash
python3 redis_server.py
# or: python3 -m miniredis
# or, after installing: miniredis
```

Use the bundled client:

```bash
python3 redis_client.py
# or: python3 -m miniredis.client
# or, after installing: miniredis-cli
```

Run a single command without entering the REPL:

```bash
python3 redis_client.py SET greeting "hello world"
python3 redis_client.py GET greeting
```

Use any normal Redis client:

```bash
redis-cli -p 6767 SET greeting hello
redis-cli -p 6767 GET greeting
```

```python
import redis

r = redis.Redis(port=6767, decode_responses=True)
r.set("greeting", "hello")
print(r.get("greeting"))
```

## Server options

```text
--host ADDRESS          bind address (default 127.0.0.1)
--port PORT             TCP port (default 6767)
--databases N           number of logical databases (default 16)
--dir DIRECTORY         directory for the snapshot file (default .)
--dbfilename NAME       snapshot file name (default dump.mrd)
--save-interval SECS    autosave interval; 0 disables periodic saves (default 60)
--no-persistence        never read or write snapshots
--requirepass PASSWORD  require AUTH before commands
--log-level LEVEL       debug, info, warning or error
```

Examples:

```bash
python3 redis_server.py --port 6379 --host 0.0.0.0
python3 redis_server.py --no-persistence
python3 redis_server.py --requirepass secret --dir /tmp/miniredis
```

## Supported commands

### Strings

`APPEND`, `DECR`, `DECRBY`, `GET`, `GETDEL`, `GETEX`, `GETRANGE`, `GETSET`,
`INCR`, `INCRBY`, `INCRBYFLOAT`, `MGET`, `MSET`, `MSETNX`, `SET`, `SETNX`,
`SETRANGE`, `STRLEN`, `SUBSTR`

### Keys and databases

`COPY`, `DBSIZE`, `DEL`, `EXISTS`, `EXPIRE`, `EXPIREAT`, `EXPIRETIME`,
`FLUSHALL`, `FLUSHDB`, `KEYS`, `MOVE`, `OBJECT`, `PERSIST`, `PEXPIRE`,
`PEXPIREAT`, `PEXPIRETIME`, `PTTL`, `RANDOMKEY`, `RENAME`, `RENAMENX`,
`SCAN`, `SELECT`, `SWAPDB`, `TOUCH`, `TTL`, `TYPE`, `UNLINK`

### Lists

`BLPOP`, `BRPOP`, `LINDEX`, `LINSERT`, `LLEN`, `LMOVE`, `LPOP`, `LPOS`,
`LPUSH`, `LPUSHX`, `LRANGE`, `LREM`, `LSET`, `LTRIM`, `RPOP`, `RPOPLPUSH`,
`RPUSH`, `RPUSHX`

### Hashes

`HDEL`, `HEXISTS`, `HGET`, `HGETALL`, `HINCRBY`, `HINCRBYFLOAT`, `HKEYS`,
`HLEN`, `HMGET`, `HMSET`, `HRANDFIELD`, `HSCAN`, `HSET`, `HSETNX`,
`HSTRLEN`, `HVALS`

### Sets

`SADD`, `SCARD`, `SDIFF`, `SDIFFSTORE`, `SINTER`, `SINTERCARD`,
`SINTERSTORE`, `SISMEMBER`, `SMEMBERS`, `SMISMEMBER`, `SMOVE`, `SPOP`,
`SRANDMEMBER`, `SREM`, `SSCAN`, `SUNION`, `SUNIONSTORE`

### Sorted sets

`ZADD`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZLEXCOUNT`, `ZMSCORE`, `ZPOPMAX`,
`ZPOPMIN`, `ZRANDMEMBER`, `ZRANGE`, `ZRANGEBYLEX`, `ZRANGEBYSCORE`,
`ZRANK`, `ZREM`, `ZREMRANGEBYLEX`, `ZREMRANGEBYRANK`,
`ZREMRANGEBYSCORE`, `ZREVRANGE`, `ZREVRANGEBYLEX`, `ZREVRANGEBYSCORE`,
`ZREVRANK`, `ZSCAN`, `ZSCORE`

### Pub/sub

`PSUBSCRIBE`, `PUBLISH`, `PUBSUB`, `PUNSUBSCRIBE`, `SUBSCRIBE`,
`UNSUBSCRIBE`

### Transactions

`DISCARD`, `EXEC`, `MULTI`, `UNWATCH`, `WATCH`

Transactions use optimistic locking: a write to a watched key between
`WATCH` and `EXEC` aborts the transaction. Pub/sub commands are rejected
inside `MULTI`, matching Redis behaviour.

### Connection and server

`AUTH`, `BGSAVE`, `CLIENT`, `COMMAND`, `CONFIG`, `ECHO`, `HELLO`, `INFO`,
`LASTSAVE`, `LOLWUT`, `PING`, `QUIT`, `RESET`, `ROLE`, `SAVE`, `SHUTDOWN`,
`TIME`

## RESP2 and RESP3

Connections start in RESP2. `HELLO 3` switches a connection to RESP3. The
server then uses RESP3 reply types where Redis does:

- nulls are `_`
- maps are `%` (`HGETALL`, `CONFIG GET`, `HELLO`, ...)
- doubles are `,` (`ZSCORE`, `ZINCRBY`, `INCRBYFLOAT`, `HINCRBYFLOAT`, ...)
- sorted-set score pairs are nested arrays
- pub/sub messages are out-of-band push frames `>`

`redis-py` 8.x enables RESP3 by default and works with both protocols.

## Persistence

Snapshots are pickled dictionaries written atomically (a temporary file
followed by `os.replace`), so an interrupted save cannot leave a partially
written dump behind.

- `SAVE` writes synchronously.
- `BGSAVE` serializes on the event loop and writes in a worker thread.
- Autosave runs when `--save-interval` is non-zero and the database is dirty.
- `SHUTDOWN`, `SIGINT` and `SIGTERM` save dirty data before exiting.
- `--no-persistence` disables loading and writing entirely.

> **Security note:** snapshots use Python's `pickle` format, which can
> execute arbitrary code when loaded. Only load snapshot files you trust.

## Project layout

```text
miniredis/
  protocol.py        RESP2/RESP3 encoding and parsing
  server.py          asyncio server, connection dispatch, lifecycle
  database.py        keyspace, expiry, key versioning, export/import
  notifier.py        wakeup mechanism for blocking list commands
  persistence.py     atomic snapshot save/load
  types.py           list/hash/set/zset value types
  utils.py           parsing, float formatting, glob matching
  commands/          command handlers grouped by data type
  cli.py             server command-line entry point
  client.py          bundled interactive client
redis_server.py      thin wrapper around miniredis.cli
redis_client.py      thin wrapper around miniredis.client
tests/               unittest suite (protocol + integration)
```

## Tests

Run the standard-library test suite from the project root:

```bash
python3 -m unittest discover -s tests -t . -v
```

The integration tests start a real server on an ephemeral port and exercise
the wire protocol, including RESP3 maps, push frames, doubles and nulls.

## Installation

```bash
python3 -m pip install .
miniredis --port 6767
miniredis-cli
```

## Limitations

This is a learning-oriented Redis clone, not a drop-in replacement for
production Redis. In particular:

- Snapshots use `pickle` rather than RDB and there is no AOF.
- There is no replication, clustering, Sentinel, ACL system or TLS.
- Streams, Lua scripting, `SORT`, bitmaps, HyperLogLog, geospatial commands
  and modules are not implemented.
- Expiry is checked opportunistically and by a 100 ms background sweep, so
  keys are not removed at the exact millisecond they expire.
- The bundled client uses a single connection and a blocking input loop.
  Pub/sub pushes are not demultiplexed from command replies, and messages
  that arrive while it waits for input are only read with the next command;
  use `redis-py` for serious pub/sub work.
- A blocking `BLPOP`/`BRPOP` client that disconnects may not be noticed until
  its timeout expires.
- Persistence and command execution share one asyncio event loop; a very
  large synchronous `SAVE` can briefly pause command processing.

"""RESP2/RESP3 protocol encoding, command parsing and reply parsing.

The server accepts both RESP arrays (what real clients send) and the simple
inline format (handy for ``nc``/``telnet``):

    PING
    SET greeting hello
"""

import asyncio

from .errors import ProtocolError

CRLF = b"\r\n"
MAX_BULK_LENGTH = 512 * 1024 * 1024
MAX_INLINE_LENGTH = 64 * 1024


class RespError:
    """An error reply (``-ERR ...``)."""

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

    def __repr__(self):
        return f"RespError({self.message!r})"

    def __eq__(self, other):
        return isinstance(other, RespError) and other.message == self.message

    def __hash__(self):
        return hash(self.message)


class SimpleString:
    """A RESP simple string reply (``+OK``)."""

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

    def __repr__(self):
        return f"SimpleString({self.value!r})"

    def __eq__(self, other):
        return isinstance(other, SimpleString) and other.value == self.value

    def __hash__(self):
        return hash(self.value)


class Map:
    """A RESP3 map reply (``%``).  Ignored/encoded as an array on RESP2."""

    def __init__(self, items):
        if hasattr(items, "items"):
            items = items.items()
        self.items = list(items)

    def __repr__(self):
        return f"Map({self.items!r})"


class Push:
    """A RESP3 out-of-band push (``>``); encoded as an array on RESP2."""

    def __init__(self, values):
        self.values = list(values)

    def __repr__(self):
        return f"Push({self.values!r})"


class Double:
    """A RESP3 double reply (``,``)."""

    def __init__(self, value):
        self.value = float(value)

    def __repr__(self):
        return f"Double({self.value!r})"

    def __eq__(self, other):
        return isinstance(other, Double) and self.value == other.value

    def __hash__(self):
        return hash(self.value)


class NoReply:
    """Sentinel returned by handlers that already wrote their replies."""


NO_REPLY = NoReply()


def encode(value, resp3=False):
    """Serialize a Python value to RESP bytes."""
    if isinstance(value, SimpleString):
        return b"+" + value.value.encode("utf-8") + CRLF
    if isinstance(value, RespError):
        message = value.message.replace("\r", " ").replace("\n", " ")
        return b"-" + message.encode("utf-8", "replace") + CRLF
    if value is None:
        return b"_\r\n" if resp3 else b"$-1\r\n"
    if isinstance(value, bytes):
        return b"$" + str(len(value)).encode() + CRLF + value + CRLF
    if isinstance(value, str):
        data = value.encode("utf-8")
        return b"$" + str(len(data)).encode() + CRLF + data + CRLF
    if isinstance(value, bool):
        return b":" + (b"1" if value else b"0") + CRLF
    if isinstance(value, int):
        return b":" + str(value).encode() + CRLF
    if isinstance(value, Map):
        parts = [b"%" + str(len(value.items)).encode() + CRLF]
        for key, item in value.items:
            parts.append(encode(key, resp3))
            parts.append(encode(item, resp3))
        return b"".join(parts)
    if isinstance(value, Push):
        return (
            b">"
            + str(len(value.values)).encode()
            + CRLF
            + b"".join(encode(item, resp3) for item in value.values)
        )
    if isinstance(value, Double):
        number = repr(value.value)
        if number.endswith(".0"):
            number = number[:-2]
        return b"," + number.encode() + CRLF
    if isinstance(value, (list, tuple)):
        parts = [b"*" + str(len(value)).encode() + CRLF]
        parts.extend(encode(item, resp3) for item in value)
        return b"".join(parts)
    raise TypeError(f"cannot encode {type(value)!r} as RESP")


def encode_command(args):
    """Encode a command as a RESP array (used by the client and blocking tests)."""
    parts = [b"*" + str(len(args)).encode() + CRLF]
    for arg in args:
        if isinstance(arg, str):
            arg = arg.encode("utf-8")
        elif isinstance(arg, int):
            arg = str(arg).encode()
        parts.append(b"$" + str(len(arg)).encode() + CRLF + arg + CRLF)
    return b"".join(parts)


async def read_command(reader):
    """Read one command from ``reader``.

    Returns a list of byte strings, or ``None`` on a clean EOF.
    """
    try:
        line = await reader.readline()
    except (ValueError, asyncio.LimitOverrunError) as exc:
        raise ProtocolError("too big inline request") from exc
    if not line:
        return None
    if not line.endswith(CRLF):
        raise ProtocolError("invalid protocol: expected CRLF")

    line = line[:-2]
    if line.startswith(b"*"):
        try:
            count = int(line[1:])
        except ValueError:
            raise ProtocolError("invalid multibulk length") from None
        if count <= 0:
            return []
        if count > 1024 * 1024:
            raise ProtocolError("invalid multibulk length")
        args = []
        for _ in range(count):
            try:
                header = await reader.readline()
            except (ValueError, asyncio.LimitOverrunError) as exc:
                raise ProtocolError("too big bulk request") from exc
            if not header or not header.endswith(CRLF):
                raise ProtocolError("unexpected end of request")
            header = header[:-2]
            if not header.startswith(b"$"):
                raise ProtocolError("expected '$', got something else")
            try:
                length = int(header[1:])
            except ValueError:
                raise ProtocolError("invalid bulk length") from None
            if length < 0 or length > MAX_BULK_LENGTH:
                raise ProtocolError("invalid bulk length")
            try:
                data = await reader.readexactly(length + 2)
            except asyncio.IncompleteReadError as exc:
                raise ProtocolError("unexpected end of request") from exc
            if not data.endswith(CRLF):
                raise ProtocolError("invalid bulk string terminator")
            args.append(data[:-2])
        return args

    # Inline command.  Supports simple quoting, which is enough for humans.
    if len(line) > MAX_INLINE_LENGTH:
        raise ProtocolError("too big inline request")
    return _split_inline(line)


def _split_inline(line):
    args = []
    current = bytearray()
    quote = None
    escaped = False
    started = False
    for byte in line:
        char = bytes((byte,))
        if escaped:
            current.extend(char)
            escaped = False
            started = True
        elif byte == 0x5C:  # backslash
            escaped = True
            started = True
        elif quote is not None:
            if char == quote:
                quote = None
            else:
                current.extend(char)
            started = True
        elif char in (b"'", b'"'):
            quote = char
            started = True
        elif char in (b" ", b"\t"):
            if started:
                args.append(bytes(current))
                current.clear()
                started = False
        else:
            current.extend(char)
            started = True
    if started:
        args.append(bytes(current))
    return args


async def read_reply(reader):
    """Parse a RESP2 reply (used by the bundled interactive client)."""
    try:
        line = await reader.readline()
    except (ValueError, asyncio.LimitOverrunError) as exc:
        raise ProtocolError("reply is too large") from exc
    if not line:
        raise ProtocolError("connection closed")
    if not line.endswith(CRLF):
        raise ProtocolError("malformed reply")
    line = line[:-2]
    if not line:
        raise ProtocolError("empty reply")
    prefix, rest = line[:1], line[1:]
    if prefix == b"+":
        return SimpleString(rest.decode("utf-8", "replace"))
    if prefix == b"_":
        return None
    if prefix == b"#":
        return rest == b"t"
    if prefix == b",":
        try:
            return Double(rest)
        except ValueError:
            raise ProtocolError("malformed double reply") from None
    if prefix == b"-":
        return RespError(rest.decode("utf-8", "replace"))
    if prefix == b":":
        try:
            return int(rest)
        except ValueError:
            raise ProtocolError("malformed integer reply") from None
    if prefix == b"$":
        try:
            length = int(rest)
        except ValueError:
            raise ProtocolError("malformed bulk length") from None
        if length == -1:
            return None
        if length < 0 or length > MAX_BULK_LENGTH:
            raise ProtocolError("malformed bulk length")
        try:
            data = await reader.readexactly(length + 2)
        except asyncio.IncompleteReadError as exc:
            raise ProtocolError("truncated bulk reply") from exc
        return data[:-2]
    if prefix == b"*":
        try:
            count = int(rest)
        except ValueError:
            raise ProtocolError("malformed array length") from None
        if count == -1:
            return None
        if count < -1 or count > 1024 * 1024:
            raise ProtocolError("malformed array length")
        return [await read_reply(reader) for _ in range(count)]
    if prefix in (b"%", b">", b"~"):
        try:
            count = int(rest)
        except ValueError:
            raise ProtocolError("malformed aggregate reply") from None
        if count < 0 or count > 1024 * 1024:
            raise ProtocolError("malformed aggregate reply")
        if prefix == b"%":
            pairs = [await read_reply(reader) for _ in range(count * 2)]
            return Map(zip(pairs[0::2], pairs[1::2]))
        values = [await read_reply(reader) for _ in range(count)]
        return Push(values) if prefix == b">" else values
    raise ProtocolError(f"unknown reply type byte: {prefix!r}")

"""Error types used by the server."""

WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"


class RedisError(Exception):
    """An error that is sent to the client as a RESP error reply.

    The message is used verbatim, so it should already contain the Redis
    error code (``ERR``, ``WRONGTYPE``, ``NOAUTH``, ...).
    """


class ProtocolError(Exception):
    """Raised when a client sends data that is not valid RESP."""

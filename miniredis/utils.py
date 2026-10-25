"""Small parsing and formatting helpers shared by command implementations."""

import fnmatch
import math
import re

from .errors import RedisError

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

_INT_RE = re.compile(rb"^[+-]?[0-9]+$")


def parse_int(value, message="ERR value is not an integer or out of range", *, check_range=True):
    """Parse a strict base-10 integer (Redis does not accept underscores etc.)."""
    if not isinstance(value, bytes) or not _INT_RE.match(value):
        raise RedisError(message)
    result = int(value)
    if check_range and not INT64_MIN <= result <= INT64_MAX:
        raise RedisError(message)
    return result


def parse_float(value, message="ERR value is not a valid float"):
    """Parse a finite float."""
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise RedisError(message) from None
    if not math.isfinite(result):
        raise RedisError(message)
    return result


def parse_score(value, message="ERR value is not a valid float"):
    """Parse a double that is allowed to be +/-infinity (but not NaN)."""
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise RedisError(message) from None
    if math.isnan(result):
        raise RedisError(message)
    return result


def format_float(value):
    """Render a float the way Redis renders scores and floats."""
    if value == math.inf:
        return b"inf"
    if value == -math.inf:
        return b"-inf"
    if value == 0:
        # Avoid "-0".
        return b"0"
    if value == int(value) and abs(value) < 1e17:
        return str(int(value)).encode()
    return repr(value).encode()


def glob_match(name, pattern):
    """Redis-style glob matching on byte strings.

    Latin-1 maps bytes 1:1 to code points, so matching is byte-exact.
    """
    return fnmatch.fnmatchcase(name.decode("latin-1"), pattern.decode("latin-1"))


def wrong_args(command):
    return RedisError(f"ERR wrong number of arguments for '{command}' command")


def syntax_error():
    return RedisError("ERR syntax error")

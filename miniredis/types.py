"""Value containers used in the keyspace.

Plain Python types are enough for strings (``bytes``), lists (``list``) and
sets (``set``), but hashes and sorted sets would both be ``dict``.  Using
explicit subclasses keeps ``TYPE`` and type checks unambiguous (and pickles
fine for snapshots).
"""


class Hash(dict):
    """Field -> value mapping (both bytes)."""


class ZSet(dict):
    """Member -> score mapping (bytes -> float)."""


def type_name(value):
    if value is None:
        return "none"
    if isinstance(value, bytes):
        return "string"
    if isinstance(value, list):
        return "list"
    if isinstance(value, set):
        return "set"
    if isinstance(value, Hash):
        return "hash"
    if isinstance(value, ZSet):
        return "zset"
    raise TypeError(f"unsupported value type: {type(value)!r}")

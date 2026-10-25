"""Command registry.

Every command module registers handlers with ``@command(...)``.  Handlers are
async functions taking ``(client, args)`` where ``args`` excludes the command
name itself.  They return a value to be RESP-encoded, or ``NO_REPLY`` when
they have already written their replies.
"""

from dataclasses import dataclass
from typing import Callable, Optional


@dataclass(frozen=True)
class Command:
    name: str
    handler: Callable
    min_args: int
    max_args: Optional[int]

    @property
    def arity(self):
        """Redis-style arity value (negative means 'at least')."""
        if self.max_args is None:
            return -(self.min_args + 1)
        return self.max_args + 1

    def accepts(self, arg_count):
        if arg_count < self.min_args:
            return False
        if self.max_args is not None and arg_count > self.max_args:
            return False
        return True


COMMANDS = {}


def command(name, min_args, max_args=None):
    def decorator(handler):
        entry = Command(name.upper(), handler, min_args, max_args)
        if entry.name in COMMANDS:
            raise RuntimeError(f"duplicate command: {entry.name}")
        COMMANDS[entry.name] = entry
        return handler

    return decorator


# Importing the modules populates the registry.
from . import (  # noqa: E402,F401
    hashes,
    keys,
    lists,
    pubsub,
    server_cmds,
    sets,
    strings,
    transactions,
    zsets,
)

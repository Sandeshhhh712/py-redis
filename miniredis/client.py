"""Interactive client for miniredis (and any RESP2/RESP3 server).

Usage:
    python -m miniredis.client
    python -m miniredis.client --host 127.0.0.1 --port 6767
    python -m miniredis.client SET greeting "hello world"
"""

import argparse
import asyncio
import shlex
import sys

from . import protocol


def _quote(data):
    text = (
        data.decode("utf-8", "replace")
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{text}"'


def _format_double(value):
    number = repr(float(value))
    if number.endswith(".0"):
        number = number[:-2]
    return number


def _render_inline(value):
    """Render a leaf value on a single line."""
    if isinstance(value, protocol.RespError):
        return f"(error) {value.message}"
    if isinstance(value, protocol.SimpleString):
        return value.value
    if isinstance(value, protocol.Double):
        return f"(double) {_format_double(value.value)}"
    if isinstance(value, bool):
        return "(true)" if value else "(false)"
    if isinstance(value, float):
        return f"(double) {_format_double(value)}"
    if value is None:
        return "(nil)"
    if isinstance(value, bytes):
        return _quote(value)
    if isinstance(value, int):
        return f"(integer) {value}"
    return repr(value)


def _is_aggregate(value):
    return isinstance(value, (list, tuple, protocol.Map, protocol.Push))


def _render_lines(value, indent):
    pad = " " * indent
    if isinstance(value, protocol.Map):
        if not value.items:
            return [pad + "(empty map)"]
        lines = []
        for number, (key, item) in enumerate(value.items, 1):
            prefix = f"{number}# "
            key_text = _render_inline(key)
            if _is_aggregate(item):
                lines.append(pad + prefix + key_text + " =>")
                lines.extend(_render_lines(item, indent + len(prefix)))
            else:
                lines.append(
                    pad + prefix + key_text + " => " + _render_inline(item)
                )
        return lines
    if isinstance(value, (list, tuple, protocol.Push)):
        items = value.values if isinstance(value, protocol.Push) else value
        if not items:
            return [pad + "(empty array)"]
        lines = []
        for number, item in enumerate(items, 1):
            prefix = f"{number}) "
            child_indent = indent + len(prefix)
            child = _render_lines(item, child_indent)
            lines.append(pad + prefix + child[0][child_indent:])
            lines.extend(child[1:])
        return lines
    return [pad + _render_inline(value)]


def render(value):
    return "\n".join(_render_lines(value, 0))


async def send_command(reader, writer, args):
    writer.write(protocol.encode_command(args))
    await writer.drain()
    return await protocol.read_reply(reader)


async def interactive(reader, writer, host, port):
    print(f"Connected to miniredis at {host}:{port}")
    print("Type Redis commands, or .exit to quit. Try: SET foo bar / GET foo")
    while True:
        try:
            line = await asyncio.to_thread(input, f"{host}:{port}> ")
        except (EOFError, KeyboardInterrupt):
            print()
            break
        line = line.strip()
        if not line:
            continue
        if line in (".exit", ".quit", "exit()"):
            break
        try:
            args = shlex.split(line)
        except ValueError as exc:
            print(f"(error) {exc}")
            continue
        if not args:
            continue
        try:
            reply = await send_command(reader, writer, args)
        except (ConnectionError, protocol.ProtocolError) as exc:
            print(f"(error) connection closed: {exc}")
            break
        print(render(reply))


async def amain(options):
    try:
        reader, writer = await asyncio.open_connection(options.host, options.port)
    except OSError as exc:
        print(
            f"Could not connect to {options.host}:{options.port}: {exc}",
            file=sys.stderr,
        )
        return 1
    try:
        if options.command:
            reply = await send_command(reader, writer, options.command)
            print(render(reply))
        else:
            await interactive(reader, writer, options.host, options.port)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionError:
            pass
    return 0


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Interactive miniredis client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6767)
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="optional command to run instead of starting a REPL",
    )
    return parser.parse_args(argv)


def main(argv=None):
    options = parse_args(argv)
    try:
        return asyncio.run(amain(options))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())

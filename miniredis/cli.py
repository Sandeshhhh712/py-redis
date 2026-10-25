"""Command-line entry point for the miniredis server."""

import argparse
import asyncio
import logging
import signal
import sys

from .server import Server


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="A small Redis-compatible server written in Python."
    )
    parser.add_argument("--host", default="127.0.0.1", help="bind address")
    parser.add_argument("--port", type=int, default=6767, help="TCP port")
    parser.add_argument(
        "--databases", type=int, default=16, help="number of logical databases"
    )
    parser.add_argument(
        "--dir", default=".", help="directory for the snapshot file"
    )
    parser.add_argument(
        "--dbfilename", default="dump.mrd", help="snapshot file name"
    )
    parser.add_argument(
        "--save-interval",
        type=int,
        default=60,
        metavar="SECONDS",
        help="autosave interval; 0 disables periodic saves",
    )
    parser.add_argument(
        "--no-persistence",
        action="store_true",
        help="disable snapshots entirely (nothing is written to disk)",
    )
    parser.add_argument(
        "--requirepass",
        default=None,
        metavar="PASSWORD",
        help="require clients to AUTH before running commands",
    )
    parser.add_argument(
        "--log-level",
        default="info",
        choices=["debug", "info", "warning", "error"],
        help="logging verbosity",
    )
    return parser.parse_args(argv)


async def run(args):
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="[%(levelname)s] %(message)s",
    )
    server = Server(
        host=args.host,
        port=args.port,
        databases=args.databases,
        data_dir=args.dir,
        dbfilename=args.dbfilename,
        persistence=not args.no_persistence,
        save_interval=args.save_interval,
        requirepass=args.requirepass,
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, server.request_shutdown)
        except NotImplementedError:
            # Signal handlers are not available on every platform (Windows).
            pass

    await server.start()
    print(
        f"[miniredis] listening on {args.host}:{server.port} "
        f"({len(server.databases)} databases)"
    )
    try:
        await server.wait_shutdown()
    finally:
        await server.stop()
        print("[miniredis] bye")


def main(argv=None):
    args = parse_args(argv)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass
    except OSError as exc:
        print(f"[miniredis] cannot start: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

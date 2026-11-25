"""Command line: ``minidis --port 6380 --aof data.aof``."""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

from . import __version__
from .aof import AOF, FSYNC_POLICIES
from .server import MinidisServer


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="minidis", description="A small Redis-compatible server.")
    p.add_argument("--host", default="127.0.0.1",
                   help="address to listen on (default 127.0.0.1; use 0.0.0.0 only with a password)")
    p.add_argument("--port", type=int, default=6380, help="TCP port (default 6380, next to Redis's 6379)")
    p.add_argument("--aof", metavar="FILE", help="append-only file for persistence (default: memory only)")
    p.add_argument("--fsync", choices=FSYNC_POLICIES, default="everysec", help="when to fsync the AOF")
    p.add_argument("--version", action="version", version=f"minidis {__version__}")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # The password comes from the environment, not a flag: command-line
    # arguments are visible to every user on the machine through `ps`.
    password = os.environ.get("MINIDIS_PASSWORD")
    if args.host not in ("127.0.0.1", "::1", "localhost") and not password:
        logging.warning("listening on %s without MINIDIS_PASSWORD: anyone who can reach the port "
                        "can read and change every key", args.host)

    server = MinidisServer(host=args.host, port=args.port,
                           aof=AOF(args.aof, args.fsync) if args.aof else None,
                           password=password.encode() if password else None)
    try:
        asyncio.run(server.serve_forever())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())

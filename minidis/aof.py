"""Append-only file: every write command is logged, and replayed on start.

The file is plain RESP, the same bytes a client would send, so it can be read
with ``cat`` and replayed with ``redis-cli --pipe``.

Durability depends on when the data reaches the disk (``fsync``):

    always    fsync after every write command: safest, slowest
    everysec  fsync once a second: at most about a second of writes is lost
    no        leave it to the operating system (usually within 30 s)

A crash can cut the last command in half. On load, a truncated tail is
dropped with a warning and the file is cut back to the last whole command.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from .commands import Ctx, Server, Session, execute
from .resp import Error, Parser, encode
from .store import Store

log = logging.getLogger("minidis.aof")

FSYNC_POLICIES = ("always", "everysec", "no")


class AOFError(Exception):
    pass


class AOF:
    def __init__(self, path: str | os.PathLike, fsync: str = "everysec"):
        if fsync not in FSYNC_POLICIES:
            raise ValueError(f"fsync must be one of {FSYNC_POLICIES}")
        self.path = Path(path)
        self.fsync = fsync
        self._file = None
        self._dirty = False

    # -- loading -----------------------------------------------------------------
    def load(self, server: Server) -> int:
        """Replay the file into ``server.store``; returns the number of commands."""
        if not self.path.exists():
            return 0
        data = self.path.read_bytes()
        parser = Parser()
        parser.feed(data)
        ctx = Ctx(Server(server.store), Session(authenticated=True))  # no AOF: replay must not log again
        count = 0
        for argv in parser.commands():
            reply = execute(ctx, argv)
            if isinstance(reply, Error):
                raise AOFError(f"command #{count + 1} failed while loading {self.path}: {reply}")
            count += 1
        if parser.pending:
            good = len(data) - parser.pending
            log.warning("AOF ends with a truncated command (%d bytes); cutting the file back to %d bytes",
                        parser.pending, good)
            with open(self.path, "r+b") as f:
                f.truncate(good)
        return count

    # -- writing -----------------------------------------------------------------
    def open(self) -> None:
        self._file = open(self.path, "ab")

    def append(self, argv: list[bytes]) -> None:
        if self._file is None:
            self.open()
        self._file.write(encode(argv))
        if self.fsync == "always":
            self._sync()
        else:
            self._dirty = True

    def tick(self) -> None:
        """Called once a second by the server."""
        if self._dirty and self._file is not None:
            if self.fsync == "everysec":
                self._sync()
            else:
                self._file.flush()
            self._dirty = False

    def close(self) -> None:
        if self._file is not None:
            self._sync()
            self._file.close()
            self._file = None

    def _sync(self) -> None:
        self._file.flush()               # Python buffer -> operating system
        os.fsync(self._file.fileno())    # operating system -> disk

    # -- compaction ----------------------------------------------------------------
    def rewrite(self, store: Store) -> int:
        """Replace the log with the shortest list of commands that rebuilds ``store``.

        After a million INCRs on one key the log holds a million lines; the
        rewritten file holds one SET. The new file is written next to the old
        one and renamed over it, so a crash halfway leaves the old file intact.

        Redis does this in a forked child process, using copy-on-write memory
        to get a frozen snapshot while it keeps serving clients. minidis does
        it in place, which blocks the server for the duration.
        """
        tmp = self.path.with_name(self.path.name + ".rewrite")
        count = 0
        with open(tmp, "wb") as f:
            for argv in snapshot(store):
                f.write(encode(argv))
                count += 1
            f.flush()
            os.fsync(f.fileno())
        if self._file is not None:
            self._file.close()
        os.replace(tmp, self.path)  # atomic on POSIX and Windows
        self.open()
        return count


def snapshot(store: Store):
    """Commands that recreate every live key, with absolute expiry times."""
    for key in store.keys():
        value = store.data[key]
        if isinstance(value, bytes):
            yield [b"SET", key, value]
        elif isinstance(value, dict):
            yield [b"HSET", key, *[x for pair in value.items() for x in pair]]
        elif isinstance(value, set):
            yield [b"SADD", key, *sorted(value)]
        else:
            yield [b"RPUSH", key, *value]
        deadline = store.expires.get(key)
        if deadline is not None:
            yield [b"PEXPIREAT", key, str(deadline).encode()]

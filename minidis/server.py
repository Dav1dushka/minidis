"""The network part: one asyncio event loop, many clients.

Each connection gets its own coroutine, but commands themselves run as plain
synchronous functions between two ``await``s. Since only one runs at a time,
every command is atomic without a single lock, which is the same reason Redis
itself is (mostly) single-threaded.
"""
from __future__ import annotations

import asyncio
import itertools
import logging

from .aof import AOF
from .commands import Ctx, Server, Session, execute
from .resp import OK, Error, Parser, ProtocolError, encode
from .store import Store

log = logging.getLogger("minidis")

READ_SIZE = 64 * 1024


class MinidisServer:
    def __init__(self, store: Store | None = None, host: str = "127.0.0.1", port: int = 6380,
                 aof: AOF | None = None, password: bytes | None = None):
        self.state = Server(store or Store(), aof=aof, password=password)
        self.host, self.port = host, port
        self._server: asyncio.Server | None = None
        self._tasks: list[asyncio.Task] = []
        self._ids = itertools.count(1)

    @property
    def address(self) -> tuple[str, int]:
        return self._server.sockets[0].getsockname()[:2]

    async def start(self) -> None:
        if self.state.aof is not None:
            loaded = self.state.aof.load(self.state)
            log.info("loaded %d commands from %s", loaded, self.state.aof.path)
            self.state.aof.open()
        self._server = await asyncio.start_server(self._client, self.host, self.port)
        self._tasks = [asyncio.create_task(self._cron())]
        log.info("minidis listening on %s:%d", *self.address)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self.state.aof is not None:
            self.state.aof.close()

    async def serve_forever(self) -> None:
        await self.start()
        try:
            await self._server.serve_forever()
        finally:
            await self.stop()

    # -- background work ---------------------------------------------------------
    async def _cron(self) -> None:
        """Ten times a second: expire keys. Once a second: flush the AOF."""
        for tick in itertools.count():
            await asyncio.sleep(0.1)
            self.state.store.expire_cycle()
            if tick % 10 == 9 and self.state.aof is not None:
                self.state.aof.tick()

    # -- one client --------------------------------------------------------------
    async def _client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        session = Session(id=next(self._ids), authenticated=self.state.password is None)
        ctx = Ctx(self.state, session)
        parser = Parser()
        self.state.clients += 1
        try:
            while data := await reader.read(READ_SIZE):
                parser.feed(data)
                out = bytearray()
                closing = False
                try:
                    # All commands of a pipelined batch run back to back and
                    # their replies go out in one write.
                    for argv in parser.commands():
                        if argv[0].lower() == b"quit":
                            out += encode(OK)
                            closing = True
                            break
                        out += encode(execute(ctx, argv))
                except ProtocolError as exc:
                    out += encode(Error(f"ERR Protocol error: {exc}"))
                    closing = True
                writer.write(out)
                await writer.drain()  # back-pressure: wait if the client reads slowly
                if closing:
                    break
        except ConnectionError:
            pass
        finally:
            self.state.clients -= 1
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

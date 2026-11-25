from __future__ import annotations

import asyncio
import threading

import pytest

from minidis.commands import Ctx, Server, Session, execute
from minidis.server import MinidisServer
from minidis.store import Store

from .client import Client


class FakeClock:
    def __init__(self, t: float = 1_700_000_000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def run(clock):
    """Execute commands directly, without a socket: run("SET", "k", "v")."""
    ctx = Ctx(Server(Store(clock)), Session(authenticated=True))

    def call(*args):
        return execute(ctx, [a if isinstance(a, bytes) else str(a).encode() for a in args])

    call.ctx = ctx
    return call


class ServerThread:
    """A real minidis on a random port, with its event loop in a background thread."""

    def __init__(self, **kwargs):
        self.loop = asyncio.new_event_loop()
        self.server = MinidisServer(port=0, **kwargs)
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        asyncio.run_coroutine_threadsafe(self.server.start(), self.loop).result(5)
        self.host, self.port = self.server.address
        return self

    def __exit__(self, *exc):
        asyncio.run_coroutine_threadsafe(self.server.stop(), self.loop).result(5)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        self.loop.close()

    def client(self) -> Client:
        return Client(self.host, self.port)


@pytest.fixture
def server():
    with ServerThread() as s:
        yield s


@pytest.fixture
def client(server):
    c = server.client()
    yield c
    c.close()

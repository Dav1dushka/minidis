"""A deliberately small blocking RESP client, used only by the tests."""
from __future__ import annotations

import socket

from minidis.resp import encode


class ReplyError(Exception):
    pass


class Client:
    def __init__(self, host: str, port: int, timeout: float = 5.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.buf = b""

    def close(self) -> None:
        self.sock.close()

    def send_raw(self, data: bytes) -> None:
        self.sock.sendall(data)

    def __call__(self, *args):
        """Send one command and return its reply; error replies are raised."""
        self.send_raw(encode([a if isinstance(a, bytes) else str(a).encode() for a in args]))
        return self.reply()

    def pipeline(self, *commands):
        payload = b"".join(encode([str(a).encode() if not isinstance(a, bytes) else a for a in c])
                           for c in commands)
        self.send_raw(payload)
        return [self.reply(raise_errors=False) for _ in commands]

    def reply(self, raise_errors: bool = True):
        kind, line = self._line()
        if kind == "+":
            return line.decode()
        if kind == "-":
            err = ReplyError(line.decode())
            if raise_errors:
                raise err
            return err
        if kind == ":":
            return int(line)
        if kind == "$":
            n = int(line)
            if n == -1:
                return None
            data = self._exact(n + 2)
            return data[:-2]
        if kind == "*":
            return [self.reply(raise_errors) for _ in range(int(line))]
        raise AssertionError(f"unexpected reply type {kind!r}")

    def _line(self) -> tuple[str, bytes]:
        while b"\r\n" not in self.buf:
            self._recv()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return chr(line[0]), line[1:]

    def _exact(self, n: int) -> bytes:
        while len(self.buf) < n:
            self._recv()
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def _recv(self) -> None:
        chunk = self.sock.recv(65536)
        if not chunk:
            raise ConnectionError("server closed the connection")
        self.buf += chunk

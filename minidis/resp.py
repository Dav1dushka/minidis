"""RESP2, the protocol Redis clients speak.

Every value starts with a type byte and ends with CRLF:

    +OK\\r\\n                         simple string
    -ERR message\\r\\n                error
    :42\\r\\n                         integer
    $5\\r\\nhello\\r\\n                 bulk string ($-1 is nil)
    *2\\r\\n$3\\r\\nGET\\r\\n$1\\r\\nk\\r\\n   array

Clients send each command as an array of bulk strings. Typing ``PING`` in
telnet sends an *inline command* instead: one plain line, split on spaces.
"""
from __future__ import annotations

from collections.abc import Iterator

# Limits protect the server from a client that announces a huge value and
# makes us buffer it. Real Redis uses 512 MB for bulk strings.
MAX_BULK = 64 * 1024 * 1024
MAX_ARRAY = 1024 * 1024
MAX_LINE = 64 * 1024


class ProtocolError(Exception):
    """The client sent bytes that are not valid RESP. The connection is closed."""


class Simple(str):
    """A simple-string reply, such as OK or PONG."""


class Error(str):
    """An error reply. By convention the first word is the error type: ERR, WRONGTYPE, ..."""


OK = Simple("OK")


class Parser:
    """Incremental command parser: feed bytes as they arrive, take out whole commands.

    A command can arrive split across many TCP reads, and one read can hold
    many commands (pipelining). Nothing is consumed from the buffer until a
    whole command is there.
    """

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes) -> None:
        self._buf += data

    @property
    def pending(self) -> int:
        """Bytes received that do not form a complete command yet."""
        return len(self._buf)

    def commands(self) -> Iterator[list[bytes]]:
        while (cmd := self._parse_one()) is not None:
            if cmd:  # an empty line or "*0" is ignored, like Redis does
                yield cmd

    # -- internals -------------------------------------------------------------
    def _parse_one(self) -> list[bytes] | None:
        buf = self._buf
        if not buf:
            return None
        if buf[0] != ord("*"):
            return self._inline()

        line, pos = self._line(0)
        if line is None:
            return None
        count = _number(line[1:], "multibulk length")
        if count > MAX_ARRAY:
            raise ProtocolError("invalid multibulk length")
        args: list[bytes] = []
        for _ in range(count):
            if pos >= len(buf):
                return None
            if buf[pos] != ord("$"):
                raise ProtocolError(f"expected '$', got {chr(buf[pos])!r}")
            line, start = self._line(pos)
            if line is None:
                return None
            size = _number(line[1:], "bulk length")
            if not 0 <= size <= MAX_BULK:
                raise ProtocolError("invalid bulk length")
            end = start + size
            if len(buf) < end + 2:
                return None  # the value is still on its way
            if buf[end:end + 2] != b"\r\n":
                raise ProtocolError("bulk string is not followed by CRLF")
            args.append(bytes(buf[start:end]))
            pos = end + 2
        del buf[:pos]
        return args

    def _inline(self) -> list[bytes] | None:
        end = self._buf.find(b"\n")
        if end == -1:
            if len(self._buf) > MAX_LINE:
                raise ProtocolError("too big inline request")
            return None
        line = bytes(self._buf[:end]).rstrip(b"\r")
        del self._buf[:end + 1]
        return line.split()

    def _line(self, start: int) -> tuple[bytes | None, int]:
        end = self._buf.find(b"\r\n", start)
        if end == -1:
            if len(self._buf) - start > MAX_LINE:
                raise ProtocolError("header line too long")
            return None, start
        return bytes(self._buf[start:end]), end + 2


def _number(raw: bytes, what: str) -> int:
    try:
        return int(raw)
    except ValueError:
        raise ProtocolError(f"invalid {what}") from None


def encode(value: object) -> bytes:
    """Turn a Python value into a RESP reply."""
    if value is None:
        return b"$-1\r\n"
    if isinstance(value, Error):
        return b"-" + _one_line(value) + b"\r\n"
    if isinstance(value, Simple):
        return b"+" + _one_line(value) + b"\r\n"
    if isinstance(value, bool):  # bool is an int subclass; never send True as :1 by accident
        raise TypeError("booleans are not RESP2 values")
    if isinstance(value, int):
        return b":%d\r\n" % value
    if isinstance(value, str):
        value = value.encode()
    if isinstance(value, (bytes, bytearray)):
        return b"$%d\r\n%s\r\n" % (len(value), value)
    if isinstance(value, (list, tuple)):
        return b"*%d\r\n" % len(value) + b"".join(encode(v) for v in value)
    raise TypeError(f"cannot encode {type(value).__name__}")


def _one_line(text: str) -> bytes:
    # Simple strings and errors cannot contain CR or LF, or the client
    # would read the rest as a new reply.
    return text.replace("\r", " ").replace("\n", " ").encode()

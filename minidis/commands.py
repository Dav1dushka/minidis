"""Command table and implementations.

Every command is a function ``(ctx, args) -> reply`` registered with
``@command(name, arity, write=...)``. Arity follows the Redis convention and
counts the command name itself: ``2`` means exactly one argument (GET key),
``-3`` means at least two (DEL key [key ...]).

Write commands are appended to the AOF after they run. Most are logged as
they were sent; a few are rewritten first (``ctx.propagate``) so that
replaying the file gives the same result later: a relative TTL like
``EXPIRE k 10`` becomes the absolute ``PEXPIREAT k <unix ms>``.
"""
from __future__ import annotations

import hmac
import re
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from itertools import islice
from typing import TYPE_CHECKING, Any

from . import __version__
from .resp import OK, Error, Simple
from .store import Store, type_name

if TYPE_CHECKING:
    from .aof import AOF

INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1


class CommandError(Exception):
    pass


WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"
NOT_INT = "ERR value is not an integer or out of range"
SYNTAX = "ERR syntax error"


@dataclass
class Session:
    """State of one client connection."""
    id: int = 0
    name: bytes | None = None
    authenticated: bool = False


@dataclass
class Server:
    """Everything commands may need besides the arguments."""
    store: Store
    aof: AOF | None = None
    password: bytes | None = None
    started: float = field(default_factory=time.time)
    clients: int = 0
    commands_processed: int = 0


@dataclass
class Ctx:
    server: Server
    session: Session
    propagate: list[list[bytes]] | None = None  # what to log in the AOF instead of argv

    @property
    def store(self) -> Store:
        return self.server.store


@dataclass
class Command:
    name: str
    handler: Callable[[Ctx, list[bytes]], Any]
    arity: int
    write: bool


COMMANDS: dict[str, Command] = {}
NO_AUTH_NEEDED = {"auth", "ping", "quit", "hello"}


def command(name: str, arity: int, write: bool = False):
    def register(fn):
        COMMANDS[name] = Command(name, fn, arity, write)
        return fn
    return register


def execute(ctx: Ctx, argv: list[bytes]) -> Any:
    """Run one command and return its reply (errors are returned, not raised)."""
    name = argv[0].decode("utf-8", "replace").lower()
    cmd = COMMANDS.get(name)
    if cmd is None:
        shown = " ".join(f"'{a.decode('utf-8', 'replace')[:40]}'" for a in argv[1:4])
        return Error(f"ERR unknown command '{name[:40]}', with args beginning with: {shown}")
    if ctx.server.password is not None and not ctx.session.authenticated and name not in NO_AUTH_NEEDED:
        return Error("NOAUTH Authentication required.")
    if (cmd.arity > 0 and len(argv) != cmd.arity) or (cmd.arity < 0 and len(argv) < -cmd.arity):
        return Error(f"ERR wrong number of arguments for '{name}' command")

    ctx.propagate = None
    try:
        reply = cmd.handler(ctx, argv[1:])
    except CommandError as exc:
        return Error(str(exc))
    ctx.server.commands_processed += 1
    aof = ctx.server.aof
    if cmd.write and aof is not None:
        for logged in (ctx.propagate if ctx.propagate is not None else [argv]):
            aof.append(logged)
    return reply


# -- helpers --------------------------------------------------------------------
def to_int(raw: bytes) -> int:
    # Redis accepts only canonical integers: no spaces, no "+", no "007".
    text = raw.decode("ascii", "replace")
    if not re.fullmatch(r"-?(0|[1-9][0-9]*)", text):
        raise CommandError(NOT_INT)
    value = int(text)
    if not INT64_MIN <= value <= INT64_MAX:
        raise CommandError(NOT_INT)
    return value


def typed(ctx: Ctx, key: bytes, kind: type):
    value = ctx.store.get(key)
    if value is not None and not isinstance(value, kind):
        raise CommandError(WRONGTYPE)
    return value


def glob_to_regex(pattern: bytes) -> re.Pattern[bytes]:
    """Redis glob syntax: * ? [abc] [^a] [a-z] and \\x for a literal x."""
    out, i = [], 0
    while i < len(pattern):
        c = pattern[i:i + 1]
        if c == b"*":
            out.append(b".*")
        elif c == b"?":
            out.append(b".")
        elif c == b"\\" and i + 1 < len(pattern):
            i += 1
            out.append(re.escape(pattern[i:i + 1]))
        elif c == b"[":
            end = pattern.find(b"]", i + 1)
            if end == -1:
                out.append(re.escape(c))
            else:
                body = pattern[i + 1:end]
                negate = body.startswith(b"^")
                if negate:
                    body = body[1:]
                parts = []
                j = 0
                while j < len(body):
                    if j + 2 < len(body) and body[j + 1:j + 2] == b"-":
                        lo, hi = sorted((body[j:j + 1], body[j + 2:j + 3]))
                        parts.append(re.escape(lo) + b"-" + re.escape(hi))
                        j += 3
                    else:
                        parts.append(re.escape(body[j:j + 1]))
                        j += 1
                out.append(b"[" + (b"^" if negate else b"") + b"".join(parts) + b"]" if parts
                           else (b"." if negate else b"(?!)"))
                i = end
        else:
            out.append(re.escape(c))
        i += 1
    return re.compile(b"".join(out), re.DOTALL)


def index_range(start: int, stop: int, length: int) -> range:
    """LRANGE-style inclusive indexes, negative ones counting from the end."""
    if start < 0:
        start = max(length + start, 0)
    if stop < 0:
        stop += length
    stop = min(stop, length - 1)
    return range(start, stop + 1) if start <= stop else range(0)


# -- connection and server ------------------------------------------------------
@command("ping", -1)
def ping(ctx, args):
    if len(args) > 1:
        raise CommandError("ERR wrong number of arguments for 'ping' command")
    return args[0] if args else Simple("PONG")


@command("echo", 2)
def echo(ctx, args):
    return args[0]


@command("auth", -2)
def auth(ctx, args):
    password = ctx.server.password
    if len(args) > 2:
        raise CommandError(SYNTAX)
    if password is None:
        raise CommandError("ERR AUTH <password> called without any password configured for the default user. "
                           "Are you sure your configuration is correct?")
    given = args[-1]  # AUTH password, or AUTH username password
    user_ok = len(args) == 1 or args[0] == b"default"
    if user_ok and hmac.compare_digest(given, password):  # constant time, no timing leak
        ctx.session.authenticated = True
        return OK
    raise CommandError("WRONGPASS invalid username-password pair or user is disabled.")


@command("select", 2)
def select(ctx, args):
    if to_int(args[0]) != 0:
        raise CommandError("ERR DB index is out of range (minidis has a single database)")
    return OK


@command("client", -2)
def client(ctx, args):
    sub = args[0].lower()
    if sub == b"setname" and len(args) == 2:
        ctx.session.name = args[1] or None
        return OK
    if sub == b"getname":
        return ctx.session.name
    if sub == b"id":
        return ctx.session.id
    if sub == b"setinfo":  # sent by client libraries on connect
        return OK
    raise CommandError(f"ERR unknown subcommand '{sub.decode('utf-8', 'replace')}'")


@command("command", -1)
def command_info(ctx, args):
    # redis-cli asks for COMMAND DOCS to show hints; an empty answer is fine.
    if args and args[0].lower() == b"count":
        return len(COMMANDS)
    return []


@command("dbsize", 1)
def dbsize(ctx, args):
    return len(ctx.store)


@command("flushall", -1, write=True)
def flushall(ctx, args):
    ctx.store.flush()
    ctx.propagate = [[b"FLUSHALL"]]
    return OK


COMMANDS["flushdb"] = Command("flushdb", flushall, -1, True)


@command("info", -1)
def info(ctx, args):
    s = ctx.server
    store = s.store
    sections = {
        "server": {"minidis_version": __version__, "redis_version": "7.0.0",  # some clients read this to pick features
                   "uptime_in_seconds": int(time.time() - s.started)},
        "clients": {"connected_clients": s.clients},
        "persistence": {"aof_enabled": int(s.aof is not None),
                        "aof_fsync": s.aof.fsync if s.aof else "off"},
        "stats": {"total_commands_processed": s.commands_processed, "expired_keys": store.expired_count},
        "keyspace": {"db0": f"keys={len(store)},expires={len(store.expires)}"},
    }
    wanted = args[0].decode("utf-8", "replace").lower() if args else None
    lines = []
    for title, fields in sections.items():
        if wanted not in (None, "all", "everything", title):
            continue
        lines.append(f"# {title.capitalize()}")
        lines += [f"{k}:{v}" for k, v in fields.items()]
        lines.append("")
    return "\r\n".join(lines).encode()


@command("bgrewriteaof", 1)
def bgrewriteaof(ctx, args):
    if ctx.server.aof is None:
        raise CommandError("ERR AOF is disabled (start minidis with --aof)")
    ctx.server.aof.rewrite(ctx.store)
    return Simple("Append only file rewritten")  # synchronously, see AOF.rewrite


# -- keys -------------------------------------------------------------------------
@command("del", -2, write=True)
def delete(ctx, args):
    return sum(ctx.store.delete(k) for k in args)


COMMANDS["unlink"] = Command("unlink", delete, -2, True)


@command("exists", -2)
def exists(ctx, args):
    return sum(k in ctx.store for k in args)  # duplicates count twice, as in Redis


@command("type", 2)
def type_(ctx, args):
    value = ctx.store.get(args[0])
    return Simple("none" if value is None else type_name(value))


@command("keys", 2)
def keys(ctx, args):
    rx = glob_to_regex(args[0])
    return [k for k in ctx.store.keys() if rx.fullmatch(k)]


@command("rename", 3, write=True)
def rename(ctx, args):
    src, dst = args
    store = ctx.store
    value = store.get(src)
    if value is None:
        raise CommandError("ERR no such key")
    deadline = store.expires.get(src)
    store.delete(src)
    store.set(dst, value)
    if deadline is not None:
        store.expire_at(dst, deadline)
    return OK


def _expire(ctx, key: bytes, deadline_ms: int) -> int:
    if not ctx.store.expire_at(key, deadline_ms):
        ctx.propagate = []
        return 0
    if key in ctx.store:
        ctx.propagate = [[b"PEXPIREAT", key, str(deadline_ms).encode()]]
    else:
        ctx.propagate = [[b"DEL", key]]  # deadline already passed: the key is gone
    return 1


@command("expire", 3, write=True)
def expire(ctx, args):
    return _expire(ctx, args[0], ctx.store.now_ms() + to_int(args[1]) * 1000)


@command("pexpire", 3, write=True)
def pexpire(ctx, args):
    return _expire(ctx, args[0], ctx.store.now_ms() + to_int(args[1]))


@command("expireat", 3, write=True)
def expireat(ctx, args):
    return _expire(ctx, args[0], to_int(args[1]) * 1000)


@command("pexpireat", 3, write=True)
def pexpireat(ctx, args):
    return _expire(ctx, args[0], to_int(args[1]))


@command("ttl", 2)
def ttl(ctx, args):
    ms = ctx.store.ttl_ms(args[0])
    return ms if ms < 0 else (ms + 500) // 1000


@command("pttl", 2)
def pttl(ctx, args):
    return ctx.store.ttl_ms(args[0])


@command("persist", 2, write=True)
def persist(ctx, args):
    return int(ctx.store.persist(args[0]))


# -- strings ----------------------------------------------------------------------
@command("get", 2)
def get(ctx, args):
    return typed(ctx, args[0], bytes)


@command("set", -3, write=True)
def set_(ctx, args):
    key, value, *opts = args
    store = ctx.store
    mode = deadline = None
    keep_ttl = want_old = False
    i = 0
    while i < len(opts):
        opt = opts[i].upper()
        if opt in (b"NX", b"XX") and mode is None:
            mode = opt
        elif opt == b"GET":
            want_old = True
        elif opt == b"KEEPTTL" and deadline is None:
            keep_ttl = True
        elif opt in (b"EX", b"PX", b"EXAT", b"PXAT") and deadline is None and not keep_ttl and i + 1 < len(opts):
            i += 1
            n = to_int(opts[i])
            if n <= 0:
                raise CommandError("ERR invalid expire time in 'set' command")
            deadline = {b"EX": store.now_ms() + n * 1000, b"PX": store.now_ms() + n,
                        b"EXAT": n * 1000, b"PXAT": n}[opt]
        else:
            raise CommandError(SYNTAX)
        i += 1

    old = typed(ctx, key, bytes) if want_old else store.get(key)
    if (mode == b"NX" and old is not None) or (mode == b"XX" and old is None):
        ctx.propagate = []
        return old if want_old else None
    store.set(key, value, keep_ttl=keep_ttl)
    ctx.propagate = [[b"SET", key, value] + ([b"KEEPTTL"] if keep_ttl else [])]
    if deadline is not None:
        store.expire_at(key, deadline)
        ctx.propagate.append([b"PEXPIREAT", key, str(deadline).encode()])
    return old if want_old else OK


@command("mget", -2)
def mget(ctx, args):
    out = []
    for key in args:
        value = ctx.store.get(key)
        out.append(value if isinstance(value, bytes) else None)  # wrong type reads as nil
    return out


@command("mset", -3, write=True)
def mset(ctx, args):
    if len(args) % 2:
        raise CommandError("ERR wrong number of arguments for 'mset' command")
    for key, value in zip(args[::2], args[1::2], strict=True):
        ctx.store.set(key, value)
    return OK


def _incr(ctx, key: bytes, by: int) -> int:
    current = typed(ctx, key, bytes)
    n = 0 if current is None else to_int(current)
    result = n + by
    if not INT64_MIN <= result <= INT64_MAX:
        raise CommandError("ERR increment or decrement would overflow")
    ctx.store.set(key, str(result).encode(), keep_ttl=True)
    return result


@command("incr", 2, write=True)
def incr(ctx, args):
    return _incr(ctx, args[0], 1)


@command("decr", 2, write=True)
def decr(ctx, args):
    return _incr(ctx, args[0], -1)


@command("incrby", 3, write=True)
def incrby(ctx, args):
    return _incr(ctx, args[0], to_int(args[1]))


@command("decrby", 3, write=True)
def decrby(ctx, args):
    by = to_int(args[1])
    if by == INT64_MIN:
        raise CommandError("ERR decrement would overflow")
    return _incr(ctx, args[0], -by)


@command("append", 3, write=True)
def append(ctx, args):
    current = typed(ctx, args[0], bytes) or b""
    ctx.store.set(args[0], current + args[1], keep_ttl=True)
    return len(current) + len(args[1])


@command("strlen", 2)
def strlen(ctx, args):
    return len(typed(ctx, args[0], bytes) or b"")


# -- lists --------------------------------------------------------------------------
def _push(ctx, args, left: bool) -> int:
    key, *items = args
    lst = typed(ctx, key, deque)
    if lst is None:
        lst = deque()
        ctx.store.set(key, lst)
    (lst.extendleft if left else lst.extend)(items)
    return len(lst)


@command("lpush", -3, write=True)
def lpush(ctx, args):
    return _push(ctx, args, left=True)


@command("rpush", -3, write=True)
def rpush(ctx, args):
    return _push(ctx, args, left=False)


def _pop(ctx, args, left: bool):
    if len(args) > 2:
        raise CommandError(SYNTAX)
    key = args[0]
    count = to_int(args[1]) if len(args) == 2 else None
    if count is not None and count < 0:
        raise CommandError("ERR value is out of range, must be positive")
    lst = typed(ctx, key, deque)
    if lst is None:
        return None
    take = lst.popleft if left else lst.pop
    if count is None:
        item = take()
        out = item
    else:
        out = [take() for _ in range(min(count, len(lst)))]
    ctx.store.drop_if_empty(key)
    return out


@command("lpop", -2, write=True)
def lpop(ctx, args):
    return _pop(ctx, args, left=True)


@command("rpop", -2, write=True)
def rpop(ctx, args):
    return _pop(ctx, args, left=False)


@command("llen", 2)
def llen(ctx, args):
    lst = typed(ctx, args[0], deque)
    return 0 if lst is None else len(lst)


@command("lrange", 4)
def lrange(ctx, args):
    lst = typed(ctx, args[0], deque)
    if lst is None:
        return []
    idx = index_range(to_int(args[1]), to_int(args[2]), len(lst))
    # Indexing a deque in the middle is O(n); islice walks it once instead.
    return list(islice(lst, idx.start, idx.stop))


# -- hashes -------------------------------------------------------------------------
@command("hset", -4, write=True)
def hset(ctx, args):
    key, *pairs = args
    if len(pairs) % 2:
        raise CommandError("ERR wrong number of arguments for 'hset' command")
    h = typed(ctx, key, dict)
    if h is None:
        h = {}
        ctx.store.set(key, h)
    added = 0
    for field_, value in zip(pairs[::2], pairs[1::2], strict=True):
        added += field_ not in h
        h[field_] = value
    return added


@command("hget", 3)
def hget(ctx, args):
    h = typed(ctx, args[0], dict)
    return None if h is None else h.get(args[1])


@command("hdel", -3, write=True)
def hdel(ctx, args):
    key, *fields = args
    h = typed(ctx, key, dict)
    if h is None:
        return 0
    removed = sum(h.pop(f, None) is not None for f in fields)
    ctx.store.drop_if_empty(key)
    return removed


@command("hgetall", 2)
def hgetall(ctx, args):
    h = typed(ctx, args[0], dict) or {}
    return [x for pair in h.items() for x in pair]


@command("hlen", 2)
def hlen(ctx, args):
    return len(typed(ctx, args[0], dict) or {})


@command("hexists", 3)
def hexists(ctx, args):
    return int(args[1] in (typed(ctx, args[0], dict) or {}))


@command("hincrby", 4, write=True)
def hincrby(ctx, args):
    key, field_, by = args
    h = typed(ctx, key, dict)
    if h is None:
        h = {}
        ctx.store.set(key, h)
    current = h.get(field_)
    try:
        n = 0 if current is None else to_int(current)
    except CommandError:
        raise CommandError("ERR hash value is not an integer") from None
    result = n + to_int(by)
    if not INT64_MIN <= result <= INT64_MAX:
        raise CommandError("ERR increment or decrement would overflow")
    h[field_] = str(result).encode()
    return result


# -- sets ---------------------------------------------------------------------------
@command("sadd", -3, write=True)
def sadd(ctx, args):
    key, *members = args
    s = typed(ctx, key, set)
    if s is None:
        s = set()
        ctx.store.set(key, s)
    before = len(s)
    s.update(members)
    return len(s) - before


@command("srem", -3, write=True)
def srem(ctx, args):
    key, *members = args
    s = typed(ctx, key, set)
    if s is None:
        return 0
    before = len(s)
    s.difference_update(members)
    ctx.store.drop_if_empty(key)
    return before - len(s)


@command("smembers", 2)
def smembers(ctx, args):
    return sorted(typed(ctx, args[0], set) or ())


@command("sismember", 3)
def sismember(ctx, args):
    return int(args[1] in (typed(ctx, args[0], set) or ()))


@command("scard", 2)
def scard(ctx, args):
    return len(typed(ctx, args[0], set) or ())

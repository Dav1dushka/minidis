import pytest

from minidis.aof import AOF, AOFError
from minidis.commands import Ctx, Server, Session, execute
from minidis.resp import encode
from minidis.store import Store

from .conftest import FakeClock


def make(tmp_path, clock, fsync="always"):
    aof = AOF(tmp_path / "data.aof", fsync)
    server = Server(Store(clock), aof=aof)
    aof.load(server)
    aof.open()
    ctx = Ctx(server, Session(authenticated=True))

    def run(*args):
        return execute(ctx, [str(a).encode() for a in args])

    return aof, server, run


def reopen(tmp_path, clock):
    aof, server, run = make(tmp_path, clock)
    return server.store, run


def test_writes_survive_a_restart(tmp_path):
    clock = FakeClock()
    aof, _, run = make(tmp_path, clock)
    run("SET", "name", "minidis")
    run("INCR", "visits")
    run("INCR", "visits")
    run("RPUSH", "queue", "a", "b")
    run("LPOP", "queue")
    run("HSET", "user", "lang", "python")
    run("SADD", "tags", "x", "y")
    run("DEL", "tags")
    run("GET", "name")                        # reads are not logged
    aof.close()

    _, run2 = reopen(tmp_path, clock)
    assert run2("GET", "name") == b"minidis"
    assert run2("GET", "visits") == b"2"
    assert run2("LRANGE", "queue", "0", "-1") == [b"b"]
    assert run2("HGET", "user", "lang") == b"python"
    assert run2("EXISTS", "tags") == 0


def test_relative_ttl_is_logged_as_an_absolute_deadline(tmp_path):
    clock = FakeClock()
    aof, _, run = make(tmp_path, clock)
    run("SET", "session", "abc", "EX", "60")
    aof.close()
    assert b"PEXPIREAT" in aof.path.read_bytes()
    assert b"EX" not in aof.path.read_bytes().replace(b"PEXPIREAT", b"")

    clock.advance(45)                         # the server was down for 45 s
    _, run2 = reopen(tmp_path, clock)
    assert run2("TTL", "session") == 15       # not 60 again

    clock.advance(30)
    _, run3 = reopen(tmp_path, clock)
    assert run3("EXISTS", "session") == 0     # expired while the server was down


def test_commands_that_change_nothing_are_not_logged(tmp_path):
    clock = FakeClock()
    aof, _, run = make(tmp_path, clock)
    run("SET", "k", "v")
    size = aof.path.stat().st_size
    run("SET", "k", "other", "NX")            # refused: key exists
    run("EXPIRE", "missing", "10")            # no such key
    assert aof.path.stat().st_size == size


def test_truncated_tail_is_dropped_and_cut(tmp_path):
    path = tmp_path / "data.aof"
    good = encode([b"SET", b"a", b"1"]) + encode([b"SET", b"b", b"2"])
    path.write_bytes(good + encode([b"SET", b"c", b"3"])[:-4])   # crash mid-write

    store, run = reopen(tmp_path, FakeClock())
    assert run("GET", "a") == b"1"
    assert run("GET", "b") == b"2"
    assert run("GET", "c") is None
    assert path.read_bytes() == good


def test_corrupt_file_refuses_to_load(tmp_path):
    (tmp_path / "data.aof").write_bytes(encode([b"INCR", b"k"]) * 2 + encode([b"RPUSH", b"k", b"x"]))
    with pytest.raises(AOFError, match="WRONGTYPE"):
        reopen(tmp_path, FakeClock())


def test_rewrite_compacts_the_log(tmp_path):
    clock = FakeClock()
    aof, server, run = make(tmp_path, clock)
    for _ in range(500):
        run("INCR", "counter")
    run("RPUSH", "l", "a", "b", "c")
    run("SET", "temp", "x", "PX", "5000")
    run("SET", "gone", "x")
    run("DEL", "gone")
    before = aof.path.stat().st_size

    assert run("BGREWRITEAOF") == "Append only file rewritten"
    assert aof.path.stat().st_size < before / 10
    run("SET", "after", "rewrite")            # appending still works after the swap
    aof.close()

    clock.advance(1)
    _, run2 = reopen(tmp_path, clock)
    assert run2("GET", "counter") == b"500"
    assert run2("LRANGE", "l", "0", "-1") == [b"a", b"b", b"c"]
    assert run2("PTTL", "temp") == 4000
    assert run2("EXISTS", "gone") == 0
    assert run2("GET", "after") == b"rewrite"
    assert not (tmp_path / "data.aof.rewrite").exists()


def test_everysec_writes_reach_the_file_on_tick(tmp_path):
    clock = FakeClock()
    aof, _, run = make(tmp_path, clock, fsync="everysec")
    run("SET", "k", "v")
    aof.tick()
    assert aof.path.read_bytes() == encode([b"SET", b"k", b"v"])

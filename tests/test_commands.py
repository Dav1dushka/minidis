import pytest

from minidis.commands import glob_to_regex
from minidis.resp import Error


def is_error(reply, prefix: str) -> bool:
    return isinstance(reply, Error) and reply.startswith(prefix)


# -- strings ----------------------------------------------------------------------
def test_set_get_and_overwrite(run):
    assert run("SET", "k", "v1") == "OK"
    assert run("GET", "k") == b"v1"
    run("SET", "k", "v2")
    assert run("GET", "k") == b"v2"
    assert run("GET", "missing") is None


def test_set_nx_xx_and_get(run):
    assert run("SET", "k", "a", "NX") == "OK"
    assert run("SET", "k", "b", "NX") is None
    assert run("SET", "other", "x", "XX") is None
    assert run("SET", "k", "c", "XX", "GET") == b"a"
    assert run("GET", "k") == b"c"
    assert is_error(run("SET", "k", "v", "NX", "XX"), "ERR syntax error")


def test_incr_family(run):
    assert run("INCR", "n") == 1
    assert run("INCRBY", "n", "41") == 42
    assert run("DECRBY", "n", "2") == 40
    assert run("DECR", "n") == 39
    run("SET", "s", "abc")
    assert is_error(run("INCR", "s"), "ERR value is not an integer")
    run("SET", "big", str(2**63 - 1))
    assert is_error(run("INCR", "big"), "ERR increment or decrement would overflow")


@pytest.mark.parametrize("text", ["01", " 1", "1.5", "+1", "", "9223372036854775808"])
def test_only_canonical_integers_are_accepted(run, text):
    assert is_error(run("INCRBY", "n", text), "ERR value is not an integer")


def test_append_strlen_mget_mset(run):
    assert run("APPEND", "k", "Hello") == 5
    assert run("APPEND", "k", " World") == 11
    assert run("STRLEN", "k") == 11
    run("MSET", "a", "1", "b", "2")
    run("RPUSH", "list", "x")
    assert run("MGET", "a", "b", "nope", "list") == [b"1", b"2", None, None]
    assert is_error(run("MSET", "a", "1", "b"), "ERR wrong number of arguments")


# -- keys and types ---------------------------------------------------------------------
def test_wrong_type_errors(run):
    run("RPUSH", "l", "a")
    assert is_error(run("GET", "l"), "WRONGTYPE")
    assert is_error(run("HSET", "l", "f", "v"), "WRONGTYPE")
    assert is_error(run("SADD", "l", "m"), "WRONGTYPE")
    assert run("TYPE", "l") == "list"
    assert run("TYPE", "nothing") == "none"
    run("SET", "l", "now a string")  # SET replaces any type
    assert run("TYPE", "l") == "string"


def test_del_exists_rename_dbsize(run):
    run("MSET", "a", "1", "b", "2")
    assert run("EXISTS", "a", "a", "c") == 2  # duplicates count twice, as in Redis
    assert run("DEL", "a", "c") == 1
    assert run("RENAME", "b", "z") == "OK"
    assert run("GET", "z") == b"2"
    assert is_error(run("RENAME", "nope", "x"), "ERR no such key")
    assert run("DBSIZE") == 1


def test_keys_glob(run):
    for k in ["user:1", "user:2", "user:10", "session:a", "u[1]"]:
        run("SET", k, "x")
    assert sorted(run("KEYS", "user:*")) == [b"user:1", b"user:10", b"user:2"]
    assert sorted(run("KEYS", "user:?")) == [b"user:1", b"user:2"]
    assert sorted(run("KEYS", "user:[12]")) == [b"user:1", b"user:2"]
    assert run("KEYS", "u\\[1\\]") == [b"u[1]"]
    assert len(run("KEYS", "*")) == 5


@pytest.mark.parametrize("pattern,yes,no", [
    (b"h?llo", [b"hello", b"hallo"], [b"hllo", b"heello"]),
    (b"h*llo", [b"hllo", b"heeeello"], [b"hello!"]),
    (b"h[ae]llo", [b"hello", b"hallo"], [b"hillo"]),
    (b"h[^e]llo", [b"hallo", b"hbllo"], [b"hello"]),
    (b"h[a-b]llo", [b"hallo", b"hbllo"], [b"hcllo"]),
    (b"a.b", [b"a.b"], [b"axb"]),  # regex characters are literal in globs
])
def test_glob_matches_redis_rules(pattern, yes, no):
    rx = glob_to_regex(pattern)
    assert all(rx.fullmatch(s) for s in yes)
    assert not any(rx.fullmatch(s) for s in no)


# -- expiry -----------------------------------------------------------------------------
def test_ttl_lifecycle(run, clock):
    run("SET", "k", "v", "EX", "10")
    assert run("TTL", "k") == 10
    assert run("PTTL", "k") == 10_000
    clock.advance(4)
    assert run("TTL", "k") == 6
    clock.advance(6)
    assert run("GET", "k") is None          # lazy expiry on access
    assert run("TTL", "k") == -2
    run("SET", "p", "v")
    assert run("TTL", "p") == -1


def test_expire_persist_and_overwrite(run, clock):
    run("SET", "k", "v")
    assert run("EXPIRE", "k", "100") == 1
    assert run("PERSIST", "k") == 1
    assert run("TTL", "k") == -1
    run("EXPIRE", "k", "100")
    run("SET", "k", "new")                   # a plain SET clears the TTL
    assert run("TTL", "k") == -1
    run("EXPIRE", "k", "100")
    run("APPEND", "k", "!")                  # modifying commands keep it
    assert run("TTL", "k") == 100
    assert run("EXPIRE", "missing", "10") == 0


def test_expire_in_the_past_deletes(run):
    run("SET", "k", "v")
    assert run("EXPIRE", "k", "-1") == 1
    assert run("EXISTS", "k") == 0


def test_active_expiry_frees_keys_nobody_reads(run, clock):
    store = run.ctx.store
    for i in range(100):
        run("SET", f"k{i}", "v", "PX", str(100 + i))
    run("SET", "forever", "v")
    clock.advance(0.150)
    removed = store.expire_cycle()
    assert removed == 51                     # k0..k50 are due, the rest are not
    assert len(store.data) == 50
    clock.advance(10)
    store.expire_cycle()
    assert list(store.data) == [b"forever"]


def test_active_expiry_ignores_stale_heap_entries(run, clock):
    run("SET", "k", "v", "EX", "1")
    run("EXPIRE", "k", "100")                # the first deadline is now stale
    clock.advance(2)
    assert run.ctx.store.expire_cycle() == 0
    assert run("EXISTS", "k") == 1


def test_expire_cycle_respects_its_budget(run, clock):
    for i in range(50):
        run("SET", f"k{i}", "v", "PX", "10")
    clock.advance(1)
    assert run.ctx.store.expire_cycle(budget=20) == 20
    assert run.ctx.store.expire_cycle(budget=20) == 20


# -- lists, hashes, sets ----------------------------------------------------------------
def test_lists(run):
    assert run("RPUSH", "l", "b", "c") == 2
    assert run("LPUSH", "l", "a", "z") == 4      # LPUSH a z -> z a b c
    assert run("LRANGE", "l", "0", "-1") == [b"z", b"a", b"b", b"c"]
    assert run("LRANGE", "l", "1", "2") == [b"a", b"b"]
    assert run("LRANGE", "l", "-2", "100") == [b"b", b"c"]
    assert run("LRANGE", "l", "3", "1") == []
    assert run("LPOP", "l") == b"z"
    assert run("RPOP", "l", "2") == [b"c", b"b"]
    assert run("LLEN", "l") == 1
    run("LPOP", "l")
    assert run("EXISTS", "l") == 0               # an empty list disappears


def test_hashes(run):
    assert run("HSET", "h", "name", "Ada", "lang", "py") == 2
    assert run("HSET", "h", "lang", "rust") == 0
    assert run("HGET", "h", "lang") == b"rust"
    assert run("HGETALL", "h") == [b"name", b"Ada", b"lang", b"rust"]
    assert run("HINCRBY", "h", "visits", "5") == 5
    assert is_error(run("HINCRBY", "h", "name", "1"), "ERR hash value is not an integer")
    assert run("HDEL", "h", "name", "lang", "visits", "nope") == 3
    assert run("EXISTS", "h") == 0


def test_sets(run):
    assert run("SADD", "s", "b", "a", "b") == 2
    assert run("SISMEMBER", "s", "a") == 1
    assert run("SMEMBERS", "s") == [b"a", b"b"]
    assert run("SCARD", "s") == 2
    assert run("SREM", "s", "a", "b", "c") == 2
    assert run("EXISTS", "s") == 0


# -- protocol-level behaviour ---------------------------------------------------------------
def test_arity_and_unknown_commands(run):
    assert is_error(run("GET"), "ERR wrong number of arguments for 'get' command")
    assert is_error(run("GET", "a", "b"), "ERR wrong number of arguments")
    assert is_error(run("FLY", "away"), "ERR unknown command 'fly'")
    assert run("get", "x") is None                # command names are case-insensitive


def test_auth_required_when_password_set(run):
    ctx = run.ctx
    ctx.server.password = b"s3cret-pass"
    ctx.session.authenticated = False
    assert is_error(run("GET", "k"), "NOAUTH")
    assert run("PING") == "PONG"                  # PING is allowed, as in Redis
    assert is_error(run("AUTH", "wrong"), "WRONGPASS")
    assert is_error(run("AUTH", "admin", "s3cret-pass"), "WRONGPASS")
    assert run("AUTH", "s3cret-pass") == "OK"
    assert run("GET", "k") is None


def test_hello_handshake(run):
    reply = run("HELLO", "2", "SETNAME", "worker")
    assert dict(zip(reply[::2], reply[1::2], strict=True))[b"proto"] == 2
    assert run("CLIENT", "GETNAME") == b"worker"
    assert is_error(run("HELLO", "3"), "NOPROTO")      # RESP3 is not implemented
    assert is_error(run("HELLO", "9"), "NOPROTO")


def test_hello_can_authenticate(run):
    run.ctx.server.password = b"pw-1234567"
    run.ctx.session.authenticated = False
    assert is_error(run("HELLO", "2"), "NOAUTH")
    assert is_error(run("HELLO", "2", "AUTH", "default", "nope"), "WRONGPASS")
    assert run("HELLO", "2", "AUTH", "default", "pw-1234567")[0] == b"server"
    assert run("GET", "k") is None

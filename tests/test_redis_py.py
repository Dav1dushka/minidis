"""Compatibility with the official Python client, redis-py (installed in CI)."""
import pytest

redis = pytest.importorskip("redis")


def test_redis_py_talks_to_minidis(server):
    # protocol=2: minidis implements RESP2 only, and recent redis-py versions ask for RESP3 by default.
    r = redis.Redis(host=server.host, port=server.port, protocol=2, decode_responses=True)
    assert r.ping() is True
    assert r.set("greeting", "hello", ex=30) is True
    assert r.get("greeting") == "hello"
    assert 0 < r.ttl("greeting") <= 30
    assert r.incr("n", 5) == 5
    r.rpush("queue", "a", "b", "c")
    assert r.lrange("queue", 0, -1) == ["a", "b", "c"]
    r.hset("user:1", mapping={"name": "Ada", "lang": "python"})
    assert r.hgetall("user:1") == {"name": "Ada", "lang": "python"}
    assert r.sadd("tags", "x", "y") == 2
    assert r.smembers("tags") == {"x", "y"}
    with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
        r.get("queue")

    pipe = r.pipeline(transaction=False)
    for _ in range(100):
        pipe.incr("counter")
    assert pipe.execute()[-1] == 100
    r.close()

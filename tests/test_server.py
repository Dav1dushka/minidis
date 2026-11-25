"""End-to-end tests over real TCP sockets."""
import socket
import threading
import time

import pytest

from minidis.aof import AOF

from .client import ReplyError
from .conftest import ServerThread


def test_basic_round_trip(client):
    assert client("PING") == "PONG"
    assert client("SET", "k", "v") == "OK"
    assert client("GET", "k") == b"v"
    with pytest.raises(ReplyError, match="WRONGTYPE"):
        client("RPUSH", "k", "x")


def test_pipelining_keeps_order(client):
    commands = [("INCR", "n")] * 500 + [("GET", "n")]
    replies = client.pipeline(*commands)
    assert replies[:500] == list(range(1, 501))
    assert replies[-1] == b"500"


def test_large_value_split_across_many_packets(client):
    value = bytes(range(256)) * 8192          # 2 MB, every byte value
    assert client("SET", "big", value) == "OK"
    assert client("STRLEN", "big") == len(value)
    assert client("GET", "big") == value


def test_inline_commands_work_over_telnet(server):
    with socket.create_connection((server.host, server.port), timeout=5) as s:
        s.sendall(b"PING\r\nSET a 1\r\nGET a\r\n")
        data = b""
        while data.count(b"\r\n") < 4:
            data += s.recv(1024)
    assert data == b"+PONG\r\n+OK\r\n$1\r\n1\r\n"


def test_protocol_error_closes_only_that_connection(server):
    bad = server.client()
    bad.send_raw(b"*1\r\n$abc\r\n")
    assert str(bad.reply(raise_errors=False)).startswith("ERR Protocol error")
    with pytest.raises(ConnectionError):
        bad.reply()
    good = server.client()
    assert good("PING") == "PONG"
    good.close()


def test_quit(client):
    assert client("QUIT") == "OK"
    with pytest.raises(ConnectionError):
        client.reply()


def test_many_clients_increment_atomically(server):
    def worker():
        c = server.client()
        c.pipeline(*[("INCR", "shared")] * 200)
        c.close()

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    c = server.client()
    assert c("GET", "shared") == b"2000"      # no lost updates without any lock
    c.close()


def test_active_expiry_runs_in_the_background(client, server):
    for i in range(50):
        client("SET", f"tmp{i}", "x", "PX", "50")
    time.sleep(0.4)
    # Nobody read the keys, yet the background cycle removed them.
    assert len(server.server.state.store.data) == 0


def test_password_protection():
    with ServerThread(password=b"correct horse battery") as s:
        c = s.client()
        with pytest.raises(ReplyError, match="NOAUTH"):
            c("GET", "k")
        assert c("AUTH", "correct horse battery") == "OK"
        assert c("SET", "k", "v") == "OK"
        c.close()


def test_data_survives_server_restart(tmp_path):
    path = tmp_path / "db.aof"
    with ServerThread(aof=AOF(path, "always")) as s:
        c = s.client()
        c("SET", "persisted", "yes")
        c("RPUSH", "jobs", "a", "b")
        c.close()
    with ServerThread(aof=AOF(path, "always")) as s:
        c = s.client()
        assert c("GET", "persisted") == b"yes"
        assert c("LRANGE", "jobs", "0", "-1") == [b"a", b"b"]
        c.close()


def test_info_reports_clients_and_keys(client):
    client("SET", "a", "1")
    info = client("INFO").decode()
    assert "connected_clients:1" in info
    assert "db0:keys=1,expires=0" in info


def test_client_library_handshake_commands(client):
    # redis-py and others send these right after connecting.
    assert client("CLIENT", "SETINFO", "LIB-NAME", "redis-py") == "OK"
    assert client("CLIENT", "SETNAME", "worker-1") == "OK"
    assert client("CLIENT", "GETNAME") == b"worker-1"
    assert client("SELECT", "0") == "OK"
    assert client("COMMAND", "DOCS") == []



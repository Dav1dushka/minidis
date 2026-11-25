import pytest

from minidis.resp import MAX_BULK, Error, Parser, ProtocolError, Simple, encode


def parse_all(data: bytes):
    p = Parser()
    p.feed(data)
    return list(p.commands()), p.pending


def test_array_of_bulk_strings():
    cmds, pending = parse_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n")
    assert cmds == [[b"SET", b"k", b"hello"]]
    assert pending == 0


def test_values_may_contain_crlf_and_any_byte():
    value = b"line1\r\nline2\x00\xff"
    cmds, _ = parse_all(encode([b"SET", b"k", value]))
    assert cmds == [[b"SET", b"k", value]]


def test_byte_by_byte_feeding_gives_the_same_result():
    data = encode([b"SET", b"key", b"x" * 300]) + encode([b"GET", b"key"])
    p = Parser()
    got = []
    for i in range(len(data)):
        p.feed(data[i:i + 1])
        got += list(p.commands())
    assert got == [[b"SET", b"key", b"x" * 300], [b"GET", b"key"]]
    assert p.pending == 0


def test_pipelined_commands_and_incomplete_tail():
    data = encode([b"PING"]) * 3 + b"*2\r\n$3\r\nGET\r\n$1\r"
    cmds, pending = parse_all(data)
    assert cmds == [[b"PING"]] * 3
    assert pending == len(b"*2\r\n$3\r\nGET\r\n$1\r")


def test_inline_commands_like_telnet():
    cmds, _ = parse_all(b"PING\r\nSET  a   1\n\r\n")
    assert cmds == [[b"PING"], [b"SET", b"a", b"1"]]


@pytest.mark.parametrize("bad", [
    b"*x\r\n",                       # not a number
    b"*1\r\n:5\r\n",                 # arguments must be bulk strings
    b"*1\r\n$-5\r\n",                # negative length
    b"*1\r\n$3\r\nabcXY",            # missing CRLF after the value
    b"*1\r\n$%d\r\n" % (MAX_BULK + 1),  # announces a value larger than the limit
])
def test_malformed_input_is_rejected(bad):
    with pytest.raises(ProtocolError):
        parse_all(bad)


def test_endless_inline_line_is_rejected():
    with pytest.raises(ProtocolError):
        parse_all(b"A" * 70_000)


def test_encode_every_type():
    assert encode(Simple("OK")) == b"+OK\r\n"
    assert encode(Error("ERR bad")) == b"-ERR bad\r\n"
    assert encode(42) == b":42\r\n"
    assert encode(b"hi") == b"$2\r\nhi\r\n"
    assert encode(None) == b"$-1\r\n"
    assert encode([b"a", 1, None, [b"b"]]) == b"*4\r\n$1\r\na\r\n:1\r\n$-1\r\n*1\r\n$1\r\nb\r\n"


def test_simple_strings_cannot_smuggle_a_second_reply():
    assert encode(Error("ERR a\r\n+OK")) == b"-ERR a  +OK\r\n"

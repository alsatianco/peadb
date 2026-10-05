#!/usr/bin/env python3
"""Batch 5 — protocol fidelity & command gaps.

Covers: HyperLogLog (PFADD/PFCOUNT/PFMERGE), SMOVE, ZRANGESTORE, PUBSUB introspection,
sharded pub/sub aliases, *_RO variants, COMMAND (full table / COUNT / GETKEYS), and
RESP3 HGETALL maps.
"""
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
SERVER = ROOT / "peadb-server"


def wait_up(port, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5) as s:
                s.sendall(b"*1\r\n$4\r\nPING\r\n")
                if s.recv(64).startswith(b"+PONG"):
                    return True
        except OSError:
            pass
        time.sleep(0.05)
    return False


def conn(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=2)
    s.settimeout(2)
    return s


def rx(s, n):
    b = b""
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c:
            raise RuntimeError("closed")
        b += c
    return b


def rl(s):
    b = b""
    while not b.endswith(b"\r\n"):
        b += rx(s, 1)
    return b[:-2]


def recv(s):
    p = rx(s, 1)
    if p == b"+":
        return rl(s).decode()
    if p == b"-":
        return ("ERR", rl(s).decode())
    if p == b":":
        return int(rl(s))
    if p == b",":  # RESP3 double
        return float(rl(s))
    if p in (b"$", b"="):
        n = int(rl(s))
        if n == -1:
            return None
        v = rx(s, n); rx(s, 2); return v.decode("latin1")
    if p in (b"*", b"~", b">"):
        n = int(rl(s))
        if n == -1:
            return None
        return [recv(s) for _ in range(n)]
    if p == b"%":
        n = int(rl(s))
        return {recv(s): recv(s) for _ in range(n)}
    if p == b"_":
        rl(s); return None
    raise RuntimeError("unexpected type %r" % p)


def cmd(s, *a):
    out = b"*%d\r\n" % len(a)
    for x in a:
        b = x.encode() if isinstance(x, str) else x
        out += b"$%d\r\n%s\r\n" % (len(b), b)
    s.sendall(out)
    return recv(s)


def expect(c, m):
    if not c:
        raise AssertionError(m)


def start(port, *extra):
    return subprocess.Popen([str(SERVER), "--port", str(port), "--bind", "127.0.0.1", "--loglevel", "warning", *extra])


def test_hyperloglog():
    port = 6520
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        for e in ("a", "b", "c", "d", "e"):
            cmd(s, "PFADD", "hll", e)
        n = cmd(s, "PFCOUNT", "hll")
        expect(isinstance(n, int) and 4 <= n <= 6, f"PFCOUNT ~5, got {n}")
        # add many; cardinality should grow well past 5
        for i in range(2000):
            cmd(s, "PFADD", "hll", f"x{i}")
        n2 = cmd(s, "PFCOUNT", "hll")
        expect(n2 > 1800 and n2 < 2200, f"PFCOUNT ~2005 (±~1%%), got {n2}")
        # merge
        cmd(s, "PFADD", "h2", "p", "q", "r")
        expect(cmd(s, "PFMERGE", "dest", "hll", "h2") == "OK", "PFMERGE ok")
        nm = cmd(s, "PFCOUNT", "dest")
        expect(nm >= n2, f"merged count >= {n2}, got {nm}")
        # wrong type
        cmd(s, "SET", "str", "notanhll")
        r = cmd(s, "PFADD", "str", "x")
        expect(isinstance(r, tuple) and "WRONGTYPE" in r[1], f"PFADD on plain string should be WRONGTYPE, got {r}")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_smove_zrangestore():
    port = 6521
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        cmd(s, "SADD", "s1", "x", "y")
        expect(cmd(s, "SMOVE", "s1", "s2", "x") == 1, "SMOVE moves member")
        expect(cmd(s, "SISMEMBER", "s2", "x") == 1, "member now in dest")
        expect(cmd(s, "SISMEMBER", "s1", "x") == 0, "member gone from src")
        expect(cmd(s, "SMOVE", "s1", "s2", "nope") == 0, "SMOVE missing member → 0")
        cmd(s, "ZADD", "z", "1", "a", "2", "b", "3", "c")
        expect(cmd(s, "ZRANGESTORE", "dst", "z", "0", "-1") == 3, "ZRANGESTORE stores 3")
        expect(cmd(s, "ZCARD", "dst") == 3, "dst has 3 members")
        expect(cmd(s, "ZRANGESTORE", "dst2", "z", "0", "1") == 2, "ZRANGESTORE range of 2")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_pubsub_introspection():
    port = 6522
    p = start(port)
    try:
        expect(wait_up(port), "up")
        sub = conn(port)
        cmd(sub, "SUBSCRIBE", "news")  # reply consumed by recv (subscribe confirmation)
        s = conn(port)
        chans = cmd(s, "PUBSUB", "CHANNELS")
        expect(isinstance(chans, list) and "news" in chans, f"PUBSUB CHANNELS should list 'news', got {chans}")
        numsub = cmd(s, "PUBSUB", "NUMSUB", "news")
        expect(numsub == ["news", 1], f"PUBSUB NUMSUB news → 1, got {numsub}")
        expect(isinstance(cmd(s, "PUBSUB", "NUMPAT"), int), "PUBSUB NUMPAT is an int")
        sub.close(); s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_ro_variants_and_command():
    port = 6523
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        cmd(s, "RPUSH", "mylist", "3", "1", "2")
        r = cmd(s, "SORT_RO", "mylist", "STORE", "dest")
        expect(isinstance(r, tuple) and r[0] == "ERR", "SORT_RO STORE should error")
        sorted_ok = cmd(s, "SORT_RO", "mylist")
        expect(sorted_ok == ["1", "2", "3"], f"SORT_RO sorts, got {sorted_ok}")
        # COMMAND
        expect(cmd(s, "COMMAND", "COUNT") > 200, "COMMAND COUNT > 200")
        full = cmd(s, "COMMAND")
        expect(isinstance(full, list) and len(full) > 200, "COMMAND (no args) returns full table")
        keys = cmd(s, "COMMAND", "GETKEYS", "SET", "foo", "bar")
        expect(keys == ["foo"], f"COMMAND GETKEYS SET foo bar → [foo], got {keys}")
        keys2 = cmd(s, "COMMAND", "GETKEYS", "MSET", "k1", "v1", "k2", "v2")
        expect("k1" in keys2 and "k2" in keys2, f"COMMAND GETKEYS MSET → k1,k2, got {keys2}")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_sharded_pubsub_and_more_ro():
    port = 6525
    p = start(port)
    try:
        expect(wait_up(port), "up")
        # sharded pub/sub (aliases regular pub/sub in standalone)
        sub = conn(port)
        cmd(sub, "SSUBSCRIBE", "shard1")  # confirmation consumed
        pub = conn(port)
        n = cmd(pub, "SPUBLISH", "shard1", "hi")
        expect(isinstance(n, int) and n >= 1, f"SPUBLISH delivered to >=1, got {n}")
        cmd(sub, "SUNSUBSCRIBE", "shard1")
        sub.close()
        # BITFIELD_RO: GET allowed, SET rejected
        got = cmd(pub, "BITFIELD_RO", "bf", "GET", "u8", "0")
        expect(isinstance(got, list), f"BITFIELD_RO GET returns array, got {got}")
        r = cmd(pub, "BITFIELD_RO", "bf", "SET", "u8", "0", "5")
        expect(isinstance(r, tuple) and r[0] == "ERR", "BITFIELD_RO SET should error")
        # GEORADIUS_RO / GEORADIUSBYMEMBER_RO reject STORE
        cmd(pub, "GEOADD", "geo", "13.361389", "38.115556", "palermo")
        r = cmd(pub, "GEORADIUS_RO", "geo", "15", "37", "200", "km", "STORE", "x")
        expect(isinstance(r, tuple) and r[0] == "ERR", "GEORADIUS_RO STORE should error")
        r = cmd(pub, "GEORADIUSBYMEMBER_RO", "geo", "palermo", "200", "km", "STORE", "x")
        expect(isinstance(r, tuple) and r[0] == "ERR", "GEORADIUSBYMEMBER_RO STORE should error")
        pub.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_resp3_hgetall_map():
    port = 6524
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        cmd(s, "HELLO", "3")  # switch to RESP3 (reply consumed)
        cmd(s, "HSET", "h", "f1", "v1", "f2", "v2")
        r = cmd(s, "HGETALL", "h")
        expect(isinstance(r, dict) and r.get("f1") == "v1" and r.get("f2") == "v2",
               f"RESP3 HGETALL should be a map, got {r!r}")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def main():
    test_hyperloglog()
    test_smove_zrangestore()
    test_pubsub_introspection()
    test_ro_variants_and_command()
    test_sharded_pubsub_and_more_ro()
    test_resp3_hgetall_map()
    print("B5 command/protocol tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

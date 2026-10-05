#!/usr/bin/env python3
"""OBJECT ENCODING thresholds (*-max-listpack-* configs) + sticky one-way conversion."""
import pathlib
import socket
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready

PORT = 6542


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
    if p == b"$":
        n = int(rl(s))
        if n == -1:
            return None
        v = rx(s, n)
        rx(s, 2)
        return v.decode()
    if p == b"*":
        n = int(rl(s))
        return [recv(s) for _ in range(n)]
    raise RuntimeError(p)


def cmd(s, *a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        b = x.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    s.sendall(d)
    return recv(s)


def main():
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", str(PORT),
                          "--bind", "127.0.0.1", "--loglevel", "warning"])
    try:
        wait_until_ready(PORT)
        with socket.create_connection(("127.0.0.1", PORT), timeout=3) as s:
            assert cmd(s, "FLUSHALL") == "OK"
            enc = lambda k: cmd(s, "OBJECT", "ENCODING", k)

            # Hash: default small -> listpack; entries threshold 0 disables listpack.
            assert cmd(s, "HSET", "h1", "f", "v") == 1
            assert enc("h1") == "listpack"
            assert cmd(s, "CONFIG", "SET", "hash-max-listpack-entries", "0") == "OK"
            assert cmd(s, "HSET", "h2", "f", "v") == 1
            assert enc("h2") == "hashtable"
            assert cmd(s, "CONFIG", "SET", "hash-max-listpack-entries", "128") == "OK"
            # Sticky: once observed big, raising the limit doesn't convert back.
            assert enc("h2") == "hashtable"
            # Value-size threshold.
            assert cmd(s, "HSET", "h3", "f", "x" * 100) == 1
            assert enc("h3") == "hashtable"

            # Set: intset for strict ints (no leading zeros), listpack, hashtable.
            assert cmd(s, "SADD", "s1", "1", "2", "3") == 3
            assert enc("s1") == "intset"
            assert cmd(s, "SADD", "s2", "a", "b") == 2
            assert enc("s2") == "listpack"
            assert cmd(s, "SADD", "s3", "x" * 100) == 1
            assert enc("s3") == "hashtable"
            assert cmd(s, "SADD", "s4", "032") == 1
            assert enc("s4") == "listpack"
            assert cmd(s, "CONFIG", "SET", "set-max-intset-entries", "2") == "OK"
            assert cmd(s, "SADD", "s5", "1", "2", "3") == 3
            assert enc("s5") == "listpack"  # intset overflow degrades to listpack

            # ZSet: member-size threshold.
            assert cmd(s, "CONFIG", "SET", "zset-max-listpack-value", "3") == "OK"
            assert cmd(s, "ZADD", "z1", "1", "abcd") == 1
            assert enc("z1") == "skiplist"
            assert cmd(s, "ZADD", "z2", "1", "ab") == 1
            assert enc("z2") == "listpack"

            # List: size threshold + sticky across shrink.
            # Lists convert BOTH ways (Redis 7.2 listTypeTryConversion), unlike the
            # sticky one-way hash/set/zset shims.
            assert cmd(s, "CONFIG", "SET", "list-max-listpack-size", "2") == "OK"
            assert cmd(s, "RPUSH", "l1", "a", "b", "c") == 3
            assert enc("l1") == "quicklist"
            assert cmd(s, "RPOP", "l1", "2") == ["c", "b"]
            assert enc("l1") == "listpack"  # shrank back under the limit

            # String: embstr boundary at 44 bytes (Redis 7).
            assert cmd(s, "SET", "str44", "x" * 44) == "OK"
            assert enc("str44") == "embstr"
            assert cmd(s, "SET", "str45", "x" * 45) == "OK"
            assert enc("str45") == "raw"

            # ziplist/listpack aliases share storage in CONFIG GET/SET.
            assert cmd(s, "CONFIG", "GET", "hash-max-ziplist-entries") == ["hash-max-ziplist-entries", "128"]
            assert cmd(s, "CONFIG", "SET", "zset-max-ziplist-entries", "64") == "OK"
            assert cmd(s, "CONFIG", "GET", "zset-max-listpack-entries") == ["zset-max-listpack-entries", "64"]
        print("P4 encoding shim tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

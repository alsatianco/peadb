#!/usr/bin/env python3
"""LPOP/RPOP with COUNT (Redis 6.2+) and READONLY/READWRITE registration."""
import pathlib
import socket
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready

PORT = 6530


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
        if n == -1:
            return None
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
        wait_until_ready(PORT, process=p)
        with socket.create_connection(("127.0.0.1", PORT), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"

            # Plain LPOP/RPOP unchanged.
            assert cmd(s, "LPOP", "nolist") is None
            assert cmd(s, "RPUSH", "l", "a", "b", "c", "d", "e") == 5
            assert cmd(s, "LPOP", "l") == "a"
            assert cmd(s, "RPOP", "l") == "e"

            # COUNT form: pops in order, up to count.
            assert cmd(s, "LPOP", "l", "2") == ["b", "c"]
            assert cmd(s, "RPOP", "l", "5") == ["d"]  # over-count clamps
            assert cmd(s, "EXISTS", "l") == 0  # emptied list is deleted

            # COUNT on a missing key -> null array (RESP2 *-1).
            assert cmd(s, "LPOP", "nolist", "2") is None
            assert cmd(s, "RPOP", "nolist", "2") is None

            # COUNT 0 on an existing key -> empty array.
            assert cmd(s, "RPUSH", "l2", "x") == 1
            assert cmd(s, "LPOP", "l2", "0") == []
            assert cmd(s, "LLEN", "l2") == 1

            # Errors match Redis wording.
            r = cmd(s, "LPOP", "l2", "-1")
            assert r[0] == "ERR" and "value is out of range, must be positive" in r[1], r
            r = cmd(s, "RPOP", "l2", "abc")
            assert r[0] == "ERR" and "value is not an integer or out of range" in r[1], r
            r = cmd(s, "LPOP", "l2", "1", "junk")
            assert r[0] == "ERR" and "wrong number of arguments" in r[1], r

            # WRONGTYPE still enforced for the COUNT form.
            assert cmd(s, "SET", "str", "v") == "OK"
            r = cmd(s, "LPOP", "str", "1")
            assert r[0] == "ERR" and r[1].startswith("WRONGTYPE"), r

            # READONLY / READWRITE are registered; without cluster mode Redis refuses them.
            disabled = ("ERR", "ERR This instance has cluster support disabled")
            assert cmd(s, "READONLY") == disabled
            assert cmd(s, "READWRITE") == disabled
            r = cmd(s, "READONLY", "extra")
            assert r[0] == "ERR" and "wrong number of arguments" in r[1], r
        print("P4 LPOP/RPOP COUNT + READONLY/READWRITE tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

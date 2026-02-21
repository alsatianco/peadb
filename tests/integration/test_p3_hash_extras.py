#!/usr/bin/env python3
"""Tests for HINCRBY, HINCRBYFLOAT, HKEYS, HMGET, HMSET, HSETNX, HVALS."""
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def read_exact(s: socket.socket, n: int) -> bytes:
    b = bytearray()
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c:
            raise RuntimeError("closed")
        b.extend(c)
    return bytes(b)


def read_line(s: socket.socket) -> bytes:
    b = bytearray()
    while True:
        b.extend(read_exact(s, 1))
        if b[-2:] == b"\r\n":
            return bytes(b[:-2])


def recv(s: socket.socket):
    p = read_exact(s, 1)
    if p == b"+":
        return read_line(s).decode()
    if p == b"-":
        return ("ERR", read_line(s).decode())
    if p == b":":
        return int(read_line(s).decode())
    if p == b"$":
        n = int(read_line(s).decode())
        if n == -1:
            return None
        b = read_exact(s, n)
        read_exact(s, 2)
        return b.decode()
    if p == b"*":
        n = int(read_line(s).decode())
        if n == -1:
            return None
        return [recv(s) for _ in range(n)]
    raise RuntimeError(p)


def cmd(s: socket.socket, *args: str):
    data = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode()
        data += f"${len(b)}\r\n".encode() + b + b"\r\n"
    s.sendall(data)
    return recv(s)


def main() -> int:
    proc = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6520",
                             "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6520), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"

            # ── HMSET / HMGET ────────────────────────────────────
            assert cmd(s, "HMSET", "h", "f1", "v1", "f2", "v2", "f3", "v3") == "OK"
            vals = cmd(s, "HMGET", "h", "f1", "f3", "missing")
            assert vals == ["v1", "v3", None]

            # HMSET wrong number of args
            err = cmd(s, "HMSET", "h", "f1")
            assert err[0] == "ERR"

            # ── HSETNX ───────────────────────────────────────────
            assert cmd(s, "HSETNX", "h", "f1", "new") == 0  # exists
            assert cmd(s, "HGET", "h", "f1") == "v1"        # not overwritten
            assert cmd(s, "HSETNX", "h", "f4", "v4") == 1   # new
            assert cmd(s, "HGET", "h", "f4") == "v4"

            # ── HKEYS / HVALS ────────────────────────────────────
            keys = cmd(s, "HKEYS", "h")
            assert isinstance(keys, list)
            assert set(keys) == {"f1", "f2", "f3", "f4"}

            vals = cmd(s, "HVALS", "h")
            assert isinstance(vals, list)
            assert set(vals) == {"v1", "v2", "v3", "v4"}

            # HKEYS / HVALS on non-existent key
            assert cmd(s, "HKEYS", "nokey") == []
            assert cmd(s, "HVALS", "nokey") == []

            # ── HINCRBY ──────────────────────────────────────────
            assert cmd(s, "HINCRBY", "h", "counter", "5") == 5
            assert cmd(s, "HINCRBY", "h", "counter", "3") == 8
            assert cmd(s, "HINCRBY", "h", "counter", "-2") == 6
            assert cmd(s, "HGET", "h", "counter") == "6"

            # HINCRBY on non-integer field
            assert cmd(s, "HSET", "h", "str", "abc") == 1
            err = cmd(s, "HINCRBY", "h", "str", "1")
            assert err[0] == "ERR" and "integer" in err[1]

            # ── HINCRBYFLOAT ─────────────────────────────────────
            assert cmd(s, "HSET", "h", "flt", "10.5") == 1
            r = cmd(s, "HINCRBYFLOAT", "h", "flt", "0.1")
            assert float(r) == 10.6
            r = cmd(s, "HINCRBYFLOAT", "h", "flt", "-5")
            assert float(r) == 5.6

            # HINCRBYFLOAT on new field
            r = cmd(s, "HINCRBYFLOAT", "h", "flt2", "3.14")
            assert float(r) == 3.14

            # ── WRONGTYPE checks ─────────────────────────────────
            assert cmd(s, "SET", "str", "x") == "OK"
            for c in ["HMGET", "HKEYS", "HVALS"]:
                if c == "HMGET":
                    err = cmd(s, c, "str", "f1")
                else:
                    err = cmd(s, c, "str")
                assert err[0] == "ERR" and "WRONGTYPE" in err[1], f"{c} wrongtype failed"

        print("P3 hash extras tests passed")
        return 0
    finally:
        proc.terminate()
        proc.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

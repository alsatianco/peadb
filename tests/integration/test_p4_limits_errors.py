#!/usr/bin/env python3
"""proto-max-bulk-len bounds (SETBIT/GETBIT/SETRANGE), numkeys error text, tcp-backlog config."""
import pathlib
import socket
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready

PORT = 6535


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
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", str(PORT), "--bind",
                          "127.0.0.1", "--loglevel", "warning", "--tcp-backlog", "300"])
    try:
        wait_until_ready(PORT)
        with socket.create_connection(("127.0.0.1", PORT), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"

            # SETBIT/GETBIT: bit offset capped at proto-max-bulk-len * 8.
            max_bits = 512 * 1024 * 1024 * 8
            r = cmd(s, "SETBIT", "b", str(max_bits), "1")
            assert r[0] == "ERR" and "bit offset is not an integer or out of range" in r[1], r
            r = cmd(s, "GETBIT", "b", str(max_bits))
            assert r[0] == "ERR" and "bit offset is not an integer or out of range" in r[1], r
            # (In-range boundary not exercised: SETBIT at max_bits-1 would allocate 512MB.)

            # SETRANGE: offset + len capped at proto-max-bulk-len.
            r = cmd(s, "SETRANGE", "s", str(512 * 1024 * 1024 - 1), "xx")
            assert r[0] == "ERR" and "string exceeds maximum allowed size (proto-max-bulk-len)" in r[1], r

            # numkeys error text matches Redis.
            r = cmd(s, "LMPOP", "0", "k", "LEFT")
            assert r[0] == "ERR" and "numkeys should be greater than 0" in r[1], r
            r = cmd(s, "SINTERCARD", "-1", "k")
            assert r[0] == "ERR" and "numkeys should be greater than 0" in r[1], r

            # tcp-backlog visible in CONFIG GET.
            assert cmd(s, "CONFIG", "GET", "tcp-backlog") == ["tcp-backlog", "300"]
        print("P4 limits/errors tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

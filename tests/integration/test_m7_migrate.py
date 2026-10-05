#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready


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
    raise RuntimeError(p)


def cmd(s, *a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        b = x.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    s.sendall(d)
    return recv(s)


def main():
    # SETSLOT MIGRATING/IMPORTING now resolves its node argument against a real node table
    # (see docs/decisions/p5-cluster-addressing-and-version-provenance.md) instead of accepting
    # any token, and the whole CLUSTER command is gated on --cluster-enabled
    # (cluster.c:5888-5891), so both servers MEET each other before using their real
    # host:port as the node argument.
    p1 = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6431", "--bind", "127.0.0.1",
                          "--cluster-enabled", "yes", "--cluster-announce-ip", "127.0.0.1",
                          "--loglevel", "warning"])
    p2 = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6432", "--bind", "127.0.0.1",
                          "--cluster-enabled", "yes", "--cluster-announce-ip", "127.0.0.1",
                          "--loglevel", "warning"])
    try:
        wait_until_ready(6431)
        wait_until_ready(6432)
        with socket.create_connection(("127.0.0.1", 6431), timeout=2) as a, socket.create_connection(("127.0.0.1", 6432), timeout=2) as b:
            assert cmd(a, "FLUSHALL") == "OK"
            assert cmd(b, "FLUSHALL") == "OK"
            assert cmd(a, "CLUSTER", "MEET", "127.0.0.1", "6432") == "OK"
            assert cmd(a, "SET", "migrate-key", "v1") == "OK"
            assert cmd(a, "PEXPIRE", "migrate-key", "5000") == 1
            slot = cmd(a, "CLUSTER", "KEYSLOT", "migrate-key")
            assert cmd(a, "CLUSTER", "SETSLOT", str(slot), "MIGRATING", "127.0.0.1:6432") == "OK"
            assert cmd(b, "CLUSTER", "SETSLOT", str(slot), "IMPORTING", "self") == "OK"

            r = cmd(a, "MIGRATE", "127.0.0.1", "6432", "migrate-key", "0", "2000")
            assert r == "OK"
            assert cmd(a, "CLUSTER", "SETSLOT", str(slot), "NODE", "self") == "OK"
            assert cmd(b, "CLUSTER", "SETSLOT", str(slot), "NODE", "self") == "OK"
            assert cmd(a, "GET", "migrate-key") is None
            assert cmd(b, "GET", "migrate-key") == "v1"
            ttl = cmd(b, "PTTL", "migrate-key")
            assert isinstance(ttl, int) and ttl > 0
        print("M7 migrate tests passed")
        return 0
    finally:
        p2.terminate()
        p2.wait(timeout=3)
        p1.terminate()
        p1.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

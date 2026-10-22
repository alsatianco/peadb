#!/usr/bin/env python3
"""Batch 14 — lazyfree background deletion (conc-6).

Covers: UNLINK of a large key returns immediately and removes it; FLUSHALL/FLUSHDB ASYNC
empty the keyspace; the server stays responsive and data on other keys is intact (the
background free must not corrupt live data — single-writer ownership transfer).
"""
import pathlib
import socket
import time

from _isolated_server import IsolatedServer

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
    s = socket.create_connection(("127.0.0.1", port), timeout=10); s.settimeout(10); return s


def rx(s, n):
    b = b""
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c: raise RuntimeError("closed")
        b += c
    return b


def rl(s):
    b = b""
    while not b.endswith(b"\r\n"): b += rx(s, 1)
    return b[:-2]


def recv(s):
    p = rx(s, 1)
    if p == b"+": return rl(s).decode()
    if p == b"-": return ("ERR", rl(s).decode())
    if p == b":": return int(rl(s))
    if p in (b"$", b"="):
        n = int(rl(s))
        if n == -1: return None
        v = rx(s, n); rx(s, 2); return v.decode("latin1")
    if p in (b"*", b"~", b">"):
        n = int(rl(s))
        if n == -1: return None
        return [recv(s) for _ in range(n)]
    raise RuntimeError("type %r" % p)


def cmd(s, *a):
    out = b"*%d\r\n" % len(a)
    for x in a:
        b = x.encode() if isinstance(x, str) else x
        out += b"$%d\r\n%s\r\n" % (len(b), b)
    s.sendall(out)
    return recv(s)


def expect(c, m):
    if not c: raise AssertionError(m)


def start(port):
    return IsolatedServer([str(SERVER), "--port", str(port), "--bind", "127.0.0.1", "--loglevel", "warning"])


def test_unlink_big_key():
    port = 6610
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        # Big list + an unrelated key that must survive the background free intact.
        cmd(s, "EVAL", "for i=1,200000 do redis.call('rpush','big',i) end return 1", "0")
        cmd(s, "SET", "keep", "value")
        t0 = time.time()
        expect(cmd(s, "UNLINK", "big") == 1, "UNLINK returns 1")
        dt = (time.time() - t0) * 1000
        expect(dt < 50, f"UNLINK of a 200k-element key should return promptly, took {dt:.1f}ms")
        expect(cmd(s, "EXISTS", "big") == 0, "big key removed from keyspace")
        # server responsive + unrelated data intact while the bg free runs
        expect(cmd(s, "PING") == "PONG", "server responsive after UNLINK")
        expect(cmd(s, "GET", "keep") == "value", "unrelated key intact (no corruption from bg free)")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=5)


def test_flush_async():
    port = 6611
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        for i in range(1000):
            cmd(s, "SET", f"k{i}", "v")
        expect(cmd(s, "DBSIZE") == 1000, "1000 keys")
        expect(cmd(s, "FLUSHALL", "ASYNC") == "OK", "flushall async")
        expect(cmd(s, "DBSIZE") == 0, "keyspace empty after async flush")
        cmd(s, "SET", "x", "1")
        expect(cmd(s, "FLUSHDB", "ASYNC") == "OK", "flushdb async")
        expect(cmd(s, "DBSIZE") == 0, "db empty after async flush")
        # syntax check
        expect(isinstance(cmd(s, "FLUSHALL", "WRONG"), tuple), "bad flush arg errors")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=5)


def main():
    test_unlink_big_key()
    test_flush_async()
    print("B14 lazyfree tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

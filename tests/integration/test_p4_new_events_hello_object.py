#!/usr/bin/env python3
"""Keyspace 'new' events (n flag), dynamic HELLO reply, OBJECT IDLETIME/FREQ."""
import pathlib
import socket
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready

PORT = 6532


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
    if p in (b"*", b">", b"~"):
        n = int(rl(s))
        if n == -1:
            return None
        return [recv(s) for _ in range(n)]
    if p == b"%":
        n = int(rl(s))
        return {recv(s): recv(s) for _ in range(n)}
    if p == b"_":
        rl(s)
        return None
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
                          "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        wait_until_ready(PORT)
        with socket.create_connection(("127.0.0.1", PORT), timeout=3) as s, \
             socket.create_connection(("127.0.0.1", PORT), timeout=3) as sub:
            assert cmd(s, "FLUSHALL") == "OK"

            # ── keyspace 'new' events ──
            assert cmd(s, "CONFIG", "SET", "notify-keyspace-events", "En") == "OK"
            assert cmd(sub, "SUBSCRIBE", "__keyevent@0__:new")[0] == "subscribe"
            assert cmd(s, "SET", "brand-new", "v") == "OK"
            msg = recv(sub)
            assert msg == ["message", "__keyevent@0__:new", "brand-new"], msg
            # Overwriting an existing key must NOT fire 'new'.
            assert cmd(s, "SET", "brand-new", "v2") == "OK"
            # Creation via a different type also fires.
            assert cmd(s, "LPUSH", "new-list", "a") == 1
            msg = recv(sub)
            assert msg == ["message", "__keyevent@0__:new", "new-list"], msg
            assert cmd(s, "CONFIG", "SET", "notify-keyspace-events", "") == "OK"

            # ── dynamic HELLO ──
            h2 = cmd(s, "HELLO")
            d2 = {h2[i]: h2[i + 1] for i in range(0, len(h2), 2)}
            my_id = cmd(s, "CLIENT", "ID")
            assert d2["proto"] == 2 and d2["role"] == "master" and d2["mode"] == "standalone", d2
            assert d2["id"] == my_id, (d2, my_id)
            h3 = cmd(s, "HELLO", "3")
            d3 = h3 if isinstance(h3, dict) else {h3[i]: h3[i + 1] for i in range(0, len(h3), 2)}
            assert d3["proto"] == 3 and d3["id"] == my_id, d3
            assert cmd(s, "HELLO", "2") is not None  # back to RESP2

            # ── OBJECT IDLETIME / FREQ ──
            assert cmd(s, "SET", "ok", "v") == "OK"
            v = cmd(s, "OBJECT", "IDLETIME", "ok")
            assert isinstance(v, int) and 0 <= v <= 5, v
            r = cmd(s, "OBJECT", "FREQ", "ok")
            assert r[0] == "ERR" and "An LFU maxmemory policy is not selected" in r[1], r
            assert cmd(s, "CONFIG", "SET", "maxmemory-policy", "allkeys-lfu") == "OK"
            assert cmd(s, "GET", "ok") == "v"  # touch under LFU
            v = cmd(s, "OBJECT", "FREQ", "ok")
            assert isinstance(v, int) and v >= 0, v
            r = cmd(s, "OBJECT", "IDLETIME", "ok")
            assert r[0] == "ERR" and "An LFU maxmemory policy is selected" in r[1], r
            assert cmd(s, "CONFIG", "SET", "maxmemory-policy", "noeviction") == "OK"
            assert cmd(s, "OBJECT", "IDLETIME", "missing-key") is None
            assert cmd(s, "OBJECT", "FREQ", "missing-key") is None
            r = cmd(s, "OBJECT", "BOGUS", "ok")
            assert r[0] == "ERR" and "Try OBJECT HELP" in r[1], r

        print("P4 new-events/HELLO/OBJECT tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

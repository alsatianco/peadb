#!/usr/bin/env python3
"""Batch 13 — per-class client-output-buffer-limit (conc-3).

Covers: CONFIG GET/SET round-trip of the per-class limits, and that a slow (non-reading)
pub/sub subscriber whose output buffer exceeds the pubsub hard limit is disconnected,
while a normal client (unlimited by default) handling a large reply is unaffected.
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
    s = socket.create_connection(("127.0.0.1", port), timeout=5); s.settimeout(5); return s


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


def start(port, *extra):
    return subprocess.Popen([str(SERVER), "--port", str(port), "--bind", "127.0.0.1", "--loglevel", "error", *extra])


def test_config_roundtrip():
    port = 6600
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        g = cmd(s, "CONFIG", "GET", "client-output-buffer-limit")
        expect(g[0] == "client-output-buffer-limit", "key present")
        expect("slave 268435456 67108864 60" in g[1], f"default replica limit present, got {g[1]}")
        expect(cmd(s, "CONFIG", "SET", "client-output-buffer-limit", "pubsub 4096 2048 1") == "OK", "set pubsub")
        g = cmd(s, "CONFIG", "GET", "client-output-buffer-limit")
        expect("pubsub 4096 2048 1" in g[1], f"pubsub limit updated, got {g[1]}")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_slow_subscriber_disconnected():
    port = 6601
    # tiny pubsub hard limit so a non-reading subscriber trips it fast
    p = start(port, "--client-output-buffer-limit", "pubsub 8192 4096 1")
    try:
        expect(wait_up(port), "up")
        sub = conn(port)
        r = cmd(sub, "SUBSCRIBE", "ch")
        expect(isinstance(r, list) and r[0] == "subscribe", "subscribed")
        # Publish large messages WITHOUT the subscriber reading them → server buffers them
        # for the subscriber until the pubsub hard limit trips and the subscriber is closed.
        pub = conn(port)
        payload = "x" * 200000
        closed = False
        for _ in range(200):
            cmd(pub, "PUBLISH", "ch", payload)
            # Once the subscriber is dropped, PUBLISH returns 0 receivers.
            n = cmd(pub, "PUBLISH", "ch", "ping")
            if n == 0:
                closed = True
                break
        expect(closed, "non-reading subscriber over the pubsub buffer limit must be disconnected")
        pub.close()
        try: sub.close()
        except OSError: pass
    finally:
        p.terminate(); p.wait(timeout=3)


def test_normal_client_large_reply_ok():
    port = 6602
    p = start(port)  # normal class unlimited by default
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        # Build a large list and read it back in one big reply — must not be disconnected.
        big = "y" * 1000
        for i in range(2000):
            cmd(s, "RPUSH", "biglist", big)
        res = cmd(s, "LRANGE", "biglist", "0", "-1")
        expect(isinstance(res, list) and len(res) == 2000, f"large reply delivered intact, got {len(res) if isinstance(res, list) else res}")
        expect(cmd(s, "PING") == "PONG", "normal client still alive after large reply")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def main():
    test_config_roundtrip()
    test_slow_subscriber_disconnected()
    test_normal_client_large_reply_ok()
    print("B13 output-buffer-limit tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

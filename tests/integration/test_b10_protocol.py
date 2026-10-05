#!/usr/bin/env python3
"""Batch 10 — protocol/command completeness (P1 inline parsing, ZRANGESTORE BY*).

Covers: quote/escape-aware inline command parsing (sdssplitargs semantics) and the
BYSCORE/BYLEX (+ REV + LIMIT) forms of ZRANGESTORE.
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


def start(port):
    return subprocess.Popen([str(SERVER), "--port", str(port), "--bind", "127.0.0.1", "--loglevel", "warning"])


def test_inline_quoting():
    port = 6570
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        # Inline (not RESP) with a double-quoted arg containing a space + a \x41 escape.
        s.sendall(b'SET k "hello world\\x21"\r\n')
        expect(recv(s) == "OK", "inline SET ok")
        expect(cmd(s, "GET", "k") == "hello world!", "quoted value with space + \\x escape preserved")
        # Single quotes are literal.
        s.sendall(b"SET k2 'a b c'\r\n")
        expect(recv(s) == "OK", "inline single-quote ok")
        expect(cmd(s, "GET", "k2") == "a b c", "single-quoted literal preserved")
        # Unbalanced quote → protocol error + connection close.
        s2 = conn(port)
        s2.sendall(b'SET k3 "unterminated\r\n')
        r = recv(s2)
        expect(isinstance(r, tuple) and "rotocol" in r[1], f"unbalanced quote is a protocol error, got {r}")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_zrangestore_byscore_bylex():
    port = 6571
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        cmd(s, "ZADD", "src", "1", "a", "2", "b", "3", "c", "4", "d")
        expect(cmd(s, "ZRANGESTORE", "d1", "src", "2", "3", "BYSCORE") == 2, "byscore count")
        expect(cmd(s, "ZRANGE", "d1", "0", "-1") == ["b", "c"], "byscore members")
        # BYLEX over equal-score members
        cmd(s, "ZADD", "z", "0", "a", "0", "b", "0", "c", "0", "d")
        expect(cmd(s, "ZRANGESTORE", "d2", "z", "[a", "(c", "BYLEX") == 2, "bylex count")
        expect(cmd(s, "ZRANGE", "d2", "0", "-1") == ["a", "b"], "bylex members (exclusive c)")
        # BYSCORE REV + LIMIT
        expect(cmd(s, "ZRANGESTORE", "d3", "src", "4", "1", "BYSCORE", "REV", "LIMIT", "1", "2") == 2, "rev+limit count")
        expect(cmd(s, "ZRANGE", "d3", "0", "-1") == ["b", "c"], "rev+limit members")
        # LIMIT without BY* is an error
        r = cmd(s, "ZRANGESTORE", "d4", "src", "0", "-1", "LIMIT", "0", "2")
        expect(isinstance(r, tuple), "LIMIT requires BYSCORE/BYLEX")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def main():
    test_inline_quoting()
    test_zrangestore_byscore_bylex()
    print("B10 protocol tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

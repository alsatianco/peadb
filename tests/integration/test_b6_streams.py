#!/usr/bin/env python3
"""Batch 6 — streams (incl. stream RDB).

Covers: stream RDB persistence across restart (entries + consumer group + PEL),
XSETID, XTRIM, XCLAIM, XAUTOCLAIM, and blocking XREAD.
"""
import pathlib
import socket
import subprocess
import tempfile
import threading
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
    s = socket.create_connection(("127.0.0.1", port), timeout=3); s.settimeout(3); return s


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
    if p == b"%":
        n = int(rl(s)); return {recv(s): recv(s) for _ in range(n)}
    if p == b"_": rl(s); return None
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
    return subprocess.Popen([str(SERVER), "--port", str(port), "--bind", "127.0.0.1", "--loglevel", "warning", *extra])


def test_stream_rdb_persistence():
    port = 6530
    with tempfile.TemporaryDirectory(prefix="b6rdb-", ignore_cleanup_errors=True) as td:
        p = start(port, "--dir", td)
        try:
            expect(wait_up(port), "up")
            s = conn(port)
            cmd(s, "XADD", "st", "1-1", "f", "a")
            cmd(s, "XADD", "st", "2-2", "f", "b")
            cmd(s, "XGROUP", "CREATE", "st", "g1", "0")
            cmd(s, "XREADGROUP", "GROUP", "g1", "c1", "COUNT", "1", "STREAMS", "st", ">")
            expect(cmd(s, "SAVE") == "OK", "SAVE with a stream should now succeed")
            s.close()
        finally:
            p.terminate(); p.wait(timeout=3)
        # restart and verify the stream + group survived
        p = start(port, "--dir", td)
        try:
            expect(wait_up(port), "restart up")
            s = conn(port)
            expect(cmd(s, "XLEN", "st") == 2, "stream entries survive reload")
            rng = cmd(s, "XRANGE", "st", "-", "+")
            expect(isinstance(rng, list) and len(rng) == 2, "XRANGE returns both entries after reload")
            # pending entry from the group should still be pending
            pend = cmd(s, "XPENDING", "st", "g1")
            expect(isinstance(pend, list) and pend[0] == 1, f"PEL (1 pending) survives reload, got {pend}")
            s.close()
        finally:
            p.terminate(); p.wait(timeout=3)


def test_xsetid_xtrim():
    port = 6531
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        cmd(s, "XADD", "s", "5-5", "f", "v")
        expect(cmd(s, "XSETID", "s", "100-0") == "OK", "XSETID ok")
        newid = cmd(s, "XADD", "s", "*", "f", "w")
        expect(int(newid.split("-")[0]) >= 100, f"new id after XSETID should be >= 100, got {newid}")
        # XTRIM MAXLEN
        for i in range(5): cmd(s, "XADD", "t", "*", "n", str(i))
        removed = cmd(s, "XTRIM", "t", "MAXLEN", "2")
        expect(removed == 4 or cmd(s, "XLEN", "t") == 2, "XTRIM keeps newest 2")
        expect(cmd(s, "XLEN", "t") == 2, "XLEN after trim is 2")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_xclaim_xautoclaim():
    port = 6532
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        cmd(s, "XADD", "st", "1-1", "f", "a")
        cmd(s, "XADD", "st", "2-2", "f", "b")
        cmd(s, "XGROUP", "CREATE", "st", "g", "0")
        # c1 reads both → they become pending for c1
        cmd(s, "XREADGROUP", "GROUP", "g", "c1", "COUNT", "5", "STREAMS", "st", ">")
        # c2 claims 1-1 with min-idle 0 → ownership moves to c2
        claimed = cmd(s, "XCLAIM", "st", "g", "c2", "0", "1-1")
        expect(isinstance(claimed, list) and len(claimed) == 1 and claimed[0][0] == "1-1",
               f"XCLAIM should return the claimed entry, got {claimed}")
        # XAUTOCLAIM the rest for c3
        res = cmd(s, "XAUTOCLAIM", "st", "g", "c3", "0", "0")
        expect(isinstance(res, list) and len(res) == 3, f"XAUTOCLAIM returns [cursor, entries, deleted], got {res}")
        expect(isinstance(res[1], list) and len(res[1]) >= 1, "XAUTOCLAIM claimed at least one entry")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_blocking_xread():
    port = 6533
    p = start(port)
    try:
        expect(wait_up(port), "up")
        r = conn(port)
        out = {}
        def reader():
            try:
                out["v"] = cmd(r, "XREAD", "BLOCK", "3000", "STREAMS", "bs", "$")
            except Exception as e:
                out["v"] = ("err", e)
        t = threading.Thread(target=reader); t.start()
        time.sleep(0.4)  # ensure the reader is blocked
        w = conn(port)
        cmd(w, "XADD", "bs", "*", "field", "value")
        t.join(4)
        v = out.get("v")
        expect(isinstance(v, list) and v and v[0][0] == "bs", f"blocking XREAD should return the new entry, got {v}")
        r.close(); w.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_block_in_multi_does_not_hang():
    # A blocking XREAD inside MULTI/EXEC must return immediately (null), never block.
    port = 6534
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        expect(cmd(s, "MULTI") == "OK", "MULTI")
        expect(cmd(s, "XREAD", "BLOCK", "0", "STREAMS", "nope", "$") == "QUEUED", "queued")
        res = cmd(s, "EXEC")  # must not hang
        expect(isinstance(res, list) and len(res) == 1, f"EXEC returns one result, got {res}")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def main():
    test_stream_rdb_persistence()
    test_xsetid_xtrim()
    test_xclaim_xautoclaim()
    test_blocking_xread()
    test_block_in_multi_does_not_hang()
    print("B6 stream tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

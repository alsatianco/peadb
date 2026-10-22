#!/usr/bin/env python3
"""Batch 8 — replication safety (repl-4, repl-7, obs-3).

Covers: a randomized per-process replid (two instances differ, not the legacy ...0001);
replid2 + second_repl_offset rotation on REPLICAOF NO ONE promotion; INFO sync_full counter
on a real full resync; and WAIT resolving quickly against a connected replica (GETACK).
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
    s = socket.create_connection(("127.0.0.1", port), timeout=8); s.settimeout(8); return s


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
    return IsolatedServer([str(SERVER), "--port", str(port), "--bind", "127.0.0.1",
                             "--loglevel", "warning", *extra])


def info_field(s, field):
    txt = cmd(s, "INFO", "all")
    for line in txt.replace("\r", "").split("\n"):
        if line.startswith(field + ":"):
            return line.split(":", 1)[1]
    return None


def test_replid_random_and_unique():
    pa, pb = 6550, 6551
    a = start(pa); b = start(pb)
    try:
        expect(wait_up(pa) and wait_up(pb), "both up")
        sa, sb = conn(pa), conn(pb)
        ida = info_field(sa, "master_replid")
        idb = info_field(sb, "master_replid")
        legacy = "0000000000000000000000000000000000000001"
        expect(len(ida) == 40, f"replid is 40 hex chars, got {ida!r}")
        expect(ida != legacy, "replid must be randomized, not the legacy ...0001")
        expect(ida != idb, "two instances must have different replids")
        sa.close(); sb.close()
    finally:
        a.terminate(); a.wait(timeout=3); b.terminate(); b.wait(timeout=3)


def test_promotion_rotates_replid():
    mp, rp = 6552, 6553
    m = start(mp); r = start(rp)
    try:
        expect(wait_up(mp) and wait_up(rp), "both up")
        sm, sr = conn(mp), conn(rp)
        cmd(sm, "SET", "k", "v")
        expect(cmd(sr, "REPLICAOF", "127.0.0.1", str(mp)) == "OK", "replicaof")
        # wait for sync
        synced = False
        for _ in range(50):
            if info_field(sr, "master_link_status") == "up" and cmd(sr, "GET", "k") == "v":
                synced = True; break
            time.sleep(0.1)
        expect(synced, "replica synced from master")
        expect(int(info_field(sm, "sync_full")) >= 1,
               "master counts a full resync (sync_full>=1)")
        replid_before = info_field(sr, "master_replid")
        # promote
        expect(cmd(sr, "REPLICAOF", "NO", "ONE") == "OK", "promote")
        replid_after = info_field(sr, "master_replid")
        replid2 = info_field(sr, "master_replid2")
        expect(replid_after != replid_before, "promotion adopts a fresh replid")
        expect(replid2 == replid_before, "old replid moved into replid2")
        expect(int(info_field(sr, "second_repl_offset")) >= 0, "second_repl_offset recorded")
        sm.close(); sr.close()
    finally:
        m.terminate(); m.wait(timeout=3); r.terminate(); r.wait(timeout=3)


def test_wait_resolves_fast_with_replica():
    mp, rp = 6554, 6555
    m = start(mp); r = start(rp)
    try:
        expect(wait_up(mp) and wait_up(rp), "both up")
        sm, sr = conn(mp), conn(rp)
        cmd(sr, "REPLICAOF", "127.0.0.1", str(mp))
        ok = False
        for _ in range(50):
            if info_field(sr, "master_link_status") == "up":
                ok = True; break
            time.sleep(0.1)
        expect(ok, "replica linked")
        cmd(sm, "SET", "x", "1")
        # WAIT for 1 replica, generous 3s timeout; with GETACK it should resolve well under 1s.
        t0 = time.time()
        res = cmd(sm, "WAIT", "1", "3000")
        dt = time.time() - t0
        expect(res == 1, f"WAIT should see 1 acked replica, got {res}")
        expect(dt < 1.5, f"WAIT should resolve quickly via GETACK, took {dt:.2f}s")
        sm.close(); sr.close()
    finally:
        m.terminate(); m.wait(timeout=3); r.terminate(); r.wait(timeout=3)


def main():
    test_replid_random_and_unique()
    test_promotion_rotates_replid()
    test_wait_resolves_fast_with_replica()
    print("B8 replication-safety tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

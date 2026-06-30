#!/usr/bin/env python3
"""Batch 7 — maxmemory-policy eviction (the cache use case).

Covers: allkeys-* evict under pressure and keep serving (no -OOM); volatile-* only
evict keys with a TTL and -OOM when none are evictable; noeviction still -OOM;
evicted_keys INFO stat increments; CONFIG GET/SET maxmemory-policy + maxmemory-samples
round-trip; an __keyevent@0__:evicted subscriber receives events.
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
    return subprocess.Popen([str(SERVER), "--port", str(port), "--bind", "127.0.0.1",
                             "--loglevel", "error", *extra])


def info_field(s, field):
    txt = cmd(s, "INFO", "all")
    for line in txt.replace("\r", "").split("\n"):
        if line.startswith(field + ":"):
            return line.split(":", 1)[1]
    return None


def fill_until_evictions(s, prefix, n=80000, valsize=200):
    """Write keys until the server reports at least one eviction or n is reached."""
    val = "x" * valsize
    for i in range(n):
        r = cmd(s, "SET", f"{prefix}{i}", val)
        # Under a working eviction policy SET must keep succeeding (never -OOM).
        if isinstance(r, tuple):
            return r  # an error — caller asserts on it
        if i % 500 == 0:
            ev = int(info_field(s, "evicted_keys") or "0")
            if ev > 0:
                return ev
    return int(info_field(s, "evicted_keys") or "0")


def test_config_roundtrip():
    port = 6540
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        # default
        g = cmd(s, "CONFIG", "GET", "maxmemory-policy")
        expect(g == ["maxmemory-policy", "noeviction"], f"default policy noeviction, got {g}")
        expect(cmd(s, "CONFIG", "SET", "maxmemory-policy", "allkeys-lru") == "OK", "set lru")
        g = cmd(s, "CONFIG", "GET", "maxmemory-policy")
        expect(g == ["maxmemory-policy", "allkeys-lru"], f"policy now allkeys-lru, got {g}")
        expect(info_field(s, "maxmemory_policy") == "allkeys-lru", "INFO reflects policy")
        # invalid policy rejected
        r = cmd(s, "CONFIG", "SET", "maxmemory-policy", "bogus-policy")
        expect(isinstance(r, tuple), "invalid policy rejected")
        # samples roundtrip
        expect(cmd(s, "CONFIG", "SET", "maxmemory-samples", "10") == "OK", "set samples")
        g = cmd(s, "CONFIG", "GET", "maxmemory-samples")
        expect(g == ["maxmemory-samples", "10"], f"samples 10, got {g}")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_allkeys_lru_evicts_and_keeps_serving():
    port = 6541
    p = start(port, "--maxmemory", "8mb", "--maxmemory-policy", "allkeys-lru")
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        ev = fill_until_evictions(s, "k:")
        expect(not isinstance(ev, tuple), f"writes must not be rejected under allkeys-lru, got {ev}")
        expect(int(ev) > 0, f"allkeys-lru should have evicted keys, evicted_keys={ev}")
        # server still serving and bounded
        expect(cmd(s, "SET", "after", "v") == "OK", "still accepts writes after eviction")
        used = int(info_field(s, "used_memory"))
        expect(used <= 8 * 1024 * 1024 * 1.5, f"used_memory bounded near maxmemory, got {used}")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_allkeys_random_evicts():
    port = 6542
    p = start(port, "--maxmemory", "8mb", "--maxmemory-policy", "allkeys-random")
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        ev = fill_until_evictions(s, "r:")
        expect(not isinstance(ev, tuple), "writes must not be rejected under allkeys-random")
        expect(int(ev) > 0, f"allkeys-random should evict, evicted_keys={ev}")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_noeviction_rejects():
    port = 6543
    p = start(port, "--maxmemory", "6mb", "--maxmemory-policy", "noeviction")
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        val = "y" * 200
        saw_oom = False
        for i in range(50000):
            r = cmd(s, "SET", f"n:{i}", val)
            if isinstance(r, tuple):
                expect("OOM" in r[1], f"rejection should be -OOM, got {r}")
                saw_oom = True
                break
        expect(saw_oom, "noeviction must eventually reject writes with -OOM")
        expect(int(info_field(s, "evicted_keys")) == 0, "noeviction evicts nothing")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_volatile_policy_needs_ttl():
    # volatile-lru with NO volatile keys → cannot evict → -OOM (like noeviction).
    port = 6544
    p = start(port, "--maxmemory", "6mb", "--maxmemory-policy", "volatile-lru")
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        val = "z" * 200
        saw_oom = False
        for i in range(50000):
            r = cmd(s, "SET", f"v:{i}", val)  # no TTL → not eligible for volatile-*
            if isinstance(r, tuple):
                expect("OOM" in r[1], f"expected -OOM, got {r}")
                saw_oom = True
                break
        expect(saw_oom, "volatile-lru with no TTL keys must reject with -OOM")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_volatile_ttl_evicts_only_ttl_keys():
    port = 6545
    p = start(port, "--maxmemory", "10mb", "--maxmemory-policy", "volatile-ttl")
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        val = "w" * 200
        # A few persistent keys that must NEVER be evicted by volatile-ttl.
        for i in range(5):
            cmd(s, "SET", f"keep:{i}", val)
        # Many volatile keys to create eviction pressure.
        ev = 0
        for i in range(60000):
            r = cmd(s, "SET", f"vol:{i}", val, "EX", "10000")
            if isinstance(r, tuple):
                break
            if i % 500 == 0:
                ev = int(info_field(s, "evicted_keys") or "0")
                if ev > 0:
                    break
        expect(int(info_field(s, "evicted_keys")) > 0, "volatile-ttl should evict volatile keys")
        # persistent keys survived
        for i in range(5):
            expect(cmd(s, "GET", f"keep:{i}") == val, f"persistent keep:{i} must survive volatile-ttl")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_evicted_keyspace_notification():
    port = 6546
    p = start(port, "--maxmemory", "8mb", "--maxmemory-policy", "allkeys-lru")
    try:
        expect(wait_up(port), "up")
        sub = conn(port)
        expect(cmd(sub, "CONFIG", "SET", "notify-keyspace-events", "Ee") == "OK", "enable evict notifs")
        expect(cmd(sub, "PSUBSCRIBE", "__keyevent@0__:evicted") is not None, "subscribe")
        w = conn(port)
        val = "x" * 200
        for i in range(80000):
            r = cmd(w, "SET", f"e:{i}", val)
            if isinstance(r, tuple):
                break
            if i % 500 == 0 and int(info_field(w, "evicted_keys") or "0") > 0:
                break
        # read one pmessage from the subscriber
        sub.settimeout(3)
        got_evicted = False
        try:
            for _ in range(5):
                msg = recv(sub)
                if isinstance(msg, list) and msg and msg[0] == "pmessage" and "evicted" in str(msg[1]):
                    got_evicted = True
                    break
        except Exception:
            pass
        expect(got_evicted, "subscriber should receive an evicted keyspace event")
        sub.close(); w.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def main():
    test_config_roundtrip()
    test_allkeys_lru_evicts_and_keeps_serving()
    test_allkeys_random_evicts()
    test_noeviction_rejects()
    test_volatile_policy_needs_ttl()
    test_volatile_ttl_evicts_only_ttl_keys()
    test_evicted_keyspace_notification()
    print("B7 eviction tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

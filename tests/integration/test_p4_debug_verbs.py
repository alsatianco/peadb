#!/usr/bin/env python3
"""DEBUG DIGEST / RELOAD / OBJECT / POPULATE / LOADAOF / SET-SKIP-CHECKSUM-VALIDATION / PAUSE-CRON."""
import pathlib
import socket
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready

PORT = 6536


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
        return v
    if p == b"*":
        n = int(rl(s))
        if n == -1:
            return None
        return [recv(s) for _ in range(n)]
    raise RuntimeError(p)


def cmd(s, *a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        b = x if isinstance(x, bytes) else x.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    s.sendall(d)
    return recv(s)


def main():
    workdir = tempfile.mkdtemp(prefix="peadb-debug-test-")
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", str(PORT),
                          "--bind", "127.0.0.1", "--loglevel", "warning",
                          "--dir", workdir, "--appendonly", "yes"])
    try:
        wait_until_ready(PORT, process=p)
        with socket.create_connection(("127.0.0.1", PORT), timeout=5) as s:
            assert cmd(s, "FLUSHALL") == "OK"

            # ── DIGEST: empty = 40 zeros; changes with data; order-independent ──
            assert cmd(s, "DEBUG", "DIGEST") == "0" * 40
            assert cmd(s, "SET", "k1", "v1") == "OK"
            d1 = cmd(s, "DEBUG", "DIGEST")
            assert d1 != "0" * 40 and len(d1) == 40, d1

            # ── RELOAD: dataset (all types) survives an RDB save+flush+load ──
            assert cmd(s, "RPUSH", "l", "a", "b", "c") == 3
            assert cmd(s, "HSET", "h", "f1", "x", "f2", "y") == 2
            assert cmd(s, "SADD", "st", "m1", "m2") == 2
            assert cmd(s, "ZADD", "z", "1", "one", "2", "two") == 2
            assert isinstance(cmd(s, "XADD", "str1", "*", "f", "v"), bytes)
            assert cmd(s, "PEXPIRE", "k1", "900000") == 1
            pre = cmd(s, "DEBUG", "DIGEST")
            assert cmd(s, "DEBUG", "RELOAD") == "OK"
            post = cmd(s, "DEBUG", "DIGEST")
            assert pre == post, (pre, post)
            assert cmd(s, "LRANGE", "l", "0", "-1") == [b"a", b"b", b"c"]
            ttl = cmd(s, "PTTL", "k1")
            assert 0 < ttl <= 900000, ttl

            # ── DEBUG OBJECT: real fields ──
            r = cmd(s, "DEBUG", "OBJECT", "k1")
            assert "encoding:" in r and "serializedlength:" in r and "lru_seconds_idle:" in r, r
            r = cmd(s, "DEBUG", "OBJECT", "missing")
            assert r[0] == "ERR" and "no such key" in r[1], r

            # ── LOADAOF: state survives flush + AOF replay ──
            # (Must run before POPULATE: DEBUG POPULATE bypasses the AOF, like Redis,
            # so populated keys legitimately vanish on an AOF replay.)
            pre = cmd(s, "DEBUG", "DIGEST")
            assert cmd(s, "DEBUG", "LOADAOF") == "OK"
            post = cmd(s, "DEBUG", "DIGEST")
            assert pre == post, (pre, post)

            # ── POPULATE ──
            assert cmd(s, "DEBUG", "POPULATE", "100") == "OK"
            assert cmd(s, "GET", "key:42") == b"value:42"
            assert cmd(s, "DEBUG", "POPULATE", "5", "pfx:", "20") == "OK"
            v = cmd(s, "GET", "pfx:3")
            assert len(v) == 20 and v.startswith(b"value:3"), v

            # ── SET-SKIP-CHECKSUM-VALIDATION: corrupt-CRC RESTORE accepted when on ──
            dump = cmd(s, "DUMP", "k1")
            corrupted = dump[:-1] + bytes([dump[-1] ^ 0xFF])
            r = cmd(s, "RESTORE", "k1copy", "0", corrupted)
            assert r[0] == "ERR", r
            assert cmd(s, "DEBUG", "SET-SKIP-CHECKSUM-VALIDATION", "1") == "OK"
            assert cmd(s, "RESTORE", "k1copy", "0", corrupted) == "OK"
            assert cmd(s, "GET", "k1copy") == b"v1"
            assert cmd(s, "DEBUG", "SET-SKIP-CHECKSUM-VALIDATION", "0") == "OK"

            # ── misc accepted verbs ──
            assert cmd(s, "DEBUG", "PAUSE-CRON", "1") == "OK"
            assert cmd(s, "DEBUG", "PAUSE-CRON", "0") == "OK"
            assert cmd(s, "DEBUG", "QUICKLIST-PACKED-THRESHOLD", "100") == "OK"
            assert cmd(s, "DEBUG", "JMAP") == "OK"
        print("P4 DEBUG verbs tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

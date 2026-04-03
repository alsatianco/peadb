#!/usr/bin/env python3
"""Integration tests for ecosystem-compatibility commands.

Covers: CLIENT, MEMORY, SLOWLOG, LATENCY, TOUCH, ROLE, RESET,
list ops (LINDEX LSET LREM LPOS LPUSHX RPUSHX LINSERT RPOPLPUSH LTRIM LMPOP),
sorted-set ops (ZCARD ZSCORE ZMSCORE ZCOUNT ZRANK ZREVRANK ZREM ZINCRBY
  ZRANGEBYSCORE ZREVRANGEBYSCORE ZREVRANGE ZLEXCOUNT ZRANDMEMBER
  ZUNIONSTORE ZINTERSTORE ZDIFFSTORE ZUNION ZINTER ZDIFF),
set ops (SRANDMEMBER SINTER SUNION SDIFF SMISMEMBER SINTERCARD
  SINTERSTORE SUNIONSTORE SDIFFSTORE),
hash ops (HRANDFIELD),
bit ops (BITCOUNT BITPOS BITOP BITFIELD),
geo ops (GEODIST GEOPOS GEOHASH GEORADIUS GEORADIUSBYMEMBER GEOSEARCH GEOSEARCHSTORE),
stream ops (XTRIM XAUTOCLAIM).
"""
import pathlib, socket, subprocess, time, sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
PORT = 6490

def enc(*a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        x = str(x)
        b = x.encode(); d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return d

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
    if p == b'+': return rl(s).decode()
    if p == b'-': return ("ERR", rl(s).decode())
    if p == b':': return int(rl(s))
    if p == b'$':
        n = int(rl(s))
        if n == -1: return None
        v = rx(s, n); rx(s, 2); return v.decode()
    if p == b'*':
        n = int(rl(s))
        if n == -1: return None
        return [recv(s) for _ in range(n)]
    if p == b'_': rl(s); return None  # RESP3 null
    raise RuntimeError(f"unexpected prefix {p!r}")

def cmd(s, *a):
    s.sendall(enc(*a)); return recv(s)

def ok(test_name, cond, detail=""):
    if not cond:
        print(f"  FAIL: {test_name} {detail}", file=sys.stderr)
        return False
    return True

def main():
    binary = ROOT / "peadb-server"
    if not binary.exists():
        binary = ROOT / "build" / "peadb-server"
    p = subprocess.Popen([str(binary), "--port", str(PORT), "--bind", "127.0.0.1", "--loglevel", "error"])
    passed = failed = 0
    try:
        time.sleep(0.5)
        s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
        cmd(s, "FLUSHALL")

        def check(name, result, expected):
            nonlocal passed, failed
            if ok(name, result == expected, f"got {result!r}, expected {expected!r}"):
                passed += 1
            else:
                failed += 1

        def check_type(name, result, expected_type):
            nonlocal passed, failed
            if ok(name, isinstance(result, expected_type), f"got {type(result).__name__}={result!r}"):
                passed += 1
            else:
                failed += 1

        # ── CLIENT ─────────────────────────────────────────
        check_type("CLIENT ID", cmd(s, "CLIENT", "ID"), int)
        check("CLIENT SETNAME", cmd(s, "CLIENT", "SETNAME", "test"), "OK")
        check("CLIENT GETNAME", cmd(s, "CLIENT", "GETNAME"), "test")
        check("CLIENT SETINFO", cmd(s, "CLIENT", "SETINFO", "LIB-NAME", "pytest"), "OK")
        check_type("CLIENT LIST", cmd(s, "CLIENT", "LIST"), str)
        check_type("CLIENT INFO", cmd(s, "CLIENT", "INFO"), str)

        # ── MEMORY ─────────────────────────────────────────
        cmd(s, "SET", "mk", "hello")
        check_type("MEMORY USAGE", cmd(s, "MEMORY", "USAGE", "mk"), int)

        # ── SLOWLOG / LATENCY ──────────────────────────────
        check_type("SLOWLOG GET", cmd(s, "SLOWLOG", "GET"), list)
        check("SLOWLOG LEN", cmd(s, "SLOWLOG", "LEN"), 0)
        check_type("LATENCY LATEST", cmd(s, "LATENCY", "LATEST"), list)

        # ── TOUCH / ROLE / RESET ───────────────────────────
        check("TOUCH existing", cmd(s, "TOUCH", "mk"), 1)
        check("TOUCH missing", cmd(s, "TOUCH", "no-such"), 0)
        r = cmd(s, "ROLE")
        check("ROLE", isinstance(r, list) and r[0] == "master", True)
        cmd(s, "RESET")

        # ── LIST operations ────────────────────────────────
        cmd(s, "RPUSH", "lst", "a", "b", "c", "d", "e")
        check("LINDEX 0", cmd(s, "LINDEX", "lst", "0"), "a")
        check("LINDEX -1", cmd(s, "LINDEX", "lst", "-1"), "e")
        check("LPOS b", cmd(s, "LPOS", "lst", "b"), 1)
        check("LPUSHX", cmd(s, "LPUSHX", "lst", "z"), 6)
        check("RPUSHX", cmd(s, "RPUSHX", "lst", "y"), 7)
        check("LINSERT", cmd(s, "LINSERT", "lst", "BEFORE", "a", "x"), 8)
        check("LSET", cmd(s, "LSET", "lst", "0", "X"), "OK")
        check("LREM", cmd(s, "LREM", "lst", "1", "X"), 1)
        check("LTRIM", cmd(s, "LTRIM", "lst", "0", "3"), "OK")
        cmd(s, "DEL", "lst", "dst")
        cmd(s, "RPUSH", "lst", "a", "b")
        check_type("RPOPLPUSH", cmd(s, "RPOPLPUSH", "lst", "dst"), str)
        cmd(s, "DEL", "lst", "dst")
        cmd(s, "RPUSH", "lst", "a", "b", "c")
        r = cmd(s, "LMPOP", "1", "lst", "LEFT")
        check("LMPOP", isinstance(r, list) and r[0] == "lst", True)

        # ── SORTED SET operations ──────────────────────────
        cmd(s, "DEL", "zs", "zs2", "zdst")
        cmd(s, "ZADD", "zs", "1", "a", "2", "b", "3", "c")
        check("ZCARD", cmd(s, "ZCARD", "zs"), 3)
        check("ZSCORE a", cmd(s, "ZSCORE", "zs", "a"), "1")
        r = cmd(s, "ZMSCORE", "zs", "a", "b", "missing")
        check("ZMSCORE", r[0] == "1" and r[1] == "2" and r[2] is None, True)
        check("ZCOUNT", cmd(s, "ZCOUNT", "zs", "-inf", "+inf"), 3)
        check("ZRANK a", cmd(s, "ZRANK", "zs", "a"), 0)
        check("ZREVRANK a", cmd(s, "ZREVRANK", "zs", "a"), 2)
        check("ZREM b", cmd(s, "ZREM", "zs", "b"), 1)
        check("ZCARD after rem", cmd(s, "ZCARD", "zs"), 2)
        r = cmd(s, "ZINCRBY", "zs", "10", "a")
        check("ZINCRBY", r, "11")

        cmd(s, "DEL", "zs"); cmd(s, "ZADD", "zs", "1", "a", "2", "b", "3", "c")
        r = cmd(s, "ZRANGEBYSCORE", "zs", "1", "2")
        check("ZRANGEBYSCORE", r, ["a", "b"])
        r = cmd(s, "ZREVRANGEBYSCORE", "zs", "3", "2")
        check("ZREVRANGEBYSCORE", r, ["c", "b"])
        r = cmd(s, "ZREVRANGE", "zs", "0", "-1")
        check("ZREVRANGE", r, ["c", "b", "a"])
        check("ZLEXCOUNT", cmd(s, "ZLEXCOUNT", "zs", "-", "+"), 3)
        check_type("ZRANDMEMBER", cmd(s, "ZRANDMEMBER", "zs"), str)

        cmd(s, "ZADD", "zs2", "10", "a", "20", "d")
        check_type("ZUNIONSTORE", cmd(s, "ZUNIONSTORE", "zdst", "2", "zs", "zs2"), int)
        cmd(s, "DEL", "zdst")
        check_type("ZINTERSTORE", cmd(s, "ZINTERSTORE", "zdst", "2", "zs", "zs2"), int)
        cmd(s, "DEL", "zdst")
        check_type("ZDIFFSTORE", cmd(s, "ZDIFFSTORE", "zdst", "2", "zs", "zs2"), int)
        check_type("ZUNION", cmd(s, "ZUNION", "2", "zs", "zs2"), list)
        check_type("ZINTER", cmd(s, "ZINTER", "2", "zs", "zs2"), list)
        check_type("ZDIFF", cmd(s, "ZDIFF", "2", "zs", "zs2"), list)

        # ── SET operations ─────────────────────────────────
        cmd(s, "DEL", "sa", "sb", "sdst")
        cmd(s, "SADD", "sa", "x", "y", "z")
        cmd(s, "SADD", "sb", "y", "z", "w")
        check_type("SRANDMEMBER", cmd(s, "SRANDMEMBER", "sa"), str)
        r = cmd(s, "SINTER", "sa", "sb"); r.sort()
        check("SINTER", r, ["y", "z"])
        r = cmd(s, "SUNION", "sa", "sb"); r.sort()
        check("SUNION", r, ["w", "x", "y", "z"])
        r = cmd(s, "SDIFF", "sa", "sb"); r.sort()
        check("SDIFF", r, ["x"])
        r = cmd(s, "SMISMEMBER", "sa", "x", "w")
        check("SMISMEMBER", r, [1, 0])
        check("SINTERCARD", cmd(s, "SINTERCARD", "2", "sa", "sb"), 2)
        check_type("SINTERSTORE", cmd(s, "SINTERSTORE", "sdst", "sa", "sb"), int)
        cmd(s, "DEL", "sdst")
        check_type("SUNIONSTORE", cmd(s, "SUNIONSTORE", "sdst", "sa", "sb"), int)
        cmd(s, "DEL", "sdst")
        check_type("SDIFFSTORE", cmd(s, "SDIFFSTORE", "sdst", "sa", "sb"), int)

        # ── HASH ───────────────────────────────────────────
        cmd(s, "HSET", "hh", "f1", "v1", "f2", "v2", "f3", "v3")
        check_type("HRANDFIELD", cmd(s, "HRANDFIELD", "hh"), str)

        # ── BIT operations ─────────────────────────────────
        cmd(s, "SET", "bk", "hello")
        check_type("BITCOUNT", cmd(s, "BITCOUNT", "bk"), int)
        check_type("BITPOS 1", cmd(s, "BITPOS", "bk", "1"), int)
        check_type("BITOP AND", cmd(s, "BITOP", "AND", "bdst", "bk"), int)
        r = cmd(s, "BITFIELD", "bk", "GET", "u8", "0")
        check("BITFIELD GET", isinstance(r, list) and isinstance(r[0], int), True)

        # ── GEO operations ─────────────────────────────────
        cmd(s, "GEOADD", "geo", "13.361389", "38.115556", "palermo",
            "15.087269", "37.502669", "catania")
        r = cmd(s, "GEODIST", "geo", "palermo", "catania", "km")
        check("GEODIST", r is not None and float(r) > 100, True)
        r = cmd(s, "GEOPOS", "geo", "palermo")
        check("GEOPOS", isinstance(r, list) and isinstance(r[0], list), True)
        r = cmd(s, "GEOHASH", "geo", "palermo")
        check("GEOHASH", isinstance(r, list) and isinstance(r[0], str), True)
        check_type("GEORADIUS", cmd(s, "GEORADIUS", "geo", "15", "37", "200", "km"), list)
        check_type("GEORADIUSBYMEMBER", cmd(s, "GEORADIUSBYMEMBER", "geo", "palermo", "200", "km"), list)
        check_type("GEOSEARCH", cmd(s, "GEOSEARCH", "geo", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"), list)
        check_type("GEOSEARCHSTORE", cmd(s, "GEOSEARCHSTORE", "geodst", "geo", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"), int)

        # ── STREAM operations ──────────────────────────────
        cmd(s, "XADD", "st", "*", "k", "v1")
        cmd(s, "XADD", "st", "*", "k", "v2")
        cmd(s, "XADD", "st", "*", "k", "v3")
        check_type("XTRIM", cmd(s, "XTRIM", "st", "MAXLEN", "2"), int)
        cmd(s, "XGROUP", "CREATE", "st", "grp", "0", "MKSTREAM")
        r = cmd(s, "XAUTOCLAIM", "st", "grp", "consumer", "0", "0-0")
        check("XAUTOCLAIM", isinstance(r, list), True)

        cmd(s, "FLUSHALL")
        s.close()

        print(f"P3 ecosystem commands: {passed} passed, {failed} failed")
        return 1 if failed > 0 else 0
    finally:
        p.terminate(); p.wait(timeout=3)

if __name__ == "__main__":
    raise SystemExit(main())

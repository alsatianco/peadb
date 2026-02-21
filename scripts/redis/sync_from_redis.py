#!/usr/bin/env python3
"""sync_from_redis.py -- Copy keys from one Redis-compatible instance to another.

Purpose
    Pragmatic one-shot migration helper for copying the entire keyspace from
    a source Redis-compatible server (e.g. upstream Redis) to a target
    (e.g. PeaDB), or vice versa.  Useful for smoke-testing PeaDB against a
    real dataset or for simple cross-instance migrations.

What it does
    1. Connects to the source and target servers via raw TCP sockets (no
       external dependencies).
    2. Optionally flushes the target (--flush-target).
    3. Iterates the source keyspace using SCAN (COUNT 1000).
    4. For each key, reads its TYPE and value from the source and replays
       the equivalent write command to the target.  Supported types:
         - string  (GET -> SET)
         - hash    (HGETALL -> HSET)
         - list    (LRANGE 0 -1 -> RPUSH)
         - set     (SMEMBERS -> SADD)
         - zset    (ZRANGE 0 -1 WITHSCORES -> ZADD)
         - stream  (XRANGE - + -> XADD with original entry IDs)
    5. Preserves per-key TTLs via PTTL + PEXPIRE when the source TTL is
       positive.

How to run
    From the repo root:

        python3 scripts/redis/sync_from_redis.py \
            --source-port 6379 --target-port 6380

    Optional flags:
        --source-host HOST   Source host (default: 127.0.0.1)
        --target-host HOST   Target host (default: 127.0.0.1)
        --flush-target       Run FLUSHALL on target before syncing

Prerequisites
    - Python 3.9+ (stdlib only, no pip packages).
    - Both source and target servers must be running and reachable.

Interpreting the output
    On success:
        sync complete
        (exit code 0)

    On failure, a Python exception is printed (e.g. connection refused,
    protocol parse error).

Exit codes
    0   Sync succeeded.
    Non-zero   Connection or protocol errors.

Limitations
    - This is a point-in-time snapshot, not a live continuous sync.
    - Keys of unsupported types are silently skipped.
    - Does not preserve eviction policies, ACLs, module data, or Lua
      scripts.
    - Large datasets are slow due to per-key round-trip overhead.
    - PTTL precision may drift slightly between read and apply.
"""
from __future__ import annotations

import argparse
import socket


def _arg_to_bytes(a: str | bytes | int) -> bytes:
    if isinstance(a, bytes):
        return a
    return str(a).encode("utf-8", errors="surrogatepass")


def _to_text(v: str | bytes) -> str:
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return v


def enc(args: list[str | bytes | int]) -> bytes:
    out = [f"*{len(args)}\r\n".encode()]
    for a in args:
        b = _arg_to_bytes(a)
        out.append(f"${len(b)}\r\n".encode())
        out.append(b + b"\r\n")
    return b"".join(out)


def rx(sock: socket.socket, n: int) -> bytes:
    b = bytearray()
    while len(b) < n:
        c = sock.recv(n - len(b))
        if not c:
            raise RuntimeError("connection closed")
        b.extend(c)
    return bytes(b)


def rl(sock: socket.socket) -> bytes:
    b = bytearray()
    while True:
        b.extend(rx(sock, 1))
        if len(b) >= 2 and b[-2:] == b"\r\n":
            return bytes(b[:-2])


def dec(sock: socket.socket):
    p = rx(sock, 1)
    if p == b"+":
        return rl(sock).decode()
    if p == b"-":
        return ("ERR", rl(sock).decode())
    if p == b":":
        return int(rl(sock).decode())
    if p == b"$":
        n = int(rl(sock).decode())
        if n == -1:
            return None
        b = rx(sock, n)
        rx(sock, 2)
        return b
    if p == b"*":
        n = int(rl(sock).decode())
        if n == -1:
            return None
        return [dec(sock) for _ in range(n)]
    raise RuntimeError(f"unsupported prefix {p!r}")


def cmd(sock: socket.socket, *args: str | bytes | int):
    sock.sendall(enc(list(args)))
    return dec(sock)


def iter_keys(sock: socket.socket):
    cursor = "0"
    while True:
        resp = cmd(sock, "SCAN", cursor, "COUNT", 1000)
        if not isinstance(resp, list) or len(resp) != 2:
            return
        cursor = _to_text(resp[0])
        keys = resp[1] or []
        for key in keys:
            yield key
        if cursor == "0":
            break


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-host", default="127.0.0.1")
    ap.add_argument("--source-port", type=int, required=True)
    ap.add_argument("--target-host", default="127.0.0.1")
    ap.add_argument("--target-port", type=int, required=True)
    ap.add_argument("--flush-target", action="store_true")
    args = ap.parse_args()

    with socket.create_connection((args.source_host, args.source_port), timeout=5) as src, socket.create_connection((args.target_host, args.target_port), timeout=5) as dst:
        if args.flush_target:
            cmd(dst, "FLUSHALL")

        for k in iter_keys(src):
            t = cmd(src, "TYPE", k)
            if isinstance(t, bytes):
                t = _to_text(t)
            if t == "string":
                v = cmd(src, "GET", k)
                cmd(dst, "SET", k, v if v is not None else "")
            elif t == "hash":
                fv = cmd(src, "HGETALL", k) or []
                if fv:
                    cmd(dst, "HSET", k, *fv)
            elif t == "list":
                vals = cmd(src, "LRANGE", k, "0", "-1") or []
                if vals:
                    cmd(dst, "RPUSH", k, *vals)
            elif t == "set":
                vals = cmd(src, "SMEMBERS", k) or []
                if vals:
                    cmd(dst, "SADD", k, *vals)
            elif t == "zset":
                vals = cmd(src, "ZRANGE", k, "0", "-1", "WITHSCORES") or []
                if vals:
                    # ZRANGE returns [member, score, ...]; ZADD expects [score, member, ...]
                    zadd_args = []
                    for i in range(0, len(vals), 2):
                        zadd_args.append(vals[i + 1])  # score
                        zadd_args.append(vals[i])      # member
                    cmd(dst, "ZADD", k, *zadd_args)
            elif t == "stream":
                entries = cmd(src, "XRANGE", k, "-", "+") or []
                for e in entries:
                    sid = e[0]
                    fields = e[1] if len(e) > 1 else []
                    cmd(dst, "XADD", k, sid, *fields)

            pttl = cmd(src, "PTTL", k)
            if isinstance(pttl, int) and pttl > 0:
                cmd(dst, "PEXPIRE", k, pttl)

    print("sync complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

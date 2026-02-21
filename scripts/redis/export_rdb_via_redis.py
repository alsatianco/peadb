#!/usr/bin/env python3
"""export_rdb_via_redis.py -- Export PeaDB data into an RDB file via redis-server.

Purpose
    Creates a standard Redis RDB snapshot of the data stored in a running
    PeaDB instance.  This is useful for migrating data from PeaDB to Redis,
    creating backups in a format that Redis can ingest, or inspecting the
    dataset with tools that read RDB files.

What it does
    1. Starts a temporary ``redis-server`` in a fresh temp directory (on a
       configurable port, default 6512) with persistence disabled (save "").
    2. Connects to both the running PeaDB instance (--peadb-port) and the
       temporary Redis instance.
    3. Iterates the PeaDB keyspace using SCAN (COUNT 1000).
    4. For each key, reads its TYPE and value from PeaDB and replays the
       equivalent write command to Redis.  Supported types:
         - string (GET -> SET)
         - hash   (HGETALL -> HSET)
         - list   (LRANGE 0 -1 -> RPUSH)
         - set    (SMEMBERS -> SADD)
         - zset   (ZRANGE 0 -1 WITHSCORES -> ZADD)
         - stream (XRANGE - + -> XADD)
    5. Preserves per-key TTLs: reads PTTL from PeaDB and applies PEXPIRE
       on the Redis side when the TTL is positive.
    6. Calls SAVE on the temporary Redis to flush data to ``dump.rdb``.
    7. Copies the resulting ``dump.rdb`` to the ``--out`` path.
    8. Terminates and cleans up the temporary Redis server.

How to run
    From the repo root:

        python3 scripts/redis/export_rdb_via_redis.py \
            --peadb-port 6379 --out /tmp/peadb-dump.rdb

    Optional flags:
        --peadb-host HOST     PeaDB host (default: 127.0.0.1)
        --target-port PORT    Port for the temporary Redis (default: 6512)

Configuration (env vars)
    REDIS_SERVER    Path to the ``redis-server`` binary if it is not in PATH.

Prerequisites
    - Python 3.9+
    - ``redis-server`` installed and either in PATH or pointed to by
      REDIS_SERVER.
    - A running PeaDB instance on the specified host/port.

Interpreting the output
    On success:
        export complete
        (exit code 0)
    The RDB file at --out is a standard Redis dump that can be loaded by
    ``redis-server`` or ``import_rdb_via_redis.py``.

    On failure, a Python exception or RuntimeError is printed (exit code 1).

Exit codes
    0   Export succeeded.
    Non-zero   Missing binaries, connection failures, or protocol errors.

Limitations
    - Keys of unsupported types (e.g. module-specific types) are silently
      skipped.
    - Large datasets may take significant time due to per-key round trips.
    - PTTL precision may drift slightly between read and apply.
"""
from __future__ import annotations

import argparse
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time
import os

ROOT = pathlib.Path(__file__).resolve().parents[2]


def resolve_redis_server() -> str:
    env = os.environ.get("REDIS_SERVER")
    if env:
        return env
    third_party = str(ROOT / "third_party" / "redis" / "src" / "redis-server")
    if os.path.isfile(third_party) and os.access(third_party, os.X_OK):
        return third_party
    which = shutil.which("redis-server")
    if which:
        return which
    raise RuntimeError("redis-server not found (set REDIS_SERVER or install redis-server)")


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


def wait_ready(port: int) -> None:
    end = time.time() + 5
    while time.time() < end:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("redis target not ready")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--peadb-port", required=True, type=int)
    ap.add_argument("--peadb-host", default="127.0.0.1")
    ap.add_argument("--out", required=True)
    ap.add_argument("--target-port", type=int, default=6512)
    args = ap.parse_args()

    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    redis_server = resolve_redis_server()

    with tempfile.TemporaryDirectory(prefix="peadb-rdb-export-") as td:
        tdp = pathlib.Path(td)
        conf = tdp / "redis.conf"
        conf.write_text(
            "\n".join(
                [
                    "bind 127.0.0.1",
                    f"port {args.target_port}",
                    f"dir {tdp}",
                    "dbfilename dump.rdb",
                    "appendonly no",
                    'save ""',
                    "daemonize no",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        proc = subprocess.Popen([redis_server, str(conf)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            wait_ready(args.target_port)
            with socket.create_connection((args.peadb_host, args.peadb_port), timeout=3) as src, socket.create_connection(("127.0.0.1", args.target_port), timeout=3) as dst:
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

                cmd(dst, "SAVE")
            shutil.copy2(tdp / "dump.rdb", out_path)
        finally:
            proc.terminate()
            proc.wait(timeout=3)

    print("export complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

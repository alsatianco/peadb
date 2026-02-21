#!/usr/bin/env python3
"""import_rdb_via_redis.py -- Import an RDB file into PeaDB via redis-server.

Purpose
    Loads data from a standard Redis RDB snapshot into a running PeaDB
    instance.  This is the inverse of ``export_rdb_via_redis.py`` and is
    useful for bootstrapping PeaDB from an existing Redis dataset or
    restoring a backup.

What it does
    1. Copies the specified RDB file into a fresh temp directory.
    2. Starts a temporary ``redis-server`` that loads the RDB on startup
       (configurable port, default 6511; persistence disabled).
    3. Waits for the temporary Redis to become ready (up to 5 seconds).
    4. Connects to both the temporary Redis (source) and the running PeaDB
       instance (target).
    5. Optionally flushes the PeaDB target (--flush-target).
    6. Iterates the Redis keyspace using SCAN (COUNT 1000).
    7. For each key, reads its TYPE and value from Redis and replays the
       equivalent write command to PeaDB.  Supported types:
         - string, hash, list, set, zset, stream
    8. Preserves per-key TTLs via PTTL + PEXPIRE.
    9. Terminates and cleans up the temporary Redis server.

How to run
    From the repo root:

        python3 scripts/redis/import_rdb_via_redis.py \
            --rdb /path/to/dump.rdb --peadb-port 6379

    Optional flags:
        --peadb-host HOST      PeaDB host (default: 127.0.0.1)
        --source-port PORT     Port for the temporary Redis (default: 6511)
        --flush-target         Run FLUSHALL on PeaDB before importing

Configuration (env vars)
    REDIS_SERVER    Path to the ``redis-server`` binary if it is not in PATH.

Prerequisites
    - Python 3.9+
    - ``redis-server`` installed and either in PATH or pointed to by
      REDIS_SERVER.
    - A running PeaDB instance on the specified host/port.
    - The RDB file must exist and be a valid Redis dump.

Interpreting the output
    On success:
        import complete
        (exit code 0)

    On failure, a Python exception or RuntimeError is printed (exit code 1).
    Common issues:
      - "redis-server not found" -- install Redis or set REDIS_SERVER.
      - "rdb file not found: ..." -- check the --rdb path.
      - "redis source not ready" -- the temporary Redis failed to start
        (check the RDB file validity or port conflicts).

Exit codes
    0   Import succeeded.
    Non-zero   Missing binaries, invalid RDB, connection failures, or
               protocol errors.

Limitations
    - Same as export: unsupported types are silently skipped, large
      datasets are slow, PTTL may drift.
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
    raise RuntimeError("redis source not ready")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rdb", required=True)
    ap.add_argument("--peadb-port", required=True, type=int)
    ap.add_argument("--peadb-host", default="127.0.0.1")
    ap.add_argument("--source-port", type=int, default=6511)
    ap.add_argument("--flush-target", action="store_true")
    args = ap.parse_args()

    rdb = pathlib.Path(args.rdb)
    if not rdb.exists():
        raise SystemExit(f"rdb file not found: {rdb}")

    redis_server = resolve_redis_server()

    with tempfile.TemporaryDirectory(prefix="peadb-rdb-import-") as td:
        tdp = pathlib.Path(td)
        shutil.copy2(rdb, tdp / "dump.rdb")
        conf = tdp / "redis.conf"
        conf.write_text(
            "\n".join(
                [
                    "bind 127.0.0.1",
                    f"port {args.source_port}",
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
        src_proc = subprocess.Popen([redis_server, str(conf)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            wait_ready(args.source_port)
            with socket.create_connection(("127.0.0.1", args.source_port), timeout=3) as src, socket.create_connection((args.peadb_host, args.peadb_port), timeout=3) as dst:
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
        finally:
            src_proc.terminate()
            src_proc.wait(timeout=3)

    print("import complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
import os
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready
from _isolated_server import IsolatedServer
from test_replica_offset_parity import PEADB, REDIS, check
from test_replica_resync_parity import ports


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
        return v.decode()
    if p == b"*":
        n = int(rl(s))
        if n == -1:
            return None
        return [recv(s) for _ in range(n)]
    raise RuntimeError(p)


def cmd(s, *a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        b = x.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    s.sendall(d)
    return recv(s)


def snapshot(sock):
    # replicationCron sends bare newlines while the disk snapshot is pending. They
    # precede the bulk header and do not belong to the RDB payload (Redis 7.2.5).
    deadline = time.monotonic() + 10
    keepalives = 0
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('SYNC never sent an RDB bulk header')
        sock.settimeout(remaining)
        prefix = rx(sock, 1)
        if prefix != b"\n":
            break
        keepalives += 1
    assert prefix == b"$", f'unexpected SYNC header prefix: {prefix!r}'
    size = int(rl(sock))
    assert size > 9, f'invalid RDB size: {size}'
    payload = rx(sock, size)
    assert payload.startswith(b"REDIS"), 'SYNC must send the full-state native RDB'
    return keepalives


def run(binary):
    port = ports(1)[0]
    p = IsolatedServer([binary, "--port", str(port), "--bind", "127.0.0.1", "--loglevel", "warning",
                        "--save", "", "--shutdown-timeout", "0", "--repl-diskless-sync", "no",
                        "--repl-diskless-sync-delay", "0"])
    try:
        wait_until_ready(port, process=p)
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"
            assert cmd(s, "SET", "k", "v") == "OK"
            assert cmd(s, "OBJECT", "REFCOUNT", "k") == 1
            assert cmd(s, "SELECT", "1") == "OK"
            assert cmd(s, "SET", "k2", "v2") == "OK"
            assert cmd(s, "SWAPDB", "0", "1") == "OK"
            assert cmd(s, "GET", "k2") is None
            assert cmd(s, "SELECT", "0") == "OK"
            assert cmd(s, "GET", "k2") == "v2"
            # Two keys at 600 ms each guarantee a pending snapshot across a cron
            # keepalive tick; this exercises framing under load without relying on luck.
            assert cmd(s, "CONFIG", "SET", "rdb-key-save-delay", "600000") == "OK"
        with socket.create_connection(("127.0.0.1", port), timeout=5) as rs:
            rs.sendall(b"SYNC\r\n")
            assert snapshot(rs) > 0, 'delayed snapshot must exercise keepalive framing'
        print("P1 OBJECT/SWAPDB/SYNC tests passed:", binary, flush=True)
    finally:
        p.terminate()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


def main():
    check(REDIS is not None, 'pinned Redis reference required')
    version = subprocess.check_output([REDIS, '--version'], text=True)
    check(' v=7.2.5 ' in version, f'Redis 7.2.5 required: {version}')
    run(REDIS)
    run(os.environ.get('PEADB_TEST_BIN', PEADB))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

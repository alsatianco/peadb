#!/usr/bin/env python3
"""Tests for REPLCONF, PSYNC, SLAVEOF, ACL, ASKING."""
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def read_exact(s: socket.socket, n: int) -> bytes:
    b = bytearray()
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c:
            raise RuntimeError("closed")
        b.extend(c)
    return bytes(b)


def read_line(s: socket.socket) -> bytes:
    b = bytearray()
    while True:
        b.extend(read_exact(s, 1))
        if b[-2:] == b"\r\n":
            return bytes(b[:-2])


def recv(s: socket.socket):
    p = read_exact(s, 1)
    if p == b"+":
        return read_line(s).decode()
    if p == b"-":
        return ("ERR", read_line(s).decode())
    if p == b":":
        return int(read_line(s).decode())
    if p == b"$":
        n = int(read_line(s).decode())
        if n == -1:
            return None
        b = read_exact(s, n)
        read_exact(s, 2)
        return b.decode()
    if p == b"*":
        n = int(read_line(s).decode())
        if n == -1:
            return None
        return [recv(s) for _ in range(n)]
    raise RuntimeError(p)


def cmd(s: socket.socket, *args: str):
    data = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode()
        data += f"${len(b)}\r\n".encode() + b + b"\r\n"
    s.sendall(data)
    return recv(s)


def main() -> int:
    proc = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6522",
                             "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6522), timeout=2) as s:
            # ── REPLCONF ─────────────────────────────────────────
            # REPLCONF listening-port
            assert cmd(s, "REPLCONF", "listening-port", "6522") == "OK"

            # REPLCONF capa
            assert cmd(s, "REPLCONF", "capa", "eof") == "OK"

            # REPLCONF GETACK → returns array with REPLCONF ACK <offset>
            r = cmd(s, "REPLCONF", "GETACK", "*")
            assert isinstance(r, list) and len(r) == 3
            assert r[0] == "REPLCONF" and r[1] == "ACK"

        with socket.create_connection(("127.0.0.1", 6522), timeout=2) as s:
            # ── PSYNC ────────────────────────────────────────────
            # PSYNC ? -1 → +FULLRESYNC <replid> <offset>
            s.sendall(f"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode())
            # Read the +FULLRESYNC line
            p = read_exact(s, 1)
            assert p == b"+", f"expected + got {p!r}"
            line = read_line(s).decode()
            assert line.startswith("FULLRESYNC "), f"expected FULLRESYNC got {line!r}"
            parts = line.split()
            assert len(parts) == 3  # FULLRESYNC <replid> <offset>
            # Then comes $<len>\r\n<rdb-data>
            p2 = read_exact(s, 1)
            assert p2 == b"$"
            rdb_len = int(read_line(s).decode())
            assert rdb_len >= 0
            if rdb_len > 0:
                read_exact(s, rdb_len)

        with socket.create_connection(("127.0.0.1", 6522), timeout=2) as s:
            # ── SLAVEOF ──────────────────────────────────────────
            # SLAVEOF NO ONE → same as REPLICAOF NO ONE
            r = cmd(s, "SLAVEOF", "NO", "ONE")
            assert r == "OK"

        with socket.create_connection(("127.0.0.1", 6522), timeout=2) as s:
            # ── ACL ──────────────────────────────────────────────
            assert cmd(s, "ACL", "SETUSER", "testuser") == "OK"

            # Unknown subcommand
            err = cmd(s, "ACL", "NOSUCHCMD")
            assert err[0] == "ERR"

        with socket.create_connection(("127.0.0.1", 6522), timeout=2) as s:
            # ── ASKING ───────────────────────────────────────────
            assert cmd(s, "ASKING") == "OK"

        print("P3 replication/cluster surface tests passed")
        return 0
    finally:
        proc.terminate()
        proc.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

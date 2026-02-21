#!/usr/bin/env python3
"""Tests for SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE."""
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
    proc = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6521",
                             "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6521), timeout=2) as s:
            # ── SUBSCRIBE ────────────────────────────────────────
            r = cmd(s, "SUBSCRIBE", "ch1")
            assert r == ["subscribe", "ch1", 1]

        with socket.create_connection(("127.0.0.1", 6521), timeout=2) as s:
            # SUBSCRIBE to multiple channels
            s.sendall(f"*3\r\n$9\r\nSUBSCRIBE\r\n$3\r\nch1\r\n$3\r\nch2\r\n".encode())
            r1 = recv(s)
            r2 = recv(s)
            assert r1 == ["subscribe", "ch1", 1]
            assert r2 == ["subscribe", "ch2", 2]

        with socket.create_connection(("127.0.0.1", 6521), timeout=2) as s:
            # ── UNSUBSCRIBE ──────────────────────────────────────
            r = cmd(s, "UNSUBSCRIBE", "ch1")
            assert r == ["unsubscribe", "ch1", 0]

            # UNSUBSCRIBE with no args
            r = cmd(s, "UNSUBSCRIBE")
            assert r == ["unsubscribe", None, 0]

        with socket.create_connection(("127.0.0.1", 6521), timeout=2) as s:
            # ── PSUBSCRIBE ───────────────────────────────────────
            r = cmd(s, "PSUBSCRIBE", "ch.*")
            assert r == ["psubscribe", "ch.*", 1]

        with socket.create_connection(("127.0.0.1", 6521), timeout=2) as s:
            # PSUBSCRIBE to multiple patterns
            s.sendall(f"*3\r\n$10\r\nPSUBSCRIBE\r\n$4\r\nch.*\r\n$5\r\nfoo.*\r\n".encode())
            r1 = recv(s)
            r2 = recv(s)
            assert r1 == ["psubscribe", "ch.*", 1]
            assert r2 == ["psubscribe", "foo.*", 2]

        with socket.create_connection(("127.0.0.1", 6521), timeout=2) as s:
            # ── PUNSUBSCRIBE ─────────────────────────────────────
            r = cmd(s, "PUNSUBSCRIBE", "ch.*")
            assert r == ["punsubscribe", "ch.*", 0]

            # PUNSUBSCRIBE with no args
            r = cmd(s, "PUNSUBSCRIBE")
            assert r == ["punsubscribe", None, 0]

        print("P3 pub/sub surface tests passed")
        return 0
    finally:
        proc.terminate()
        proc.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

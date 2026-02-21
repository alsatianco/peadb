#!/usr/bin/env python3
"""Tests for SORT and ZMPOP commands."""
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
    proc = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6523",
                             "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6523), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"

            # ── SORT on list (numeric) ───────────────────────────
            cmd(s, "RPUSH", "nums", "3", "1", "2", "5", "4")
            r = cmd(s, "SORT", "nums")
            assert r == ["1", "2", "3", "4", "5"]

            # SORT DESC
            r = cmd(s, "SORT", "nums", "DESC")
            assert r == ["5", "4", "3", "2", "1"]

            # SORT ALPHA
            cmd(s, "RPUSH", "words", "banana", "apple", "cherry")
            r = cmd(s, "SORT", "words", "ALPHA")
            assert r == ["apple", "banana", "cherry"]

            # SORT ALPHA DESC
            r = cmd(s, "SORT", "words", "ALPHA", "DESC")
            assert r == ["cherry", "banana", "apple"]

            # SORT LIMIT
            r = cmd(s, "SORT", "nums", "LIMIT", "1", "3")
            assert r == ["2", "3", "4"]

            # SORT STORE
            n = cmd(s, "SORT", "nums", "STORE", "sorted")
            assert n == 5
            r = cmd(s, "LRANGE", "sorted", "0", "-1")
            assert r == ["1", "2", "3", "4", "5"]

            # SORT on empty/non-existent key
            r = cmd(s, "SORT", "nokey")
            assert r == []

            # SORT on set
            cmd(s, "SADD", "myset", "10", "2", "30")
            r = cmd(s, "SORT", "myset")
            assert r == ["2", "10", "30"]

            # ── ZMPOP ────────────────────────────────────────────
            cmd(s, "ZADD", "zs1", "1", "a", "2", "b", "3", "c")

            # ZMPOP 1 key MIN
            r = cmd(s, "ZMPOP", "1", "zs1", "MIN")
            assert isinstance(r, list) and len(r) == 2
            assert r[0] == "zs1"
            assert r[1] == ["a", "1"]

            # ZMPOP 1 key MAX
            r = cmd(s, "ZMPOP", "1", "zs1", "MAX")
            assert r[0] == "zs1"
            assert r[1] == ["c", "3"]

            # ZMPOP with COUNT
            cmd(s, "ZADD", "zs2", "1", "x", "2", "y", "3", "z")
            r = cmd(s, "ZMPOP", "1", "zs2", "MIN", "COUNT", "2")
            assert r[0] == "zs2"
            assert len(r[1]) == 4  # 2 members × 2 (member + score)

            # ZMPOP on empty key → nil
            r = cmd(s, "ZMPOP", "1", "emptykey", "MIN")
            assert r is None

            # ZMPOP multiple keys — uses first non-empty
            cmd(s, "ZADD", "zs3", "10", "m")
            r = cmd(s, "ZMPOP", "2", "emptykey", "zs3", "MIN")
            assert r[0] == "zs3"
            assert r[1] == ["m", "10"]

            # ── EVALSHA_RO / FCALL_RO surface ────────────────────
            # EVALSHA_RO with non-existent SHA → NOSCRIPT error
            err = cmd(s, "EVALSHA_RO", "0000000000000000000000000000000000000000", "0")
            assert err[0] == "ERR" and "NOSCRIPT" in err[1]

            # FCALL_RO with non-existent function → error
            err = cmd(s, "FCALL_RO", "nosuchfunc", "0")
            assert err[0] == "ERR"

        print("P3 SORT/ZMPOP/scripting-ro tests passed")
        return 0
    finally:
        proc.terminate()
        proc.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

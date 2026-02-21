#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


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


def to_dict(arr):
    assert isinstance(arr, list) and len(arr) % 2 == 0
    d = {}
    for i in range(0, len(arr), 2):
        d[arr[i]] = arr[i + 1]
    return d


def main():
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6478", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6478), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"
            assert cmd(s, "SET", "a", "ohmytext") == "OK"
            assert cmd(s, "SET", "b", "mynewtext") == "OK"

            assert cmd(s, "LCS", "a", "b") == "mytext"
            assert cmd(s, "LCS", "a", "b", "LEN") == 6

            idx = cmd(s, "LCS", "a", "b", "IDX")
            d = to_dict(idx)
            assert d["len"] == 6
            assert isinstance(d["matches"], list) and len(d["matches"]) >= 1

            idx_len = cmd(s, "LCS", "a", "b", "IDX", "WITHMATCHLEN", "MINMATCHLEN", "2")
            d2 = to_dict(idx_len)
            assert d2["len"] == 6
            for m in d2["matches"]:
                assert len(m) == 3 and isinstance(m[2], int) and m[2] >= 2

        print("P1 LCS tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

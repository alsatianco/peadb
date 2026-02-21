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


def main():
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6473", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6473), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"
            assert cmd(s, "XADD", "x", "100", "a", "1") == "100-0"
            assert cmd(s, "XADD", "x", "101", "a", "2") == "101-0"
            assert cmd(s, "XADD", "x", "102", "a", "3") == "102-0"
            assert cmd(s, "XLEN", "x") == 3

            assert cmd(s, "XDEL", "x", "101-0") == 1
            assert cmd(s, "XLEN", "x") == 2
            rows = cmd(s, "XRANGE", "x", "-", "+")
            ids = [r[0] for r in rows]
            assert ids == ["100-0", "102-0"]

            assert cmd(s, "XDEL", "x", "101-0", "999-0") == 0

            assert cmd(s, "SET", "k", "v") == "OK"
            err = cmd(s, "XDEL", "k", "1-0")
            assert isinstance(err, tuple) and err[0] == "ERR" and "WRONGTYPE" in err[1]

        print("P1 XDEL tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

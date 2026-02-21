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
    raise RuntimeError(p)


def cmd(s, *a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        b = x.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    s.sendall(d)
    return recv(s)


def main():
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6479", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6479), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"
            assert cmd(s, "SET", "k", "v") == "OK"

            assert cmd(s, "EXPIRE", "k", "100", "NX") == 1
            ttl1 = cmd(s, "TTL", "k")
            assert isinstance(ttl1, int) and ttl1 > 0
            assert cmd(s, "EXPIRE", "k", "200", "NX") == 0

            assert cmd(s, "EXPIRE", "k", "200", "XX") == 1
            assert cmd(s, "EXPIRE", "k", "300", "GT") == 1
            assert cmd(s, "EXPIRE", "k", "100", "GT") == 0
            assert cmd(s, "EXPIRE", "k", "50", "LT") == 1
            assert cmd(s, "EXPIRE", "k", "200", "LT") == 0

            err = cmd(s, "EXPIRE", "k", "10", "NX", "XX")
            assert isinstance(err, tuple) and err[0] == "ERR"

        print("P1 EXPIRE options tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

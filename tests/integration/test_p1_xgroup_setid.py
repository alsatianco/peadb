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
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6472", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6472), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"
            assert cmd(s, "XADD", "x", "100", "a", "1") == "100-0"
            assert cmd(s, "XADD", "x", "101", "a", "2") == "101-0"
            assert cmd(s, "XADD", "x", "102", "a", "3") == "102-0"
            assert cmd(s, "XGROUP", "CREATE", "x", "g1", "0") == "OK"

            r1 = cmd(s, "XREADGROUP", "GROUP", "g1", "c1", "COUNT", "1", "STREAMS", "x", ">")
            assert r1[0][1][0][0] == "100-0"

            assert cmd(s, "XGROUP", "SETID", "x", "g1", "101-0") == "OK"
            r2 = cmd(s, "XREADGROUP", "GROUP", "g1", "c1", "COUNT", "1", "STREAMS", "x", ">")
            assert r2[0][1][0][0] == "102-0"

            assert cmd(s, "XGROUP", "SETID", "x", "g1", "$") == "OK"
            assert cmd(s, "XADD", "x", "103", "a", "4") == "103-0"
            r3 = cmd(s, "XREADGROUP", "GROUP", "g1", "c1", "COUNT", "1", "STREAMS", "x", ">")
            assert r3[0][1][0][0] == "103-0"

        print("P1 XGROUP SETID tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

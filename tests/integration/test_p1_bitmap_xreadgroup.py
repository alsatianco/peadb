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
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6469", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6469), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"

            # SETBIT/GETBIT base behavior and bit-order semantics.
            assert cmd(s, "SETBIT", "mykey", "1", "1") == 0
            assert cmd(s, "GET", "mykey") == "@"
            assert cmd(s, "GETBIT", "mykey", "0") == 0
            assert cmd(s, "GETBIT", "mykey", "1") == 1
            assert cmd(s, "GETBIT", "mykey", "2") == 0
            assert cmd(s, "GETBIT", "mykey", "8") == 0

            # Existing string updates should report previous bit.
            assert cmd(s, "SET", "mykey", "@") == "OK"
            assert cmd(s, "SETBIT", "mykey", "2", "1") == 0
            assert cmd(s, "GET", "mykey") == "`"
            assert cmd(s, "SETBIT", "mykey", "1", "0") == 1
            assert cmd(s, "GET", "mykey") == " "

            # Wrongtype and range/bit validation.
            assert cmd(s, "LPUSH", "alist", "x") == 1
            err = cmd(s, "SETBIT", "alist", "0", "1")
            assert isinstance(err, tuple) and err[0] == "ERR" and "WRONGTYPE" in err[1]

            err = cmd(s, "SETBIT", "mykey", str(4 * 1024 * 1024 * 1024), "1")
            assert isinstance(err, tuple) and err[0] == "ERR" and "out of range" in err[1]
            err = cmd(s, "SETBIT", "mykey", "0", "2")
            assert isinstance(err, tuple) and err[0] == "ERR" and "out of range" in err[1]

            # XREADGROUP option parser should accept NOACK + COUNT before STREAMS.
            assert cmd(s, "DEL", "x") == 0
            assert cmd(s, "XADD", "x", "100", "a", "1") == "100-0"
            assert cmd(s, "XGROUP", "CREATE", "x", "g1", "0") == "OK"
            rows = cmd(s, "XREADGROUP", "GROUP", "g1", "bob", "NOACK", "COUNT", "1", "STREAMS", "x", ">")
            assert isinstance(rows, list) and len(rows) == 1
            assert rows[0][0] == "x"
            assert rows[0][1][0][0] == "100-0"

        print("P1 bitmap/XREADGROUP option tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

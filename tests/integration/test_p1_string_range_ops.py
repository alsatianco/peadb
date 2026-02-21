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
        return v.decode("latin1")
    if p == b"*":
        n = int(rl(s))
        if n == -1:
            return None
        return [recv(s) for _ in range(n)]
    raise RuntimeError(p)


def cmd(s, *a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        b = x.encode("latin1")
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    s.sendall(d)
    return recv(s)


def main():
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6470", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6470), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"

            # Non-existing key behavior.
            assert cmd(s, "SETRANGE", "k", "0", "foo") == 3
            assert cmd(s, "GET", "k") == "foo"
            assert cmd(s, "DEL", "k") == 1
            assert cmd(s, "SETRANGE", "k", "1", "foo") == 4
            assert cmd(s, "GET", "k") == "\x00foo"
            assert cmd(s, "DEL", "k") == 1
            assert cmd(s, "SETRANGE", "k", "0", "") == 0
            assert cmd(s, "EXISTS", "k") == 0

            # Existing key overwrite + append semantics.
            assert cmd(s, "SET", "k", "foo") == "OK"
            assert cmd(s, "SETRANGE", "k", "0", "b") == 3
            assert cmd(s, "GET", "k") == "boo"
            assert cmd(s, "SETRANGE", "k", "4", "bar") == 7
            assert cmd(s, "GET", "k") == "boo\x00bar"

            # GETRANGE / SUBSTR compatibility.
            assert cmd(s, "GETRANGE", "k", "0", "2") == "boo"
            assert cmd(s, "GETRANGE", "k", "-3", "-1") == "bar"
            assert cmd(s, "GETRANGE", "k", "10", "20") == ""
            assert cmd(s, "SUBSTR", "k", "0", "2") == "boo"
            assert cmd(s, "GETRANGE", "nope", "0", "-1") == ""

            # Wrongtype and validation.
            assert cmd(s, "LPUSH", "alist", "x") == 1
            err = cmd(s, "SETRANGE", "alist", "0", "x")
            assert isinstance(err, tuple) and err[0] == "ERR" and "WRONGTYPE" in err[1]
            err = cmd(s, "GETRANGE", "alist", "0", "1")
            assert isinstance(err, tuple) and err[0] == "ERR" and "WRONGTYPE" in err[1]

            err = cmd(s, "SETRANGE", "k", "-1", "x")
            assert isinstance(err, tuple) and err[0] == "ERR" and "out of range" in err[1]

        print("P1 string range tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def enc(*a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        b = x.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return d


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
    s.sendall(enc(*a))
    return recv(s)


def parse_repl_cmd(s):
    p = rx(s, 1)
    if p != b"*":
        raise RuntimeError("expected array")
    n = int(rl(s))
    out = []
    for _ in range(n):
        assert rx(s, 1) == b"$"
        ln = int(rl(s))
        out.append(rx(s, ln).decode())
        rx(s, 2)
    return out


def main():
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6483", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6483), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"
            assert cmd(s, "SET", "foo", "bar") == "OK"
            e = cmd(s, "EXPIRE", "foo", "10", "LT", "GT")
            assert e == ("ERR", "ERR GT and LT options at the same time are not compatible")
            e = cmd(s, "EXPIRE", "foo", "10", "NX", "XX")
            assert e == ("ERR", "ERR NX and XX, GT or LT options at the same time are not compatible")
            e = cmd(s, "EXPIRE", "foo", "10", "AB")
            assert e == ("ERR", "ERR Unsupported option AB")

        with socket.create_connection(("127.0.0.1", 6483), timeout=2) as c, socket.create_connection(("127.0.0.1", 6483), timeout=2) as r:
            assert cmd(c, "FLUSHALL") == "OK"
            assert cmd(c, "DEBUG", "SET-ACTIVE-EXPIRE", "0") == "OK"
            assert cmd(c, "SET", "foo", "bar", "PX", "1") == "OK"
            r.sendall(b"SYNC\r\n")
            assert rx(r, 1) == b"$"
            _ = rl(r)
            rx(r, 2)  # consume trailing CRLF of empty RDB bulk
            time.sleep(0.12)
            assert cmd(c, "GET", "foo") is None
            assert cmd(c, "SET", "x", "1") == "OK"
            sel = parse_repl_cmd(r)  # select 0 (emitted with first write)
            assert sel[0].lower() == "select"
            a = parse_repl_cmd(r)
            b = parse_repl_cmd(r)
            assert a[0].lower() == "del" and a[1] == "foo"
            assert b[0].lower() == "set" and b[1] == "x"

        print("P1 expire error/lazy-get tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

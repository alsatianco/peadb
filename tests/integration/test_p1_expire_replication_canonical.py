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
    raise RuntimeError(p)


def repl_cmd(s):
    assert rx(s, 1) == b"*"
    n = int(rl(s))
    out = []
    for _ in range(n):
        assert rx(s, 1) == b"$"
        ln = int(rl(s))
        out.append(rx(s, ln).decode())
        rx(s, 2)
    return out


def cmd(s, *a):
    s.sendall(enc(*a))
    return recv(s)


def main():
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6486", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6486), timeout=2) as c, socket.create_connection(("127.0.0.1", 6486), timeout=2) as r:
            assert cmd(c, "FLUSHALL") == "OK"
            r.sendall(b"SYNC\r\n")
            assert rx(r, 1) == b"$"
            _ = rl(r)

            assert cmd(c, "SET", "a", "v", "EX", "10") == "OK"
            sel = repl_cmd(r)  # select 0 (emitted with first write)
            assert sel[0].lower() == "select"
            ev = repl_cmd(r)
            assert ev[0].lower() == "set" and ev[3].upper() == "PXAT"

            assert cmd(c, "SET", "b", "v") == "OK"
            assert cmd(c, "EXPIRE", "b", "10") == 1
            ev = repl_cmd(r)  # set b v
            assert ev[0].lower() == "set"
            ev = repl_cmd(r)  # pexpireat b
            assert ev[0].lower() == "pexpireat"

            assert cmd(c, "SET", "c", "v", "EX", "10") == "OK"
            _ = repl_cmd(r)
            assert cmd(c, "GETEX", "c", "PERSIST") == "v"
            ev = repl_cmd(r)
            assert ev[0].lower() == "persist"

        print("P1 expire replication canonical tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

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
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6477", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6477), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"

            assert cmd(s, "SADD", "s", "1", "2", "3", "a") == 4
            assert cmd(s, "OBJECT", "ENCODING", "s") == "listpack"
            for i in range(4, 200):
                cmd(s, "SADD", "s", str(i))
            assert cmd(s, "OBJECT", "ENCODING", "s") == "hashtable"

            assert cmd(s, "ZADD", "z", "1", "a") == 1
            assert cmd(s, "OBJECT", "ENCODING", "z") == "listpack"
            assert cmd(s, "CONFIG", "SET", "zset-max-ziplist-entries", "0") == "OK"
            assert cmd(s, "ZADD", "z", "2", "b") == 1
            assert cmd(s, "OBJECT", "ENCODING", "z") == "skiplist"

        print("P1 object encoding policy tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

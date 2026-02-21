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
    m = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6436", "--bind", "127.0.0.1", "--loglevel", "error"])
    r = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6437", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.3)
        with socket.create_connection(("127.0.0.1", 6436), timeout=2) as master, socket.create_connection(("127.0.0.1", 6437), timeout=2) as replica:
            assert cmd(master, "FLUSHALL") == "OK"
            assert cmd(master, "SET", "promote:key", "v1") == "OK"
            assert cmd(replica, "REPLICAOF", "127.0.0.1", "6436") == "OK"
            deadline = time.time() + 8
            ok = False
            while time.time() < deadline:
                if cmd(replica, "GET", "promote:key") == "v1":
                    ok = True
                    break
                time.sleep(0.1)
            assert ok

            assert cmd(replica, "REPLICAOF", "NO", "ONE") == "OK"
            assert cmd(replica, "SET", "promote:new", "v2") == "OK"
            assert cmd(replica, "GET", "promote:new") == "v2"
        print("M9 failover promotion tests passed")
        return 0
    finally:
        r.terminate()
        r.wait(timeout=3)
        m.terminate()
        m.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def read_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        c = sock.recv(n - len(buf))
        if not c:
            raise RuntimeError("closed")
        buf.extend(c)
    return bytes(buf)


def read_line(sock: socket.socket) -> bytes:
    b = bytearray()
    while True:
        b.extend(read_exact(sock, 1))
        if b[-2:] == b"\r\n":
            return bytes(b[:-2])


def recv_resp(sock: socket.socket):
    p = read_exact(sock, 1)
    if p == b"+":
        return read_line(sock).decode()
    if p == b"-":
        return ("ERR", read_line(sock).decode())
    if p == b":":
        return int(read_line(sock).decode())
    if p == b"$":
        n = int(read_line(sock).decode())
        if n == -1:
            return None
        b = read_exact(sock, n)
        read_exact(sock, 2)
        return b.decode()
    if p == b"*":
        n = int(read_line(sock).decode())
        return [recv_resp(sock) for _ in range(n)]
    raise RuntimeError(f"unsupported {p!r}")


def cmd(sock: socket.socket, *args: str):
    data = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode()
        data += f"${len(b)}\r\n".encode() + b + b"\r\n"
    sock.sendall(data)
    return recv_resp(sock)


def main() -> int:
    proc = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6395", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6395), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"
            assert cmd(s, "SET", "k", "v") == "OK"
            assert cmd(s, "GET", "k") == "v"
            assert cmd(s, "TYPE", "k") == "string"
            assert cmd(s, "EXISTS", "k") == 1

            assert cmd(s, "SET", "n", "1") == "OK"
            assert cmd(s, "INCR", "n") == 2
            assert cmd(s, "INCRBY", "n", "5") == 7
            assert cmd(s, "DECR", "n") == 6
            assert cmd(s, "DECRBY", "n", "3") == 3
            assert cmd(s, "INCRBYFLOAT", "f", "1.5") == "1.5"

            assert cmd(s, "MSET", "a", "1", "b", "2") == "OK"
            assert cmd(s, "MGET", "a", "b", "c") == ["1", "2", None]
            assert cmd(s, "APPEND", "a", "z") == 2
            assert cmd(s, "STRLEN", "a") == 2

            assert cmd(s, "SET", "tmp", "x", "PX", "120") == "OK"
            ttl = cmd(s, "PTTL", "tmp")
            assert isinstance(ttl, int) and ttl > 0
            time.sleep(0.2)
            assert cmd(s, "GET", "tmp") is None
            assert cmd(s, "PTTL", "tmp") == -2

            assert cmd(s, "SET", "k2", "x") == "OK"
            assert cmd(s, "RENAME", "k2", "k3") == "OK"
            assert cmd(s, "GET", "k3") == "x"
            assert cmd(s, "RENAMENX", "k3", "k") == 0

            assert cmd(s, "SET", "z", "1") == "OK"
            assert cmd(s, "EXPIRE", "z", "1") == 1
            t = cmd(s, "TTL", "z")
            assert isinstance(t, int)
            assert cmd(s, "PERSIST", "z") == 1
            assert cmd(s, "TTL", "z") == -1

            assert cmd(s, "SELECT", "1") == "OK"
            assert cmd(s, "GET", "k") is None
            assert cmd(s, "SET", "db1", "v") == "OK"
            assert cmd(s, "SELECT", "0") == "OK"
            assert cmd(s, "GET", "db1") is None

        print("M2 strings/expire tests passed")
        return 0
    finally:
        proc.terminate()
        proc.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

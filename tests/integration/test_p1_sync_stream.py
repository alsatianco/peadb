#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def send_cmd(sock, *a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        b = x.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    sock.sendall(d)


def rx(s, n):
    b = b""
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c:
            raise RuntimeError("closed")
        b += c
    return b


def read_line(s):
    b = b""
    while not b.endswith(b"\r\n"):
        b += rx(s, 1)
    return b[:-2]


def read_bulk_reply(s):
    p = rx(s, 1)
    assert p == b"$"
    n = int(read_line(s))
    if n == -1:
        return None
    d = rx(s, n)
    rx(s, 2)
    return d.decode()


def read_int_reply(s):
    p = rx(s, 1)
    assert p == b":"
    return int(read_line(s))


def parse_repl_cmd(s):
    p = rx(s, 1)
    if p != b"*":
        return None
    n = int(read_line(s))
    out = []
    for _ in range(n):
        out.append(read_bulk_reply(s))
    return out


def main():
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6468", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6468), timeout=2) as c, socket.create_connection(("127.0.0.1", 6468), timeout=2) as r:
            send_cmd(c, "FLUSHALL")
            assert read_bulk_reply.__name__
            assert rx(c, 1) == b"+"
            read_line(c)

            r.sendall(b"SYNC\r\n")
            assert rx(r, 1) == b"$"
            assert read_line(r) == b"0"

            send_cmd(c, "SET", "foo", "bar")
            assert rx(c, 1) == b"+"
            read_line(c)

            deadline = time.time() + 2
            cmd = None
            while time.time() < deadline and cmd is None:
                try:
                    cmd = parse_repl_cmd(r)
                except TimeoutError:
                    continue
            assert cmd is not None
            if cmd[0].lower() == "select":
                cmd = parse_repl_cmd(r)
            assert cmd is not None and cmd[0].lower() == "set" and cmd[1] == "foo" and cmd[2] == "bar"

        print("P1 SYNC stream tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

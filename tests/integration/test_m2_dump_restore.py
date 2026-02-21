#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def read_exact(sock: socket.socket, n: int) -> bytes:
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            raise RuntimeError("connection closed")
        out.extend(chunk)
    return bytes(out)


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
        body = read_exact(sock, n)
        read_exact(sock, 2)
        return body  # Return raw bytes to preserve binary DUMP payloads
    raise RuntimeError(f"unsupported {p!r}")


def cmd(sock: socket.socket, *args):
    data = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode() if isinstance(a, str) else a
        data += f"${len(b)}\r\n".encode() + b + b"\r\n"
    sock.sendall(data)
    return recv_resp(sock)


def main() -> int:
    proc = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6397", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6397), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"
            assert cmd(s, "DUMP", "missing") is None

            assert cmd(s, "SET", "src", "value") == "OK"
            payload = cmd(s, "DUMP", "src")
            assert isinstance(payload, bytes) and len(payload) > 0

            assert cmd(s, "RESTORE", "dst", "0", payload) == "OK"
            assert cmd(s, "GET", "dst") == b"value"

            busy = cmd(s, "RESTORE", "dst", "0", payload)
            assert busy[0] == "ERR" and "BUSYKEY" in busy[1]

            assert cmd(s, "RESTORE", "dst", "0", payload, "REPLACE") == "OK"
            assert cmd(s, "GET", "dst") == b"value"

            assert cmd(s, "RESTORE", "exp", "120", payload, "REPLACE") == "OK"
            time.sleep(0.2)
            assert cmd(s, "GET", "exp") is None

            bad = cmd(s, "RESTORE", "bad", "0", "nonsense12345678")
            assert bad[0] == "ERR" and "checksum" in bad[1]

        print("M2 DUMP/RESTORE tests passed")
        return 0
    finally:
        proc.terminate()
        proc.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

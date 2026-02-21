#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
import socket


@dataclass
class RespError:
    message: str


def encode_command(args: list[str]) -> bytes:
    out = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        b = arg.encode()
        out.append(f"${len(b)}\r\n".encode())
        out.append(b + b"\r\n")
    return b"".join(out)


def _read_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed")
        buf.extend(chunk)
    return bytes(buf)


def _read_line(sock: socket.socket) -> bytes:
    buf = bytearray()
    while True:
        c = _read_exact(sock, 1)
        buf.extend(c)
        if len(buf) >= 2 and buf[-2:] == b"\r\n":
            return bytes(buf[:-2])


def read_resp(sock: socket.socket):
    prefix = _read_exact(sock, 1)
    if prefix == b"+":
        return _read_line(sock).decode()
    if prefix == b"-":
        return RespError(_read_line(sock).decode())
    if prefix == b":":
        return int(_read_line(sock).decode())
    if prefix == b"$":
        n = int(_read_line(sock).decode())
        if n == -1:
            return None
        data = _read_exact(sock, n)
        _read_exact(sock, 2)
        return data.decode()
    if prefix == b"*":
        n = int(_read_line(sock).decode())
        if n == -1:
            return None
        return [read_resp(sock) for _ in range(n)]
    if prefix == b"%":
        n = int(_read_line(sock).decode())
        out = {}
        for _ in range(n):
            k = read_resp(sock)
            v = read_resp(sock)
            out[k] = v
        return out
    raise ValueError(f"Unsupported RESP prefix: {prefix!r}")


def roundtrip(host: str, port: int, args: list[str]):
    with socket.create_connection((host, port), timeout=3) as sock:
        sock.sendall(encode_command(args))
        return read_resp(sock)

#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def read_exact(s: socket.socket, n: int) -> bytes:
    b = bytearray()
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c:
            raise RuntimeError("closed")
        b.extend(c)
    return bytes(b)


def read_line(s: socket.socket) -> bytes:
    b = bytearray()
    while True:
        b.extend(read_exact(s, 1))
        if b[-2:] == b"\r\n":
            return bytes(b[:-2])


def recv(s: socket.socket):
    p = read_exact(s, 1)
    if p == b"+":
      return read_line(s).decode()
    if p == b"-":
      return ("ERR", read_line(s).decode())
    if p == b":":
      return int(read_line(s).decode())
    if p == b"$":
      n = int(read_line(s).decode())
      if n == -1:
        return None
      b = read_exact(s, n)
      read_exact(s, 2)
      return b.decode()
    if p == b"*":
      n = int(read_line(s).decode())
      return [recv(s) for _ in range(n)]
    raise RuntimeError(p)


def cmd(s: socket.socket, *args: str):
    data = f"*{len(args)}\r\n".encode()
    for a in args:
      b = a.encode()
      data += f"${len(b)}\r\n".encode() + b + b"\r\n"
    s.sendall(data)
    return recv(s)


def main() -> int:
    proc = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6398", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
      time.sleep(0.2)
      with socket.create_connection(("127.0.0.1", 6398), timeout=2) as s:
        assert cmd(s, "FLUSHALL") == "OK"

        assert cmd(s, "HSET", "h", "f1", "v1") == 1
        assert cmd(s, "HSET", "h", "f1", "v2", "f2", "v3") == 1
        assert cmd(s, "HGET", "h", "f1") == "v2"
        assert cmd(s, "HEXISTS", "h", "f2") == 1
        assert cmd(s, "HLEN", "h") == 2
        allv = cmd(s, "HGETALL", "h")
        assert isinstance(allv, list) and len(allv) == 4
        assert cmd(s, "TYPE", "h") == "hash"
        assert cmd(s, "OBJECT", "ENCODING", "h") == "listpack"

        scan = cmd(s, "HSCAN", "h", "0", "COUNT", "10")
        assert isinstance(scan, list) and len(scan) == 2
        assert scan[0] == "0"
        assert isinstance(scan[1], list)

        assert cmd(s, "HDEL", "h", "f1") == 1
        assert cmd(s, "HGET", "h", "f1") is None

        assert cmd(s, "SET", "s", "x") == "OK"
        w = cmd(s, "HGET", "s", "f")
        assert w[0] == "ERR" and "WRONGTYPE" in w[1]

      print("M4 hash tests passed")
      return 0
    finally:
      proc.terminate()
      proc.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

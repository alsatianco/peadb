#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def cmd(sock: socket.socket, *args: str):
    data = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode()
        data += f"${len(b)}\r\n".encode() + b + b"\r\n"
    sock.sendall(data)

    p = sock.recv(1)
    def line():
      b = bytearray()
      while True:
        b.extend(sock.recv(1))
        if b[-2:] == b"\r\n":
          return bytes(b[:-2])
    if p == b"+":
      return line().decode()
    if p == b":":
      return int(line())
    if p == b"$":
      n = int(line())
      if n == -1:
        return None
      b = sock.recv(n)
      sock.recv(2)
      return b.decode()
    if p == b"-":
      return ("ERR", line().decode())
    raise RuntimeError(p)


def main() -> int:
    proc = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6396", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1", 6396), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"
            assert cmd(s, "OBJECT", "ENCODING", "missing") is None
            assert cmd(s, "SET", "short", "abc") == "OK"
            assert cmd(s, "OBJECT", "ENCODING", "short") in ("embstr", "raw")
            assert cmd(s, "OBJECT", "FREQ", "short")[0] == "ERR"
        print("M3 object encoding test passed")
        return 0
    finally:
        proc.terminate()
        proc.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

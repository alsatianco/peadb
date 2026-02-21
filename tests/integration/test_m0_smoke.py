#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def send_resp_ping(port: int) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
        s.sendall(b"*1\r\n$4\r\nPING\r\n")
        return s.recv(128)


def main() -> int:
    server = ROOT / "peadb-server"
    proc = subprocess.Popen([str(server), "--port", "6391", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                out = send_resp_ping(6391)
                if out == b"+PONG\r\n":
                    print("M0 smoke passed")
                    return 0
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("server did not return PONG")
    finally:
        proc.terminate()
        proc.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

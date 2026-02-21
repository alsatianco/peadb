#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def recv_resp(sock: socket.socket):
    lead = sock.recv(1)
    if not lead:
        raise RuntimeError("connection closed")

    def read_exact(n: int) -> bytes:
        out = bytearray()
        while len(out) < n:
            chunk = sock.recv(n - len(out))
            if not chunk:
                raise RuntimeError("connection closed")
            out.extend(chunk)
        return bytes(out)

    def line() -> bytes:
        data = bytearray()
        while True:
            c = read_exact(1)
            data.extend(c)
            if data[-2:] == b"\r\n":
                return bytes(data[:-2])

    if lead == b"+":
        return line().decode()
    if lead == b"-":
        return ("ERR", line().decode())
    if lead == b":":
        return int(line().decode())
    if lead == b"$":
        n = int(line().decode())
        if n == -1:
            return None
        body = read_exact(n)
        read_exact(2)
        return body.decode()
    if lead == b"*":
        n = int(line().decode())
        return [recv_resp(sock) for _ in range(n)]
    if lead == b"%":
        n = int(line().decode())
        out = {}
        for _ in range(n):
            k = recv_resp(sock)
            if isinstance(k, list):
                k = tuple(k)
            out[k] = recv_resp(sock)
        return out
    if lead == b"_":
        _ = line()
        return None
    raise RuntimeError(f"unsupported lead: {lead!r}")


def send_cmd(sock: socket.socket, *args: str):
    payload = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode()
        payload += f"${len(b)}\r\n".encode() + b + b"\r\n"
    sock.sendall(payload)
    return recv_resp(sock)


def main() -> int:
    server = ROOT / "peadb-server"
    proc = subprocess.Popen([str(server), "--port", "6394", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", 6394), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.05)

        with socket.create_connection(("127.0.0.1", 6394), timeout=2) as s:
            assert send_cmd(s, "PING") == "PONG"
            assert send_cmd(s, "ECHO", "abc") == "abc"
            hello3 = send_cmd(s, "HELLO", "3")
            assert isinstance(hello3, dict) and hello3.get("proto") == 3
            assert send_cmd(s, "COMMAND", "COUNT") >= 8
            info = send_cmd(s, "COMMAND", "INFO", "PING")
            assert isinstance(info, list) and len(info) == 1 and isinstance(info[0], list)
            docs = send_cmd(s, "COMMAND", "DOCS")
            assert docs == {}
            cfg = send_cmd(s, "CONFIG", "GET", "*")
            assert isinstance(cfg, list) and len(cfg) % 2 == 0
            assert send_cmd(s, "CONFIG", "SET", "port", "6394") == "OK"
            server_info = send_cmd(s, "INFO")
            assert "redis_version:7.2.5" in server_info
            assert send_cmd(s, "QUIT") == "OK"

        with socket.create_connection(("127.0.0.1", 6394), timeout=2) as s2:
            s2.sendall(b"PING\r\n")
            assert recv_resp(s2) == "PONG"

        print("M1 command tests passed")
        return 0
    finally:
        proc.terminate()
        proc.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

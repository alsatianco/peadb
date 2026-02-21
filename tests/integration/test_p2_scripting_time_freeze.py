#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def enc(*args: str) -> bytes:
    out = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode()
        out += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return out


def rx(sock: socket.socket, n: int) -> bytes:
    b = b""
    while len(b) < n:
        c = sock.recv(n - len(b))
        if not c:
            raise RuntimeError("closed")
        b += c
    return b


def rl(sock: socket.socket) -> bytes:
    b = b""
    while not b.endswith(b"\r\n"):
        b += rx(sock, 1)
    return b[:-2]


def recv(sock: socket.socket):
    p = rx(sock, 1)
    if p == b"+":
        return rl(sock).decode()
    if p == b"-":
        return ("ERR", rl(sock).decode())
    if p == b":":
        return int(rl(sock))
    if p == b"$":
        n = int(rl(sock))
        if n == -1:
            return None
        d = rx(sock, n)
        rx(sock, 2)
        return d.decode()
    if p == b"*":
        n = int(rl(sock))
        if n == -1:
            return None
        return [recv(sock) for _ in range(n)]
    if p == b"%":
        n = int(rl(sock))
        m = {}
        for _ in range(n):
            k = recv(sock)
            v = recv(sock)
            m[k] = v
        return m
    if p == b"_":
        rl(sock)
        return None
    raise RuntimeError(f"prefix {p!r}")


def cmd(sock: socket.socket, *args: str):
    sock.sendall(enc(*args))
    return recv(sock)


def load_one_function(sock: socket.socket, body: str) -> None:
    lib = (
        "#!lua name=testlib\n"
        "redis.register_function('test', function(KEYS, ARGV)\n"
        f"{body}\n"
        "end)"
    )
    assert cmd(sock, "FUNCTION", "FLUSH") == "OK"
    assert cmd(sock, "FUNCTION", "LOAD", "REPLACE", lib) == "testlib"


def fcall(sock: socket.socket, numkeys: int, *keys: str):
    return cmd(sock, "FCALL", "test", str(numkeys), *keys)


def main() -> int:
    port = 6506
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", str(port), "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.25)
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            assert cmd(s, "DEBUG", "SET-DISABLE-DENY-SCRIPTS", "1") == "OK"

            load_one_function(
                s,
                "  redis.call(\"SET\", \"key\", \"value\", \"PX\", \"1\")\n"
                "  redis.call(\"DEBUG\", \"SLEEP\", 0.01)\n"
                "  return redis.call(\"EXISTS\", \"key\")",
            )
            assert fcall(s, 1, "key") == 1
            assert cmd(s, "EXISTS", "key") == 0

            assert cmd(s, "DEBUG", "SET-ACTIVE-EXPIRE", "0") == "OK"
            assert cmd(s, "SET", "key", "value", "PX", "1") == "OK"
            time.sleep(0.005)
            obj = cmd(s, "DEBUG", "OBJECT", "key")
            assert isinstance(obj, str) and "encoding" in obj
            load_one_function(s, "  return redis.call('EXISTS', 'key')")
            assert fcall(s, 1, "key") == 0
            assert cmd(s, "EXISTS", "key") == 0
            assert cmd(s, "DEBUG", "SET-ACTIVE-EXPIRE", "1") == "OK"

            load_one_function(
                s,
                "  local result1 = {redis.call(\"TIME\")}\n"
                "  redis.call(\"DEBUG\", \"SLEEP\", 0.01)\n"
                "  local result2 = {redis.call(\"TIME\")}\n"
                "  return {result1, result2}",
            )
            out = fcall(s, 0)
            assert isinstance(out, list) and len(out) == 2 and out[0] == out[1]

        print("P2 scripting time-freeze tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

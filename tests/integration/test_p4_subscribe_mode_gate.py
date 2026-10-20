#!/usr/bin/env python3
"""RESP2 subscribe-mode command gate + PING array reply (Redis server.c pubsub gate)."""
import pathlib
import socket
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready

PORT = 6531


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
    if p == b"=":
        # RESP3 verbatim string: same "$<len>\r\n<payload>\r\n" shape, but the payload carries
        # a leading 3-char format tag + ":" (e.g. "txt:") that the length header counts too.
        n = int(rl(s))
        v = rx(s, n)
        rx(s, 2)
        return v.decode()
    if p in (b"*", b">", b"~"):
        n = int(rl(s))
        if n == -1:
            return None
        return [recv(s) for _ in range(n)]
    if p == b"%":
        n = int(rl(s))
        return {recv(s): recv(s) for _ in range(n)}
    if p == b"_":
        rl(s)
        return None
    if p == b"#":
        return rl(s) == b"t"
    if p == b",":
        return float(rl(s))
    raise RuntimeError(p)


def cmd(s, *a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        b = x.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    s.sendall(d)
    return recv(s)


def main():
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", str(PORT),
                          "--bind", "127.0.0.1", "--loglevel", "warning"])
    try:
        wait_until_ready(PORT, process=p)
        with socket.create_connection(("127.0.0.1", PORT), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"
            assert cmd(s, "SET", "k", "v") == "OK"

            # Enter subscribe mode (RESP2).
            assert cmd(s, "SUBSCRIBE", "ch1") == ["subscribe", "ch1", 1]

            # Non-exempt commands are rejected with Redis's exact wording.
            r = cmd(s, "GET", "k")
            assert r[0] == "ERR" and "Can't execute 'get'" in r[1] \
                and "only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context" in r[1], r
            r = cmd(s, "PUBLISH", "ch1", "m")  # PUBLISH is NOT exempt
            assert r[0] == "ERR" and "Can't execute 'publish'" in r[1], r
            r = cmd(s, "MULTI")
            assert r[0] == "ERR" and "Can't execute 'multi'" in r[1], r

            # PING is exempt but replies as a 2-element array in subscribe mode.
            assert cmd(s, "PING") == ["pong", ""]
            assert cmd(s, "PING", "hello") == ["pong", "hello"]

            # Exempt subscription-management commands still work.
            assert cmd(s, "PSUBSCRIBE", "news.*") == ["psubscribe", "news.*", 2]
            assert cmd(s, "PUNSUBSCRIBE", "news.*") == ["punsubscribe", "news.*", 1]
            assert cmd(s, "UNSUBSCRIBE", "ch1") == ["unsubscribe", "ch1", 0]

            # Fully unsubscribed: normal commands allowed again, PING back to +PONG.
            assert cmd(s, "GET", "k") == "v"
            assert cmd(s, "PING") == "PONG"

        # RESP3 clients have no subscribe-mode restrictions.
        with socket.create_connection(("127.0.0.1", PORT), timeout=2) as s3:
            hello = cmd(s3, "HELLO", "3")
            assert isinstance(hello, (dict, list)), hello
            r = cmd(s3, "SUBSCRIBE", "ch2")
            assert r == ["subscribe", "ch2", 1], r
            assert cmd(s3, "GET", "k") == "v"

        print("P4 subscribe-mode gate tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())

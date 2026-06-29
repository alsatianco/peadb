#!/usr/bin/env python3
"""Batch 1 — security & DoS hardening.

Covers: requirepass enforcement (NOAUTH/WRONGPASS), HELLO AUTH, the "no password
set" path, the Lua os/io sandbox, MODULE LOAD gating, and the protocol
multibulk-length DoS guard.
"""
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
SERVER = ROOT / "peadb-server"


def wait_up(port, auth_required=False, timeout=5.0):
    """Up = responds to PING with +PONG, or -NOAUTH when auth is required."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5) as s:
                s.sendall(b"*1\r\n$4\r\nPING\r\n")
                data = s.recv(64)
                if data.startswith(b"+PONG") or (auth_required and data.startswith(b"-NOAUTH")):
                    return
        except OSError:
            pass
        time.sleep(0.05)
    raise RuntimeError(f"server on {port} did not come up")


def cmd(*parts):
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        b = p.encode() if isinstance(p, str) else p
        out += b"$%d\r\n%s\r\n" % (len(b), b)
    return out


def recv_reply(s):
    s.settimeout(2.0)
    buf = b""
    while not buf.endswith(b"\r\n"):
        c = s.recv(1)
        if not c:
            break
        buf += c
    return buf


def expect(cond, msg):
    if not cond:
        raise AssertionError(msg)


def start(port, *extra):
    return subprocess.Popen(
        [str(SERVER), "--port", str(port), "--bind", "127.0.0.1", "--loglevel", "error", *extra]
    )


def test_auth_enforced():
    port = 6488
    p = start(port, "--requirepass", "s3cret")
    try:
        wait_up(port, auth_required=True)
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            s.sendall(cmd("GET", "foo"))
            r = recv_reply(s)
            expect(r.startswith(b"-NOAUTH"), f"unauth GET should be NOAUTH, got {r!r}")

            s.sendall(cmd("AUTH", "wrong"))
            r = recv_reply(s)
            expect(r.startswith(b"-WRONGPASS"), f"bad AUTH should be WRONGPASS, got {r!r}")

            s.sendall(cmd("AUTH", "s3cret"))
            r = recv_reply(s)
            expect(r == b"+OK\r\n", f"good AUTH should be +OK, got {r!r}")

            s.sendall(cmd("SET", "foo", "bar"))
            expect(recv_reply(s) == b"+OK\r\n", "SET after AUTH should work")
            s.sendall(cmd("GET", "foo"))
            time.sleep(0.05)
            r = s.recv(64)
            expect(b"bar" in r, f"GET after AUTH should return value, got {r!r}")

        # HELLO must authenticate too
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            s.sendall(cmd("HELLO", "3"))
            r = recv_reply(s)
            expect(r.startswith(b"-NOAUTH"), f"HELLO without auth should be NOAUTH, got {r!r}")
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            s.sendall(cmd("HELLO", "3", "AUTH", "default", "s3cret"))
            r = s.recv(256)
            expect(not r.startswith(b"-"), f"HELLO AUTH should succeed, got {r!r}")
    finally:
        p.terminate(); p.wait(timeout=3)


def test_auth_without_password_set():
    port = 6489
    p = start(port)
    try:
        wait_up(port)
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            s.sendall(cmd("AUTH", "anything"))
            r = recv_reply(s)
            expect(r.startswith(b"-ERR Client sent AUTH"), f"AUTH w/o password set should error, got {r!r}")
            # PING still works without auth when no password is configured
            s.sendall(cmd("PING"))
            expect(recv_reply(s) == b"+PONG\r\n", "PING should work when no password set")
    finally:
        p.terminate(); p.wait(timeout=3)


def test_lua_os_io_sandboxed():
    port = 6490
    p = start(port)
    try:
        wait_up(port)
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            # os and io must be unreachable from scripts.
            s.sendall(cmd("EVAL", "return os.time()", "0"))
            r = recv_reply(s)
            expect(r.startswith(b"-"), f"os.* must be blocked in Lua, got {r!r}")
            s.sendall(cmd("EVAL", "return io.open('/etc/passwd')", "0"))
            r = recv_reply(s)
            expect(r.startswith(b"-"), f"io.* must be blocked in Lua, got {r!r}")
            # A normal script still works.
            s.sendall(cmd("EVAL", "return 1+1", "0"))
            expect(recv_reply(s) == b":2\r\n", "normal Lua should still work")
    finally:
        p.terminate(); p.wait(timeout=3)


def test_module_load_gated():
    port = 6491
    p = start(port)
    try:
        wait_up(port)
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            s.sendall(cmd("MODULE", "LOAD", "/tmp/whatever.so"))
            r = recv_reply(s)
            expect(b"not allowed" in r, f"MODULE LOAD should be gated by default, got {r!r}")
    finally:
        p.terminate(); p.wait(timeout=3)


def test_protocol_multibulk_dos():
    port = 6492
    p = start(port)
    try:
        wait_up(port)
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            # A tiny header claiming 2e9 elements must be rejected, not reserved.
            s.sendall(b"*2000000000\r\n")
            r = recv_reply(s)
            expect(r.startswith(b"-ERR Protocol error"), f"huge multibulk should error, got {r!r}")
            # connection should be closed by the server afterwards
            s.settimeout(2.0)
            try:
                tail = s.recv(16)
                expect(tail == b"", "server should close connection after protocol error")
            except OSError:
                pass  # closed/reset is also acceptable
    finally:
        p.terminate(); p.wait(timeout=3)


def main():
    test_auth_enforced()
    test_auth_without_password_set()
    test_lua_os_io_sandboxed()
    test_module_load_gated()
    test_protocol_multibulk_dos()
    print("B1 security tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

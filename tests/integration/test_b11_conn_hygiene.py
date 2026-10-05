#!/usr/bin/env python3
"""Batch 11 — connection hygiene (conc-7: idle-client timeout reaping).

Covers: CONFIG GET/SET timeout round-trip; an idle client past `timeout` is closed; an
actively-pinging client is NOT reaped; a pub/sub subscriber is exempt from the timeout.
"""
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
SERVER = ROOT / "peadb-server"


def wait_up(port, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5) as s:
                s.sendall(b"*1\r\n$4\r\nPING\r\n")
                if s.recv(64).startswith(b"+PONG"):
                    return True
        except OSError:
            pass
        time.sleep(0.05)
    return False


def conn(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=8); s.settimeout(8); return s


def rx(s, n):
    b = b""
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c: raise RuntimeError("closed")
        b += c
    return b


def rl(s):
    b = b""
    while not b.endswith(b"\r\n"): b += rx(s, 1)
    return b[:-2]


def recv(s):
    p = rx(s, 1)
    if p == b"+": return rl(s).decode()
    if p == b"-": return ("ERR", rl(s).decode())
    if p == b":": return int(rl(s))
    if p in (b"$", b"="):
        n = int(rl(s))
        if n == -1: return None
        v = rx(s, n); rx(s, 2); return v.decode("latin1")
    if p in (b"*", b"~", b">"):
        n = int(rl(s))
        if n == -1: return None
        return [recv(s) for _ in range(n)]
    raise RuntimeError("type %r" % p)


def cmd(s, *a):
    out = b"*%d\r\n" % len(a)
    for x in a:
        b = x.encode() if isinstance(x, str) else x
        out += b"$%d\r\n%s\r\n" % (len(b), b)
    s.sendall(out)
    return recv(s)


def expect(c, m):
    if not c: raise AssertionError(m)


def is_closed(s):
    """Return True if the peer closed the connection."""
    s.settimeout(2)
    try:
        s.sendall(b"*1\r\n$4\r\nPING\r\n")
        d = s.recv(64)
        return d == b""
    except OSError:
        return True


def test_timeout_config_roundtrip():
    port = 6580
    p = subprocess.Popen([str(SERVER), "--port", str(port), "--bind", "127.0.0.1", "--loglevel", "warning"])
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        expect(cmd(s, "CONFIG", "GET", "timeout") == ["timeout", "0"], "default timeout 0")
        expect(cmd(s, "CONFIG", "SET", "timeout", "5") == "OK", "set timeout")
        expect(cmd(s, "CONFIG", "GET", "timeout") == ["timeout", "5"], "timeout now 5")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_idle_reaped_active_survives():
    port = 6581
    p = subprocess.Popen([str(SERVER), "--port", str(port), "--bind", "127.0.0.1",
                          "--loglevel", "warning", "--timeout", "2"])
    try:
        expect(wait_up(port), "up")
        idle = conn(port); active = conn(port)
        expect(cmd(idle, "PING") == "PONG", "idle client ok initially")
        # Keep `active` busy across the timeout window; leave `idle` silent.
        deadline = time.time() + 3.5
        while time.time() < deadline:
            expect(cmd(active, "PING") == "PONG", "active stays alive")
            time.sleep(0.4)
        expect(is_closed(idle), "idle client past timeout must be closed")
        # active was pinged within the window — must still be open
        expect(cmd(active, "PING") == "PONG", "active client must NOT be reaped")
        active.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_subscriber_exempt():
    port = 6582
    p = subprocess.Popen([str(SERVER), "--port", str(port), "--bind", "127.0.0.1",
                          "--loglevel", "warning", "--timeout", "2"])
    try:
        expect(wait_up(port), "up")
        sub = conn(port)
        r = cmd(sub, "SUBSCRIBE", "ch")
        expect(isinstance(r, list) and r[0] == "subscribe", "subscribed")
        time.sleep(3.5)  # idle past timeout, but subscribers are exempt
        # publish from another client; the subscriber must still be connected to receive it
        pub = conn(port)
        expect(cmd(pub, "PUBLISH", "ch", "hi") == 1, "publish delivered to 1 subscriber (still connected)")
        msg = recv(sub)
        expect(isinstance(msg, list) and msg[0] == "message" and msg[2] == "hi",
               f"subscriber received message after idle, got {msg}")
        sub.close(); pub.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def main():
    test_timeout_config_roundtrip()
    test_idle_reaped_active_survives()
    test_subscriber_exempt()
    print("B11 connection-hygiene tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

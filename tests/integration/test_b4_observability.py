#!/usr/bin/env python3
"""Batch 4 — observability.

Covers: real SLOWLOG, keyspace notifications (set + expired), accurate INFO fields,
runtime loglevel, MONITOR feed, and CLIENT KILL.
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
    s = socket.create_connection(("127.0.0.1", port), timeout=2)
    s.settimeout(2)
    return s


def cmd(s, *parts):
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        b = p.encode() if isinstance(p, str) else p
        out += b"$%d\r\n%s\r\n" % (len(b), b)
    s.sendall(out)


def line(s):
    buf = b""
    while not buf.endswith(b"\r\n"):
        c = s.recv(1)
        if not c:
            break
        buf += c
    return buf


def recv_some(s, n=4096):
    try:
        return s.recv(n)
    except OSError:
        return b""


def recv_until(s, needle, timeout=2.0):
    s.settimeout(timeout)
    buf = b""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            c = s.recv(4096)
        except OSError:
            break
        if not c:
            break
        buf += c
        if needle in buf:
            return buf
    return buf


def expect(cond, msg):
    if not cond:
        raise AssertionError(msg)


def start(port, *extra):
    return subprocess.Popen([str(SERVER), "--port", str(port), "--bind", "127.0.0.1",
                             "--loglevel", "warning", *extra])


def test_slowlog_and_loglevel():
    port = 6513
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        cmd(s, "CONFIG", "SET", "slowlog-log-slower-than", "0"); expect(line(s) == b"+OK\r\n", "set slowlog threshold")
        cmd(s, "PING"); line(s)
        cmd(s, "SLOWLOG", "LEN")
        r = line(s)
        expect(r.startswith(b":") and int(r[1:-2]) > 0, f"SLOWLOG LEN should be >0, got {r!r}")
        cmd(s, "SLOWLOG", "GET")
        expect(recv_some(s).startswith(b"*"), "SLOWLOG GET returns an array")
        # Raise the threshold FIRST so RESET itself isn't re-logged, then reset.
        cmd(s, "CONFIG", "SET", "slowlog-log-slower-than", "1000000"); line(s)
        cmd(s, "SLOWLOG", "RESET"); expect(line(s) == b"+OK\r\n", "slowlog reset")
        cmd(s, "SLOWLOG", "LEN"); expect(line(s) == b":0\r\n", "slowlog empty after reset+threshold")
        # runtime loglevel
        cmd(s, "CONFIG", "SET", "loglevel", "debug"); expect(line(s) == b"+OK\r\n", "set loglevel")
        cmd(s, "CONFIG", "GET", "loglevel")
        expect(b"debug" in recv_some(s), "CONFIG GET loglevel reflects the change")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_info_fields():
    port = 6514
    p = start(port)
    try:
        expect(wait_up(port), "up")
        s = conn(port)
        for _ in range(50):
            cmd(s, "SET", "k", "v"); line(s)
        cmd(s, "INFO")
        time.sleep(0.05)
        blob = b""
        s.settimeout(2)
        while b"# Keyspace" not in blob and b"used_cpu_user" not in blob:
            chunk = recv_some(s)
            if not chunk:
                break
            blob += chunk
        for field in (b"blocked_clients:", b"used_memory_peak:", b"used_memory_rss:",
                      b"mem_fragmentation_ratio:", b"latest_fork_usec:", b"total_forks:",
                      b"instantaneous_ops_per_sec:", b"used_cpu_user:", b"pubsub_channels:"):
            expect(field in blob, f"INFO missing {field!r}")
        s.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_keyspace_notifications():
    port = 6515
    p = start(port)
    try:
        expect(wait_up(port), "up")
        cfg = conn(port)
        cmd(cfg, "CONFIG", "SET", "notify-keyspace-events", "KEA"); expect(line(cfg) == b"+OK\r\n", "enable notifications")
        # subscriber for the "set" event
        sub = conn(port)
        cmd(sub, "PSUBSCRIBE", "__keyevent@0__:set")
        recv_until(sub, b"psubscribe", 2.0)
        cmd(cfg, "SET", "foo", "bar"); line(cfg)
        got = recv_until(sub, b"foo", 2.0)
        expect(b"__keyevent@0__:set" in got and b"foo" in got, f"should receive set notification, got {got!r}")
        sub.close()
        # expired event
        sub2 = conn(port)
        cmd(sub2, "PSUBSCRIBE", "__keyevent@0__:expired")
        recv_until(sub2, b"psubscribe", 2.0)
        cmd(cfg, "SET", "tmp", "x", "PX", "100"); line(cfg)
        got = recv_until(sub2, b"tmp", 3.0)
        expect(b"expired" in got and b"tmp" in got, f"should receive expired notification, got {got!r}")
        sub2.close(); cfg.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_monitor():
    port = 6516
    p = start(port)
    try:
        expect(wait_up(port), "up")
        mon = conn(port)
        cmd(mon, "MONITOR"); expect(line(mon) == b"+OK\r\n", "MONITOR ok")
        other = conn(port)
        cmd(other, "SET", "mfoo", "mbar"); line(other)
        got = recv_until(mon, b"mfoo", 2.0)
        expect(b"mfoo" in got and b'"set"' in got.lower() or b"mfoo" in got, f"monitor should see the command, got {got!r}")
        mon.close(); other.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def test_client_kill():
    port = 6517
    p = start(port)
    try:
        expect(wait_up(port), "up")
        victim = conn(port)
        cmd(victim, "CLIENT", "ID")
        vid = int(line(victim)[1:-2])
        killer = conn(port)
        cmd(killer, "CLIENT", "KILL", "ID", str(vid))
        r = line(killer)
        expect(r == b":1\r\n", f"CLIENT KILL ID should report 1 killed, got {r!r}")
        # victim's socket should now be closed by the server
        victim.settimeout(2)
        try:
            cmd(victim, "PING")
            tail = victim.recv(64)
            expect(tail == b"", f"victim should be disconnected, got {tail!r}")
        except OSError:
            pass
        victim.close(); killer.close()
    finally:
        p.terminate(); p.wait(timeout=3)


def main():
    test_slowlog_and_loglevel()
    test_info_fields()
    test_keyspace_notifications()
    test_monitor()
    test_client_kill()
    print("B4 observability tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

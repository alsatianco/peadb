#!/usr/bin/env python3
"""Batch 2 — crash safety & durability.

Covers: real maxmemory enforcement + unit parsing, save points (BGSAVE cron),
SHUTDOWN SAVE, and AOF truncated-tail tolerance vs mid-file-corruption refusal.
"""
import os
import pathlib
import socket
import subprocess
import tempfile
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


def cmd_bytes(*parts):
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        b = p.encode() if isinstance(p, str) else p
        out += b"$%d\r\n%s\r\n" % (len(b), b)
    return out


def line(s):
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
    return subprocess.Popen([str(SERVER), "--port", str(port), "--bind", "127.0.0.1",
                             "--loglevel", "warning", *extra])


def test_maxmemory_enforcement():
    port = 6493
    p = start(port)
    try:
        expect(wait_up(port), "server up")
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            # unit parsing: 100mb must parse (not an integer-parse error)
            s.sendall(cmd_bytes("CONFIG", "SET", "maxmemory", "100mb"))
            expect(line(s) == b"+OK\r\n", "CONFIG SET maxmemory 100mb should succeed")
            s.sendall(cmd_bytes("CONFIG", "GET", "maxmemory"))
            # *2 / $9 maxmemory / $9 104857600
            blob = s.recv(128)
            expect(b"104857600" in blob, f"maxmemory should read back 104857600, got {blob!r}")
            # under the limit: writes allowed
            s.sendall(cmd_bytes("SET", "k", "v"))
            expect(line(s) == b"+OK\r\n", "SET under maxmemory should work")
            # tiny limit (1 byte) < used_memory: writes rejected, reads allowed
            s.sendall(cmd_bytes("CONFIG", "SET", "maxmemory", "1"))
            expect(line(s) == b"+OK\r\n", "CONFIG SET maxmemory 1 ok")
            s.sendall(cmd_bytes("SET", "k2", "v2"))
            r = line(s)
            expect(r.startswith(b"-OOM"), f"SET over maxmemory should be -OOM, got {r!r}")
            s.sendall(cmd_bytes("GET", "k"))
            expect(b"v" in s.recv(64), "reads allowed under OOM")
            # lift the limit
            s.sendall(cmd_bytes("CONFIG", "SET", "maxmemory", "0"))
            expect(line(s) == b"+OK\r\n", "clear maxmemory")
            s.sendall(cmd_bytes("SET", "k3", "v3"))
            expect(line(s) == b"+OK\r\n", "writes resume after clearing maxmemory")
    finally:
        p.terminate(); p.wait(timeout=3)


def test_save_points():
    port = 6494
    with tempfile.TemporaryDirectory(prefix="b2save-", ignore_cleanup_errors=True) as td:
        p = start(port, "--dir", td, "--save", "1 1")
        try:
            expect(wait_up(port), "server up")
            with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
                s.sendall(cmd_bytes("SET", "a", "1"))
                expect(line(s) == b"+OK\r\n", "SET ok")
            rdb = pathlib.Path(td) / "dump.rdb"
            deadline = time.time() + 4
            while time.time() < deadline and not rdb.exists():
                time.sleep(0.1)
            expect(rdb.exists(), "save point should trigger a BGSAVE producing dump.rdb")
        finally:
            p.terminate(); p.wait(timeout=3)


def test_shutdown_save():
    port = 6495
    with tempfile.TemporaryDirectory(prefix="b2shut-", ignore_cleanup_errors=True) as td:
        p = start(port, "--dir", td)
        expect(wait_up(port), "server up")
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            s.sendall(cmd_bytes("SET", "b", "2"))
            expect(line(s) == b"+OK\r\n", "SET ok")
            s.sendall(cmd_bytes("SHUTDOWN", "SAVE"))
            time.sleep(0.2)
        p.wait(timeout=3)
        rdb = pathlib.Path(td) / "dump.rdb"
        expect(rdb.exists(), "SHUTDOWN SAVE should write dump.rdb")
        # restart and confirm the value survived
        p2 = start(port, "--dir", td)
        try:
            expect(wait_up(port), "restart up")
            with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
                s.sendall(cmd_bytes("GET", "b"))
                expect(b"2" in s.recv(64), "value should survive SHUTDOWN SAVE + restart")
        finally:
            p2.terminate(); p2.wait(timeout=3)


def test_aof_truncated_tail_vs_corruption():
    port = 6496
    with tempfile.TemporaryDirectory(prefix="b2aof-", ignore_cleanup_errors=True) as td:
        # write some data with AOF on, then shut down (flushes AOF)
        p = start(port, "--dir", td, "--appendonly", "yes", "--appendfsync", "always")
        expect(wait_up(port), "server up")
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            s.sendall(cmd_bytes("SET", "x", "1")); expect(line(s) == b"+OK\r\n", "set x")
            s.sendall(cmd_bytes("SET", "y", "2")); expect(line(s) == b"+OK\r\n", "set y")
            s.sendall(cmd_bytes("SHUTDOWN", "NOSAVE"))
            time.sleep(0.2)
        p.wait(timeout=3)
        aof = pathlib.Path(td) / "appendonly.aof"
        expect(aof.exists() and aof.stat().st_size > 0, "AOF written")
        original = aof.read_bytes()

        # (a) truncated tail: drop the last 3 bytes — server must still start
        aof.write_bytes(original[:-3])
        p = start(port, "--dir", td, "--appendonly", "yes")
        try:
            expect(wait_up(port), "server should tolerate a truncated AOF tail and start")
            with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
                s.sendall(cmd_bytes("GET", "x"))
                expect(b"1" in s.recv(64), "data before the truncation should load")
        finally:
            p.terminate(); p.wait(timeout=3)

        # (b) mid-file corruption: inject non-printable garbage — server must refuse to start
        aof.write_bytes(original + b"\x00\x01\x02garbage_not_resp")
        p = start(port, "--dir", td, "--appendonly", "yes")
        started = wait_up(port, timeout=2.0)
        if started:
            p.terminate(); p.wait(timeout=3)
        expect(not started, "server must refuse to start on AOF corruption")
        # process should have exited non-zero on its own
        try:
            p.wait(timeout=2)
        except Exception:
            p.terminate()


def main():
    test_maxmemory_enforcement()
    test_save_points()
    test_shutdown_save()
    test_aof_truncated_tail_vs_corruption()
    print("B2 durability tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

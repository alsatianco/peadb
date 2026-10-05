#!/usr/bin/env python3
"""Batch 9 — RDB CRC64 verification on load (persist-9).

A clean native RDB reloads fine; a corrupted native RDB (CRC mismatch) is fatal — the
server refuses to start rather than silently serving partial/empty data.
"""
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


def cli(port, *args):
    return subprocess.run(["redis-cli", "-p", str(port), *args],
                          capture_output=True, text=True).stdout.strip()


def start(port, td):
    return subprocess.Popen([str(SERVER), "--port", str(port), "--bind", "127.0.0.1",
                             "--loglevel", "warning", "--dir", td])


def expect(c, m):
    if not c: raise AssertionError(m)


def test_clean_rdb_reloads():
    port = 6560
    with tempfile.TemporaryDirectory(prefix="b9-", ignore_cleanup_errors=True) as td:
        p = start(port, td)
        try:
            expect(wait_up(port), "up")
            cli(port, "SET", "a", "1"); cli(port, "SET", "b", "2")
            expect(cli(port, "SAVE") == "OK", "SAVE ok")
        finally:
            p.terminate(); p.wait(timeout=3)
        p = start(port, td)
        try:
            expect(wait_up(port), "restart up after clean save")
            expect(cli(port, "GET", "a") == "1", "key survived clean reload")
        finally:
            p.terminate(); p.wait(timeout=3)


def test_corrupt_rdb_is_fatal():
    port = 6561
    with tempfile.TemporaryDirectory(prefix="b9c-", ignore_cleanup_errors=True) as td:
        p = start(port, td)
        try:
            expect(wait_up(port), "up")
            for i in range(20):
                cli(port, "SET", f"k{i}", f"v{i}")
            expect(cli(port, "SAVE") == "OK", "SAVE ok")
        finally:
            p.terminate(); p.wait(timeout=3)
        # Flip a body byte → CRC mismatch.
        rdb = pathlib.Path(td) / "dump.rdb"
        data = bytearray(rdb.read_bytes())
        data[15] ^= 0xFF
        rdb.write_bytes(data)
        # Server must refuse to start (exit non-zero, never accepts connections).
        p = start(port, td)
        rc = p.wait(timeout=6)
        expect(rc != 0, f"corrupt RDB must be fatal (non-zero exit), got rc={rc}")
        expect(not wait_up(port, timeout=0.5), "server must not be serving after corrupt RDB")


def main():
    test_clean_rdb_reloads()
    test_corrupt_rdb_is_fatal()
    print("B9 RDB-CRC tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

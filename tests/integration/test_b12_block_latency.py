#!/usr/bin/env python3
"""Batch 12 — blocking-command wake latency (conc-2).

A blocked BLPOP/BZPOPMIN must be served promptly when another client writes the key —
in the same event-loop iteration, not on the next ~100ms poll. Asserts the wake happens
well under the old 100ms floor.
"""
import pathlib
import socket
import statistics
import subprocess
import threading
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
    s = socket.create_connection(("127.0.0.1", port), timeout=5); s.settimeout(5); return s


def expect(c, m):
    if not c: raise AssertionError(m)


def measure(port, blkcmd, wakecmd, n=15):
    lat = []
    for _ in range(n):
        b = conn(port); w = conn(port); res = {}
        def blocker():
            b.sendall(blkcmd); b.recv(256); res["t1"] = time.time()
        th = threading.Thread(target=blocker); th.start()
        time.sleep(0.05)  # ensure blocked
        t0 = time.time(); w.sendall(wakecmd); w.recv(64); th.join(5)
        lat.append((res["t1"] - t0) * 1000)
        b.close(); w.close()
    return statistics.median(lat), max(lat)


def test_blpop_wake_is_fast():
    port = 6590
    p = subprocess.Popen([str(SERVER), "--port", str(port), "--bind", "127.0.0.1", "--loglevel", "warning"])
    try:
        expect(wait_up(port), "up")
        med, mx = measure(port, b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$1\r\n5\r\n",
                          b"*3\r\n$5\r\nLPUSH\r\n$1\r\nk\r\n$1\r\nv\r\n")
        expect(med < 50, f"BLPOP wake median {med:.1f}ms should be well under the old 100ms floor")
        expect(mx < 80, f"BLPOP wake max {mx:.1f}ms should be under 80ms")
    finally:
        p.terminate(); p.wait(timeout=3)


def test_bzpopmin_via_zadd_wake_is_fast():
    port = 6591
    p = subprocess.Popen([str(SERVER), "--port", str(port), "--bind", "127.0.0.1", "--loglevel", "warning"])
    try:
        expect(wait_up(port), "up")
        # cross-type: a ZADD must wake a blocked BZPOPMIN (not just LPUSH waking BLPOP)
        med, mx = measure(port, b"*3\r\n$8\r\nBZPOPMIN\r\n$2\r\nzk\r\n$1\r\n5\r\n",
                          b"*4\r\n$4\r\nZADD\r\n$2\r\nzk\r\n$1\r\n1\r\n$1\r\nm\r\n")
        expect(med < 50, f"BZPOPMIN(via ZADD) wake median {med:.1f}ms should be well under 100ms")
        expect(mx < 80, f"BZPOPMIN wake max {mx:.1f}ms should be under 80ms")
    finally:
        p.terminate(); p.wait(timeout=3)


def main():
    test_blpop_wake_is_fast()
    test_bzpopmin_via_zadd_wake_is_fast()
    print("B12 blocking-latency tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Batch 3 — replication correctness.

Covers: real SYNC (non-empty RDB), min-replicas-to-write enforced against the actual
connected-replica count, expiry convergence master↔replica, and automatic reconnect
after the master restarts. Uses two PeaDB instances (PeaDB master + PeaDB replica).
"""
import pathlib
import socket
import subprocess
import sys
import time

from _isolated_server import IsolatedServer

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready
from _redis_path import skip_if_no_redis_cli

CLI = skip_if_no_redis_cli()
SERVER = ROOT / "peadb-server"


def rc(port, *args):
    return subprocess.run([CLI, "-h", "127.0.0.1", "-p", str(port), *args],
                          capture_output=True, text=True).stdout.strip()


def start(port, *extra):
    p = IsolatedServer([str(SERVER), "--port", str(port), "--bind", "127.0.0.1",
                          "--loglevel", "warning", *extra])
    wait_until_ready(port)
    return p


def expect(cond, msg):
    if not cond:
        raise AssertionError(msg)


def wait_for(fn, want, timeout=5.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if last == want:
            return True
        time.sleep(0.1)
    return False


def test_min_replicas_no_replica():
    port = 6506
    p = start(port)
    try:
        expect(rc(port, "CONFIG", "SET", "min-replicas-to-write", "1") == "OK", "set min-replicas")
        out = rc(port, "SET", "k", "v")
        expect("NOREPLICAS" in out, f"write with 0 of 1 required replicas should be NOREPLICAS, got {out!r}")
        expect(rc(port, "CONFIG", "SET", "min-replicas-to-write", "0") == "OK", "clear min-replicas")
        expect(rc(port, "SET", "k", "v") == "OK", "write allowed after clearing min-replicas")
    finally:
        p.terminate(); p.wait(timeout=3)


def test_min_replicas_with_replica():
    mport, rport = 6507, 6508
    m = start(mport)
    r = start(rport)
    try:
        rc(mport, "SET", "seed", "1")
        rc(rport, "REPLICAOF", "127.0.0.1", str(mport))
        expect(wait_for(lambda: rc(rport, "GET", "seed"), "1"), "replica should sync seed key")
        # With one connected replica, min-replicas-to-write 1 must ALLOW writes
        # (the old code rejected whenever the knob was > 0).
        expect(rc(mport, "CONFIG", "SET", "min-replicas-to-write", "1") == "OK", "set min-replicas on master")
        time.sleep(0.3)
        out = rc(mport, "SET", "afterrepl", "ok")
        expect(out == "OK", f"write with 1 connected replica should succeed, got {out!r}")
    finally:
        r.terminate(); r.wait(timeout=3)
        m.terminate(); m.wait(timeout=3)


def test_expiry_convergence():
    mport, rport = 6509, 6510
    m = start(mport)
    r = start(rport)
    try:
        rc(rport, "REPLICAOF", "127.0.0.1", str(mport))
        rc(mport, "SET", "perm", "1")
        expect(wait_for(lambda: rc(rport, "GET", "perm"), "1"), "replica synced")
        rc(mport, "SET", "tmp", "x", "PX", "300")
        # After the TTL, both master and replica must converge to "key gone".
        expect(wait_for(lambda: rc(mport, "EXISTS", "tmp"), "0"), "master expires tmp")
        expect(wait_for(lambda: rc(rport, "EXISTS", "tmp"), "0"), "replica converges (master DEL propagates)")
        # Replication still healthy afterwards.
        rc(mport, "SET", "after", "y")
        expect(wait_for(lambda: rc(rport, "GET", "after"), "y"), "replication continues after expiry")
    finally:
        r.terminate(); r.wait(timeout=3)
        m.terminate(); m.wait(timeout=3)


def test_auto_reconnect():
    mport, rport = 6511, 6512
    m = start(mport)
    r = start(rport)
    try:
        rc(mport, "SET", "v1", "before")
        rc(rport, "REPLICAOF", "127.0.0.1", str(mport))
        expect(wait_for(lambda: rc(rport, "GET", "v1"), "before"), "initial sync")
        # Kill the master, then bring a fresh one back on the same port.
        m.terminate(); m.wait(timeout=3)
        time.sleep(0.5)
        m = start(mport)
        rc(mport, "SET", "v2", "after")
        # The replica's supervised reconnect loop must re-sync without manual REPLICAOF.
        expect(wait_for(lambda: rc(rport, "GET", "v2"), "after", timeout=10.0),
               "replica should auto-reconnect and re-sync after master restart")
    finally:
        r.terminate(); r.wait(timeout=3)
        m.terminate(); m.wait(timeout=3)


def main():
    test_min_replicas_no_replica()
    test_min_replicas_with_replica()
    test_expiry_convergence()
    test_auto_reconnect()
    print("B3 replication tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

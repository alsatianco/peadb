#!/usr/bin/env python3
"""test_repl_debug.py -- Ad-hoc replication stream debugger.

Purpose
    Exercises the PeaDB replication subsystem by issuing a SYNC handshake on
    one connection while sending write commands via Lua EVAL on another.  The
    replication events that arrive on the replica connection are printed for
    manual inspection.

    This is a developer debugging tool, NOT an automated test with
    pass/fail assertions on replication content.  Use it to visually verify
    that replication events are emitted correctly after command execution.

What it does
    1. Starts a local ``peadb-server`` instance on port 6514 (hardcoded).
    2. Opens two TCP connections to the server:
       a. **Client connection** -- sends FLUSHALL, then three EVAL commands
          (HMGET, INCRBYFLOAT, SET with KEEPTTL) that exercise different
          code paths through the Lua engine.
       b. **Replica connection** -- sends ``SYNC``, reads and discards the
          empty RDB payload (``$0\r\n``), then reads up to 5 replication
          events from the stream.
    3. Prints each replication event as a decoded list of strings.
    4. Terminates the server process.

How to run
    From the repo root:

        python3 scripts/debug/test_repl_debug.py

    No arguments.  The server binary is expected at ``<repo>/peadb-server``
    (the repo-root shortcut symlink or copy).  If your binary is elsewhere,
    either symlink it or modify the ROOT / binary path at the top of main().

Prerequisites
    - Python 3.9+
    - ``peadb-server`` binary at the repo root (or ``build/peadb-server``
      symlinked there).
    - Port 6514 must be free.

Interpreting the output
    Normal output looks like:

        hmget done
        incrbyfloat done
        set done
          repl event 0: ['SELECT', '0']
          repl event 1: ['HMGET', 'key', '1', '2', '3']
          ...
        Total events: 5
          [0]: ['SELECT', '0']
          ...

    If no replication events appear, the replication subsystem may not be
    propagating commands correctly.  If the script hangs, the SYNC handshake
    or event stream may be broken.

Exit codes
    0   Completed without Python exceptions (does NOT imply correctness).
    Non-zero   An unhandled exception occurred (e.g. connection refused,
               assertion failure, timeout).
"""
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


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
    if p == b"*":
        n = int(rl(s))
        if n == -1:
            return None
        return [recv(s) for _ in range(n)]
    raise RuntimeError(f"unexpected: {p}")


def enc(*a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        b = x.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return d


def cmd(s, *a):
    s.sendall(enc(*a))
    return recv(s)


def repl_cmd(s):
    p = rx(s, 1)
    if p == b"*":
        n = int(rl(s))
        out = []
        for _ in range(n):
            assert rx(s, 1) == b"$"
            l = int(rl(s))
            out.append(rx(s, l).decode())
            rx(s, 2)
        return out
    elif p == b"+":
        return [rl(s).decode()]
    elif p == b"$":
        n = int(rl(s))
        d = rx(s, n)
        rx(s, 2)
        return [d.decode()]
    elif p == b":":
        return [rl(s).decode()]
    else:
        raise RuntimeError(f"repl unexpected: {p}")


def main() -> int:
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6514", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.25)
        with socket.create_connection(("127.0.0.1", 6514), timeout=2) as c, socket.create_connection(("127.0.0.1", 6514), timeout=2) as r:
            assert cmd(c, "FLUSHALL") == "OK"
            r.sendall(b"SYNC\r\n")
            assert rx(r, 1) == b"$"
            assert rl(r) == b"0"

            assert cmd(c, "EVAL", "redis.call('hmget', KEYS[1], 1, 2, 3)", "1", "key") is None
            print("hmget done")
            assert cmd(c, "EVAL", "redis.call('incrbyfloat', KEYS[1], 1)", "1", "key") is None
            print("incrbyfloat done")
            assert cmd(c, "EVAL", "redis.call('set', KEYS[1], '1', 'KEEPTTL')", "1", "key") is None
            print("set done")

            # Read replication events
            r.settimeout(2)
            events = []
            for i in range(5):
                try:
                    ev = repl_cmd(r)
                    events.append(ev)
                    print(f"  repl event {i}: {ev}")
                except Exception as e:
                    print(f"  repl event {i}: {e}")
                    break

            print(f"\nTotal events: {len(events)}")
            for i, e in enumerate(events):
                print(f"  [{i}]: {e}")

    finally:
        p.terminate()
        p.wait(timeout=3)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

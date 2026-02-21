#!/usr/bin/env python3
"""test_eval_debug.py -- Ad-hoc RESP3 + Lua EVAL debug driver.

Purpose
    Manually exercises the RESP3 protocol path and Lua scripting debug
    commands against a running Redis-compatible server.  This is intended for
    developer debugging sessions, not automated CI.

What it does
    1. Connects to a server at 127.0.0.1:6599 (hardcoded).
    2. Sends ``HELLO 3`` to switch the connection to the RESP3 protocol and
       prints the first bytes of the reply.
    3. Sends ``DEBUG SET-DISABLE-DENY-SCRIPTS 1`` to allow debug commands
       inside Lua scripts.
    4. Sends an ``EVAL`` that calls ``redis.setresp(3)`` followed by
       ``redis.call('debug', 'protocol', 'attrib')`` and prints the raw
       bytes of the response.

How to run
    1. Start a PeaDB (or Redis) server on port 6599:

           ./build/peadb-server --port 6599

    2. Run the script:

           python3 scripts/debug/test_eval_debug.py

    No command-line arguments are accepted.  To change the host/port, edit
    the constants at the top of ``main()``.

Prerequisites
    - Python 3.9+
    - A running Redis-compatible server on 127.0.0.1:6599 that supports
      RESP3 and the ``DEBUG`` command.

Interpreting the output
    The script prints raw byte representations (``repr()``) of server
    replies.  Example:

        HELLO reply starts: b'%7\r\nserver....'
        DEBUG: b'+OK\r\n'
        RAW attrib response: b'|1\r\n...'

    Inspect the ``RAW attrib response`` to verify that RESP3 attribute
    types are correctly serialised by the server.

    If the connection is refused, ensure the server is running on the
    expected port.  If ``HELLO 3`` fails, the server may not support RESP3.

Exit codes
    0   Completed without Python exceptions.
    Non-zero   Connection error, timeout, or other unhandled exception.

Limitations
    - The HELLO reply draining uses a fixed 0.5 s sleep + non-blocking
      recv loop, which is fragile for slow or remote servers.
    - Host and port are hardcoded; there is no CLI interface.
"""
import socket
import sys
import time


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


def enc(*args):
    d = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return d


def main() -> int:
    s = socket.create_connection(("127.0.0.1", 6599), timeout=2)
    s.settimeout(2)

    # Switch to RESP3
    s.sendall(enc("HELLO", "3"))
    # Read HELLO reply
    raw = b""
    while True:
        raw += s.recv(4096)
        if b"\r\n" in raw:
            break
    print("HELLO reply starts:", repr(raw[:40]))

    # Drain the hello reply fully
    time.sleep(0.5)
    try:
        while True:
            s.settimeout(0.2)
            more = s.recv(4096)
            if not more:
                break
            raw += more
    except Exception:
        pass
    s.settimeout(2)

    # Enable debug scripts
    s.sendall(enc("DEBUG", "SET-DISABLE-DENY-SCRIPTS", "1"))
    resp = b""
    while not resp.endswith(b"\r\n"):
        resp += rx(s, 1)
    print("DEBUG:", repr(resp))

    # Test attrib
    script = "redis.setresp(3);return redis.call('debug', 'protocol', 'attrib')"
    s.sendall(enc("EVAL", script, "0"))

    # Read raw
    time.sleep(0.5)
    raw = s.recv(4096)
    print("RAW attrib response:", repr(raw))

    s.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

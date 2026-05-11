"""Shared helper: poll a peadb-server until it responds to RESP PING.

Replaces the fixed ``time.sleep(0.2)`` startup pattern that races slow CI
runners. Use right after ``subprocess.Popen([..., 'peadb-server', '--port', P, ...])``::

    from _readiness import wait_until_ready
    p = subprocess.Popen([str(ROOT/'peadb-server'), '--port', '6417', ...])
    wait_until_ready(6417)
    # ... safe to connect now ...
"""
from __future__ import annotations

import socket
import time


def wait_until_ready(port: int, host: str = "127.0.0.1",
                     timeout: float = 5.0, interval: float = 0.05) -> None:
    """Block until a RESP PING returns ``+PONG``, or raise on timeout.

    Loops at ``interval`` seconds; ``timeout`` is the absolute budget. The
    first iteration runs immediately, so the helper returns almost instantly
    once the server is up.
    """
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5) as s:
                s.sendall(b"*1\r\n$4\r\nPING\r\n")
                data = s.recv(64)
                if data.startswith(b"+PONG"):
                    return
        except (OSError, ConnectionError) as e:
            last_err = e
        time.sleep(interval)
    raise RuntimeError(
        f"peadb-server on {host}:{port} did not respond to PING within "
        f"{timeout}s (last error: {last_err!r})")

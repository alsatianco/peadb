"""Wait for a successful protocol round trip from the particular child we own.

Pass ``process=proc`` immediately after Popen. INFO server's process_id prevents
an older server on the same port from masquerading as a slow replacement child.
Authentication and loading must finish before PING can establish readiness.
"""
from __future__ import annotations

import socket
import subprocess
import time


class ObservedProcess:
    """Read-only liveness handle; the process's owner remains responsible for reaping."""

    def __init__(self, pid: int):
        self.pid = pid

    def poll(self):
        state = subprocess.run(['ps', '-o', 'stat=', '-p', str(self.pid)],
                               capture_output=True, text=True, check=False).stdout.strip()
        # ps may race exit; an absent process or zombie is no longer live.
        return None if state and not state.startswith('Z') else 1


def _command(stream, sock, *args):
    parts = [arg if isinstance(arg, bytes) else str(arg).encode() for arg in args]
    sock.sendall(f'*{len(parts)}\r\n'.encode() + b''.join(
        f'${len(part)}\r\n'.encode() + part + b'\r\n' for part in parts))
    line = stream.readline(1024)
    if not line.endswith(b'\r\n'):
        raise OSError('incomplete readiness reply')
    if line.startswith(b'$'):
        size = int(line[1:-2])
        if not 0 <= size <= 1024 * 1024:
            raise OSError('invalid readiness bulk length')
        data = stream.read(size + 2)
        if len(data) != size + 2 or not data.endswith(b'\r\n'):
            raise OSError('incomplete readiness bulk reply')
        return data[:-2]
    return line[:-2]


def wait_until_ready(port: int, host: str = '127.0.0.1',
                     timeout: float = 5.0, interval: float = 0.05, *,
                     process: subprocess.Popen | ObservedProcess,
                     password: str | bytes | None = None,
                     username: str | bytes | None = None) -> None:
    """Require PONG, matching child PID and child liveness within a bounded wait.

    The caller owns stop/wait and startup logs on failure. A socket accepting
    connections, NOAUTH, or LOADING is insufficient. The helper never signals
    another process or sends writes to a potentially unrelated server.
    """
    deadline = time.monotonic() + timeout
    last_error = 'no response'

    def check_child():
        code = process.poll()
        if code is not None:
            raise RuntimeError(f'server child {process.pid} exited with code {code} '
                               f'before readiness on {host}:{port}')

    while time.monotonic() < deadline:
        check_child()
        try:
            remaining = max(0.001, min(0.2, deadline - time.monotonic()))
            with socket.create_connection((host, port), timeout=remaining) as sock:
                with sock.makefile('rb') as stream:
                    if password is not None:
                        auth = ('AUTH', password) if username is None else ('AUTH', username, password)
                        if _command(stream, sock, *auth) != b'+OK':
                            raise OSError('readiness authentication failed')
                    if _command(stream, sock, 'PING') != b'+PONG':
                        raise OSError('readiness PING did not return PONG')
                    info = _command(stream, sock, 'INFO', 'server')
                    expected = f'process_id:{process.pid}'.encode()
                    if expected not in info.splitlines():
                        check_child()
                        raise RuntimeError(f'server on {host}:{port} is not owned child '
                                           f'{process.pid} (INFO process_id mismatch)')
                    check_child()
                    return
        except (OSError, ValueError) as error:
            last_error = str(error)
        time.sleep(max(0, min(interval, deadline - time.monotonic())))
    check_child()
    raise RuntimeError(f'server child {process.pid} on {host}:{port} did not become ready '
                       f'within {timeout}s (last error: {last_error})')

#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time

# tests/diff/harness/servers.py -> tests/diff/harness -> tests/diff -> tests
_TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]

# redis-server/redis-cli discovery is shared with the integration tests (see
# tests/integration/_redis_path.py for the resolution order and rationale) —
# do not re-implement that search order here a second time.
_INTEGRATION_DIR = str(_TESTS_DIR / "integration")
if _INTEGRATION_DIR not in sys.path:
    sys.path.insert(0, _INTEGRATION_DIR)
from _redis_path import redis_server_path  # noqa: E402


@dataclass
class ServerProc:
    name: str
    host: str
    port: int
    proc: subprocess.Popen
    workdir: pathlib.Path

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=2)
        shutil.rmtree(self.workdir, ignore_errors=True)


def _wait_ready(host: str, port: int, timeout_sec: float = 8.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"server did not become ready on {host}:{port}")


def start_peadb(root: pathlib.Path, port: int) -> ServerProc:
    server = root / "peadb-server"
    if not server.exists():
        raise FileNotFoundError("peadb-server binary missing; build first")
    work = pathlib.Path(tempfile.mkdtemp(prefix="peadb-diff-"))
    proc = subprocess.Popen(
        [
            str(server),
            "--port",
            str(port),
            "--bind",
            "127.0.0.1",
            "--loglevel",
            "warning",
            "--dir",
            str(work),
            "--dbfilename",
            "peadb-test.rdb",
        ],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _wait_ready("127.0.0.1", port)
    return ServerProc("peadb", "127.0.0.1", port, proc, work)


def start_redis(root: pathlib.Path, port: int) -> ServerProc:
    redis_server = redis_server_path()
    if redis_server is None:
        raise FileNotFoundError(
            "redis-server not found; install redis-server or build third_party/redis"
        )
    print(f"[diff] using redis-server: {redis_server}")

    work = pathlib.Path(tempfile.mkdtemp(prefix="redis-diff-"))
    conf = work / "redis.conf"
    conf.write_text(
        "\n".join(
            [
                "bind 127.0.0.1",
                f"port {port}",
                "save \"\"",
                "appendonly no",
                f"dir {work}",
                "daemonize no",
            ]
        ),
        encoding="utf-8",
    )

    proc = subprocess.Popen(
        [str(redis_server), str(conf)],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _wait_ready("127.0.0.1", port)
    return ServerProc("redis", "127.0.0.1", port, proc, work)

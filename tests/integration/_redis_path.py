"""Shared helper: locate redis-server and redis-cli binaries.

Resolution order:
  1. ``<repo>/third_party/redis/src/redis-server`` (project-local build).
  2. System PATH (``shutil.which``).

If neither is found the ``*_path`` functions return ``None``.  Call
``skip_if_no_redis_server()`` / ``skip_if_no_redis_cli()`` at the top of any
test that requires these binaries; they will print a SKIP message and
``sys.exit(0)`` so the CI runner counts the test as passed (not failed).
"""
from __future__ import annotations

import pathlib
import shutil
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]

_THIRD_PARTY_SERVER = ROOT / "third_party/redis/src/redis-server"
_THIRD_PARTY_CLI = ROOT / "third_party/redis/src/redis-cli"


def redis_server_path() -> str | None:
    """Return absolute path to redis-server, or None."""
    if _THIRD_PARTY_SERVER.exists():
        return str(_THIRD_PARTY_SERVER)
    found = shutil.which("redis-server")
    return found


def redis_cli_path() -> str | None:
    """Return absolute path to redis-cli, or None."""
    if _THIRD_PARTY_CLI.exists():
        return str(_THIRD_PARTY_CLI)
    found = shutil.which("redis-cli")
    return found


def skip_if_no_redis_server() -> str:
    """Return the redis-server path or exit-0 with SKIP message."""
    p = redis_server_path()
    if p is None:
        print("SKIP: redis-server not found (install redis-server or build third_party/redis)")
        sys.exit(0)
    return p


def skip_if_no_redis_cli() -> str:
    """Return the redis-cli path or exit-0 with SKIP message."""
    p = redis_cli_path()
    if p is None:
        print("SKIP: redis-cli not found (install redis-tools or build third_party/redis)")
        sys.exit(0)
    return p

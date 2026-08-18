"""Shared helper: locate redis-server and redis-cli binaries.

This is the single source of truth for the redis-discovery search order used
by both the integration tests (which import this module directly) and the
differential-test harness (``tests/diff/harness/servers.py``, which adds this
directory to ``sys.path`` and imports the same functions). Do not re-implement
this order a second time — see the note in ``servers.py``.

Resolution order (highest priority first):
  1. ``<repo>/third_party/redis/src/redis-server`` — an explicit project-local
     build; a developer put it there deliberately.
  2. ``$PEADB_CACHE_DIR/redis-<REDIS_VERSION>/src/redis-server``, falling back
     to ``~/.cache/peadb/redis-<REDIS_VERSION>/src/redis-server`` when
     ``PEADB_CACHE_DIR`` is unset — the pinned, version-correct build that
     ``scripts/redis/run_redis_tests.sh`` fetches and compiles.
  3. ``shutil.which("redis-server")`` — last resort, version unknown. On dev
     machines this is often the wrong major version (e.g. a Homebrew install),
     so it is deliberately tried last rather than first.

Same order for redis-cli. If nothing is found the ``*_path`` functions return
``None``. Call ``skip_if_no_redis_server()`` / ``skip_if_no_redis_cli()`` at
the top of any test that requires these binaries; they will print a SKIP
message and ``sys.exit(0)`` so the CI runner counts the test as passed (not
failed).
"""
from __future__ import annotations

import os
import pathlib
import shutil
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]

# Keep this in lockstep with REDIS_VERSION in scripts/redis/run_redis_tests.sh
# (that script is the source of truth for which Redis release PeaDB targets
# and fetches/builds into $PEADB_CACHE_DIR).
REDIS_VERSION = "7.2.5"

_THIRD_PARTY_SERVER = ROOT / "third_party/redis/src/redis-server"
_THIRD_PARTY_CLI = ROOT / "third_party/redis/src/redis-cli"

_CACHE_DIR = pathlib.Path(
    os.environ.get("PEADB_CACHE_DIR") or (pathlib.Path.home() / ".cache" / "peadb")
)
_PINNED_SERVER = _CACHE_DIR / f"redis-{REDIS_VERSION}" / "src" / "redis-server"
_PINNED_CLI = _CACHE_DIR / f"redis-{REDIS_VERSION}" / "src" / "redis-cli"


def redis_server_path() -> str | None:
    """Return absolute path to redis-server, or None.

    Resolution order: project-local third_party build, then the pinned
    Redis build in $PEADB_CACHE_DIR (or ~/.cache/peadb), then PATH.
    """
    if _THIRD_PARTY_SERVER.exists():
        return str(_THIRD_PARTY_SERVER)
    if _PINNED_SERVER.exists():
        return str(_PINNED_SERVER)
    return shutil.which("redis-server")


def redis_cli_path() -> str | None:
    """Return absolute path to redis-cli, or None. Same resolution order as
    redis_server_path()."""
    if _THIRD_PARTY_CLI.exists():
        return str(_THIRD_PARTY_CLI)
    if _PINNED_CLI.exists():
        return str(_PINNED_CLI)
    return shutil.which("redis-cli")


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

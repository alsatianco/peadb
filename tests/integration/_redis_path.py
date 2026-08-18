"""Shared helper: locate redis-server and redis-cli binaries.

This is the single source of truth for the redis-discovery search order,
used by:
  - the integration tests that need a real ``redis-server``/``redis-cli``
    (import this module directly): ``test_m5_rdb_bridge.py``,
    ``test_m6_replication_bridge.py``, ``test_b3_replication.py``,
    ``test_m6_replicaof.py``, ``test_m6_multi_replica.py``,
    ``test_m9_diff_no_mismatch.py``, ``test_p1_diff_stagea.py``.
  - the differential-test harness (``tests/diff/harness/servers.py``, which
    adds this directory to ``sys.path`` and imports the same functions).
  - the RDB migration bridge scripts (``scripts/redis/export_rdb_via_redis.py``
    and ``scripts/redis/import_rdb_via_redis.py``), which do the same.

Do not re-implement this order a second time in any of the above — see the
note in each caller.

Resolution order (highest priority first):
  1. ``<repo>/third_party/redis/src/redis-server`` — an explicit project-local
     build; a developer put it there deliberately.
  2. The pinned, version-correct build that ``scripts/redis/run_redis_tests.sh``
     fetches and compiles, at ``<redis dir>/src/redis-server`` — where
     ``<redis dir>`` is resolved with **exactly** the same environment-variable
     precedence as that shell script (see ``_resolve_redis_dir()`` below, which
     mirrors its ``REDIS_DIR="${REDIS_DIR:-$PEADB_CACHE_DIR/redis-$REDIS_VERSION}"``
     / ``PEADB_CACHE_DIR="${PEADB_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/peadb}"``
     lines): ``REDIS_DIR`` outright, else ``$PEADB_CACHE_DIR/redis-<version>``,
     else ``$XDG_CACHE_HOME/peadb/redis-<version>``, else
     ``~/.cache/peadb/redis-<version>``. A variable that is set but empty is
     treated as unset, matching bash's ``:-`` behaviour. Getting this out of
     step with the shell script is a silent wrong-version Redis, so any change
     to one must be mirrored in the other.
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


def _resolve_redis_dir() -> pathlib.Path:
    """Resolve the pinned Redis checkout directory, mirroring the precedence
    of these two lines in ``scripts/redis/run_redis_tests.sh`` exactly:

        PEADB_CACHE_DIR="${PEADB_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/peadb}"
        REDIS_DIR="${REDIS_DIR:-$PEADB_CACHE_DIR/redis-$REDIS_VERSION}"

    Reads live from ``os.environ`` on every call (rather than freezing values
    at import time) so callers — including tests — can flip environment
    variables and observe the effect without a module reload.

    Order, each falling through when the variable is unset OR set to the
    empty string (bash's ``:-`` treats both the same way):
      1. ``REDIS_DIR`` — used verbatim, no ``redis-<version>`` suffix
         appended, since the shell's own default already includes it.
      2. ``PEADB_CACHE_DIR`` — ``<value>/redis-<REDIS_VERSION>``.
      3. ``XDG_CACHE_HOME`` — ``<value>/peadb/redis-<REDIS_VERSION>``.
      4. ``~/.cache/peadb/redis-<REDIS_VERSION>``.
    """
    redis_dir = os.environ.get("REDIS_DIR") or ""
    if redis_dir:
        return pathlib.Path(redis_dir)

    peadb_cache_dir = os.environ.get("PEADB_CACHE_DIR") or ""
    if peadb_cache_dir:
        return pathlib.Path(peadb_cache_dir) / f"redis-{REDIS_VERSION}"

    xdg_cache_home = os.environ.get("XDG_CACHE_HOME") or ""
    if xdg_cache_home:
        return pathlib.Path(xdg_cache_home) / "peadb" / f"redis-{REDIS_VERSION}"

    return pathlib.Path.home() / ".cache" / "peadb" / f"redis-{REDIS_VERSION}"


_REDIS_DIR = _resolve_redis_dir()
_PINNED_SERVER = _REDIS_DIR / "src" / "redis-server"
_PINNED_CLI = _REDIS_DIR / "src" / "redis-cli"


def redis_server_path() -> str | None:
    """Return absolute path to redis-server, or None.

    Resolution order: project-local third_party build, then the pinned
    Redis build (see ``_resolve_redis_dir()`` for its own precedence), then
    PATH.
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

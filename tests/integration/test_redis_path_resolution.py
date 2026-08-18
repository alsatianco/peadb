#!/usr/bin/env python3
"""Pin the environment-variable precedence of _redis_path._resolve_redis_dir().

That function must mirror, exactly, how scripts/redis/run_redis_tests.sh
resolves the directory it fetches and builds the pinned Redis 7.2.5 into:

    PEADB_CACHE_DIR="${PEADB_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/peadb}"
    REDIS_DIR="${REDIS_DIR:-$PEADB_CACHE_DIR/redis-$REDIS_VERSION}"

If the two ever disagree, the test suite silently builds Redis into one
place and looks for it in another -- a wrong-version (or missing) Redis
with no error message. This is exactly the kind of environment-dependent
logic that rots quietly, so each of the four precedence branches (REDIS_DIR,
PEADB_CACHE_DIR, XDG_CACHE_HOME, and the final default) gets its own pinned
case here, including bash's ":-" rule that a variable which is set but
empty falls through to the default just like an unset one.

This repo's tests do not use pytest, so no monkeypatch: os.environ is saved
and restored by hand, and every fabricated value points at a tempfile
scratch directory rather than the real cache.
"""
from __future__ import annotations

import os
import pathlib
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import _redis_path  # noqa: E402

_ENV_VARS = ("REDIS_DIR", "PEADB_CACHE_DIR", "XDG_CACHE_HOME")


def _save_and_clear_env() -> dict[str, str | None]:
    saved = {name: os.environ.get(name) for name in _ENV_VARS}
    for name in _ENV_VARS:
        os.environ.pop(name, None)
    return saved


def _restore_env(saved: dict[str, str | None]) -> None:
    for name, value in saved.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


def main() -> int:
    saved = _save_and_clear_env()
    try:
        with tempfile.TemporaryDirectory(prefix="peadb-redis-path-test-") as scratch:
            scratch_path = pathlib.Path(scratch)
            version = _redis_path.REDIS_VERSION

            # Case 1: REDIS_DIR, if set and non-empty, wins outright -- used
            # verbatim with no "redis-<version>" suffix appended -- even when
            # PEADB_CACHE_DIR and XDG_CACHE_HOME are also set.
            redis_dir = scratch_path / "explicit-redis-dir"
            os.environ["REDIS_DIR"] = str(redis_dir)
            os.environ["PEADB_CACHE_DIR"] = str(scratch_path / "cache-should-be-ignored")
            os.environ["XDG_CACHE_HOME"] = str(scratch_path / "xdg-should-be-ignored")
            got = _redis_path._resolve_redis_dir()
            assert got == redis_dir, f"REDIS_DIR case: expected {redis_dir}, got {got}"

            # Case 2: REDIS_DIR set but empty counts as unset (bash ":-"),
            # falling through to $PEADB_CACHE_DIR/redis-<version>; a set
            # XDG_CACHE_HOME is ignored because PEADB_CACHE_DIR takes
            # priority over it.
            os.environ["REDIS_DIR"] = ""
            cache_dir = scratch_path / "cache-dir"
            os.environ["PEADB_CACHE_DIR"] = str(cache_dir)
            os.environ["XDG_CACHE_HOME"] = str(scratch_path / "xdg-should-be-ignored")
            got = _redis_path._resolve_redis_dir()
            expected = cache_dir / f"redis-{version}"
            assert got == expected, f"PEADB_CACHE_DIR case: expected {expected}, got {got}"

            # Case 3: REDIS_DIR unset and PEADB_CACHE_DIR set but empty (both
            # count as unset), falling through to
            # $XDG_CACHE_HOME/peadb/redis-<version>.
            os.environ.pop("REDIS_DIR", None)
            os.environ["PEADB_CACHE_DIR"] = ""
            xdg_dir = scratch_path / "xdg-dir"
            os.environ["XDG_CACHE_HOME"] = str(xdg_dir)
            got = _redis_path._resolve_redis_dir()
            expected = xdg_dir / "peadb" / f"redis-{version}"
            assert got == expected, f"XDG_CACHE_HOME case: expected {expected}, got {got}"

            # Case 4: nothing set -> ~/.cache/peadb/redis-<version>, the
            # final default. No filesystem access happens here (the function
            # only builds a path), so this is safe to check against the real
            # home directory without redirecting it.
            os.environ.pop("REDIS_DIR", None)
            os.environ.pop("PEADB_CACHE_DIR", None)
            os.environ.pop("XDG_CACHE_HOME", None)
            got = _redis_path._resolve_redis_dir()
            expected = pathlib.Path.home() / ".cache" / "peadb" / f"redis-{version}"
            assert got == expected, f"default case: expected {expected}, got {got}"
    finally:
        _restore_env(saved)

    print("redis path resolution precedence verified")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        raise SystemExit(1)

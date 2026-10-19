#!/usr/bin/env python3
"""Export all live databases and functions as a Redis 7.2.5 RDB.

Usage: PEADB_PASSWORD=... python3 scripts/redis/export_rdb_via_redis.py
       --peadb-port 6379 --out /path/to/output.rdb [--peadb-user default]

See docs/compatibility.md for authentication, consistency and failure semantics.
"""
from _rdb_bridge import run

if __name__ == '__main__':
    raise SystemExit(run('export'))

#!/usr/bin/env python3
"""Import all RDB databases and functions through a temporary Redis 7.2.5.

Usage: PEADB_PASSWORD=... python3 scripts/redis/import_rdb_via_redis.py
       --rdb /path/to/dump.rdb --peadb-port 6379 [--flush-target]

See docs/compatibility.md for authentication, consistency and failure semantics.
"""
from _rdb_bridge import run

if __name__ == '__main__':
    raise SystemExit(run('import'))

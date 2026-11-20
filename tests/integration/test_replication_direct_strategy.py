#!/usr/bin/env python3
"""Production raw-header strategy agrees with Redis, including occupied DB15 and same IDs."""
import os
from pathlib import Path
import subprocess
import tempfile

from _redis_path import redis_server_path
from test_replication_load_strategy import run

ROOT = Path(__file__).resolve().parents[2]
PEADB = os.environ.get('PEADB_BIN', str(ROOT / 'peadb-server'))


def main():
    redis = redis_server_path()
    assert redis and 'v=7.2.5 ' in subprocess.check_output([redis, '--version'], text=True)
    build = 'build-asan' if os.environ.get('PEADB_TEST_SANITIZED') == '1' else 'build'
    helper = (ROOT / build / 'replication_load_test').resolve()
    with tempfile.TemporaryDirectory(prefix='peadb-direct-strategy-') as directory:
        for index, (mode, occupied, matching) in enumerate([
            ('on-empty-db', False, False), ('on-empty-db', True, True),
            ('swapdb', True, False), ('swapdb', True, True)]):
            for label, binary in [('redis', redis), ('peadb', PEADB)]:
                work = Path(directory) / (str(index) + '-' + label)
                work.mkdir()
                run(binary, helper, mode, occupied, matching, work)


if __name__ == '__main__':
    main()

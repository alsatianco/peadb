#!/usr/bin/env python3
"""Redis-first WATCH database identity and repeated-WATCH controls (delta 8)."""
from pathlib import Path
import os
import subprocess
import tempfile

from _readiness import wait_until_ready
from _redis_path import redis_server_path
from test_replica_offset_parity import Conn, stop
from test_replica_resync_parity import ports

ROOT = Path(__file__).resolve().parents[2]
PEADB = os.environ.get('PEADB_BIN', str(ROOT / 'peadb-server'))


def run(binary, work):
    port = ports(1)[0]
    with (work / 'server.log').open('wb') as log:
        proc = subprocess.Popen([binary, '--bind', '127.0.0.1', '--port', str(port),
            '--dir', str(work), '--save', '', '--shutdown-timeout', '0'],
            stdout=log, stderr=subprocess.STDOUT)
    clients = []
    try:
        wait_until_ready(port, process=proc)
        watcher, writer = Conn(port), Conn(port)
        clients = [watcher, writer]

        # Both databases contain the same name/value when WATCH begins. A change in
        # DB15 must abort even though EXEC runs in DB0, whose value never changed.
        assert writer('SELECT', 15) == '+OK'
        assert writer('SET', 'same', 'before') == '+OK'
        assert watcher('SET', 'same', 'before') == '+OK'
        assert watcher('SELECT', 15) == '+OK'
        assert watcher('WATCH', 'same') == '+OK'
        assert watcher('SELECT', 0) == '+OK'
        assert writer('SET', 'same', 'after') == '+OK'
        assert watcher('MULTI') == '+OK'
        assert watcher('GET', 'same') == '+QUEUED'
        assert watcher('EXEC') is None, 'WATCH must retain the watched database'

        # WATCH in a second database must add to the first, including the same name.
        assert writer('SET', 'same', 'before') == '+OK'
        assert watcher('WATCH', 'same') == '+OK'
        assert watcher('SELECT', 15) == '+OK'
        assert watcher('WATCH', 'same') == '+OK'
        assert writer('SELECT', 0) == '+OK'
        assert writer('SET', 'same', 'after') == '+OK'
        assert watcher('MULTI') == '+OK'
        assert watcher('PING') == '+QUEUED'
        assert watcher('EXEC') is None, 'same-name watches in distinct DBs must coexist'

        # Repeating WATCH must not replace its earlier observation after a write.
        assert watcher('WATCH', 'same') == '+OK'
        assert writer('SELECT', 15) == '+OK'
        assert writer('SET', 'same', 'changed-again') == '+OK'
        assert watcher('WATCH', 'same') == '+OK'
        assert watcher('MULTI') == '+OK'
        assert watcher('PING') == '+QUEUED'
        assert watcher('EXEC') is None, 'repeated WATCH must retain its first observation'

        # Changes in an unwatched DB do not abort, and EXEC keeps its selected DB.
        assert watcher('WATCH', 'same', 'missing') == '+OK'
        assert writer('SELECT', 0) == '+OK'
        assert writer('SET', 'same', 'unwatched-change') == '+OK'
        assert watcher('MULTI') == '+OK'
        assert watcher('GET', 'same') == '+QUEUED'
        assert watcher('EXEC') == ['changed-again']
        assert watcher('GET', 'same') == 'changed-again'

        # UNWATCH, DISCARD and RESET all drop every database's subscriptions.
        for clear in ('UNWATCH', 'DISCARD', 'RESET'):
            assert watcher('WATCH', 'same') == '+OK'
            if clear == 'DISCARD':
                assert watcher('MULTI') == '+OK'
            assert watcher(clear) == ('+RESET' if clear == 'RESET' else '+OK')
            assert writer('SELECT', 15) == '+OK'
            assert writer('APPEND', 'same', 'x') > 0
            assert watcher('MULTI') == '+OK'
            assert watcher('PING') == '+QUEUED'
            assert watcher('EXEC') == ['+PONG']
            assert watcher('SELECT', 15) == '+OK'
        print('PASS database-aware and repeated WATCH:', binary, flush=True)
    finally:
        for client in clients:
            client.close()
        stop(proc)


def main():
    redis = redis_server_path()
    assert redis and 'v=7.2.5 ' in subprocess.check_output([redis, '--version'], text=True)
    with tempfile.TemporaryDirectory(prefix='peadb-watch-db-') as td:
        for name, binary in [('redis', redis), ('peadb', PEADB)]:
            work = Path(td) / name
            work.mkdir()
            run(binary, work)


if __name__ == '__main__':
    main()

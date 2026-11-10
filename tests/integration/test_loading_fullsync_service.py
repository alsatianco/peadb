#!/usr/bin/env python3
"""Authenticated disk-backed fullsync loading service without early stream application."""
from pathlib import Path
import subprocess
import tempfile

from _readiness import wait_until_ready
from _redis_path import redis_server_path
from test_loading_event_service import PEADB, LOADING, admission, connect_loading, stop
from test_replica_offset_parity import Conn, encode, wait_for
from test_replica_resync_parity import ports


def run(binary, work):
    pm, pr = ports(2)
    processes, conns = [], []
    try:
        for role, port in [('master', pm), ('replica', pr)]:
            directory = work / role; directory.mkdir()
            with (directory / 'server.log').open('ab') as log:
                proc = subprocess.Popen([binary, '--bind', '127.0.0.1', '--port', str(port),
                    '--dir', str(directory), '--save', '', '--shutdown-timeout', '0',
                    '--requirepass', 'loading-secret', '--masterauth', 'loading-secret',
                    '--repl-diskless-sync', 'no', '--rdbcompression', 'no',
                    '--enable-debug-command', 'yes',
                    '--loading-process-events-interval-bytes', '1024'],
                    stdout=log, stderr=subprocess.STDOUT)
            processes.append(proc)
            wait_until_ready(port, process=proc, password='loading-secret')
            c = Conn(port); conns.append(c)
            assert c('AUTH', 'loading-secret') == '+OK'
        m, r = conns
        assert m.pipeline([['SET', f'key:{i}', 'x' * 4096] for i in range(160)]) == ['+OK'] * 160
        assert m('SAVE') == '+OK'
        assert r('CONFIG', 'SET', 'key-load-delay', '20000') == '+OK'
        assert r('REPLICAOF', '127.0.0.1', pm) == '+OK'
        observer = connect_loading(pr, processes[1]); conns.append(observer)
        # Values written after the snapshot must remain unapplied until replacement ends.
        assert m('SET', 'after-snapshot', 'one') == '+OK'
        assert observer('GET', 'after-snapshot') == LOADING
        assert observer('CONFIG', 'SET', 'appendonly', 'no') == LOADING
        assert observer('MULTI') == '+OK'
        assert observer('GET', 'key:0') == LOADING
        assert observer('EXEC') == '-EXECABORT Transaction discarded because of previous errors.'
        # These loading-flagged DEBUG handlers mutate/flush before entering their loader.
        # PeaDB deliberately refuses DEBUG during a pump; Redis's DEBUG metadata allows it.
        if binary == PEADB:
            for command in [('DEBUG', 'RELOAD', 'NOSAVE'), ('DEBUG', 'LOADAOF'),
                            ('DEBUG', 'POPULATE', '2')]:
                assert observer(*command) == LOADING, command
        wait_until_ready(pr, process=processes[1], password='loading-secret', timeout=10)
        wait_for(lambda: r('GET', 'after-snapshot') == 'one', 'post-snapshot stream')
        assert r('GET', 'key:159') == 'x' * 4096
        assert m('WAIT', '1', '3000') == 1
        print(binary, 'disk-backed fullsync service and publication order passed', flush=True)
    finally:
        for c in conns:
            c.close()
        for proc in reversed(processes):
            stop(proc)


def main():
    redis = redis_server_path()
    assert redis and 'v=7.2.5 ' in subprocess.check_output([redis, '--version'], text=True)
    with tempfile.TemporaryDirectory(prefix='peadb-loading-fullsync-') as td:
        for label, binary in [('redis', redis), ('peadb', PEADB)]:
            work = Path(td) / label; work.mkdir()
            run(binary, work)


if __name__ == '__main__':
    main()

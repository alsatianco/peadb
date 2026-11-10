#!/usr/bin/env python3
"""Observe bounded AOF startup service, precedence and no-save cancellation (delta 70)."""
from pathlib import Path
import os
import subprocess
import tempfile
import time

from _readiness import wait_until_ready
from _redis_path import redis_server_path
from test_loading_event_service import (PEADB, LOADING, admission, connect_loading,
                                        persistence, stop)
from test_replica_offset_parity import Conn, encode
from test_replica_resync_parity import ports

COUNT = 600000


def run(binary, work):
    port = ports(1)[0]
    # Redis 7 accepts this legacy single-file AOF, moving it into its multipart directory.
    source = work / 'appendonly.aof'
    prefix = encode('SELECT', '5') + encode('SET', 'counter', '0')
    record = encode('INCR', 'counter')
    source.write_bytes(prefix + record * COUNT)
    expected_size = source.stat().st_size
    def start():
        with (work / 'server.log').open('ab') as log:
            return subprocess.Popen([binary, '--bind', '127.0.0.1', '--port', str(port),
                '--dir', str(work), '--save', '', '--shutdown-timeout', '0',
                '--requirepass', 'loading-secret', '--appendonly', 'yes',
                '--loading-process-events-interval-bytes', '1024'],
                stdout=log, stderr=subprocess.STDOUT)
    proc = start()
    c = None
    try:
        c = connect_loading(port, proc)
        admission(c, expected_size)
        p = persistence(c)
        assert int(p['loading_loaded_bytes']) > 0, p
        # A pipeline longer than a service budget must progress without another socket write.
        assert c.pipeline([['INFO', 'persistence']] * 140)[-1] is not None
        # GCC/ARM sanitizer replay is CPU-bound on this unchanged 600k-record fixture.
        # Keep every record/value/cancellation assertion; only allow instrumentation time.
        load_budget = 480 if os.environ.get('PEADB_TEST_SANITIZED') == '1' else 30
        wait_until_ready(port, process=proc, password='loading-secret', timeout=load_budget)
        assert c('SELECT', '5') == '+OK'
        assert c('GET', 'counter') == str(COUNT)
        assert c('SELECT', '0') == '+OK'
        assert c('GET', 'counter') is None
        c.close(); c = None
        stop(proc)
        # Restart the same source and cancel after an authenticated loading observation.
        aofs = list(work.rglob('*.aof'))
        original = {p: p.read_bytes() for p in aofs}
        proc = start()
        c = connect_loading(port, proc)
        started = time.monotonic()
        assert c('SHUTDOWN', 'SAVE') is None
        assert proc.wait(timeout=1.5) == 0
        assert time.monotonic() - started < 1.5
        assert not (work / 'dump.rdb').exists(), 'loading SHUTDOWN SAVE must not save'
        for path, data in original.items():
            assert path.read_bytes() == data, path
        print(binary, 'AOF service/DB restoration/pipeline/signal-source preservation passed', flush=True)
    finally:
        if c:
            c.close()
        stop(proc)


def main():
    redis = redis_server_path()
    assert redis and 'v=7.2.5 ' in subprocess.check_output([redis, '--version'], text=True)
    with tempfile.TemporaryDirectory(prefix='peadb-loading-aof-') as td:
        for label, binary in [('redis', redis), ('peadb', PEADB)]:
            work = Path(td) / label; work.mkdir()
            run(binary, work)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""A pending shutdown or paused cron cannot persist/strand a partial load (delta 70)."""
from pathlib import Path
import signal
import subprocess
import tempfile
import time

from _readiness import wait_until_ready
from _redis_path import redis_server_path
from test_aof_framing_parity import Conn, FakeReplica, encode
from test_loading_event_service import PEADB, stop
from test_replica_resync_parity import ports


def run(binary, work, paused):
    port = ports(1)[0]
    def start():
        with (work / 'server.log').open('ab') as log:
            return subprocess.Popen([binary, '--bind', '127.0.0.1', '--port', str(port),
                '--dir', str(work), '--save', '', '--shutdown-timeout', '1',
                '--repl-diskless-sync', 'no', '--rdbcompression', 'no',
                '--enable-debug-command', 'yes',
                '--loading-process-events-interval-bytes', '1024'],
                stdout=log, stderr=subprocess.STDOUT)
    proc = start()
    conns, replica = [], None
    try:
        wait_until_ready(port, process=proc)
        if not paused:
            replica = FakeReplica(port)  # deliberately never acknowledges the subsequent writes
        c = Conn(port); conns.append(c)
        for i in range(20):
            assert c('SET', f'key:{i}', f'value:{i}' + 'x' * 4096) == '+OK'
        assert c('SAVE') == '+OK'
        before = (work / 'dump.rdb').read_bytes()
        assert c('CONFIG', 'SET', 'key-load-delay', '100000') == '+OK'
        if paused:
            assert c('DEBUG', 'PAUSE-CRON', '1') == '+OK'
        else:
            shutdown = Conn(port); conns.append(shutdown)
            shutdown.sock.sendall(encode('SHUTDOWN', 'SAVE'))
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                if 'shutdown_in_milliseconds:' in c('INFO', 'server'):
                    break
                time.sleep(0.005)
            else:
                raise AssertionError('shutdown did not enter the replica wait')
        c.sock.sendall(encode('DEBUG', 'RELOAD', 'NOSAVE'))
        if paused:
            observer = Conn(port); conns.append(observer)
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                if 'loading:1\r\n' in observer('INFO', 'persistence'):
                    break
                time.sleep(0.005)
            else:
                raise AssertionError('reload was not observed while cron was paused')
            began = time.monotonic()
            proc.send_signal(signal.SIGTERM)
            assert proc.wait(timeout=1.5) == 0
            assert time.monotonic() - began < 1.5
        else:
            assert proc.wait(timeout=5) == 0
        # Redis defers an already-pending SAVE until DEBUG RELOAD finishes. PeaDB's
        # loading policy cancels NOSAVE instead; both must retain every source key.
        if paused or binary == PEADB:
            assert (work / 'dump.rdb').read_bytes() == before, 'partial load overwrote source'
        for connection in conns:
            connection.close()
        conns.clear()
        if replica:
            replica.close(); replica = None
        proc = start()
        wait_until_ready(port, process=proc)
        c = Conn(port); conns.append(c)
        assert c('DBSIZE') == 20
        for i in range(20):
            assert c('GET', f'key:{i}') == f'value:{i}' + 'x' * 4096
        print(binary, 'paused-cron' if paused else 'pending-save', 'source/restart passed', flush=True)
    except BaseException:
        print((work / 'server.log').read_text(), flush=True)
        raise
    finally:
        for connection in conns:
            connection.close()
        if replica:
            replica.close()
        stop(proc)


def main():
    redis = redis_server_path()
    assert redis and 'v=7.2.5 ' in subprocess.check_output([redis, '--version'], text=True)
    with tempfile.TemporaryDirectory(prefix='peadb-loading-shutdown-') as td:
        for label, binary in [('redis', redis), ('peadb', PEADB)]:
            for paused in (False, True):
                work = Path(td) / f'{label}-{paused}'; work.mkdir()
                run(binary, work, paused)


if __name__ == '__main__':
    main()

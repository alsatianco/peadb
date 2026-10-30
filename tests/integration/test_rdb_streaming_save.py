#!/usr/bin/env python3
"""SAVE emits bounded output during serialization; partial writes never publish a backup.

The delayed-save check runs pinned Redis first. Disk fault checks use an owned preload
library and scratch files; native cross-load/type/function/full-sync checks live in the
existing persistence and replication fixtures.
"""
import os
import pathlib
import select
import socket
import subprocess
import tempfile
import time

from _readiness import wait_until_ready
from test_replica_offset_parity import Conn, PEADB, REDIS, encode


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def run(binary):
    with tempfile.TemporaryDirectory(prefix='peadb-stream-save-') as work:
        port = free_port()
        with open(pathlib.Path(work) / 'server.log', 'wb') as log:
            proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1',
                                     '--dir', work, '--save', '', '--rdbcompression', 'no',
                                     '--shutdown-timeout', '0'], stdout=log, stderr=log)
            conn = None
            try:
                wait_until_ready(port, process=proc)
                conn = Conn(port)
                value = os.urandom(256 * 1024)
                for i in range(32):
                    assert conn('SET', f'bulk:{i}', value) == '+OK'
                assert conn('CONFIG', 'SET', 'rdb-key-save-delay', '20000') == '+OK'
                conn.sock.sendall(encode('SAVE'))
                deadline = time.monotonic() + 5
                observed = False
                while time.monotonic() < deadline:
                    if select.select([conn.sock], [], [], 0)[0]:
                        break
                    temps = list(pathlib.Path(work).glob('temp-*.rdb'))
                    if temps and temps[0].stat().st_size > 1024 * 1024:
                        observed = True
                        assert not (pathlib.Path(work) / 'dump.rdb').exists(), 'early publication'
                        break
                    time.sleep(0.005)
                reply = conn.read()
                assert reply == '+OK', reply
                assert observed, 'SAVE buffered the snapshot instead of writing during serialization'
                saved = pathlib.Path(work) / 'dump.rdb'
                assert saved.stat().st_size > 8 * 1024 * 1024
                assert not list(pathlib.Path(work).glob('temp-*.rdb'))
                assert conn('CONFIG', 'SET', 'rdb-key-save-delay', '0') == '+OK'
                print(pathlib.Path(binary).name, 'progressive SAVE ok', flush=True)
            finally:
                if conn:
                    conn.close()
                if proc.poll() is None:
                    proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()


if __name__ == '__main__':
    assert REDIS, 'pinned Redis 7.2.5 required'
    run(REDIS)
    run(os.environ.get('PEADB_BIN', PEADB))

#!/usr/bin/env python3
"""Redis-first real decoder delay, config bounds and startup persistence checks."""
from pathlib import Path
import os
import socket
import struct
import subprocess
import tempfile
import time

from _readiness import wait_until_ready
from _redis_path import redis_server_path
from test_rdb_redis_interop import Conn, with_checksum

ROOT = Path(__file__).resolve().parents[2]
PEADB = os.environ.get('PEADB_BIN', str(ROOT / 'peadb-server'))


def run(binary, work):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    def start(*extra):
        log = (work / 'server.log').open('ab')
        try:
            return subprocess.Popen([binary, '--bind', '127.0.0.1', '--port', str(port),
                                     '--dir', str(work), '--save', '', '--shutdown-timeout', '0',
                                     '--enable-debug-command', 'yes', *extra],
                                    stdout=log, stderr=subprocess.STDOUT)
        finally:
            log.close()
    proc = start()
    c = None
    try:
        wait_until_ready(port, process=proc)
        c = Conn(port)
        assert c('CONFIG', 'GET', 'key-load-delay') == [b'key-load-delay', b'0']
        for invalid in ('2147483648', '-2147483649', 'nope'):
            reply = c('CONFIG', 'SET', 'key-load-delay', invalid)
            assert isinstance(reply, str) and reply.startswith('-ERR'), reply
        for value in ('-2147483648', '2147483647', '-1', '0', '25000'):
            assert c('CONFIG', 'SET', 'key-load-delay', value) == '+OK'
            assert c('CONFIG', 'GET', 'key-load-delay') == [b'key-load-delay', value.encode()]
        for i in range(20):
            assert c('SET', f'key:{i}', f'value:{i}') == '+OK'
        assert c('SAVE') == '+OK'
        started = time.monotonic()
        assert c('DEBUG', 'RELOAD', 'NOSAVE') == '+OK'
        elapsed = time.monotonic() - started
        assert 0.4 <= elapsed < 5, (binary, 'reload delay', elapsed)
        assert c('DBSIZE') == 20
        assert c('GET', 'key:19') == b'value:19'
        assert c('CONFIG', 'SET', 'key-load-delay', '0') == '+OK'
        c.close()
        c = None
        proc.terminate()
        assert proc.wait(timeout=5) == 0
        started = time.monotonic()
        proc = start('--key-load-delay', '25000')
        wait_until_ready(port, process=proc)
        elapsed = time.monotonic() - started
        assert 0.4 <= elapsed < 5, (binary, 'startup delay', elapsed)
        c = Conn(port)
        assert c('GET', 'key:19') == b'value:19'
        assert c('CONFIG', 'GET', 'key-load-delay') == [b'key-load-delay', b'25000']
        # Expired records are still decoded and delayed, as in Redis's rdbLoadRio().
        body = b'REDIS0011'
        for i in range(20):
            key = f'expired:{i}'.encode()
            body += b'\xfc' + struct.pack('<Q', 1) + b'\x00' + bytes([len(key)]) + key + b'\x01v'
        (work / 'dump.rdb').write_bytes(with_checksum(body + b'\xff'))
        started = time.monotonic()
        assert c('DEBUG', 'RELOAD', 'NOSAVE') == '+OK'
        elapsed = time.monotonic() - started
        assert 0.4 <= elapsed < 5, (binary, 'expired-key delay', elapsed)
        assert c('DBSIZE') == 0
        info = c('INFO', 'persistence')
        assert b'rdb_last_load_keys_expired:20\r\n' in info, info
        print(binary, 'decoder/startup/expired delay and bounds passed', flush=True)
    finally:
        if c:
            c.close()
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        proc.wait()


def main():
    redis = redis_server_path()
    assert redis and 'v=7.2.5 ' in subprocess.check_output([redis, '--version'], text=True)
    with tempfile.TemporaryDirectory(prefix='peadb-load-delay-') as temp:
        for label, binary in (('redis', redis), ('peadb', PEADB)):
            work = Path(temp) / label
            work.mkdir()
            run(binary, work)


if __name__ == '__main__':
    main()

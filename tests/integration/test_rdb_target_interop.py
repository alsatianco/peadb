#!/usr/bin/env python3
"""Pinned Redis native/legacy snapshots decoded into independent PeaDB targets."""
from pathlib import Path
import os
import socket
import struct
import subprocess
import tempfile

from _readiness import wait_until_ready
from test_rdb_legacy_interop import fixtures, length, rdb, state, string
from test_rdb_redis_interop import Conn, REDIS

ROOT = Path(__file__).resolve().parents[2]
PEADB = os.environ.get('PEADB_BIN', str(ROOT / 'peadb-server'))
BUILD = 'build-asan' if os.environ.get('PEADB_TEST_SANITIZED') == '1' else 'build'
HELPER = os.environ.get('PEADB_RDB_TARGET_TEST', str(ROOT / BUILD / 'rdb_target_test'))


def all_fixtures():
    cases = fixtures()
    cases.append((b'plain-list', b'\x01' + length(2) + string(b'a') + string(b'b'), 'list', [b'a', b'b']))
    cases.append((b'old-zset', b'\x03' + length(1) + string(b'm') + b'\x041.25', 'zset', [(b'm', 1.25)]))
    raw_id = struct.pack('>QQ', 1, 0)
    for kind in (15, 19):
        body = length(0) + length(0) + length(7) + length(0)  # no nodes/entries, last ID 7-0
        if kind == 19:
            body += length(0) * 5  # first/max-deleted IDs and entries-added
        body += length(1) + string(b'group') + length(1) + length(0)
        if kind == 19:
            body += length(0)  # group entries-read
        body += length(1) + raw_id + struct.pack('<Q', 123456) + length(7)
        body += length(1) + string(b'consumer') + struct.pack('<Q', 123)
        body += length(1) + raw_id
        cases.append((f'stream-{kind}'.encode(), bytes([kind]) + body, 'stream', None))
    return cases


def verify_roundtrip(binary, path):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    with path.with_suffix('.log').open('wb') as log:
        proc = subprocess.Popen([binary, '--bind', '127.0.0.1', '--port', str(port),
                                 '--dir', str(path.parent), '--dbfilename', path.name,
                                 '--save', '', '--shutdown-timeout', '0'], stdout=log, stderr=subprocess.STDOUT)
    try:
        wait_until_ready(port, process=proc)
        c = Conn(port)
        try:
            assert c('SELECT', 1) == '+OK'
            for kind, first in ((15, b'18446744073709551615-18446744073709551615'), (19, b'0-0')):
                key = f'stream-{kind}'
                for suffix in ((), ('FULL',)):
                    values = c('XINFO', 'STREAM', key, *suffix)
                    info = dict(zip(values[::2], values[1::2]))
                    assert info[b'recorded-first-entry-id'] == first, (binary, path.name, kind, info)
                    assert info[b'length'] == 0 and info[b'last-generated-id'] == b'7-0'
                pending = c('XPENDING', key, 'group')
                assert pending[:3] == [1, b'1-0', b'1-0'] and pending[3][0][0] == b'consumer'
                assert c('XADD', key, '8-0', 'f', 'v') == b'8-0'
                values = c('XINFO', 'STREAM', key)
                assert dict(zip(values[::2], values[1::2]))[b'recorded-first-entry-id'] == b'8-0'
                assert c('XDEL', key, '8-0') == 1
                values = c('XINFO', 'STREAM', key)
                assert dict(zip(values[::2], values[1::2]))[b'recorded-first-entry-id'] == b'0-0'
        finally:
            c.close()
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        proc.wait()


def run():
    assert REDIS, 'pinned Redis 7.2.5 required'
    assert Path(HELPER).is_file(), 'build the rdb_target_test CMake target first'
    with tempfile.TemporaryDirectory(prefix='peadb-rdb-target-interop-') as directory:
        work = Path(directory)
        legacy = work / 'legacy.rdb'
        legacy.write_bytes(rdb(all_fixtures()))
        (work / 'dump.rdb').write_bytes(legacy.read_bytes())
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        with (work / 'server.log').open('wb') as log:
            proc = subprocess.Popen([REDIS, '--bind', '127.0.0.1', '--port', str(port),
                                     '--dir', directory, '--save', '', '--enable-debug-command', 'yes',
                                     '--shutdown-timeout', '0'], stdout=log, stderr=subprocess.STDOUT)
        try:
            wait_until_ready(port, process=proc)
            c = Conn(port)
            try:
                assert c('SELECT', 1) == '+OK'
                # Redis validates the old zipmap/ziplist/quicklist fixtures before our target.
                for key, _, kind, expected in all_fixtures():
                    if kind == 'stream':
                        pending = c('XPENDING', key, 'group')
                        assert pending[:3] == [1, b'1-0', b'1-0'] and pending[3][0][0] == b'consumer'
                    elif expected is not None:
                        assert state(c, key, kind) == expected
                    else:
                        assert float(c('ZSCORE', key, 'tiny')) == 5e-324
                for db in (0, 1, 15):
                    assert c('SELECT', db) == '+OK'
                    assert c('SET', 'large', b'binary\x00\xff' * 16000) == '+OK'
                    assert c('SET', 'integer', -123456) == '+OK'
                    assert c('SADD', 'intset', 1, -32769, 9223372036854775807) == 3
                    assert c('HSET', 'listpack-hash', 'f', 'v') == 1
                    assert c('ZADD', 'listpack-zset', 1.25, 'member') == 1
                    assert c('SADD', 'listpack-set', 'one', 'two') == 2
                    assert c('RPUSH', 'native-quicklist', b'a' * 10000, 'small') == 2
                    assert c('XADD', 'stream', '1-0', 'field', b'v\x00\xff') == b'1-0'
                    assert c('XADD', 'stream', '2-0', 'field', 'other') == b'2-0'
                    assert c('XGROUP', 'CREATE', 'stream', 'group', '0') == '+OK'
                    assert c('XREADGROUP', 'GROUP', 'group', 'consumer', 'COUNT', 1, 'STREAMS', 'stream', '>')
                    assert c('XGROUP', 'CREATECONSUMER', 'stream', 'group', 'empty') == 1
                    assert c('XDEL', 'stream', '2-0') == 1
                    assert c('PEXPIREAT', 'large', 9007199254740993) == 1
                    # Force hashtable/plain encodings as well as Redis's compact defaults.
                    for setting in ('hash-max-listpack-entries', 'set-max-intset-entries',
                                    'set-max-listpack-entries', 'zset-max-listpack-entries'):
                        assert c('CONFIG', 'SET', setting, 0) == '+OK'
                    assert c('HSET', 'plain-hash', 'f', 'v') == 1
                    assert c('SADD', 'plain-set', 'member') == 1
                    assert c('ZADD', 'plain-zset', 2.5, 'member') == 1
                assert c('FUNCTION', 'LOAD', "#!lua name=private\nredis.register_function{function_name='private',callback=function() return 42 end,flags={'no-writes'}}") == b'private'
                assert c('SAVE') == '+OK'
            finally:
                c.close()
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
            proc.wait()
        checker = str(Path(REDIS).with_name('redis-check-rdb'))
        for path in (legacy, work / 'dump.rdb'):
            subprocess.run([checker, str(path)], check=True, stdout=subprocess.DEVNULL)
        # Linux ASAN/UBSAN needs ~30 CPU seconds for both complete large fixtures.
        # Keep the 65,536-element legacy list and every assertion; Release remains at 15s.
        budget = 60 if os.environ.get('PEADB_TEST_SANITIZED') == '1' else 15
        subprocess.run([HELPER, str(legacy), str(work / 'dump.rdb')], check=True, timeout=budget)
        for original in (legacy, work / 'dump.rdb'):
            output = Path(str(original) + '.peadb.rdb')
            subprocess.run([checker, str(output)], check=True, stdout=subprocess.DEVNULL)
            for binary in (REDIS, PEADB):
                verify_roundtrip(binary, output)
        print('Redis native/legacy independent targets passed', flush=True)


if __name__ == '__main__':
    run()

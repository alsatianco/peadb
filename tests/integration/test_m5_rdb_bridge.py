#!/usr/bin/env python3
"""Real Redis 7.2.5 bridge reliability, authentication and lossless round trips."""
import contextlib
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time

from _redis_path import redis_server_path
from test_rdb_redis_interop import Conn, LIB
from test_dump_native_interop import populate, state, port

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts' / 'redis'))
from _rdb_bridge import Connection, BridgeError
PEADB = sys.argv[1] if len(sys.argv) > 1 else str(ROOT / 'peadb-server')
REDIS = redis_server_path()
PASSWORD = 'bridge-test-secret'


@contextlib.contextmanager
def server(binary, directory, password=None, rdb=None, *extra):
    directory = pathlib.Path(directory)
    directory.mkdir()
    if rdb:
        shutil.copyfile(rdb, directory / 'dump.rdb')
    p = port()
    with (directory / 'server.log').open('wb') as log:
        args = [binary, '--port', str(p), '--bind', '127.0.0.1', '--dir', str(directory),
                '--save', '', '--appendonly', 'no', *extra]
        if password:
            args += ['--requirepass', password]
        proc = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
        c = None
        try:
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                assert proc.poll() is None, (directory / 'server.log').read_text()
                try:
                    c = Conn(p)
                    if password:
                        assert c('AUTH', password) == '+OK'
                    assert c('PING') == '+PONG'
                    break
                except OSError:
                    if c:
                        c.close()
                    time.sleep(.03)
            else:
                raise AssertionError('server not ready')
            yield p, c
        finally:
            if c:
                c.close()
            if proc.poll() is None:
                proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def bridge(kind, p, path, *args, password=None, success=True, redis_binary=None):
    env = os.environ.copy()
    env.pop('PEADB_PASSWORD', None)
    env.pop('REDISCLI_AUTH', None)
    env['REDIS_SERVER'] = redis_binary or REDIS
    if password is not None:
        env['PEADB_PASSWORD'] = password
    result = subprocess.run([sys.executable, str(ROOT / 'scripts/redis' / f'{kind}_rdb_via_redis.py'),
                             '--peadb-port', str(p), '--out' if kind == 'export' else '--rdb',
                             str(path), *args], env=env, capture_output=True, text=True, timeout=12)
    assert (result.returncode == 0) == success, (kind, result.returncode, result.stdout, result.stderr)
    assert PASSWORD not in result.stdout + result.stderr
    assert ('complete' in result.stdout) == success
    print(f'{kind} {"success" if success else "expected refusal"}: '
          f'{result.stdout.strip() or result.stderr.strip()}', flush=True)
    return result


def dataset(c):
    keys = populate(c)
    # Lua numbers cannot represent every signed 64-bit expiry timestamp.
    for key, expiry in (('far-expiry', 9007199254740993), ('max-expiry', 9223372036854775807)):
        assert c('SET', key, 'precise') == '+OK'
        assert c('PEXPIREAT', key, expiry) == 1
        keys.append(key)
    assert c('FUNCTION', 'LOAD', LIB) == b'interoplib'
    expected = {0: {k: state(c, k) for k in keys}}
    for db in (1, 15):
        assert c('SELECT', db) == '+OK'
        c('SET', b'\x00binary\xff', b'\xff\x00value')
        c('RPUSH', 'list', 'original', db)
        c('PEXPIREAT', 'list', 2000000000000)
        expected[db] = {k: state(c, k) for k in (b'\x00binary\xff', 'list')}
    c('SELECT', 0)
    return expected


def verify(c, expected):
    for db, keys in expected.items():
        assert c('SELECT', db) == '+OK'
        assert c('DBSIZE') == len(keys), (db, c('KEYS', '*'))
        for k, wanted in keys.items():
            assert state(c, k) == wanted, (db, k, state(c, k), wanted)
    assert c('FCALL', 'interop_f', 0) == b'from-lib'
    c('SELECT', 0)


def protocol_errors():
    # A short bulk, invalid delimiter and nested error must fail loudly;
    # native DUMP values may contain binary bytes and embedded CRLF.
    for payload in (b'$5\r\nab', b'$1\r\naXX', b'$-2\r\n',
                    b'+OK\n', b'?unknown\r\n',
                    b'*2\r\n$1\r\na\r\n-NOPERM denied\r\n'):
        a, b = socket.socketpair()
        c = Connection.__new__(Connection)
        c.sock, c.reader = a, a.makefile('rb')
        try:
            b.sendall(payload)
            b.shutdown(socket.SHUT_WR)
            try:
                c.read()
            except (BridgeError, ValueError):
                pass
            else:
                raise AssertionError('malformed/error reply accepted')
        finally:
            c.close()
            b.close()


def main():
    protocol_errors()
    assert REDIS, 'Redis reference required'
    assert 'v=7.2.5' in subprocess.check_output([REDIS, '--version'], text=True)
    with tempfile.TemporaryDirectory(prefix='peadb-bridge-regression-') as td:
        td = pathlib.Path(td)
        # A real Redis with SAVE disabled exercises a refusal after data was copied.
        no_save = td / 'redis-without-save'
        no_save.write_text('#!/usr/bin/env python3\nimport os, sys\n'
                           'args = sys.argv[1:]\n'
                           'if "--version" not in args:\n'
                           '    args += ["--rename-command", "SAVE", ""]\n'
                           f'os.execv({REDIS!r}, [{REDIS!r}, *args])\n')
        no_save.chmod(0o700)
        # Run the same bridge expectations on Redis first, then PeaDB.
        for label, binary in (('redis', REDIS), ('peadb', PEADB)):
            with server(binary, td / f'{label}-source', PASSWORD) as (p, c):
                expected = dataset(c)
                output = td / f'{label}.rdb'
                output.write_bytes(b'existing-backup')
                bridge('export', p, output, success=False)
                assert output.read_bytes() == b'existing-backup'
                bridge('export', p, output, password='wrong', success=False)
                assert output.read_bytes() == b'existing-backup'
                bridge('export', p, output, password=PASSWORD,
                       redis_binary=str(no_save), success=False)
                assert output.read_bytes() == b'existing-backup'
                # Publication failure must also exit nonzero after a successful SAVE.
                blocked_output = td / f'{label}-output-directory'
                blocked_output.mkdir()
                (blocked_output / 'sentinel').write_bytes(b'kept')
                bridge('export', p, blocked_output, password=PASSWORD, success=False)
                assert (blocked_output / 'sentinel').read_bytes() == b'kept'
                assert not list(td.glob('.peadb-rdb-*'))
                bridge('export', p, output, '--peadb-user', 'default', password=PASSWORD)
                subprocess.run([str(pathlib.Path(REDIS).with_name('redis-check-rdb')), str(output)],
                               check=True, capture_output=True)
                # Port collision must fail and must never SAVE the existing listener.
                bridge('export', p, output, '--target-port', str(p), password=PASSWORD, success=False)
                with server(REDIS, td / f'{label}-reload', None, output) as (_, reloaded):
                    verify(reloaded, expected)
                c('FLUSHALL')
                c('FUNCTION', 'FLUSH')
                empty_output = td / f'{label}-empty.rdb'
                result = bridge('export', p, empty_output, password=PASSWORD)
                assert '0 key transfers across 16 databases' in result.stdout
                with server(REDIS, td / f'{label}-empty', None, empty_output) as (_, empty):
                    for db in range(16):
                        assert empty('SELECT', db) == '+OK'
                        assert empty('DBSIZE') == 0
                    assert empty('FUNCTION', 'LIST') == []
            with server(binary, td / f'{label}-target', PASSWORD) as (p, c):
                c('SET', 'untouched', 'value')
                bridge('import', p, output, '--flush-target', success=False)
                assert c('GET', 'untouched') == b'value'
                # Existing values and TTLs must be replaced, not appended or merged.
                c('DEL', 'untouched')
                c('RPUSH', 'list', 'stale')
                c('HSET', 'hash', 'stale', 'value')
                c('SET', 'ints16', 'wrong-type', 'PX', 60000)
                bridge('import', p, output, password=PASSWORD)
                verify(c, expected)
                bridge('import', p, output, '--flush-target', '--peadb-password', PASSWORD)
                verify(c, expected)
                bridge('import', p, output, '--flush-target', password='wrong', success=False)
                verify(c, expected)
                # READONLY RESTORE cannot be reported as a successful import.
                c('REPLICAOF', '127.0.0.1', port())
                bridge('import', p, output, password=PASSWORD, success=False)
                c('REPLICAOF', 'NO', 'ONE')
                bad = td / f'{label}-bad.rdb'
                bad.write_bytes(b'not an RDB')
                bridge('import', p, bad, '--flush-target', password=PASSWORD, success=False)
                verify(c, expected)
        # ACL credentials are sent using AUTH user password on the live endpoint.
        with server(REDIS, td / 'acl') as (p, c):
            c('SET', 'acl-key', 'value')
            c('ACL', 'SETUSER', 'bridge', 'on', '>' + PASSWORD, '~*', '&*', '+@all', '-eval')
            c('ACL', 'SETUSER', 'default', 'off')
            bridge('export', p, td / 'acl.rdb', '--peadb-user', 'bridge', success=False)
            bridge('export', p, td / 'acl.rdb', '--peadb-user', 'bridge', password=PASSWORD)
            before = (td / 'acl.rdb').read_bytes()
            c('ACL', 'SETUSER', 'bridge', '-scan')
            bridge('export', p, td / 'acl.rdb', '--peadb-user', 'bridge', password=PASSWORD, success=False)
            assert (td / 'acl.rdb').read_bytes() == before
            c('ACL', 'SETUSER', 'bridge', '+scan', '-dump')
            bridge('export', p, td / 'acl.rdb', '--peadb-user', 'bridge', password=PASSWORD, success=False)
            assert (td / 'acl.rdb').read_bytes() == before
            c('ACL', 'SETUSER', 'bridge', '+dump', '-function|dump')
            c('SET', 'sentinel', 'kept')
            bridge('import', p, td / 'acl.rdb', '--peadb-user', 'bridge',
                   password=PASSWORD, success=True)
            assert c('GET', 'sentinel') == b'kept'
            c('ACL', 'SETUSER', 'bridge', '-restore')
            bridge('import', p, td / 'acl.rdb', '--peadb-user', 'bridge',
                   password=PASSWORD, success=False)
        with server(REDIS, td / 'db32', None, None, '--databases', '32') as (p, c):
            c('SELECT', 31)
            c('SET', 'outside16', 'value')
            bridge('export', p, td / 'db32.rdb')
        with server(PEADB, td / 'db16') as (p, c):
            c('SET', 'sentinel', 'kept')
            bridge('import', p, td / 'db32.rdb', '--flush-target', success=False)
            assert c('GET', 'sentinel') == b'kept'
            bridge('import', p, td / 'db32.rdb', '--databases', '32',
                   '--flush-target', success=False)
            assert c('GET', 'sentinel') == b'kept'
    print('M5 authenticated RDB bridge tests passed')


if __name__ == '__main__':
    main()

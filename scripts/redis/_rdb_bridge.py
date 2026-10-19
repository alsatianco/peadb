"""Checked RESP2 transport and shared offline RDB bridge implementation.

The migration procedure and consistency limits live in docs/compatibility.md.
"""
from __future__ import annotations

import argparse
import contextlib
import os
import pathlib
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests' / 'integration'))
from _redis_path import redis_server_path  # noqa: E402


class BridgeError(RuntimeError):
    pass


class Connection:
    def __init__(self, host, port, timeout, password=None, user=None):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.reader = self.sock.makefile('rb')
        try:
            if password is not None:
                argv = ('AUTH', password) if user is None else ('AUTH', user, password)
                self.expect_ok(*argv)
            elif user is not None:
                raise BridgeError('--peadb-user requires a password')
            if self.command('PING') != b'PONG':
                raise BridgeError('unexpected PING reply')
        except BaseException:
            self.close()
            raise

    def close(self):
        self.reader.close()
        self.sock.close()

    def read(self):
        line = self.reader.readline()
        if not line or not line.endswith(b'\r\n'):
            raise BridgeError('truncated RESP reply')
        kind, data = line[:1], line[1:-2]
        if kind == b'+':
            return data
        if kind == b'-':
            # The error class is enough to diagnose refusal without echoing keys,
            # values, script text or passwords from an untrusted endpoint.
            error_class = data.split(b' ', 1)[0]
            allowed = {b'ERR', b'NOAUTH', b'WRONGPASS', b'NOPERM', b'READONLY', b'OOM',
                       b'MISCONF', b'LOADING', b'MASTERDOWN', b'BUSYKEY', b'BUSY',
                       b'CLUSTERDOWN', b'MOVED', b'ASK', b'NOSCRIPT', b'IOERR'}
            safe = error_class.decode() if error_class in allowed else 'server error'
            raise BridgeError(f'server refused command ({safe})')
        if kind == b':':
            return int(data)
        if kind in (b'$', b'*'):
            n = int(data)
            if n == -1:
                return None
            if n < 0:
                raise BridgeError('invalid RESP length')
            if kind == b'*':
                return [self.read() for _ in range(n)]
            value = self.reader.read(n)
            if len(value) != n or self.reader.read(2) != b'\r\n':
                raise BridgeError('truncated RESP bulk reply')
            return value
        raise BridgeError('unsupported RESP reply')

    @staticmethod
    def encode(args):
        parts = [f'*{len(args)}\r\n'.encode()]
        for arg in args:
            value = arg if isinstance(arg, bytes) else str(arg).encode()
            parts += [f'${len(value)}\r\n'.encode(), value, b'\r\n']
        return b''.join(parts)

    def reply(self, command):
        try:
            return self.read()
        except BridgeError as exc:
            # Only static command names appear here, never arguments.
            raise BridgeError(f'{command}: {exc}') from None

    def command(self, *args):
        self.sock.sendall(self.encode(args))
        return self.reply(args[0])

    def read_key(self, key):
        # A pipelined transaction reads the pair without interleaved writes or Lua's double
        # conversion, which rounds valid expiry timestamps above 2**53.
        commands = (('MULTI',), ('DUMP', key), ('PEXPIRETIME', key), ('EXEC',))
        self.sock.sendall(b''.join(self.encode(args) for args in commands))
        replies = [self.reply(args[0]) for args in commands]
        if replies[:3] != [b'OK', b'QUEUED', b'QUEUED']:
            raise BridgeError('DUMP/PEXPIRETIME: unexpected transaction reply')
        return replies[3]

    def expect_ok(self, *args):
        if self.command(*args) != b'OK':
            raise BridgeError(f'{args[0]}: expected OK')

    def databases(self):
        result = self.command('CONFIG', 'GET', 'databases')
        if not isinstance(result, list) or len(result) != 2 or result[0] != b'databases':
            raise BridgeError('CONFIG GET databases: unexpected reply')
        n = int(result[1])
        if n <= 0:
            raise BridgeError('invalid database count')
        return n

    def keys(self):
        cursor = b'0'
        while True:
            result = self.command('SCAN', cursor, 'COUNT', 1000)
            if (not isinstance(result, list) or len(result) != 2
                    or not isinstance(result[0], bytes) or not result[0].isdigit()
                    or not isinstance(result[1], list)
                    or any(not isinstance(k, bytes) for k in result[1])):
                raise BridgeError('SCAN: unexpected reply')
            cursor, keys = result
            yield from keys
            if cursor == b'0':
                return


@contextlib.contextmanager
def connection(*args, **kwargs):
    c = Connection(*args, **kwargs)
    try:
        yield c
    finally:
        c.close()


@contextlib.contextmanager
def temporary_redis(directory, port, databases, timeout):
    binary = os.environ.get('REDIS_SERVER') or redis_server_path()
    if not binary:
        raise BridgeError('redis-server not found; set REDIS_SERVER')
    version = subprocess.run([binary, '--version'], check=True, capture_output=True,
                             text=True, timeout=timeout).stdout
    if 'v=7.2.5 ' not in version:
        raise BridgeError('bridge requires Redis 7.2.5; set REDIS_SERVER to the pinned build')
    # Refuse an occupied explicit port; never attach to somebody else's server.
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', port))
        port = reservation.getsockname()[1]
    password = secrets.token_hex(32)
    conf = pathlib.Path(directory) / 'redis.conf'
    quoted_dir = str(directory).replace('\\', '\\\\').replace('"', '\\"')
    quoted_dir = quoted_dir.replace('\n', '\\n').replace('\r', '\\r')
    conf.write_text(f'bind 127.0.0.1\nport {port}\ndir "{quoted_dir}"\n'
                    f'databases {databases}\ndbfilename dump.rdb\nappendonly no\nsave ""\n'
                    f'daemonize no\nrequirepass {password}\n', encoding='utf-8')
    conf.chmod(0o600)
    with (pathlib.Path(directory) / 'redis.log').open('wb') as log:
        proc = subprocess.Popen([binary, str(conf)], stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise BridgeError('temporary Redis failed to load/start; check RDB and database count')
                try:
                    with connection('127.0.0.1', port, min(timeout, .5), password):
                        break
                except (OSError, BridgeError):
                    time.sleep(.05)
            else:
                raise BridgeError('temporary Redis readiness timed out')
            # The random AUTH secret also detects the bind race: another listener
            # cannot be mistaken for the child between reservation and exec.
            if proc.poll() is not None:
                raise BridgeError('temporary Redis exited during readiness')
            with connection('127.0.0.1', port, timeout, password) as c:
                yield c
        finally:
            if proc.poll() is None:
                proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def copy_dataset(src, dst, flush=False):
    databases = src.databases()
    if dst.databases() < databases:
        raise BridgeError('target has fewer databases than source; refusing before copying')
    functions = src.command('FUNCTION', 'DUMP')
    if not isinstance(functions, bytes):
        raise BridgeError('FUNCTION DUMP: unexpected reply')
    if flush:
        dst.expect_ok('FLUSHALL')
        dst.expect_ok('FUNCTION', 'FLUSH')
    dst.expect_ok('FUNCTION', 'RESTORE', functions, 'REPLACE')
    copied = 0
    for db in range(databases):
        src.expect_ok('SELECT', db)
        dst.expect_ok('SELECT', db)
        for key in src.keys():
            result = src.read_key(key)
            if not isinstance(result, list) or len(result) != 2 or type(result[1]) is not int:
                raise BridgeError('DUMP/PEXPIRETIME: unexpected reply')
            dump, expires = result
            if dump is None and expires == -2:
                continue
            if not isinstance(dump, bytes) or expires < -1:
                raise BridgeError('DUMP/PEXPIRETIME: inconsistent key state')
            dst.expect_ok('RESTORE', key, 0 if expires == -1 else expires,
                          dump, 'REPLACE', 'ABSTTL')
            copied += 1
    return copied, databases


def publish_rdb(source, output):
    output = pathlib.Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.peadb-rdb-', dir=output.parent)
    try:
        with os.fdopen(fd, 'wb') as f, open(source, 'rb') as src:
            shutil.copyfileobj(src, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, output)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def positive_int(value):
    n = int(value)
    if n <= 0:
        raise argparse.ArgumentTypeError('must be positive')
    return n


def run(kind):
    parser = argparse.ArgumentParser(description=f'{kind.title()} an RDB through Redis 7.2.5; '
                                    'quiesce application writes first (docs/compatibility.md).')
    parser.add_argument('--peadb-port', required=True, type=int)
    parser.add_argument('--peadb-host', default='127.0.0.1')
    parser.add_argument('--peadb-user', help='AUTH username on the live endpoint')
    parser.add_argument('--peadb-password', help='AUTH password (prefer PEADB_PASSWORD env to hide argv)')
    parser.add_argument('--timeout', type=positive_int, default=30, help='socket/startup timeout in seconds')
    if kind == 'export':
        parser.add_argument('--out', required=True)
        parser.add_argument('--target-port', type=int, default=0, help='temporary Redis port (0: ephemeral)')
    else:
        parser.add_argument('--rdb', required=True)
        parser.add_argument('--source-port', type=int, default=0, help='temporary Redis port (0: ephemeral)')
        parser.add_argument('--databases', type=positive_int, default=16,
                            help='database count of the input RDB (default: 16)')
        parser.add_argument('--flush-target', action='store_true',
                            help='clear all target keys and functions before copying')
    args = parser.parse_args()
    password = args.peadb_password
    if password is None:
        password = os.environ.get('PEADB_PASSWORD')
    try:
        with connection(args.peadb_host, args.peadb_port, args.timeout,
                        password, args.peadb_user) as live:
            with tempfile.TemporaryDirectory(prefix=f'peadb-rdb-{kind}-') as td:
                if kind == 'export':
                    with temporary_redis(td, args.target_port, live.databases(), args.timeout) as target:
                        copied, databases = copy_dataset(live, target)
                        target.expect_ok('SAVE')
                    publish_rdb(pathlib.Path(td) / 'dump.rdb', args.out)
                else:
                    shutil.copyfile(args.rdb, pathlib.Path(td) / 'dump.rdb')
                    with temporary_redis(td, args.source_port, args.databases, args.timeout) as source:
                        copied, databases = copy_dataset(source, live, args.flush_target)
        print(f'{kind} complete: {copied} key transfers across {databases} databases')
        return 0
    except (BridgeError, OSError, ValueError, subprocess.SubprocessError) as exc:
        # OSError/SubprocessError can include argument data; only BridgeError has
        # deliberately sanitized detail. Never display credentials in a traceback.
        detail = str(exc) if isinstance(exc, BridgeError) else type(exc).__name__
        consequence = ('import may be partial; verify before retrying' if kind == 'import'
                       else 'output was not replaced')
        print(f'{kind} failed: {detail}; {consequence}', file=sys.stderr)
        return 1

#!/usr/bin/env python3
"""ASKING, READONLY, READWRITE and SCRIPT DEBUG no longer reply OK and do nothing (compat/delta.md item 26).

The same commands go to peadb-server and to the pinned redis-server, and the replies must be the same: without cluster mode ASKING, READONLY and READWRITE are refused with Redis's error, and SCRIPT DEBUG checks its arguments and the transaction state like Redis. PeaDB's own answers are checked where they differ on purpose: PeaDB has no Lua debugger, so SCRIPT DEBUG YES and SYNC are refused and the next script runs normally; with cluster mode on, ASKING, READONLY and READWRITE work; and MIGRATE to a server without cluster mode no longer sends ASKING, so it still works now that such a server refuses ASKING. Without a pinned redis-server, only the PeaDB side runs.
"""
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _redis_path import redis_server_path  # noqa: E402

PEADB = str(ROOT / 'peadb-server')
REDIS = redis_server_path()
PEADB_PORT = 6572
REDIS_PORT = 6573
PEADB_SECOND_PORT = 6574  # MIGRATE target, then a cluster-mode server

SCRIPT_DEBUG_REFUSED = ('-ERR SCRIPT DEBUG YES and SYNC are not supported by PeaDB yet '
                        '(it has no Lua debugger)')

SAME_REPLY = [
    (('ASKING',), '-ERR This instance has cluster support disabled'),
    (('READONLY',), '-ERR This instance has cluster support disabled'),
    (('READWRITE',), '-ERR This instance has cluster support disabled'),
    (('asking',), '-ERR This instance has cluster support disabled'),
    (('ASKING', 'x'), "-ERR wrong number of arguments for 'asking' command"),
    (('READONLY', 'x'), "-ERR wrong number of arguments for 'readonly' command"),
    (('READWRITE', 'x'), "-ERR wrong number of arguments for 'readwrite' command"),
    (('SCRIPT', 'DEBUG'), "-ERR wrong number of arguments for 'script|debug' command"),
    (('SCRIPT', 'DEBUG', 'no', 'x'), "-ERR wrong number of arguments for 'script|debug' command"),
    (('SCRIPT', 'DEBUG', 'maybe'), '-ERR Use SCRIPT DEBUG YES/SYNC/NO'),
    (('SCRIPT', 'DEBUG', 'no'), '+OK'),
    (('SCRIPT', 'DEBUG', 'NO'), '+OK'),
]


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=5)
        self.f = self.sock.makefile('rb')

    def close(self):
        self.f.close()
        self.sock.close()

    def read(self):
        line = self.f.readline()
        if not line:
            raise ConnectionError('connection closed')
        kind, rest = line[:1], line[1:-2]
        if kind in (b'+', b'-'):
            return (kind + rest).decode()
        if kind == b':':
            return int(rest)
        if kind == b'$':
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2].decode()
        if kind == b'*':
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise ValueError(f'unexpected reply {line!r}')

    def send(self, *commands):
        """Send several commands in one write (a pipeline)."""
        out = b''
        for args in commands:
            out += f'*{len(args)}\r\n'.encode()
            for a in args:
                b = a.encode()
                out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
        self.sock.sendall(out)

    def __call__(self, *args):
        self.send(args)
        return self.read()


def start(binary, port, workdir, *extra):
    os.makedirs(workdir, exist_ok=True)
    log = open(os.path.join(workdir, 'server.log'), 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--save', '',
                             '--dir', workdir, '--loglevel', 'warning', *extra],
                            stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'{binary} exited: {open(os.path.join(workdir, "server.log")).read()}')
        try:
            socket.create_connection(('127.0.0.1', port), timeout=0.2).close()
            return proc
        except OSError:
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError(f'{binary} did not start on port {port}')


def stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def check_same_replies(c):
    for args, expected in SAME_REPLY:
        got = c(*args)
        assert got == expected, (args, got, expected)
    # SCRIPT DEBUG is refused while the client has replies not yet sent: inside a transaction,
    # and after another command in the same pipeline.
    assert c('MULTI') == '+OK'
    assert c('SCRIPT', 'DEBUG', 'no') == '+QUEUED'
    assert c('PING') == '+QUEUED'
    assert c('EXEC') == ['-ERR SCRIPT DEBUG must be called outside a pipeline', '+PONG']
    c.send(('PING',), ('SCRIPT', 'DEBUG', 'no'), ('SCRIPT', 'DEBUG', 'yes'))
    assert [c.read(), c.read(), c.read()] == ['+PONG'] + [
        '-ERR SCRIPT DEBUG must be called outside a pipeline'] * 2


def check_peadb_only(c, workdir):
    for mode in ('yes', 'YES', 'sync', 'Sync'):
        assert c('SCRIPT', 'DEBUG', mode) == SCRIPT_DEBUG_REFUSED, mode
    # The refusal leaves no debug mode behind: the next script runs normally and keeps its effects.
    assert c('EVAL', "redis.call('set','dbg','1') return 'hello'", '0') == 'hello'
    assert c('GET', 'dbg') == '1'

    # MIGRATE to a server without cluster mode works: PeaDB sends ASKING only in cluster mode.
    proc = start(PEADB, PEADB_SECOND_PORT, os.path.join(workdir, 'target'))
    try:
        assert c('SET', 'mig', 'v') == '+OK'
        assert c('MIGRATE', '127.0.0.1', str(PEADB_SECOND_PORT), 'mig', '0', '5000') == '+OK'
        assert c('EXISTS', 'mig') == 0
        t = Conn(PEADB_SECOND_PORT)
        assert t('GET', 'mig') == 'v'
        t.close()
    finally:
        stop(proc)

    # With cluster mode on, ASKING, READONLY and READWRITE work.
    proc = start(PEADB, PEADB_SECOND_PORT, os.path.join(workdir, 'cluster'),
                 '--cluster-enabled', 'yes')
    try:
        k = Conn(PEADB_SECOND_PORT)
        for cmd in ('ASKING', 'READONLY', 'READWRITE'):
            assert k(cmd) == '+OK', cmd
        k.close()
    finally:
        stop(proc)


def main():
    work = tempfile.mkdtemp(prefix='peadb-noop-replies-')
    try:
        peadb_proc = start(PEADB, PEADB_PORT, os.path.join(work, 'peadb'))
        redis_proc = start(REDIS, REDIS_PORT, os.path.join(work, 'redis')) if REDIS else None
        try:
            peadb = Conn(PEADB_PORT)
            if redis_proc is not None:
                check_same_replies(Conn(REDIS_PORT))
            else:
                print('no pinned redis-server: checking PeaDB against the expected Redis replies only')
            check_same_replies(peadb)
            check_peadb_only(peadb, work)
        finally:
            stop(peadb_proc)
            if redis_proc is not None:
                stop(redis_proc)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print('noop replies parity tests passed')


if __name__ == '__main__':
    main()

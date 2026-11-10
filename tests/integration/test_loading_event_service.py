#!/usr/bin/env python3
"""Redis-first authenticated startup and nested RDB loading service (delta 70)."""
from pathlib import Path
import os
import signal
import socket
import subprocess
import tempfile
import time

from _readiness import wait_until_ready
from _redis_path import redis_server_path
from test_replica_offset_parity import Conn, encode
from test_replica_resync_parity import ports

ROOT = Path(__file__).resolve().parents[2]
PEADB = os.environ.get('PEADB_BIN', str(ROOT / 'peadb-server'))
LOADING = '-LOADING Redis is loading the dataset in memory'


class Conn3(Conn):
    def read(self):
        line = self.f.readline()
        if not line:
            return None
        kind, rest = line[:1], line[1:-2]
        if kind in (b'+', b'-'):
            return (kind + rest).decode()
        if kind == b':':
            return int(rest)
        if kind == b'$':
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2].decode()
        if kind in (b'*', b'>'):
            return [self.read() for _ in range(int(rest))]
        if kind == b'%':
            return {self.read(): self.read() for _ in range(int(rest))}
        if kind == b'_':
            return None
        raise AssertionError(line)


def persistence(c):
    return dict(line.split(':', 1) for line in c('INFO', 'persistence').splitlines()
                if ':' in line)


def connect_loading(port, proc):
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        assert proc.poll() is None, 'loader exited before observation'
        c = None
        keep = False
        try:
            c = Conn(port)
            c.sock.settimeout(0.8)
            refusal = c('INFO')
            assert refusal == '-NOAUTH Authentication required.', refusal
            assert c('AUTH', 'loading-secret') == '+OK'
            if persistence(c)['loading'] == '1':
                keep = True
                return c
        except OSError:
            pass
        finally:
            if c and not keep:
                c.close()
        time.sleep(0.005)
    raise AssertionError('no authenticated INFO during slow loading')


def admission(c, size, get="GET"):
    p = persistence(c)
    assert p['loading'] == '1' and p['async_loading'] == '0', p
    assert int(p['loading_start_time']) > 0, p
    assert int(p['loading_total_bytes']) == size, p
    assert 0 <= int(p['loading_loaded_bytes']) <= size, p
    assert 0 <= float(p['loading_loaded_perc']) <= 100, p
    assert c(get, 'key:0') == LOADING
    assert c('CONFIG', 'SET', 'appendonly', 'no') == LOADING
    assert c('CONFIG', 'SET', 'maxmemory', '1') == '+OK'
    assert c('CONFIG', 'SET', 'maxmemory', '0') == '+OK'
    assert c.pipeline([['MULTI'], [get, 'key:0'], ['EXEC']]) == [
        '+OK', LOADING, '-EXECABORT Transaction discarded because of previous errors.']


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    proc.wait()


def run(binary, work):
    port = ports(1)[0]
    def start(*extra):
        with (work / 'server.log').open('ab') as log:
            return subprocess.Popen([binary, '--bind', '127.0.0.1', '--port', str(port),
                '--dir', str(work), '--save', '', '--shutdown-timeout', '0',
                '--requirepass', 'loading-secret', '--enable-debug-command', 'yes',
                '--loading-process-events-interval-bytes', '1024', '--rdbcompression', 'no', '--rename-command', 'GET', 'FETCH', '--rename-command', 'DEBUG', 'INSPECT', *extra],
                stdout=log, stderr=subprocess.STDOUT)
    proc = start()
    conns = []
    try:
        wait_until_ready(port, process=proc, password='loading-secret')
        c = Conn(port); conns.append(c)
        assert c('AUTH', 'loading-secret') == '+OK'
        assert c.pipeline([['SET', f'key:{i}', f'value:{i}' + 'x' * 4096] for i in range(160)]) == ['+OK'] * 160
        assert c('SAVE') == '+OK'
        c.close(); conns.remove(c)
        stop(proc)
        before = (work / 'dump.rdb').read_bytes()
        proc = start('--key-load-delay', '20000')
        c = connect_loading(port, proc); conns.append(c)
        admission(c, len(before), get="FETCH")
        # Client destruction, output and pipeline fairness while the decoder is suspended.
        for _ in range(8):
            other = Conn(port)
            other.sock.sendall(encode('AUTH', 'loading-secret') + encode('QUIT'))
            other.close()
        assert c('CONFIG', 'SET', 'key-load-delay', '0') == '+OK'
        changed = time.monotonic()
        wait_until_ready(port, process=proc, password='loading-secret')
        assert time.monotonic() - changed < 1.5, 'live load delay did not change'
        assert c('FETCH', 'key:159') == 'value:159' + 'x' * 4096
        assert 'loading_total_bytes' not in persistence(c)
        # Nested reload owns a live dispatch, input buffer and pipelined reply.
        assert c('CONFIG', 'SET', 'key-load-delay', '20000') == '+OK'
        client_id = str(c('CLIENT', 'ID'))
        c.sock.sendall(encode('INSPECT', 'RELOAD', 'NOSAVE') + encode('SET', 'must-not-run', 'bad'))
        observer = connect_loading(port, proc); conns.append(observer)
        # Kill by the address observed before the reload reply; dispatch must stay pinned.
        clients = observer('CLIENT', 'LIST')
        active = [line for line in clients.splitlines() if line.startswith(f'id={client_id} ')]
        assert len(active) == 1, clients
        assert observer('CLIENT', 'KILL', 'ID', client_id) == 1
        assert observer('FETCH', 'key:0') == LOADING
        wait_until_ready(port, process=proc, password='loading-secret')
        assert observer('FETCH', 'key:159') == 'value:159' + 'x' * 4096
        assert observer('FETCH', 'must-not-run') is None
        c.close(); conns.remove(c)
        # Redis 7.2.5 asserts !server.in_exec if DEBUG RELOAD pumps clients inside EXEC.
        # Exercise PeaDB's execution isolation without reproducing that upstream crash.
        if binary == PEADB:
            # An outer EXEC's self-directed pub/sub frame survives nested INFO dispatch.
            c3 = Conn3(port); conns.append(c3)
            assert c3('AUTH', 'loading-secret') == '+OK'
            assert c3('HELLO', '3')['proto'] == 3
            assert c3('SUBSCRIBE', 'loading-channel') == ['subscribe', 'loading-channel', 1]
            c3.sock.sendall(b''.join(encode(*cmd) for cmd in [
                ['MULTI'], ['PUBLISH', 'loading-channel', 'retained'],
                ['INSPECT', 'RELOAD', 'NOSAVE'], ['FETCH', 'key:159'], ['EXEC']]))
            nested = connect_loading(port, proc); conns.append(nested)
            assert nested('FETCH', 'key:0') == LOADING
            wait_until_ready(port, process=proc, password='loading-secret')
            assert [c3.read() for _ in range(4)] == ['+OK', '+QUEUED', '+QUEUED', '+QUEUED']
            assert c3.read() == [1, '+OK', 'value:159' + 'x' * 4096]
            assert c3.read() == ['message', 'loading-channel', 'retained']
            nested.close(); conns.remove(nested)
            c3.close(); conns.remove(c3)
        observer.close(); conns.remove(observer)
        stop(proc)
        # A loading signal never saves a partially decoded dataset over its source.
        proc = start('--key-load-delay', '20000', '--shutdown-on-sigterm', 'save')
        c = connect_loading(port, proc); conns.append(c)
        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=1.5) == 0
        assert time.monotonic() - started < 1.5
        assert (work / 'dump.rdb').read_bytes() == before
        print(binary, 'startup/reload/admission/client lifetime/signal passed', flush=True)
    except BaseException:
        print((work / 'server.log').read_text(), flush=True)
        raise
    finally:
        for c in conns:
            c.close()
        stop(proc)


def main():
    redis = redis_server_path()
    assert redis and 'v=7.2.5 ' in subprocess.check_output([redis, '--version'], text=True)
    with tempfile.TemporaryDirectory(prefix='peadb-loading-service-') as td:
        for label, binary in [('redis', redis), ('peadb', PEADB)]:
            work = Path(td) / label; work.mkdir()
            run(binary, work)


if __name__ == '__main__':
    main()

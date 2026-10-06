#!/usr/bin/env python3
"""SHUTDOWN and SIGHUP behave like Redis 7.2.5 (compat/delta.md item 33).

The same checks run on peadb-server and on the pinned redis-server: SHUTDOWN checks its arguments (an unknown word, SAVE with NOSAVE, and ABORT with any other word are syntax errors), SHUTDOWN ABORT answers "No shutdown in progress." and the server keeps running, SIGHUP does not stop the server, a successful SHUTDOWN closes the connection with no reply, and when the final save fails SHUTDOWN answers "Errors trying to SHUTDOWN. Check logs.", logs why and keeps running, unless FORCE is given, which exits anyway. Without a pinned redis-server, only the PeaDB side runs.
"""
import os
import pathlib
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _redis_path import redis_server_path  # noqa: E402

PEADB = str(ROOT / 'peadb-server')
REDIS = redis_server_path()
PORTS = {'peadb': 6566, 'redis': 6568}
SYNTAX = '-ERR syntax error'


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
            return None  # connection closed
        kind, rest = line[:1], line[1:-2]
        if kind in (b'+', b'-'):
            return (kind + rest).decode()
        if kind == b':':
            return int(rest)
        if kind == b'$':
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2].decode()
        raise ValueError(f'unexpected reply {line!r}')

    def __call__(self, *args):
        out = f'*{len(args)}\r\n'.encode()
        for a in args:
            b = a.encode()
            out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
        self.sock.sendall(out)
        return self.read()


def start(binary, port, workdir, *extra):
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    log = open(log_path, 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                             '--loglevel', 'notice', *extra],
                            stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'{binary} exited: {open(log_path).read()}')
        try:
            socket.create_connection(('127.0.0.1', port), timeout=0.2).close()
            return proc
        except OSError:
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError(f'{binary} did not start on port {port}')


def stop(proc):
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def check_engine(name, binary, work):
    port = PORTS[name]
    workdir = os.path.join(work, name)
    # One save point, so a plain SHUTDOWN saves (as Redis does by default).
    proc = start(binary, port, workdir, '--save', '3600 1')
    try:
        c = Conn(port)
        for args in (('SHUTDOWN', 'later'), ('SHUTDOWN', 'SAVE', 'NOSAVE'),
                     ('SHUTDOWN', 'nosave', 'save'), ('SHUTDOWN', 'ABORT', 'NOW'),
                     ('SHUTDOWN', 'FORCE', 'ABORT')):
            assert c(*args) == SYNTAX, (name, args)
        assert c('SHUTDOWN', 'ABORT') == '-ERR No shutdown in progress.', name
        assert c('SHUTDOWN', 'abort') == '-ERR No shutdown in progress.', name
        assert c('SHUTDOWN', 'ABORT', 'ABORT') == '-ERR No shutdown in progress.', name
        assert c('PING') == '+PONG', name

        # SIGHUP is ignored.
        proc.send_signal(signal.SIGHUP)
        time.sleep(0.3)
        assert proc.poll() is None, f'{name}: SIGHUP stopped the server'
        assert c('PING') == '+PONG', name

        # A failing final save keeps the server running and is logged; FORCE exits anyway.
        assert c('SET', 'k', 'v') == '+OK'
        os.chmod(workdir, stat.S_IRUSR | stat.S_IXUSR)
        try:
            assert c('SHUTDOWN') == '-ERR Errors trying to SHUTDOWN. Check logs.', name
            assert c('SHUTDOWN', 'SAVE') == '-ERR Errors trying to SHUTDOWN. Check logs.', name
            assert proc.poll() is None, f'{name}: exited after a failed save'
            assert c('PING') == '+PONG', name
            log = open(os.path.join(workdir, 'server.log'), errors='replace').read()
            assert "Error trying to save the DB, can't exit." in log, (name, log[-2000:])
            # A successful SHUTDOWN closes the connection without a reply.
            assert c('SHUTDOWN', 'SAVE', 'FORCE') is None, name
            proc.wait(timeout=10)
            log = open(os.path.join(workdir, 'server.log'), errors='replace').read()
            assert 'Error trying to save the DB. Exit anyway.' in log, (name, log[-2000:])
        finally:
            os.chmod(workdir, stat.S_IRWXU)
        c.close()
    finally:
        stop(proc)

    # SHUTDOWN NOSAVE: no reply, the process exits, nothing is saved. The start counts as the
    # last save, so `save 3600 1` does not save after the first write either.
    proc = start(binary, port, workdir, '--save', '3600 1', '--dbfilename', 'nosave.rdb')
    try:
        c = Conn(port)
        info = c('INFO', 'persistence')
        last_save = int(info.split('rdb_last_save_time:')[1].split()[0])
        assert abs(last_save - time.time()) < 60, (name, last_save)
        assert c('SET', 'k', 'v') == '+OK'
        assert c('SHUTDOWN', 'NOSAVE') is None, name
        proc.wait(timeout=10)
        assert not os.path.exists(os.path.join(workdir, 'nosave.rdb')), name
        c.close()
    finally:
        stop(proc)

    # A plain SHUTDOWN with save points saves before it exits.
    proc = start(binary, port, workdir, '--save', '3600 1', '--dbfilename', 'final.rdb')
    try:
        c = Conn(port)
        assert c('SET', 'k', 'v') == '+OK'
        assert c('SHUTDOWN') is None, name
        proc.wait(timeout=10)
        assert os.path.exists(os.path.join(workdir, 'final.rdb')), name
        c.close()
    finally:
        stop(proc)
    print(f'{name}: ok')


def main():
    work = tempfile.mkdtemp(prefix='peadb-shutdown-')
    try:
        if REDIS:
            check_engine('redis', REDIS, work)
        else:
            print('no pinned redis-server: skipping the Redis side')
        check_engine('peadb', PEADB, work)
    finally:
        for root, dirs, _ in os.walk(work):
            for d in dirs:
                os.chmod(os.path.join(root, d), stat.S_IRWXU)
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()

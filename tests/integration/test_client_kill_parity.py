#!/usr/bin/env python3
"""CLIENT KILL filters like Redis 7.2.5's clientKillCommand().

The same checks run on peadb-server and on the pinned redis-server (Redis side first, only when it is Redis 7.2.x): the old form (`CLIENT KILL ip:port`, `+OK` or `No such client`, a client may kill itself); the filters ID, TYPE (normal, slave/replica, pubsub, master), ADDR, LADDR, USER and SKIPME, which must all match (PeaDB accepted TYPE, LADDR, USER and MAXAGE and ignored them, so `CLIENT KILL TYPE replica` was a syntax error and `CLIENT KILL ID <id> TYPE replica` killed a normal client); their error replies (`CLIENT KILL` alone is a wrong number of arguments: it killed every other client during this change); `CLIENT KILL SKIPME yes` with no other filter killing every other client (a syntax error on PeaDB before); `SKIPME no` closing the caller after its reply; and MAXAGE, which Redis 7.2.5 does not have.

Without a pinned Redis 7.2.x, only the PeaDB side runs.
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


def redis_7_2():
    """The redis-server to compare with, only if it is Redis 7.2.x (replies differ in other versions)."""
    path = redis_server_path()
    if not path:
        return None
    version = subprocess.run([path, '--version'], capture_output=True, text=True).stdout
    return path if ' v=7.2.' in version else None


REDIS = redis_7_2()
PORTS = {'peadb': 6672, 'redis': 6673}


def encode(*args):
    out = b'*%d\r\n' % len(args)
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode()
        out += b'$%d\r\n' % len(b) + b + b'\r\n'
    return out


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=10)
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
        if kind == b'*':
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise ValueError(f'unexpected reply {line!r}')

    def __call__(self, *args):
        self.sock.sendall(encode(*args))
        return self.read()

    def closed(self):
        """Whether the server closed this connection."""
        try:
            self.sock.sendall(encode('PING'))
            return self.read() is None
        except OSError:
            return True

    def local(self):
        """This client's address as the server sees it (the peer of the server's socket)."""
        host, port = self.sock.getsockname()[:2]
        return f'{host}:{port}'

    def server_end(self):
        host, port = self.sock.getpeername()[:2]
        return f'{host}:{port}'


class FakeReplica:
    """A replica that sends PSYNC ? -1 and skips the RDB."""

    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=10)
        self.f = self.sock.makefile('rb')
        self.sock.sendall(encode('PSYNC', '?', '-1'))
        line = self.f.readline()
        assert line.startswith(b'+FULLRESYNC'), line
        while True:
            b = self.f.read(1)
            if b == b'$':
                break
            assert b == b'\n', b
        n = int(self.f.readline()[:-2])
        self.f.read(n)

    def closed(self, timeout=5):
        self.sock.settimeout(timeout)
        try:
            while True:
                data = self.sock.recv(65536)
                if not data:
                    return True
        except socket.timeout:
            return False
        except OSError:
            return True


def start(binary, port, workdir):
    os.makedirs(workdir, exist_ok=True)
    log = open(os.path.join(workdir, 'server.log'), 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                             '--loglevel', 'notice', '--save', ''], stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'{binary} exited')
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


def client_id(c):
    return c('CLIENT', 'ID')


def check_engine(name, binary, work):
    port = PORTS[name]
    proc = start(binary, port, os.path.join(work, name))
    try:
        c = Conn(port)
        # Errors.
        assert c('CLIENT', 'KILL') == "-ERR wrong number of arguments for 'client|kill' command", name
        assert c('CLIENT', 'KILL', 'ID', 0) == '-ERR client-id should be greater than 0', name
        assert c('CLIENT', 'KILL', 'ID', 'abc') == '-ERR client-id should be greater than 0', name
        assert c('CLIENT', 'KILL', 'TYPE', 'bogus') == "-ERR Unknown client type 'bogus'", name
        assert c('CLIENT', 'KILL', 'USER', 'nobody') == "-ERR No such user 'nobody'", name
        assert c('CLIENT', 'KILL', 'SKIPME', 'maybe') == '-ERR syntax error', name
        assert c('CLIENT', 'KILL', 'MAXAGE', 10) == '-ERR syntax error', name
        assert c('CLIENT', 'KILL', 'ID') == '-ERR No such client', name  # three arguments: the old form
        assert c('CLIENT', 'KILL', 'ID', 1, 'ADDR') == '-ERR syntax error', name
        assert c('CLIENT', 'KILL', '1.2.3.4:5') == '-ERR No such client', name
        # The old form kills the client at that address.
        a = Conn(port)
        assert c('CLIENT', 'KILL', a.local()) == '+OK', name
        assert a.closed(), name
        # TYPE: a normal client, a subscriber and a replica; only the asked type goes.
        normal = Conn(port)
        sub = Conn(port)
        assert sub('SUBSCRIBE', 'ch') == ['subscribe', 'ch', 1]
        rep = FakeReplica(port)
        assert c('CLIENT', 'KILL', 'TYPE', 'master') == 0, name
        assert c('CLIENT', 'KILL', 'TYPE', 'pubsub') == 1, name
        assert sub.closed(), name
        assert c('CLIENT', 'KILL', 'TYPE', 'replica') == 1, name
        assert rep.closed(), name
        assert not normal.closed(), name
        # Filters must all match: the normal client's ID with TYPE slave matches nothing.
        assert c('CLIENT', 'KILL', 'ID', client_id(normal), 'TYPE', 'slave') == 0, name
        assert not normal.closed(), name
        assert c('CLIENT', 'KILL', 'ID', client_id(normal), 'TYPE', 'normal', 'USER', 'default') == 1, name
        assert normal.closed(), name
        # ADDR and LADDR.
        b = Conn(port)
        assert c('CLIENT', 'KILL', 'ADDR', b.local(), 'LADDR', '127.0.0.1:1') == 0, name
        assert c('CLIENT', 'KILL', 'ADDR', b.local(), 'LADDR', b.server_end()) == 1, name
        assert b.closed(), name
        # SKIPME: by default the caller is spared; with SKIPME no it is closed after the reply.
        d = Conn(port)
        e = Conn(port)
        assert c('CLIENT', 'KILL', 'TYPE', 'normal') == 2, name
        assert d.closed() and e.closed(), name
        assert not c.closed(), name
        f = Conn(port)
        assert c('CLIENT', 'KILL', 'TYPE', 'normal', 'SKIPME', 'no') == 2, name
        assert c.read() is None, name  # closed after the reply
        assert f.closed(), name
        g = Conn(port)
        h1, h2 = Conn(port), Conn(port)
        assert g('CLIENT', 'KILL', 'SKIPME', 'yes') == 2, name  # no filter: every other client
        assert h1.closed() and h2.closed() and not g.closed(), name
        assert g('CLIENT', 'KILL', g.local()) == '+OK', name  # the old form may kill the caller
        assert g.read() is None, name
    finally:
        stop(proc)


def main():
    work = tempfile.mkdtemp(prefix='peadb-client-kill-')
    try:
        if REDIS:
            check_engine('redis', REDIS, work)
        check_engine('peadb', PEADB, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print('ok')


if __name__ == '__main__':
    main()

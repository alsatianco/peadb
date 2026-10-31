"""Owned protocol fixtures for length/EOF full-sync acceptance (Redis 7.2.5)."""
import contextlib
import os
from pathlib import Path
import socket
import subprocess
import tempfile

from _readiness import wait_until_ready
from test_rdb_redis_interop import Conn, LIB
from test_replica_offset_parity import (PEADB, REDIS, check, encode, read_command,
                                       stop, wait_for)
from test_replica_resync_parity import ports

PEADB = os.environ.get('PEADB_TEST_BIN', PEADB)
MARK = b'0123456789abcdef' * 2 + b'01234567'
REPLID = 'c3' * 20
# Near matches and RDB EOF bytes must be treated as ordinary payload. No compression.
VALUE = bytes(range(256)) * 700 + MARK[:-1] + b'!\xff\x00\r\n'


@contextlib.contextmanager
def server(binary, env=None):
    port = ports(1)[0]
    with tempfile.TemporaryDirectory(prefix='peadb-snapshot-wire-') as td:
        with open(Path(td) / 'server.log', 'wb') as log:
            proc = subprocess.Popen(
                [binary, '--bind', '127.0.0.1', '--port', str(port), '--dir', td,
                 '--save', '', '--shutdown-timeout', '0', '--repl-diskless-sync-delay', '0',
                 '--repl-diskless-load', 'disabled', '--rdbcompression', 'no'],
                stdout=log, stderr=subprocess.STDOUT, env=env)
        client = None
        try:
            wait_until_ready(port, process=proc)
            client = Conn(port)
            yield client, port, Path(td), proc
        except BaseException:
            print((Path(td) / 'server.log').read_text(errors='replace'), flush=True)
            raise
        finally:
            if client:
                client.close()
            stop(proc)
            proc.wait(timeout=10)


def reference():
    check(REDIS is not None, 'pinned Redis 7.2.5 required')
    check(' v=7.2.5 ' in subprocess.check_output([REDIS, '--version'], text=True),
          'exact reference version')
    return REDIS


def info(c, field, section="replication"):
    return dict(line.split(":", 1) for line in c("INFO", section).decode().splitlines()
                if ":" in line and not line.startswith("#")).get(field)


def wait_link(c):
    wait_for(lambda: info(c, "master_link_status") == "up", "replica link")


def seed(c):
    check(c('SET', 'binary', VALUE) == '+OK', 'binary seed')
    check(c('FUNCTION', 'LOAD', LIB) == b'interoplib', 'function seed')
    check(c('SELECT', 5) == '+OK' and c('SET', 'db5', 'snapshot') == '+OK', 'DB5 seed')
    check(c('PEXPIRE', 'db5', 60000) == 1, 'TTL seed')
    deadline = c('PEXPIRETIME', 'db5')
    c('SELECT', 0)
    return deadline


def verify(c, deadline):
    check(c('GET', 'binary') == VALUE, 'binary snapshot including near marker/EOF bytes')
    check(c('FCALL_RO', 'interop_f', 0) == b'from-lib', 'snapshot function')
    c('SELECT', 5)
    check(c('GET', 'db5') == b'snapshot' and c('PEXPIRETIME', 'db5') == deadline,
          'DB5 value and exact absolute expiry')
    c('SELECT', 0)


def snapshot():
    with server(reference()) as (c, _, td, _):
        deadline = seed(c)
        check(c('SAVE') == '+OK', 'reference snapshot')
        return (td / 'dump.rdb').read_bytes(), deadline


class Master:
    """Synchronous owned listener: the replica process drives the other side."""
    def __init__(self):
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.port = self.listener.getsockname()[1]
        self.listener.listen()
        self.listener.settimeout(8)
        self.peers = []
        self.commands = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        for sock, stream in self.peers:
            stream.close()
            sock.close()
        self.listener.close()

    def accept(self):
        sock, _ = self.listener.accept()
        sock.settimeout(8)
        stream = sock.makefile('rb')
        self.peers.append((sock, stream))
        while True:
            args = read_command(stream)
            check(args is not None, 'handshake socket alive')
            self.commands.append(args)
            if args[0].upper() == 'PSYNC':
                return sock, stream, args
            sock.sendall(b'+PONG\r\n' if args[0].upper() == 'PING' else b'+OK\r\n')

    def header(self, sock, header):
        sock.sendall(b'\n\r\n+FULLRESYNC ' + REPLID.encode() + b' 0\r\n\n\r\n' +
                     header + b'\r\n')

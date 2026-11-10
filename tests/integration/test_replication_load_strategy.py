#!/usr/bin/env python3
"""Compare the internal direct-load selector with actual pinned Redis loading modes.

PeaDB's production non-disabled strategies remain unsupported until transactional publication.
The helper exercises the real internal selector; Redis is observed during a slow full sync.
"""
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading

from _readiness import wait_until_ready
from _redis_path import redis_server_path
from test_replica_offset_parity import Conn, info, stop, wait_for
from test_replica_resync_parity import ports

ROOT = Path(__file__).resolve().parents[2]


class Master:
    def __init__(self, replid):
        self.replid = replid
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(1)
        self.listener.settimeout(8)
        self.port = self.listener.getsockname()[1]
        self.sock = None
        self.errors = []
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def run(self):
        try:
            self.sock, _ = self.listener.accept()
            self.sock.settimeout(8)
            with self.sock.makefile('rb') as source:
                while not self.stopped.is_set():
                    header = source.readline()
                    if not header:
                        return
                    if header in (b'\n', b'\r\n'):
                        continue  # Redis replicationSendNewlineToMaster while loading.
                    args = []
                    for _ in range(int(header[1:-2])):
                        size = int(source.readline()[1:-2])
                        args.append(source.read(size + 2)[:-2])
                    if args[0].upper() == b'PSYNC':
                        # Zero checksum is Redis's explicit no-checksum format. Key delay
                        # exposes both direct and file parsing without stalling the socket.
                        body = b'REDIS0011' + b''.join(
                            b'\x00' + bytes([len(key)]) + key + b'\x03new'
                            for key in (f'key:{i}'.encode() for i in range(160)))
                        body += b'\xff' + b'\x00' * 8
                        self.sock.sendall(b'+FULLRESYNC ' + self.replid.encode() +
                                          b' 0\r\n$' + str(len(body)).encode() + b'\r\n' + body)
                    elif args[0].upper() == b'PING':
                        self.sock.sendall(b'+PONG\r\n')
                    elif args[0].upper() != b'REPLCONF' or args[1].upper() != b'ACK':
                        self.sock.sendall(b'+OK\r\n')
        except (OSError, ValueError) as error:
            if not self.stopped.is_set():
                self.errors.append(error)

    def close(self):
        self.stopped.set()
        self.listener.close()
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()
        self.thread.join(timeout=10)
        assert not self.thread.is_alive(), 'owned fake master did not exit'
        assert not self.errors, self.errors


def run(redis, helper, mode, occupied, matching, work):
    port = ports(1)[0]
    with (work / 'server.log').open('wb') as log:
        proc = subprocess.Popen([redis, '--bind', '127.0.0.1', '--port', str(port),
            '--dir', str(work), '--save', '', '--shutdown-timeout', '0',
            '--enable-debug-command', 'yes', '--repl-diskless-load', mode,
            '--loading-process-events-interval-bytes', '1024'], stdout=log, stderr=subprocess.STDOUT)
    master = None
    c = None
    try:
        wait_until_ready(port, process=proc)
        c = Conn(port)
        if occupied:
            assert c('SELECT', 15) == '+OK'
            assert c('SET', 'old', 'old-value') == '+OK'
            assert c('SELECT', 0) == '+OK'
        current_id = info(c, 'master_replid', 'replication')
        master = Master(current_id if matching else 'f' * 40)
        assert c('CONFIG', 'SET', 'key-load-delay', '10000') == '+OK'
        assert c('REPLICAOF', '127.0.0.1', master.port) == '+OK'
        observed = None

        def loading():
            nonlocal observed
            # Sample old data in the same pump opportunity as INFO. Separate round
            # trips can cross publication between INFO, SELECT and GET.
            state, _, old, _ = c.pipeline([
                ['INFO', 'persistence'], ['SELECT', 15], ['GET', 'old'], ['SELECT', 0]])
            if 'async_loading:1\r\n' in state:
                observed = 'async'
                if occupied:
                    assert old == 'old-value', old
                return True
            if 'loading:1\r\n' in state:
                observed = 'disk' if (work / 'dump.rdb').exists() else 'blocking'
                return True
            return False

        wait_for(loading, 'observable Redis loading strategy', timeout=8)
        wait_until_ready(port, process=proc, timeout=8)
        wait_for(lambda: c('GET', 'key:159') == 'new', 'complete snapshot')
        assert (work / 'dump.rdb').exists() == (observed == 'disk')
        selected = subprocess.check_output([str(helper), '--strategy', mode,
            'occupied' if occupied else 'empty', 'matching' if matching else 'different'],
            text=True, timeout=10).strip()
        assert selected == observed, (mode, occupied, matching, selected, observed)
        print(mode, 'occupied=' + str(occupied), 'matching=' + str(matching), observed,
              'matches pinned Redis', flush=True)
    finally:
        if sys.exc_info()[0] is not None:
            print((work / 'server.log').read_text(errors='replace'), file=sys.stderr)
        if c:
            c.close()
        try:
            if master:
                master.close()
        finally:
            stop(proc)


def main():
    redis = redis_server_path()
    assert redis and 'v=7.2.5 ' in subprocess.check_output([redis, '--version'], text=True)
    candidates = [Path.cwd() / 'replication_load_test', ROOT / 'build' / 'replication_load_test']
    helper = Path(os.environ.get('PEADB_LOAD_HELPER', next(
        (str(p) for p in candidates if p.is_file()), '')))
    assert helper.is_file(), 'build replication_load_test first'
    with tempfile.TemporaryDirectory(prefix='peadb-load-strategy-') as td:
        for index, (mode, occupied, matching) in enumerate([
            ('disabled', False, True), ('disabled', True, False),
            ('on-empty-db', False, False), ('on-empty-db', True, True),
            ('swapdb', True, False), ('swapdb', True, True)]):
            work = Path(td) / str(index)
            work.mkdir()
            run(redis, helper.resolve(), mode, occupied, matching, work)


if __name__ == '__main__':
    main()

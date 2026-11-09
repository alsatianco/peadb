#!/usr/bin/env python3
"""Redis-first socket-target framing, progressive output, ACK gate and disk controls."""
import os
from pathlib import Path
import select
import socket
import subprocess
import time

from _replication_snapshot import PEADB, check, encode, info, reference, server, seed, verify, wait_for
from test_replica_offset_parity import read_command
from test_rdb_child_parent_loss import children


class Peer:
    def __init__(self, port, eof=True, legacy=False, request=True):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=8)
        self.stream = self.sock.makefile('rb')
        if eof:
            self.sock.sendall(encode('REPLCONF', 'capa', 'eof', 'capa', 'psync2'))
            check(self.line() == b'+OK\r\n', 'capability handshake')
        if request:
            self.request(legacy)
            self.read_header(legacy)

    def request(self, legacy=False):
        self.sock.sendall(encode('SYNC') if legacy else encode('PSYNC', '?', -1))

    def read_header(self, legacy=False):
        self.offset = 0
        if not legacy:
            full = self.line()
            check(full.startswith(b'+FULLRESYNC '), f'full sync header: {full!r}')
            self.offset = int(full.split()[2])
        self.header = self.line()

    def line(self):
        deadline = time.monotonic() + 8
        line = self.stream.readline()
        while line in (b'\n', b'\r\n'):
            check(time.monotonic() < deadline, 'bounded header keepalive wait')
            line = self.stream.readline()
        return line

    def snapshot(self):
        if self.header.startswith(b'$EOF:'):
            mark = self.header[5:-2]
            check(len(mark) == 40, '40-byte marker')
            data = bytearray()
            deadline = time.monotonic() + 10
            # read1 returns currently available data; no blocking request beyond EOF.
            while not data.endswith(mark):
                check(time.monotonic() < deadline, 'bounded snapshot wait')
                part = self.stream.read1(65536)
                check(part, 'complete EOF snapshot')
                data.extend(part)
            return bytes(data[:-40])
        check(self.header.startswith(b'$'), f'bulk header {self.header!r}')
        return self.stream.read(int(self.header[1:-2]))

    def close(self):
        self.stream.close()
        self.sock.close()


def run(binary):
    with server(binary) as (c, port, work, proc):
        deadline = seed(c)
        # A large uncompressed value forces multiple shared chunks and real pipe
        # backpressure. Delayed small keys make progression observable independently.
        check(c('SET', 'large', os.urandom(8 * 1024 * 1024)) == '+OK', 'large seed')
        for i in range(256):
            c('SET', f'delayed:{i}', 'd' * 512)
        c('CONFIG', 'SET', 'rdb-key-save-delay', 2000)
        c('CONFIG', 'SET', 'repl-ping-replica-period', 3600)
        c('CONFIG', 'SET', 'repl-backlog-size', 16384)
        before = {field: info(c, field, 'persistence') for field in
                  ('rdb_changes_since_last_save', 'rdb_last_save_time', 'rdb_last_bgsave_status')}
        peer = Peer(port)
        try:
            check(peer.header.startswith(b'$EOF:'), 'master must use EOF pipe for capable peer')
            check(children(proc.pid), 'pipe output must start while child is still alive')
            check(not (work / 'dump.rdb').exists() and not list(work.glob('temp-*.rdb')),
                  'socket target must not create an RDB file')
            peer.sock.sendall(encode('REPLCONF', 'ACK', peer.offset))
            check(c('PING') == '+PONG', 'ordinary client responsiveness')
            # More than the ring size, held separately until ACK, across databases.
            c('SELECT', 5)
            suffix = 'post-fork' * 40000
            check(c('SET', 'after', suffix) == '+OK', 'post-fork write')
            c('SELECT', 0)
            payload = peer.snapshot()
            check(payload.startswith(b'REDIS') and payload[-9] == 255, 'complete RDB')
            saved = work / 'received.rdb'
            saved.write_bytes(payload)
            checker = Path(reference()).with_name('redis-check-rdb')
            result = subprocess.run([str(checker), str(saved)], capture_output=True)
            check(result.returncode == 0, f'cross-engine RDB validation: {result.stdout!r}')
            wait_for(lambda: not children(proc.pid), 'completion pipe child reaping')
            check(not select.select([peer.sock], [], [], 0.15)[0], 'no commands before post-load ACK')
            check(c('WAIT', 1, 40) == 0, 'snapshot receipt is not applied write acknowledgement')
            peer.sock.sendall(encode('REPLCONF', 'ACK', peer.offset))
            commands = []
            while True:
                args = read_command(peer.stream)
                check(args is not None, 'post-ACK command stream alive')
                commands.append(args)
                if args[0].upper() == 'SET' and args[1] == 'after':
                    check(args[2] == suffix, 'exact post-fork payload through wrapped backlog')
                    break
            check(any(args == ['SELECT', '5'] for args in commands), 'post-fork stream DB')
            check(sum(args[:2] == ['SET', 'after'] for args in commands) == 1, 'suffix exactly once')
            after = {field: info(c, field, 'persistence') for field in before}
            check(after['rdb_last_save_time'] == before['rdb_last_save_time'], 'disk LASTSAVE unchanged')
            check(after['rdb_last_bgsave_status'] == before['rdb_last_bgsave_status'], 'disk status unchanged')
            check(int(after['rdb_changes_since_last_save']) >= int(before['rdb_changes_since_last_save']),
                  'socket transfer must not clear dirty baseline')
            check(not (work / 'dump.rdb').exists(), 'no late disk publication')
        finally:
            peer.close()
        c('CONFIG', 'SET', 'rdb-key-save-delay', 0)
        # Capability fallback and plain SYNC still use a length-framed disk child.
        for legacy in (False, True):
            peer = Peer(port, eof=False, legacy=legacy)
            try:
                check(not peer.header.startswith(b'$EOF:'), 'non-EOF peer length fallback')
                check(peer.snapshot().startswith(b'REDIS'), 'legacy snapshot')
                check((work / 'dump.rdb').exists(), 'disk fallback persists snapshot')
            finally:
                peer.close()
        print(Path(binary).name, 'progressive pipe/CRC/ACK/wrap/disk fallback passed', flush=True)


if __name__ == '__main__':
    run(reference())
    run(PEADB)

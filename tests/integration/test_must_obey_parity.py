#!/usr/bin/env python3
"""Master/AOF commands bypass routing; replication failures are visible (delta 43).

Valid master streams run on Redis 7.2.5 first, including WRONGTYPE inside EXEC
and the replica's disk-error policy. PeaDB's static cluster topology is tested
separately with a real Redis master: normal clients still receive MOVED/ASK.
"""
import os
import pathlib
import socket
import tempfile
import threading

from test_replica_gates_parity import (
    Conn, PEADB, REDIS, encode, info_section, start, stop, wait_for,
)
from test_replica_offset_parity import read_command

PORT = 6735


class Master:
    """A local master that supplies a real empty RDB and valid RESP commands."""

    def __init__(self, rdb):
        self.rdb = rdb
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(('127.0.0.1', PORT))
        self.listener.listen(1)
        self.conn = None
        self.errors = []
        self.synced = False
        self.thread = threading.Thread(target=self.serve)
        self.thread.start()

    def serve(self):
        try:
            self.listener.settimeout(10)
            self.conn, _ = self.listener.accept()
            with self.conn.makefile('rb') as f:
                while True:
                    args = read_command(f)
                    if not args:
                        break
                    if args[0].upper() == 'PSYNC':
                        self.conn.sendall(
                            f'+FULLRESYNC {"a" * 40} 0\r\n${len(self.rdb)}\r\n'.encode() + self.rdb)
                        self.synced = True
                        # Read and discard ACKs, keeping the connection open.
                        while f.read(1):
                            pass
                        break
                    self.conn.sendall(b'+PONG\r\n' if args[0].upper() == 'PING' else b'+OK\r\n')
        except OSError as e:
            if not self.synced and self.listener.fileno() != -1:
                self.errors.append(e)

    def send(self, *commands):
        self.conn.sendall(b''.join(encode(*args) for args in commands))

    def close(self):
        for s in (self.listener, self.conn):
            if s is not None:
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                s.close()
        self.thread.join(10)
        assert not self.thread.is_alive(), 'master thread did not stop'
        assert not self.errors, self.errors


def check_errors(binary, work, rdb):
    proc = start(binary, PORT + 1, work)
    master = Master(rdb)
    c = Conn(PORT + 1)
    try:
        assert c('REPLICAOF', '127.0.0.1', PORT) == '+OK'
        wait_for(lambda: info_section(c, 'replication')['master_link_status'] == 'up', 'sync')
        master.send(('SET', 'conflict', 'string'), ('LPUSH', 'conflict', 'x'),
                    ('MULTI',), ('LPUSH', 'conflict', 'y'), ('SET', 'in-exec', 'ok'), ('EXEC',),
                    ('UNKNOWN_STREAM_COMMAND',), ('SET', 'after-errors', 'ok'))
        wait_for(lambda: c('GET', 'after-errors') == 'ok', 'stream after errors')
        assert c('GET', 'in-exec') == 'ok'
        assert c('GET', 'conflict') == 'string'
        assert info_section(c, 'stats')['unexpected_error_replies'] == '3'
        logs = pathlib.Path(work, 'server.log').read_text()
        assert logs.count('== CRITICAL == This replica is sending an error to its master:') == 3
        assert logs.count("after processing the command 'lpush'") == 1, logs
        assert logs.count("after processing the command 'exec'") == 1, logs
        assert "after processing the command '<unknown>'" in logs
        assert 'WRONGTYPE Operation against a key holding the wrong kind of value' in logs
        assert info_section(c, 'replication')['master_link_status'] == 'up'
        assert c('CONFIG', 'RESETSTAT') == '+OK'
        assert info_section(c, 'stats')['unexpected_error_replies'] == '0'
        master.send(('LPUSH', 'conflict', 'again'), ('SET', 'after-reset', 'ok'))
        wait_for(lambda: c('GET', 'after-reset') == 'ok', 'stream after stats reset')
        assert info_section(c, 'stats')['unexpected_error_replies'] == '1'
    finally:
        c.close()
        master.close()
        stop(proc)


def check_disk_policy(binary, work, rdb, ignore, runtime=False):
    extra = ('--save', '3600 1', '--dbfilename', 'blocked.rdb')
    if ignore and not runtime:
        extra += ('--replica-ignore-disk-write-errors', 'yes')
    proc = start(binary, PORT + 1, work, extra)
    master = Master(rdb)
    c = Conn(PORT + 1)
    try:
        if runtime:
            assert c('CONFIG', 'SET', 'replica-ignore-disk-write-errors', 'yes') == '+OK'
        want = 'yes' if ignore else 'no'
        assert c('CONFIG', 'GET', 'replica-ignore-disk-write-errors') == [
            'replica-ignore-disk-write-errors', want]
        assert c('REPLICAOF', '127.0.0.1', PORT) == '+OK'
        wait_for(lambda: info_section(c, 'replication')['master_link_status'] == 'up', 'sync')
        blocked = pathlib.Path(work, 'blocked.rdb')
        if blocked.exists():
            blocked.unlink()  # Redis keeps its full-sync RDB here
        blocked.mkdir()  # rename() of the BGSAVE must fail
        assert c('BGSAVE') == '+Background saving started'
        wait_for(lambda: info_section(c, 'persistence')['rdb_last_bgsave_status'] == 'err',
                 'failed BGSAVE')
        warning = 'Replica is applying a command even though it is unable to write to disk.'
        log_path = pathlib.Path(work, 'server.log')
        # processCommand() validates arity before considering disk errors. An invalid SET
        # is logged as an error and must not stop the replica or produce a disk warning.
        master.send(('SET', 'missing-value'))
        wait_for(lambda: info_section(c, 'stats')['unexpected_error_replies'] == '1',
                 'invalid master SET to be logged')
        assert proc.poll() is None
        assert warning not in log_path.read_text()
        master.send(('PING',), ('PING',))
        wait_for(lambda: warning in log_path.read_text(), 'disk-error PING warning')
        assert proc.poll() is None  # master's PING never stops the replica
        if ignore:
            master.send(('MULTI',), ('SET', 'x', '1'), ('SET', 'y', '2'), ('EXEC',),
                        ('SET', 'z', '3'))
            wait_for(lambda: c('MGET', 'x', 'y', 'z') == ['1', '2', '3'], 'disk-error writes')
            assert log_path.read_text().count(warning) == 1  # 10-second rate limit
            assert c('CONFIG', 'SET', 'replica-ignore-disk-write-errors', 'no') == '+OK'
        if runtime:
            master.send(('MULTI',), ('SET', 'must-stop', '1'))  # stop at queue time, before EXEC
        else:
            master.send(('SET', 'must-stop', '1'))
        wait_for(lambda: proc.poll() is not None, 'replica to stop on disk error')
        assert proc.returncode != 0
        assert 'Replica was unable to write command to disk.' in log_path.read_text()
    finally:
        c.close()
        master.close()
        stop(proc)


def setup_routes(c, peer_port):
    assert c('CLUSTER', 'ADDSLOTSRANGE', 0, 16383) == '+OK'
    assert c('CLUSTER', 'MEET', '127.0.0.1', peer_port) == '+OK'
    peer = f'127.0.0.1:{peer_port}'
    moved = c('CLUSTER', 'KEYSLOT', '{moved}:x')
    asked = c('CLUSTER', 'KEYSLOT', '{asked}:x')
    assert c('CLUSTER', 'SETSLOT', moved, 'NODE', peer) == '+OK'
    assert c('CLUSTER', 'SETSLOT', asked, 'MIGRATING', peer) == '+OK'
    assert c('GET', '{moved}:x') == f'-MOVED {moved} {peer}'
    assert c('GET', '{asked}:x') == f'-ASK {asked} {peer}'
    return moved, asked


def check_routing(work, master_binary):
    procs, conns = [], []
    cluster = ('--cluster-enabled', 'yes')
    try:
        for offset, extra in ((2, ('--enable-debug-command', 'yes')), (3, cluster), (4, cluster)):
            binary = master_binary if offset == 2 else PEADB
            procs.append(start(binary, PORT + offset, os.path.join(work, str(offset)), extra))
            conns.append(Conn(PORT + offset))
        m, r, peer = conns
        moved, asked = setup_routes(r, PORT + 4)
        assert r('REPLICAOF', '127.0.0.1', PORT + 2) == '+OK'
        wait_for(lambda: info_section(r, 'replication')['master_link_status'] == 'up', 'sync')
        assert r('CONFIG', 'SET', 'maxmemory', '1', 'min-replicas-to-write', '1',
                 'replica-serve-stale-data', 'no') == '+OK'
        assert m('SET', '{moved}:x', '1') == '+OK'
        assert m('MULTI') == '+OK'
        assert m('INCR', '{moved}:x') == '+QUEUED'
        assert m('SET', '{asked}:x', '2') == '+QUEUED'
        assert m('SET', 'ordinary', '3') == '+QUEUED'
        assert m('EXEC') == [2, '+OK', '+OK']
        assert m('WAIT', 1, 5000) == 1
        # Read through local routes: PeaDB's DEBUG DIGEST algorithm differs from Redis's.
        assert r('CLUSTER', 'SETSLOT', moved, 'NODE', r('CLUSTER', 'MYID')) == '+OK'
        assert r('CLUSTER', 'SETSLOT', asked, 'NODE', r('CLUSTER', 'MYID')) == '+OK'
        assert r('MGET', '{moved}:x', '{asked}:x', 'ordinary') == ['2', '2', '3'], (
            'replica acknowledged lost writes')
        peer_addr = f'127.0.0.1:{PORT + 4}'
        assert r('CLUSTER', 'SETSLOT', moved, 'NODE', peer_addr) == '+OK'
        assert r('CLUSTER', 'SETSLOT', asked, 'MIGRATING', peer_addr) == '+OK'
        # AOF replay with the same live MOVED/ASK routes must bypass them too.
        aof_path = pathlib.Path(work, '3', 'appendonly.aof')
        aof_path.write_bytes(b''.join(encode(*a) for a in [
            ('SET', '{moved}:x', '7'), ('MULTI',), ('SET', '{asked}:x', '8'),
            ('INCR', '{moved}:x'), ('EXEC',), ('SET', 'ordinary', '9')]))
        assert r('DEBUG', 'LOADAOF') == '+OK'
        assert r('REPLICAOF', 'NO', 'ONE') == '+OK'
        assert r('CONFIG', 'SET', 'maxmemory', '0', 'min-replicas-to-write', '0') == '+OK'
        assert r('GET', '{moved}:x').startswith('-MOVED ')
        assert r('GET', '{asked}:x').startswith('-ASK ')
        assert r('CLUSTER', 'SETSLOT', moved, 'NODE', r('CLUSTER', 'MYID')) == '+OK'
        assert r('CLUSTER', 'SETSLOT', asked, 'NODE', r('CLUSTER', 'MYID')) == '+OK'
        assert r('MGET', '{moved}:x', '{asked}:x', 'ordinary') == ['8', '8', '9']
    finally:
        for c in conns:
            c.close()
        for p in reversed(procs):
            stop(p)


def main():
    assert REDIS, 'Redis 7.2.x is required for the master-stream parity check'
    with tempfile.TemporaryDirectory(prefix='peadb-must-obey-') as work:
        seed_dir = os.path.join(work, 'seed')
        seed = start(REDIS, PORT + 2, seed_dir)
        c = Conn(PORT + 2)
        try:
            assert c('SAVE') == '+OK'
            rdb = pathlib.Path(seed_dir, 'dump.rdb').read_bytes()
        finally:
            c.close()
            stop(seed)
        for name, binary in [('redis', REDIS), ('peadb', PEADB)]:
            check_errors(binary, os.path.join(work, name + '-errors'), rdb)
            for ignore, runtime in [(False, False), (True, False), (True, True)]:
                check_disk_policy(binary, os.path.join(work, f'{name}-disk-{ignore}-{runtime}'),
                                  rdb, ignore, runtime)
            print(name + ': master stream errors and disk policy passed')
        check_routing(os.path.join(work, 'routing'), REDIS)
        print('peadb: master/AOF routing exemptions passed against a Redis master')


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""A reconnect discards partial RESP/MULTI and paused input, using only applied bytes."""
import queue
import socket
import tempfile
import threading

from test_replica_offset_parity import (Conn, PEADB, REDIS, check, encode, info, read_command,
                                       read_rdb, start, stop, wait_for, wait_link)
from test_replica_resync_parity import ports

REPLID = 'a1' * 20
NEXT_ID = 'b2' * 20


class ControlledMaster:
    """Ordinary replication protocol fixture, with explicit disconnections between commands."""
    def __init__(self, stall_ping=False):
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.port = self.listener.getsockname()[1]
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.ready = queue.Queue()
        self.stopped = threading.Event()
        self.peers = []
        self.errors = []
        self.stall_ping = stall_ping
        self.thread = threading.Thread(target=self.serve)
        self.thread.start()

    def serve(self):
        try:
            while not self.stopped.is_set():
                try:
                    sock, _ = self.listener.accept()
                except socket.timeout:
                    continue
                self.peers.append(sock)
                sock.settimeout(8)
                f = sock.makefile('rb')
                try:
                    while not self.stopped.is_set():
                        args = read_command(f)
                        if not args:
                            break
                        if self.stall_ping or args[0].upper() == 'PSYNC':
                            self.ready.put((sock, args))
                            break
                        sock.sendall(b'+PONG\r\n' if args[0].upper() == 'PING' else b'+OK\r\n')
                finally:
                    f.close()
        except OSError as e:
            if not self.stopped.is_set():
                self.errors.append(e)

    def accept(self):
        return self.ready.get(timeout=8)

    def close(self):
        self.stopped.set()
        for sock in self.peers:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        self.thread.join(10)
        self.listener.close()
        check(not self.thread.is_alive() and not self.errors, f'fixture failed: {self.errors}')


def run(binary, rdb, invalid_rdb=None):
    p, child_port = ports(2)
    fake = ControlledMaster()
    proc, r, child_proc, child = None, None, None, None
    with tempfile.TemporaryDirectory(prefix='peadb-reconnect-stream-') as td:
        try:
            proc = start(binary, p, td)
            r = Conn(p)
            r('REPLICAOF', '127.0.0.1', fake.port)
            link, _ = fake.accept()
            base = encode('SELECT', 5) + encode('SET', 'counter', 0)
            link.sendall(f'+FULLRESYNC {REPLID} 0\r\n${len(rdb)}\r\n'.encode() + rdb + base)
            wait_link(r); r('SELECT', 5)
            wait_for(lambda: r('GET', 'counter') == '0', 'initial stream')
            applied = len(base)
            child_proc = start(binary, child_port, td + '/child')
            child = Conn(child_port)
            r('CONFIG', 'SET', 'repl-diskless-sync', 'no')
            child('REPLICAOF', '127.0.0.1', p); wait_link(child)
            child('SELECT', 5)
            full = int(info(r, 'sync_full', 'stats'))

            # The master's transaction has completed, but the receiver loses the link before
            # EXEC. Its applied offset excludes the whole unit and it must replay it once.
            transaction = encode('MULTI') + encode('INCR', 'counter') + encode('EXEC')
            link.sendall(transaction[:-len(encode('EXEC'))])
            wait_for(lambda: int(info(r, 'slave_read_repl_offset', 'replication')) > applied,
                     'unfinished transaction read')
            link.shutdown(socket.SHUT_RDWR); link.close()
            link, args = fake.accept()
            check(args == ['PSYNC', REPLID, str(applied + 1)], f'cached MULTI offset: {args}')
            link.sendall(f'+CONTINUE {NEXT_ID}\r\n'.encode() + transaction)
            wait_link(r)
            wait_for(lambda: r('GET', 'counter') == '1', 'transaction replay exactly once')
            check(info(r, 'master_replid2', 'replication') == REPLID, 'CONTINUE old history')
            check(int(info(r, 'second_repl_offset', 'replication')) == applied + 1,
                  'CONTINUE old history boundary')
            wait_for(lambda: info(child, 'master_replid', 'replication') == NEXT_ID,
                     'changed CONTINUE reconnects downstream replica')
            wait_for(lambda: child('GET', 'counter') == '1', 'downstream transaction')
            check(int(info(r, 'sync_full', 'stats')) == full, 'downstream resumes partially')
            applied += len(transaction)

            command = encode('SET', 'partial', 'complete')
            link.sendall(command[:len(command) // 2])
            wait_for(lambda: int(info(r, 'slave_read_repl_offset', 'replication')) > applied,
                     'partial command read')
            link.shutdown(socket.SHUT_RDWR); link.close()
            link, args = fake.accept()
            check(args == ['PSYNC', NEXT_ID, str(applied + 1)], f'cached partial offset: {args}')
            link.sendall(b'+CONTINUE\r\n' + command)
            wait_link(r)
            wait_for(lambda: r('GET', 'partial') == 'complete', 'bare CONTINUE / partial replay')
            applied += len(command)

            # A full transfer interrupted before replacement keeps the cached database and
            # history. A later CONTINUE resumes that history without losing DB 5 selection.
            link.shutdown(socket.SHUT_RDWR); link.close()
            link, args = fake.accept()
            check(args == ['PSYNC', NEXT_ID, str(applied + 1)], 'cache before interrupted RDB')
            link.sendall(f'+FULLRESYNC {REPLID} 0\r\n${len(rdb)}\r\n'.encode() +
                         rdb[:len(rdb) // 2])
            wait_for(lambda: info(r, 'master_sync_in_progress', 'replication') == '1',
                     'RDB transfer started')
            link.shutdown(socket.SHUT_RDWR); link.close()
            link, args = fake.accept()
            check(args == ['PSYNC', NEXT_ID, str(applied + 1)], 'cache survives interrupted RDB')
            command = encode('INCR', 'counter')
            link.sendall(b'+CONTINUE\r\n' + command)
            wait_link(r)
            wait_for(lambda: r('GET', 'counter') == '2', 'cached database after RDB interruption')
            applied += len(command)

            r('CLIENT', 'PAUSE', '5000', 'WRITE')
            command = encode('INCR', 'counter')
            link.sendall(command)
            wait_for(lambda: int(info(r, 'slave_read_repl_offset', 'replication')) > applied,
                     'paused command read')
            check(int(info(r, 'slave_repl_offset', 'replication')) == applied, 'not applied')
            link.shutdown(socket.SHUT_RDWR); link.close()
            link, args = fake.accept()
            check(args == ['PSYNC', '?', '-1'], f'blocked master is not cached: {args}')
            replacement = (encode('SELECT', 5) + encode('SET', 'counter', 3) +
                           encode('SET', 'partial', 'complete'))
            link.sendall(f'+FULLRESYNC {NEXT_ID} 0\r\n${len(rdb)}\r\n'.encode() +
                         rdb + replacement)
            wait_link(r)
            r('CLIENT', 'UNPAUSE')
            wait_for(lambda: r('GET', 'counter') == '3', 'paused replay exactly once')
            check(int(info(r, 'slave_repl_offset', 'replication')) == len(replacement),
                  'applied stream offset')
            if invalid_rdb is not None:
                # Structural validation refuses the snapshot before replacement: the old
                # keys, selected stream DB and cached history must remain usable.
                wait_for(lambda: child('GET', 'counter') == '3', 'downstream before rejection')
                retained_full = int(info(r, 'sync_full', 'stats'))
                link.shutdown(socket.SHUT_RDWR); link.close()
                link, args = fake.accept()
                check(args[1] == NEXT_ID, 'cached master before rejected snapshot')
                link.sendall(f'+FULLRESYNC {REPLID} 0\r\n${len(invalid_rdb)}\r\n'.encode() +
                             invalid_rdb)
                link, args = fake.accept()
                check(args == ['PSYNC', NEXT_ID, str(len(replacement) + 1)],
                      f'preflight failure keeps applied history: {args}')
                check(r('GET', 'counter') == '3', 'preflight failure keeps old dataset')
                command = encode('INCR', 'counter')
                link.sendall(b'+CONTINUE\r\n' + command)
                wait_link(r)
                wait_for(lambda: r('GET', 'counter') == '4', 'resume after rejected snapshot')
                wait_for(lambda: child('GET', 'counter') == '4', 'retained downstream history')
                check(int(info(r, 'sync_full', 'stats')) == retained_full,
                      'structural rejection does not force a downstream full resync')

                # A function compilation error is outside structural preflight. Once apply
                # begins, the old dataset/history are discarded, so reconnect and promotion
                # must require a fresh full sync rather than resume the replaced history.
                broken = apply_failure_rdb()
                link.shutdown(socket.SHUT_RDWR); link.close()
                link, args = fake.accept()
                check(args == ['PSYNC', NEXT_ID, str(len(replacement) + len(command) + 1)],
                      'cached history before apply-time error')
                link.sendall(f'+FULLRESYNC {REPLID} 0\r\n${len(broken)}\r\n'.encode() + broken)
                link, args = fake.accept()
                check(args == ['PSYNC', '?', '-1'], f'failed load cannot resume: {args}')
                check(r('GET', 'counter') == 'untrusted', 'valid prefix was decoded before failure')
                full = int(info(r, 'sync_full', 'stats'))
                r('REPLICAOF', 'NO', 'ONE')
                wait_for(lambda: int(info(r, 'sync_full', 'stats')) > full,
                         'promotion after rejected replacement requires a new full sync')
                wait_link(child)
            print('PASS partial RESP, interrupted MULTI, paused replay:', binary)
        finally:
            fake.close()
            if child:
                child.close()
            if r:
                r.close()
            if child_proc:
                stop(child_proc)
            if proc:
                stop(proc)


def empty_rdb():
    p = ports(1)[0]
    with tempfile.TemporaryDirectory(prefix='peadb-reconnect-rdb-') as td:
        proc = start(REDIS or PEADB, p, td)
        try:
            c = Conn(p)
            c('CONFIG', 'SET', 'repl-diskless-sync', 'no')
            c.close()
            return read_rdb(p)
        finally:
            stop(proc)


def malformed_rdb():
    # Ordinary RDB corruption fixture: one readable DB-5 string before an unknown type.
    # Structural preflight must refuse it before the dataset/history replacement boundary.
    from test_rdb_redis_interop import with_checksum
    body = b'REDIS0011\xfe\x05\x00\x07counter\x09untrusted\x63\x06broken\xff'
    return with_checksum(body)


def apply_failure_rdb():
    # Structurally valid FUNCTION2, with a Lua compilation error after a valid DB-5 key.
    from test_rdb_redis_interop import with_checksum
    code = b'#!lua name=broken\nthis is not valid Lua'
    return with_checksum(b'REDIS0011\xfe\x05\x00\x07counter\x09untrusted' +
                         b'\xf5' + bytes([len(code)]) + code + b'\xff')


if __name__ == '__main__':
    rdb = empty_rdb()
    if REDIS:
        run(REDIS, rdb)
    run(PEADB, rdb, malformed_rdb())

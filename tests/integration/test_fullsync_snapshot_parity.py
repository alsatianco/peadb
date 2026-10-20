#!/usr/bin/env python3
"""Forked full sync stays responsive and transfers RDBs above the replica buffer limit.

Run --large separately for a >256 MiB snapshot at the default replica limit.
The normal gate uses a smaller limit, and also checks slow saves, post-fork writes,
two replicas, child scheduling, cancellation and failed saves against Redis 7.2.5.
"""
import os
import pathlib
import socket
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_replica_offset_parity import (Conn, PEADB, REDIS, check, encode, info,
                                       start, stop, wait_for, wait_link)
from _readiness import wait_until_ready


class Peer:
    def __init__(self, port, command='PSYNC', pipeline=False):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=10)
        self.f = self.sock.makefile('rb')
        pre = encode('REPLCONF', 'capa', 'psync2') if pipeline else b''
        self.sock.sendall(pre + (encode('PSYNC', '?', '-1') if command == 'PSYNC'
                               else encode('SYNC')))
        self.legacy = command == 'SYNC'
        self.pipeline = pipeline

    def line(self):
        while True:
            line = self.f.readline()
            check(line, 'snapshot connection closed prematurely')
            if line.strip():
                return line

    def snapshot(self):
        if self.pipeline:
            check(self.line() == b'+OK\r\n', 'pipelined handshake replies must come first')
        if not self.legacy:
            header = self.line()
            check(header.startswith(b'+FULLRESYNC '), f'full sync header: {header!r}')
        bulk = self.line()
        check(bulk.startswith(b'$'), f'RDB bulk header: {bulk!r}')
        left = size = int(bulk[1:])
        first = b''
        while left:
            chunk = self.f.read(min(left, 65536))
            check(chunk, 'short snapshot')
            if not first:
                first = chunk[:9]
            left -= len(chunk)
        check(first.startswith(b'REDIS'), 'not an RDB')
        return size

    def close(self):
        self.f.close()
        self.sock.close()


def run(binary, replica_binary, large=False):
    # Bind 0 avoids adding fixed-port collisions to the repository's ratchet.
    ports = []
    for _ in range(3):
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            ports.append(s.getsockname()[1])
    procs, conns, peers = [], [], []
    with tempfile.TemporaryDirectory(prefix='peadb-fullsync-') as td:
        try:
            procs.append(start(binary, ports[0], td + '/master'))
            m = Conn(ports[0]); conns.append(m)
            check(m('CONFIG', 'SET', 'repl-diskless-sync', 'no') == '+OK', 'disk sync')
            # Incompressible values prevent Redis's RDB compression hiding the test size.
            value = os.urandom(1024 * 1024 if large else 128 * 1024)
            count = 260 if large else 40
            for i in range(count):
                check(m('SET', f'bulk:{i}', value) == '+OK', 'populate')
            if not large:
                check(m('CONFIG', 'SET', 'repl-backlog-size', '16kb',
                        'client-output-buffer-limit', 'replica 1mb 0 0') == '+OK', 'small limit')
            before = int(info(m, 'sync_full', 'stats'))
            peer = Peer(ports[0]); peers.append(peer)
            # Leave the receiver idle long enough for an eager whole-RDB reply to overflow.
            time.sleep(0.25)
            t0 = time.monotonic()
            check(m('PING') == '+PONG', 'master serves other clients during sync')
            check(time.monotonic() - t0 < 0.5, 'snapshot blocks master event loop')
            size = peer.snapshot()
            check(size > (256 * 1024 * 1024 if large else 1024 * 1024),
                  f'snapshot did not exceed output limit: {size}')
            peer.close(); peers.remove(peer)
            check(int(info(m, 'sync_full', 'stats')) == before + 1, 'one full sync, no retry loop')
            if large:
                procs.append(start(replica_binary, ports[1], td + '/large-replica'))
                r = Conn(ports[1]); conns.append(r)
                r('REPLICAOF', '127.0.0.1', ports[0])
                ping_times = []
                def synced():
                    begin = time.monotonic()
                    check(m('PING') == '+PONG', 'large sync master responsive')
                    ping_times.append(time.monotonic() - begin)
                    return info(r, 'master_link_status', 'replication') == 'up'
                wait_for(synced, 'large replica RDB loaded', timeout=60)
                check(max(ping_times) < 0.5, 'large full sync stalls master')
                check(r('DBSIZE') == count and r('STRLEN', f'bulk:{count - 1}') == len(value),
                      'large replica dataset incomplete')
                check(m('WAIT', '1', '3000') == 1, 'large replica applies and acknowledges')
                print(f'{binary}: streamed/loaded {size} bytes above the default 256 MiB limit; '
                      f'worst master PING {max(ping_times) * 1000:.1f} ms')
                return

            # A slow snapshot must keep serving clients and retain all post-fork bytes,
            # including writes larger than the tiny replication backlog.
            check(m('FLUSHALL') == '+OK', 'flush')
            for i in range(30):
                m('SET', f'slow:{i}', 'value')
            m('CONFIG', 'SET', 'rdb-key-save-delay', '100000')
            procs.append(start(replica_binary, ports[1], td + '/replica'))
            r = Conn(ports[1]); conns.append(r)
            r('REPLICAOF', '127.0.0.1', ports[0])
            wait_for(lambda: info(m, 'rdb_bgsave_in_progress', 'persistence') == '1', 'sync child')
            check(m('ROLE')[2] == [], 'a syncing replica is not online')
            check(m('WAIT', '1', '1') == 0, 'a syncing replica must not satisfy WAIT')
            t0 = time.monotonic()
            check(m('SET', 'after-fork', value) == '+OK', 'post-fork write')
            check(time.monotonic() - t0 < 0.5, 'slow child blocks writes')
            # A late replica waits for another snapshot; it must not consume an old snapshot
            # with a new offset. AOF rewrites and BGSAVEs obey the same one-child rule.
            procs.append(start(replica_binary, ports[2], td + '/late'))
            late = Conn(ports[2]); conns.append(late)
            late('REPLICAOF', '127.0.0.1', ports[0])
            check(m('BGSAVE') == '-ERR Background save already in progress', 'one RDB child')
            check(m('BGREWRITEAOF') == '+Background append only file rewriting scheduled',
                  'schedule AOF beside sync')
            m('CONFIG', 'SET', 'rdb-key-save-delay', '0')
            wait_link(r); wait_link(late)
            check(m('WAIT', '2', '3000') == 2, 'both replicas apply post-fork writes')
            for c in (r, late):
                check(c('STRLEN', 'after-fork') == len(value), 'missing post-fork write')
                check(c('DBSIZE') == 31, 'wrong snapshot or offset')
            print(f'{binary} -> {replica_binary}: buffer limit, slow snapshot, queued writes, late replica OK')
        finally:
            for p in peers:
                p.close()
            for c in conns:
                c.close()
            for p in reversed(procs):
                stop(p)


def lifecycle(binary):
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
    peers = []
    with tempfile.TemporaryDirectory(prefix='peadb-sync-lifecycle-') as td:
        with open(td + '/server.log', 'wb') as log:
            proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1',
                                     '--dir', td, '--save', '', '--shutdown-timeout', '0',
                                     '--enable-protected-configs', 'yes',
                                     '--repl-diskless-sync', 'no'],
                                    stdout=log, stderr=subprocess.STDOUT)
        m = None
        try:
            wait_until_ready(port, timeout=10, process=proc)
            m = Conn(port)
            for i in range(20):
                m('SET', f'k:{i}', 'v')
            peer = Peer(port, pipeline=True); peers.append(peer)
            check(peer.line() == b'+OK\r\n', 'REPLCONF reply')
            check(peer.line() == b'-ERR SYNC and PSYNC are invalid with pending output\r\n',
                  'SYNC must refuse pending handshake output')
            peer.close(); peers = []
            # Batch two waiters onto one child, then use the very same path for plain SYNC.
            before = int(info(m, 'rdb_saves', 'persistence'))
            m('CLIENT', 'PAUSE', '300', 'ALL')
            peers = [Peer(port), Peer(port, command='SYNC')]
            time.sleep(0.35)
            for peer in peers:
                check(peer.snapshot() > 100, 'empty full sync')
                peer.close()
            peers = []
            # Redis may begin the child at the first request and share it with the next.
            check(int(info(m, 'rdb_saves', 'persistence')) == before + 1, 'one snapshot for batch')
            wait_for(lambda: info(m, 'connected_slaves', 'replication') == '0', 'peer cleanup')

            # A pending sync waits for an existing save; both children must stay off the loop.
            m('CONFIG', 'SET', 'rdb-key-save-delay', '60000')
            check(m('BGSAVE') == '+Background saving started', 'initial save')
            peer = Peer(port); peers.append(peer)
            time.sleep(0.1)
            t0 = time.monotonic()
            check(m('PING') == '+PONG' and time.monotonic() - t0 < 0.5, 'wait for existing child')
            m('CONFIG', 'SET', 'rdb-key-save-delay', '0')
            check(peer.snapshot() > 100, 'snapshot after existing save')
            peer.close(); peers = []
            wait_for(lambda: info(m, 'connected_slaves', 'replication') == '0', 'peer cleanup')

            # A save that cannot rename its output must close the syncing connection.
            os.mkdir(td + '/blocked.rdb')
            check(m('CONFIG', 'SET', 'dbfilename', 'blocked.rdb') == '+OK', 'failure filename')
            peer = Peer(port); peers.append(peer)
            check(peer.line().startswith(b'+FULLRESYNC'), 'failure handshake')
            check(peer.f.read().strip() == b'', 'failed child sent snapshot/stream')
            check(info(m, 'rdb_last_bgsave_status', 'persistence') == 'err', 'failed sync save status')
            peer.close(); peers = []
            m('CONFIG', 'SET', 'dbfilename', 'dump.rdb')

            # Snapshot bytes are exempt; writes buffered behind them still obey the limit.
            m('CONFIG', 'SET', 'rdb-key-save-delay', '60000', 'repl-backlog-size', '16kb',
              'client-output-buffer-limit', 'replica 1mb 0 0')
            peer = Peer(port); peers.append(peer)
            check(peer.line().startswith(b'+FULLRESYNC'), 'lagging peer handshake')
            m('SET', 'too-much-stream', os.urandom(2 * 1024 * 1024))
            check(peer.f.read().strip() == b'', 'command buffer limit did not close lagging peer')
            peer.close(); peers = []
            # Stop the in-flight child; no old snapshot may survive this replacement.
            if binary == PEADB:
                # CONFIG SET dir changes only the parent cwd, not the saving child's.
                os.mkdir(td + '/moved')
                check(m('CONFIG', 'SET', 'dir', td + '/moved') == '+OK', 'change save directory')
            check(m('FLUSHALL') == '+OK', 'cancel child')
            wait_for(lambda: info(m, 'rdb_bgsave_in_progress', 'persistence') == '0', 'child reaped')
            check(not list(pathlib.Path(td).glob('temp-*.rdb')), 'child temporary file leaked')
            print(f'{binary}: batched/pipelined/legacy sync, existing child, failed save, stream limit OK')
        finally:
            for peer in peers:
                peer.close()
            if m:
                m.close()
            stop(proc)


if __name__ == '__main__':
    large = '--large' in sys.argv
    # The reference must exist: this regression is a cross-engine acceptance gate.
    check(REDIS is not None, 'pinned Redis 7.2.x is required')
    run(REDIS, REDIS, large)
    run(PEADB, REDIS, large)
    if not large:
        run(PEADB, PEADB)
        lifecycle(REDIS)
        lifecycle(PEADB)

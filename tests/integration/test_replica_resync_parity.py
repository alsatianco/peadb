#!/usr/bin/env python3
"""Cached-master PSYNC, changed IDs, backlog fallback and idle heartbeats (delta 47)."""
import os
import socket
import tempfile
import time

from test_replica_offset_parity import (Conn, PEADB, REDIS, check, info, start, stop,
                                       wait_for, wait_link)


def ports(count):
    out = []
    for _ in range(count):
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            out.append(s.getsockname()[1])
    return out


def run(master_binary, replica_binary):
    p = ports(3)
    procs, conns = [], []
    with tempfile.TemporaryDirectory(prefix='peadb-resync-') as td:
        try:
            for i, binary in enumerate((master_binary, replica_binary, replica_binary)):
                procs.append(start(binary, p[i], td + '/' + str(i)))
                conns.append(Conn(p[i]))
            m, r, s = conns
            m('CONFIG', 'SET', 'repl-diskless-sync', 'no', 'repl-backlog-size', '16kb',
              'repl-ping-replica-period', '1')
            r('CONFIG', 'SET', 'repl-timeout', '2', 'tcp-keepalive', '1')
            m('SELECT', '5')
            m('SET', 'counter', '0')
            r('REPLICAOF', '127.0.0.1', p[0]); wait_link(r)
            r('SELECT', '5'); s('SELECT', '5')
            check(m('INCR', 'counter') == 1 and m('WAIT', '1', '3000') == 1, 'initial write')
            before = int(info(m, 'sync_partial_ok', 'stats'))
            full = int(info(m, 'sync_full', 'stats'))
            check(m('CLIENT', 'KILL', 'TYPE', 'replica') == 1, 'disconnect replica')
            # Offline writes must be replayed once, in the remembered stream database.
            m('INCR', 'counter')
            wait_for(lambda: int(info(m, 'sync_partial_ok', 'stats')) > before,
                     'cached-master partial resync')
            wait_link(r)
            wait_for(lambda: r('GET', 'counter') == '2', 'offline write in DB 5')
            check(int(info(m, 'sync_full', 'stats')) == full, 'reconnect must not reload RDB')

            s('REPLICAOF', '127.0.0.1', p[1]); wait_link(s)
            old_id = info(m, 'master_replid', 'replication')
            # Promote the replica, then demote its former master. Redis synthesizes a cached
            # master from its own stream; both directions can continue without a snapshot.
            r('REPLICAOF', 'NO', 'ONE')
            r('CONFIG', 'SET', 'repl-ping-replica-period', '1')
            before = int(info(r, 'sync_partial_ok', 'stats'))
            m('REPLICAOF', '127.0.0.1', p[1])
            wait_link(m); wait_link(s)
            wait_for(lambda: int(info(r, 'sync_partial_ok', 'stats')) >= before + 2,
                     'promotion partial resync of former master and chained replica')
            check(info(s, 'master_replid', 'replication') == info(r, 'master_replid', 'replication'),
                  'CONTINUE adopts changed replid')
            check(info(s, 'master_replid2', 'replication') == old_id, 'old replid retained')
            r('INCR', 'counter')
            check(r('WAIT', '2', '3000') == 2, 'writes after promotion reach both replicas')
            check(m('GET', 'counter') == s('GET', 'counter') == '3', 'exactly once')

            # No application writes: PINGs advance the common stream, never a replica's
            # independent history, and keep short-timeout replicas connected.
            full = int(info(r, 'sync_full', 'stats'))
            off = int(info(r, 'master_repl_offset', 'replication'))
            wait_for(lambda: int(info(r, 'master_repl_offset', 'replication')) > off,
                     'master heartbeat', timeout=3)
            wait_for(lambda: int(info(s, 'slave_repl_offset', 'replication')) > off,
                     'replicated heartbeat')
            check(int(info(r, 'sync_full', 'stats')) == full, 'idle link stays up')

            # A lag outside the backlog must fall back to FULLRESYNC, replacing stale data.
            s('CONFIG', 'SET', 'masterauth', 'wrong')
            r('CLIENT', 'KILL', 'TYPE', 'replica')
            r('CONFIG', 'SET', 'repl-backlog-size', '16kb')
            r('SET', 'large', b'x' * 65536)
            r('INCR', 'counter')
            full = int(info(r, 'sync_full', 'stats'))
            s('CONFIG', 'SET', 'masterauth', '')
            wait_for(lambda: int(info(r, 'sync_full', 'stats')) > full, 'backlog full fallback')
            wait_link(s)
            wait_for(lambda: s('GET', 'counter') == '4', 'replacement snapshot')
            check(len(s('GET', 'large')) == 65536, 'full fallback dataset')
            print('PASS cached PSYNC/IDs/heartbeat/fallback:', master_binary, replica_binary)
        finally:
            for c in conns:
                c.close()
            for proc in reversed(procs):
                stop(proc)


if __name__ == '__main__':
    if REDIS:
        run(REDIS, REDIS)
    run(PEADB, os.environ.get('PEADB_REPLICA_BIN', PEADB))
    if REDIS:
        run(REDIS, PEADB)

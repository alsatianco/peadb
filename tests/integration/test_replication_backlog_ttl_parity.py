#!/usr/bin/env python3
"""Replication backlog expiry follows the last disconnect, zero TTL and role rules."""
import os
import subprocess
import tempfile
import time

from _isolated_server import IsolatedServer
from _readiness import wait_until_ready
from test_replica_offset_parity import Conn, PEADB, REDIS, check, info, stop, wait_for, wait_link
from test_replica_resync_parity import ports


def remains(predicate, seconds, description):
    deadline = time.monotonic() + seconds
    while True:
        check(predicate(), description)
        left = deadline - time.monotonic()
        if left <= 0:
            return
        time.sleep(min(left, 0.05))


def run(binary):
    p = ports(3)
    procs, conns = [], []
    with tempfile.TemporaryDirectory(prefix='peadb-backlog-ttl-') as td:
        try:
            for i in range(3):
                with open(f'{td}/{i}.log', 'wb') as log:
                    proc = IsolatedServer(
                        [binary, '--port', str(p[i]), '--bind', '127.0.0.1', '--save', '',
                         '--shutdown-timeout', '0', '--loglevel', 'notice',
                         '--repl-diskless-sync', 'no', '--repl-diskless-sync-delay', '0',
                         '--repl-backlog-ttl', '0' if i == 0 else '1'],
                        stdout=log, stderr=subprocess.STDOUT)
                procs.append(proc)
                wait_until_ready(p[i], process=proc)
                conns.append(Conn(p[i]))
            m, r, s = conns
            check(m('CONFIG', 'GET', 'repl-backlog-ttl') == ['repl-backlog-ttl', '0'],
                  'startup TTL zero')
            check(r('CONFIG', 'GET', 'repl-backlog-ttl') == ['repl-backlog-ttl', '1'],
                  'startup TTL one')
            check(m('SET', 'kept', 'initial') == '+OK', 'initial dataset')
            for replica in (r, s):
                check(replica('REPLICAOF', '127.0.0.1', p[0]) == '+OK', 'follow master')
                wait_link(replica)
            check(m('WAIT', 2, 3000) == 2, 'both replicas online')
            check(m('CONFIG', 'SET', 'repl-backlog-ttl', '1') == '+OK', 'live TTL one')
            replid = info(m, 'master_replid', 'replication')
            r('REPLICAOF', 'NO', 'ONE')
            wait_for(lambda: info(m, 'connected_slaves', 'replication') == '1',
                     'first replica left')
            remains(lambda: info(m, 'repl_backlog_active', 'replication') == '1' and
                    info(m, 'master_replid', 'replication') == replid and
                    info(s, 'repl_backlog_active', 'replication') == '1', 3.1,
                    'attached master and replica without downstream retain backlog')

            s('REPLICAOF', 'NO', 'ONE')
            wait_for(lambda: info(m, 'connected_slaves', 'replication') == '0',
                     'last replica left')
            remains(lambda: info(m, 'repl_backlog_active', 'replication') == '1', 0.25,
                    'TTL starts at last departure, not first departure')
            check(m('SET', 'kept', 'offline') == '+OK', 'write during disconnected grace')
            wait_for(lambda: info(m, 'repl_backlog_active', 'replication') == '0',
                     'expired master backlog', timeout=5)
            check(info(m, 'repl_backlog_histlen', 'replication') == '0', 'expired history empty')
            check(info(m, 'repl_backlog_first_byte_offset', 'replication') == '0',
                  'inactive first offset')
            check(info(m, 'master_replid', 'replication') != replid, 'expiry rotates replication id')
            check(info(m, 'master_replid2', 'replication') == '0' * 40, 'expiry clears secondary id')
            offset = info(m, 'master_repl_offset', 'replication')
            check(m('SET', 'after-expiry', 'value') == '+OK', 'write after release')
            check(info(m, 'master_repl_offset', 'replication') == offset,
                  'no replication stream while backlog absent')

            # Promotion retained the old master's history on r. Its reconnect must fail the
            # cached PSYNC against the expired master and replace the dataset in a full sync.
            partial_err = int(info(m, 'sync_partial_err', 'stats'))
            full = int(info(m, 'sync_full', 'stats'))
            check(m('CONFIG', 'SET', 'repl-backlog-ttl', '0') == '+OK', 'disable expiry')
            r('REPLICAOF', '127.0.0.1', p[0])
            wait_link(r)
            check(m('WAIT', 1, 3000) == 1, 'new replica online after expiry')
            check(int(info(m, 'sync_full', 'stats')) > full, 'expired backlog requires full sync')
            check(int(info(m, 'sync_partial_err', 'stats')) > partial_err,
                  'old cached history refused')
            check(r('GET', 'kept') == 'offline' and r('GET', 'after-expiry') == 'value',
                  'full replacement has writes before and after expiry')

            r('REPLICAOF', 'NO', 'ONE')
            wait_for(lambda: info(m, 'connected_slaves', 'replication') == '0',
                     'zero-TTL replica left')
            remains(lambda: info(m, 'repl_backlog_active', 'replication') == '1', 3.1,
                    'zero TTL keeps disconnected backlog')
            check(m('CONFIG', 'SET', 'repl-backlog-size', '16kb',
                    'repl-backlog-ttl', '1') == '+OK', 'live resize and shorten TTL')
            wait_for(lambda: info(m, 'repl_backlog_active', 'replication') == '0',
                     'live TTL uses original last disconnect time', timeout=3)
            check(m('CONFIG', 'SET', 'repl-backlog-size', '32kb') == '+OK',
                  'resize released backlog')
            check(info(m, 'repl_backlog_active', 'replication') == '0',
                  'resize does not activate backlog')
            # Leave an upstream link active longer than its TTL, then promote: the age of the
            # replica's earlier disconnect must not make its retained history expire at once.
            r('REPLICAOF', '127.0.0.1', p[0])
            wait_link(r)
            remains(lambda: info(r, 'repl_backlog_active', 'replication') == '1', 3.1,
                    'replica keeps backlog without downstream')
            r('REPLICAOF', 'NO', 'ONE')
            remains(lambda: info(r, 'repl_backlog_active', 'replication') == '1', 0.25,
                    'promotion resets backlog idle age')
            wait_for(lambda: info(r, 'repl_backlog_active', 'replication') == '0',
                     'promoted master backlog expires', timeout=5)
            print('PASS backlog TTL, last disconnect, zero, role, ids and full fallback:', binary)
        except BaseException:
            for i in range(len(procs)):
                print(open(f'{td}/{i}.log').read(), flush=True)
            raise
        finally:
            for c in conns:
                c.close()
            for proc in reversed(procs):
                stop(proc)


if __name__ == '__main__':
    check(REDIS is not None, 'pinned Redis reference required')
    version = subprocess.check_output([REDIS, '--version'], text=True)
    check(' v=7.2.5 ' in version, f'Redis 7.2.5 required: {version}')
    print(version.strip(), flush=True)
    run(REDIS)
    run(os.environ.get('PEADB_TEST_BIN', PEADB))

#!/usr/bin/env python3
"""Write admission counts online replicas with fresh ACKs, as in Redis 7.2.5."""
import os
import signal
import subprocess
import tempfile

from _isolated_server import IsolatedServer
from _readiness import wait_until_ready
from test_fullsync_snapshot_parity import Peer
from test_replica_offset_parity import Conn, PEADB, REDIS, check, info, stop, wait_for, wait_link
from test_replica_resync_parity import ports


def run(binary):
    p = ports(2)
    procs, conns = [], []
    paused = None
    peer = None
    with tempfile.TemporaryDirectory(prefix='peadb-min-replicas-') as td:
        try:
            for i in range(2):
                with open(f'{td}/{i}.log', 'wb') as log:
                    proc = IsolatedServer(
                        [binary, '--port', str(p[i]), '--bind', '127.0.0.1',
                         '--save', '', '--shutdown-timeout', '0', '--loglevel', 'notice',
                         '--repl-diskless-sync', 'no', '--repl-diskless-sync-delay', '0',
                         '--min-replicas-to-write', '1', '--min-replicas-max-lag', '0'],
                        stdout=log, stderr=subprocess.STDOUT)
                procs.append(proc)
                try:
                    wait_until_ready(p[i], process=proc)
                except BaseException:
                    print(open(f'{td}/{i}.log').read(), flush=True)
                    raise
                conns.append(Conn(p[i]))
            m, r = conns
            # Either zero setting disables admission, including at startup without replicas.
            check(m('SET', 'value', 'startup') == '+OK', 'startup max-lag zero disables gate')
            check(m('CONFIG', 'SET', 'min-slaves-max-lag', '1') == '+OK', 'lag alias')
            check(m('SET', 'value', 'refused').startswith('-NOREPLICAS '), 'no replica refuses')
            check(r('REPLICAOF', '127.0.0.1', p[0]) == '+OK', 'follow master')
            wait_link(r)
            wait_for(lambda: info(m, 'min_slaves_good_slaves', 'replication') == '1',
                     'one online replica')
            check(m('SET', 'value', 'healthy') == '+OK', 'healthy replica permits write')
            check(m('WAIT', 1, 3000) == 1, 'replica applied write')

            os.kill(procs[1].pid, signal.SIGSTOP)
            paused = procs[1]
            wait_for(lambda: info(m, 'min_slaves_good_slaves', 'replication') == '0',
                     'ACK lag exceeds limit', timeout=5)
            check(info(m, 'connected_slaves', 'replication') == '1', 'lagging socket stays connected')
            check(m('SET', 'value', 'refused').startswith('-NOREPLICAS '), 'lagging replica refuses')
            check(m('GET', 'value') == 'healthy', 'refused write leaves data intact')
            check(m('PING') == '+PONG', 'reads and ping remain available')
            check(m('CONFIG', 'SET', 'min-replicas-max-lag', '0') == '+OK', 'disable lag gate')
            check(m('SET', 'value', 'zero-lag') == '+OK', 'max-lag zero disables gate')
            check(m('CONFIG', 'SET', 'min-replicas-max-lag', '1',
                    'min-replicas-to-write', '0') == '+OK', 'disable count gate')
            check(m('SET', 'value', 'zero-count') == '+OK', 'count zero disables gate')
            check(m('CONFIG', 'SET', 'min-replicas-to-write', '1') == '+OK', 'restore count gate')
            check(m('SET', 'value', 'refused').startswith('-NOREPLICAS '), 'restore refuses stale ACK')
            check(m('CONFIG', 'SET', 'min-replicas-max-lag', '60') == '+OK', 'raise lag limit')
            wait_for(lambda: info(m, 'min_slaves_good_slaves', 'replication') == '1', 'new lag limit')
            check(m('SET', 'value', 'raised-limit') == '+OK', 'runtime lag change permits write')
            os.kill(paused.pid, signal.SIGCONT)
            paused = None
            check(m('CONFIG', 'SET', 'min-replicas-max-lag', '1') == '+OK', 'restore lag limit')
            wait_for(lambda: info(m, 'min_slaves_good_slaves', 'replication') == '1', 'ACK recovery')
            check(m('SET', 'value', 'recovered') == '+OK', 'fresh ACK restores writes')
            check(m('WAIT', 1, 3000) == 1 and r('GET', 'value') == 'recovered', 'exact replicated value')

            check(r('CONFIG', 'SET', 'min-replicas-max-lag', '1') == '+OK',
                  'replica with no downstream')
            check(r('SET', 'local', 'value').startswith('-READONLY '),
                  'replica bypasses NOREPLICAS before the READONLY check')
            check(m('SET', 'value', 'stream') == '+OK' and m('WAIT', 1, 3000) == 1,
                  'master stream bypasses replica admission')
            check(r('GET', 'value') == 'stream', 'stream applied with no downstream replicas')

            # A second connected replica waiting for a slow snapshot must not satisfy min=2.
            check(m('CONFIG', 'SET', 'rdb-key-save-delay', '1000000',
                    'min-replicas-max-lag', '60', 'min-replicas-to-write', '2') == '+OK', 'slow save')
            peer = Peer(p[0])
            wait_for(lambda: info(m, 'connected_slaves', 'replication') == '2', 'syncing replica attached')
            check(info(m, 'min_slaves_good_slaves', 'replication') == '1', 'syncing replica is not good')
            check(m('SET', 'value', 'refused').startswith('-NOREPLICAS '), 'syncing replica refuses')
            check(m('GET', 'value') == 'stream', 'sync refusal leaves value intact')
            print('PASS replica ACK lag, zero settings, runtime recovery and syncing exclusion:', binary)
        finally:
            if paused is not None:
                os.kill(paused.pid, signal.SIGCONT)
            if peer is not None:
                peer.close()
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

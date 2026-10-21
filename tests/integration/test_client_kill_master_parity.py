#!/usr/bin/env python3
"""CLIENT KILL reaches the upstream master and resumes applied history, as Redis does."""
import os
import subprocess
import tempfile

from _isolated_server import IsolatedServer
from _readiness import wait_until_ready
from test_replica_offset_parity import Conn, PEADB, REDIS, check, info, stop, wait_for, wait_link
from test_replica_resync_parity import ports


def master_row(c):
    for line in c('CLIENT', 'LIST').splitlines():
        row = dict(part.split('=', 1) for part in line.split())
        if 'M' in row.get('flags', ''):
            return row
    return None


def run(binary):
    p = ports(2)
    procs, conns = [], []
    with tempfile.TemporaryDirectory(prefix='peadb-kill-master-') as td:
        try:
            for i in range(2):
                with open(f'{td}/{i}.log', 'wb') as log:
                    proc = IsolatedServer(
                        [binary, '--port', str(p[i]), '--bind', '127.0.0.1', '--save', '',
                         '--shutdown-timeout', '0', '--loglevel', 'notice',
                         '--repl-diskless-sync', 'no', '--repl-diskless-sync-delay', '0'],
                        stdout=log, stderr=subprocess.STDOUT)
                procs.append(proc)
                wait_until_ready(p[i], process=proc)
                conns.append(Conn(p[i]))
            m, r = conns
            m('SELECT', 5)
            m('SET', 'counter', 0)
            check(r('REPLICAOF', 'localhost', p[0]) == '+OK', 'follow hostname master')
            wait_link(r)
            r('SELECT', 5)
            check(m('INCR', 'counter') == 1 and m('WAIT', 1, 3000) == 1, 'initial data')
            full = int(info(m, 'sync_full', 'stats'))
            address = f'127.0.0.1:{p[0]}'
            # Check the original failure before relying on CLIENT LIST introspection.
            before = int(info(m, 'sync_partial_ok', 'stats'))
            check(r('CLIENT', 'KILL', address) == '+OK', 'legacy form kills upstream master')
            wait_for(lambda: int(info(m, 'sync_partial_ok', 'stats')) > before, 'legacy PSYNC')
            wait_link(r)
            row = master_row(r)
            check(row is not None, 'master is visible in CLIENT LIST')
            for filters in [
                ('TYPE', 'normal', 'ADDR', address),
                ('TYPE', 'master', 'ID', r('CLIENT', 'ID')),
                ('TYPE', 'master', 'ADDR', '127.0.0.1:1'),
                ('TYPE', 'master', 'LADDR', '127.0.0.1:1'),
                ('TYPE', 'master', 'USER', 'default'),
            ]:
                check(r('CLIENT', 'KILL', *filters) == 0, f'combined filters: {filters}')
            for kind in ('type', 'addr', 'laddr', 'id', 'all'):
                row = master_row(r)
                check(row is not None, 'connected master row')
                filters = {
                    'type': ('TYPE', 'master'),
                    'addr': ('ADDR', row['addr'], 'TYPE', 'master'),
                    'laddr': ('LADDR', row['laddr'], 'TYPE', 'master'),
                    'id': ('ID', row['id'], 'TYPE', 'master'),
                    'all': ('SKIPME', 'yes'),
                }[kind]
                before = int(info(m, 'sync_partial_ok', 'stats'))
                # Within one EXEC the disconnected link cannot be matched a second time.
                check(r('MULTI') == '+OK', 'MULTI')
                check(r('CLIENT', 'KILL', *filters) == '+QUEUED', 'queue first kill')
                check(r('CLIENT', 'KILL', *filters) == '+QUEUED', 'queue repeated kill')
                check(r('EXEC') == [1, 0], f'one kill per link: {kind}')
                m('INCR', 'counter')
                wait_for(lambda: int(info(m, 'sync_partial_ok', 'stats')) > before,
                         f'partial reconnect: {kind}')
                wait_link(r)
                check(m('WAIT', 1, 3000) == 1, 'reconnected ACK')
                check(r('GET', 'counter') == m('GET', 'counter'), 'exactly-once data in DB 5')
                check(master_row(r)['id'] == row['id'], 'cached master retains its client ID')
                check(int(info(m, 'sync_full', 'stats')) == full, 'no replacement snapshot')
            old_id = master_row(r)['id']
            before = int(info(m, 'sync_full', 'stats'))
            r('CONFIG', 'SET', 'masterauth', 'wrong')
            check(r('CLIENT', 'KILL', 'TYPE', 'master') == 1, 'disconnect before full fallback')
            m('CONFIG', 'SET', 'repl-backlog-size', '16kb')
            m('SET', 'outside-backlog', 'x' * 65536)
            r('CONFIG', 'SET', 'masterauth', '')
            wait_for(lambda: int(info(m, 'sync_full', 'stats')) > before, 'full fallback')
            wait_link(r)
            check(m('WAIT', 1, 3000) == 1, 'full fallback ACK')
            check(r('GET', 'outside-backlog') == 'x' * 65536, 'full fallback dataset')
            check(master_row(r)['id'] != old_id, 'full sync creates a new master client ID')
            # Promotion discards the old master client. Alternate roles while retaining a
            # common history so the later demotion can CONTINUE, with a fresh identity.
            discarded_id = master_row(r)['id']
            r('REPLICAOF', 'NO', 'ONE')
            m('REPLICAOF', '127.0.0.1', p[1])
            wait_link(m)
            m('REPLICAOF', 'NO', 'ONE')
            before = int(info(m, 'sync_partial_ok', 'stats'))
            r('REPLICAOF', '127.0.0.1', p[0])
            wait_link(r)
            wait_for(lambda: int(info(m, 'sync_partial_ok', 'stats')) > before,
                     'partial reconnect after promotion and demotion')
            check(master_row(r)['id'] != discarded_id, 'promotion discards master client ID')
            print('PASS upstream CLIENT KILL filters, repeated kill and cached PSYNC:', binary,
                  flush=True)
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

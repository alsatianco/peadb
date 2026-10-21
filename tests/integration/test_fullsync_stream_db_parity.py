#!/usr/bin/env python3
"""A queued full-sync snapshot remembers the database selected at its fork offset."""
import os
import subprocess
import tempfile

from _isolated_server import IsolatedServer
from _readiness import wait_until_ready
from test_replica_offset_parity import Conn, PEADB, REDIS, check, info, stop, wait_for, wait_link
from test_replica_resync_parity import ports


def run(binary):
    p = ports(2)
    procs, conns = [], []
    with tempfile.TemporaryDirectory(prefix='peadb-fullsync-stream-db-') as td:
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
            # The first child predates replication and cannot provide its snapshot. Three
            # keys keep that child busy while the replica queues a fresh replication BGSAVE.
            check(m('MSET', 'hold:0', 'value', 'hold:1', 'value', 'hold:2', 'value') == '+OK',
                  'initial save dataset')
            check(m('CONFIG', 'SET', 'rdb-key-save-delay', '1000000') == '+OK', 'slow first child')
            check(m('BGSAVE').startswith('+Background saving started'), 'start first child')
            wait_for(lambda: info(m, 'rdb_bgsave_in_progress', 'persistence') == '1',
                     'first save active')
            check(r('REPLICAOF', '127.0.0.1', p[0]) == '+OK', 'attach replica')
            wait_for(lambda: info(m, 'connected_slaves', 'replication') == '1',
                     'replica waiting for child slot')
            check(info(m, 'rdb_bgsave_in_progress', 'persistence') == '1',
                  'existing child still occupies slot')
            check('state=wait_bgsave' in info(m, 'slave0', 'replication'),
                  'replica snapshot has not started')
            # attach_replica() forces the next SELECT. This SELECT and its write are before
            # the queued snapshot's offset; later writes in DB 5 therefore omit SELECT.
            check(m('SELECT', 5) == '+OK', 'select stream database')
            check(m('MSET', 'before-fork', 'kept', 'counter', '0') == '+OK',
                  'write in nonzero DB before replication fork')
            check(m('CONFIG', 'SET', 'rdb-key-save-delay', '0') == '+OK',
                  'next snapshot runs without save delay')
            wait_link(r)
            check(r('SELECT', 5) == '+OK' and r('GET', 'before-fork') == 'kept' and
                  r('GET', 'counter') == '0', 'fresh full-sync snapshot in DB 5')
            check(m('INCR', 'counter') == 1 and m('SADD', 'after-fork', '-793') == 1,
                  'same-DB writes after replication fork')
            check(m('WAIT', 1, 3000) == 1, 'replica acknowledges post-fork writes')
            check(r('GET', 'counter') == '1' and r('SMEMBERS', 'after-fork') == ['-793'],
                  'post-fork stream remains in DB 5')
            check(r('SELECT', 0) == '+OK' and r('EXISTS', 'counter', 'after-fork') == 0,
                  'post-fork stream creates no DB 0 keys')
            print('PASS queued full-sync snapshot stream database:', binary, flush=True)
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

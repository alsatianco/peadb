#!/usr/bin/env python3
"""Replication inactivity limits apply to handshakes, RDB transfers and both live peers."""
import os
import signal
import tempfile

from test_fullsync_snapshot_parity import Peer
from test_replica_offset_parity import (Conn, PEADB, REDIS, check, info, start, stop,
                                       wait_for, wait_link)
from test_replica_reconnect_stream import ControlledMaster, empty_rdb
from test_replica_resync_parity import ports


def run(binary, rdb):
    p = ports(2)
    procs, conns, fixtures = [], [], []
    suspended = None
    with tempfile.TemporaryDirectory(prefix='peadb-repl-liveness-') as td:
        try:
            for i in range(2):
                procs.append(start(binary, p[i], td + '/' + str(i)))
                conns.append(Conn(p[i]))
            m, r = conns
            m('CONFIG', 'SET', 'repl-diskless-sync', 'no', 'repl-ping-replica-period', '60')
            r('CONFIG', 'SET', 'repl-timeout', '1', 'replica-serve-stale-data', 'no')
            r('REPLICAOF', '127.0.0.1', p[0]); wait_link(r)
            check(m('SET', 'kept', 'value') == '+OK' and m('WAIT', 1, 3000) == 1,
                  'initial live stream')
            before = int(info(m, 'sync_partial_ok', 'stats'))
            os.kill(procs[0].pid, signal.SIGSTOP); suspended = procs[0]
            wait_for(lambda: info(r, 'master_link_status', 'replication') == 'down',
                     'silent master timeout', timeout=5)
            check(r('GET', 'kept').startswith('-MASTERDOWN'), 'stale reads refused')
            os.kill(procs[0].pid, signal.SIGCONT); suspended = None
            wait_for(lambda: int(info(m, 'sync_partial_ok', 'stats')) > before,
                     'resume after master timeout')
            wait_link(r)
            check(r('GET', 'kept') == 'value', 'cached data after timeout')
            r('REPLICAOF', 'NO', 'ONE')
            wait_for(lambda: info(m, 'connected_slaves', 'replication') == '0',
                     'previous replica disconnect')

            # A live socket alone is not a live replica: it must periodically ACK.
            m('CONFIG', 'SET', 'repl-timeout', '1')
            peer = Peer(p[0]); fixtures.append(peer)
            peer.snapshot()
            wait_for(lambda: info(m, 'connected_slaves', 'replication') == '0',
                     'unacknowledging replica timeout', timeout=5)
            peer.close(); fixtures.remove(peer)

            for handshake in (True, False):
                fake = ControlledMaster(stall_ping=handshake); fixtures.append(fake)
                r('CONFIG', 'SET', 'repl-timeout', '60')
                r('REPLICAOF', '127.0.0.1', fake.port)
                link, args = fake.accept()
                if handshake:
                    check(args == ['PING'], 'stalled PING phase')
                else:
                    link.sendall(f'+FULLRESYNC {"c3" * 20} 0\r\n${len(rdb)}\r\n'.encode() +
                                 rdb[:len(rdb) // 2])
                    wait_for(lambda: info(r, 'master_sync_in_progress', 'replication') == '1',
                             'partial transfer started')
                # The next attempt proves the blocked read was bounded and released. Live
                # CONFIG changes shorten the current attempt too, including a partial read.
                r('CONFIG', 'SET', 'repl-timeout', '1')
                link2, _ = fake.accept()
                check(link2 is not link, 'new connection after inactive handshake/transfer')
                r('REPLICAOF', 'NO', 'ONE')
                check(r('SET', 'promoted', 'safe') == '+OK', 'promotion cancels stalled link')
                fake.close(); fixtures.remove(fake)
            print('PASS master/replica/handshake/transfer timeouts:', binary)
        finally:
            if suspended:
                os.kill(suspended.pid, signal.SIGCONT)
            for fixture in fixtures:
                fixture.close()
            for c in conns:
                c.close()
            for proc in reversed(procs):
                stop(proc)


if __name__ == '__main__':
    rdb = empty_rdb()
    if REDIS:
        run(REDIS, rdb)
    run(PEADB, rdb)

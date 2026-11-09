#!/usr/bin/env python3
"""Redis-first live delay/threshold and immediate-vs-cron admission."""
import time

from _replication_snapshot import PEADB, check, encode, info, reference, server, wait_for
from test_replication_pipe_relay import Peer
from test_rdb_child_parent_loss import children


def forks(c):
    return int(info(c, 'total_forks', 'stats'))


def finish(peer, proc):
    peer.read_header()
    check(peer.snapshot().startswith(b'REDIS'), 'complete admitted snapshot')
    wait_for(lambda: not children(proc.pid), 'admitted child reaped')
    peer.sock.sendall(encode('REPLCONF', 'ACK', peer.offset))


def run(binary):
    with server(binary) as (c, port, work, proc):
        c('SET', 'admission', 'payload')
        c('CONFIG', 'SET', 'repl-ping-replica-period', 3600)
        # Threshold zero must not mean start immediately. Live delay releases it.
        c('CONFIG', 'SET', 'repl-diskless-sync-delay', 60,
          'repl-diskless-sync-max-replicas', 0)
        before = forks(c)
        peer = Peer(port, request=False)
        try:
            peer.request()
            wait_for(lambda: info(c, 'connected_slaves') == '1', 'waiting replica')
            time.sleep(0.35)
            check(forks(c) == before, 'nonzero delay and zero threshold must wait')
            c('CONFIG', 'SET', 'repl-diskless-sync-delay', 0)
            finish(peer, proc)
            check(peer.header.startswith(b'$EOF:') and forks(c) == before + 1, 'live delay release')
        finally:
            peer.close()
        wait_for(lambda: info(c, 'connected_slaves') == '0', 'first peer cleanup')
        # A threshold update applies to existing waiting peers, one actual fork.
        c('CONFIG', 'SET', 'repl-diskless-sync-delay', 60,
          'repl-diskless-sync-max-replicas', 3)
        peers = [Peer(port, request=False), Peer(port, request=False)]
        before = forks(c)
        try:
            for peer in peers:
                peer.request()
            wait_for(lambda: info(c, 'connected_slaves') == '2', 'threshold waiters')
            time.sleep(0.2)
            check(forks(c) == before, 'below threshold')
            c('CONFIG', 'SET', 'repl-diskless-sync-max-replicas', 2)
            for peer in peers:
                finish(peer, proc)
            check(forks(c) == before + 1 and peers[0].offset == peers[1].offset,
                  'live threshold shares actual fork boundary')
        finally:
            for peer in peers:
                peer.close()
        wait_for(lambda: info(c, 'connected_slaves') == '0', 'threshold cleanup')
        # Normal delay expires even with no new requests, without threshold help.
        c('CONFIG', 'SET', 'repl-diskless-sync-delay', 2,
          'repl-diskless-sync-max-replicas', 0)
        before = forks(c)
        peer = Peer(port, request=False)
        try:
            start = time.monotonic()
            peer.request()
            wait_for(lambda: info(c, 'connected_slaves') == '1', 'delay waiter')
            time.sleep(0.4)
            check(forks(c) == before, 'configured delay not bypassed')
            finish(peer, proc)
            check(0.4 < time.monotonic() - start < 4.5, 'bounded cron delay')
        finally:
            peer.close()
        wait_for(lambda: info(c, 'connected_slaves') == '0', 'delay cleanup')
        # Non-EOF immediate admission includes compatible earlier EOF waiters,
        # selecting disk despite a long diskless delay.
        c('CONFIG', 'SET', 'repl-diskless-sync-delay', 60)
        first = Peer(port, request=False)
        second = Peer(port, eof=False, request=False)
        before = forks(c)
        try:
            first.request()
            wait_for(lambda: info(c, 'connected_slaves') == '1', 'EOF waiter before legacy')
            second.request()
            for peer in (first, second):
                finish(peer, proc)
                check(not peer.header.startswith(b'$EOF:'), 'mixed immediate disk cohort')
            check(forks(c) == before + 1, 'mixed cohort one disk fork')
            check((work / 'dump.rdb').exists(), 'mixed cohort disk publication')
        finally:
            first.close()
            second.close()
        print(binary, 'live delay/zero threshold/threshold/mixed immediate admission passed', flush=True)


if __name__ == '__main__':
    for binary in (reference(), PEADB):
        run(binary)

#!/usr/bin/env python3
"""One shared EOF relay bounds fast recipients until a slow member disconnects."""
import concurrent.futures
import os
from pathlib import Path
import signal
import time

from _replication_snapshot import PEADB, check, info, reference, server, wait_for
from test_rdb_child_parent_loss import children
from test_replication_pipe_relay import Peer
from test_replication_pipe_lifecycle import healthy, slow_peer

def cohort(binary):
    with server(binary) as (c, port, work, proc):
        c('CONFIG', 'SET', 'repl-diskless-sync-delay', 1)
        c('CONFIG', 'SET', 'repl-diskless-sync-max-replicas', 2)
        c('SET', 'cohort', os.urandom(32 * 1024 * 1024))
        c('CONFIG', 'SET', 'repl-ping-replica-period', 3600)
        peers = [Peer(port, request=False), Peer(port, request=False)]
        before = int(info(c, 'total_forks', 'stats'))
        try:
            # Queue both requests while the owned server is stopped so they share
            # one event batch/fork. Always resume it, including on send failure.
            os.kill(proc.pid, signal.SIGSTOP)
            try:
                for peer in peers:
                    peer.request()
            finally:
                os.kill(proc.pid, signal.SIGCONT)
            for peer in peers:
                peer.read_header()
                check(peer.header.startswith(b'$EOF:'), 'cohort EOF capability')
            check(peers[0].offset == peers[1].offset, 'same cohort fork offset')
            check(int(info(c, 'total_forks', 'stats')) == before + 1, 'one actual cohort fork')
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                fast = executor.submit(peers[0].snapshot)
                # The slow peer has no reader: fast cannot finish the shared relay.
                time.sleep(0.4)
                check(not fast.done(), 'slow cohort member must bound shared relay')
                peers[1].close()
                peers.pop()
                check(fast.result(timeout=8).startswith(b'REDIS'), 'survivor resumes after slow drop')
            wait_for(lambda: not children(proc.pid), 'cohort child reaped')
        finally:
            for peer in peers:
                peer.close()
        healthy(c, port, proc)
        print(Path(binary).name, 'shared fast/slow cohort and cursor removal passed', flush=True)


def shutdown(binary):
    with server(binary) as (c, port, work, proc):
        c('SET', 'shutdown-payload', os.urandom(32 * 1024 * 1024))
        peer, child = slow_peer(c, port, proc)
        try:
            # The shutting-down replica is itself inside dispatch when its child
            # callback cancels the relay. No stale session reference may be freed.
            from _replication_snapshot import encode
            peer.sock.sendall(encode('SHUTDOWN', 'NOSAVE', 'NOW'))
            check(proc.wait(timeout=5) == 0, 'clean shutdown during suspended relay')
            from _readiness import ObservedProcess
            wait_for(lambda: ObservedProcess(child).poll() is not None, 'shutdown child reaped')
            check(not (work / 'dump.rdb').exists(), 'NOSAVE relay shutdown cannot publish disk')
        finally:
            peer.close()
        print(Path(binary).name, 'recipient-issued shutdown during suspended relay passed', flush=True)


if __name__ == '__main__':
    for binary in (reference(), PEADB):
        cohort(binary)
        shutdown(binary)

#!/usr/bin/env python3
"""Late disk reuse requires the same fork boundary and complete retained suffix."""
import os
import signal

from _replication_snapshot import PEADB, check, info, reference, server, wait_for
from test_replication_pipe_relay import Peer
from test_replication_admission import forks
from test_rdb_child_parent_loss import children
from test_replica_offset_parity import read_command


def reject_incomplete_donor(c, port, proc, rdb_only):
    from test_replication_snapshot_requests import request
    first = Peer(port, eof=not rdb_only, request=False)
    second = Peer(port, eof=False, request=False)
    child = None
    before = forks(c)
    c('DEL', 'new-boundary')
    try:
        if rdb_only:
            check(request(first, 'rdb-only', 1) == b'+OK\r\n', 'RDB-only donor')
        first.request()
        full = first.line()
        check(full.startswith(b'+FULLRESYNC '), 'donor fork header')
        wait_for(lambda: len(children(proc.pid)) == 1, 'donor child')
        child = children(proc.pid)[0]
        os.kill(child, signal.SIGSTOP)
        c('SET', 'new-boundary', 'belongs-to-next-fork')
        second.request()
        wait_for(lambda: info(c, 'connected_slaves') == '2', 'incompatible late peer')
        check(forks(c) == before + 1 and children(proc.pid) == [child], 'one active donor child')
        os.kill(child, signal.SIGCONT)
        child = None
        first.header = first.line()
        check(b'new-boundary' not in first.snapshot(), 'old donor boundary')
        second.read_header()
        check(second.offset > int(full.split()[2]), 'incompatible donor must not be reused')
        check(b'new-boundary' in second.snapshot(), 'new snapshot owns new boundary')
        check(forks(c) == before + 2, 'new child for incompatible/RDB-only donor')
    finally:
        if child and child in children(proc.pid):
            os.kill(child, signal.SIGCONT)
        first.close()
        second.close()
    wait_for(lambda: info(c, 'connected_slaves') == '0', 'donor scenario cleanup')
    wait_for(lambda: not children(proc.pid), 'donor scenario reaped')


def run(binary):
    with server(binary) as (c, port, work, proc):
        for i in range(256):
            c('SET', f'snapshot:{i}', 'original')
        c('CONFIG', 'SET', 'repl-diskless-sync', 'no', 'rdb-key-save-delay', 5000,
          'repl-backlog-size', 16384, 'repl-ping-replica-period', 3600)
        peers = [Peer(port, eof=False, request=False), Peer(port, eof=False, request=False)]
        before = forks(c)
        child = None
        try:
            peers[0].request()
            full = peers[0].line()
            check(full.startswith(b'+FULLRESYNC '), 'first fork header')
            wait_for(lambda: len(children(proc.pid)) == 1, 'disk child starts')
            child = children(proc.pid)[0]
            os.kill(child, signal.SIGSTOP)
            suffix = 'retained-after-fork' * 20000
            c('SELECT', 5)
            c('SET', 'wrapped-before-join', suffix)
            c('SELECT', 0)
            peers[1].request()
            check(peers[1].line() == full, 'late disk peer must reuse original fork boundary')
            check(children(proc.pid) == [child] and forks(c) == before + 1, 'same actual disk child')
            c('SET', 'after-join', 'both')
            os.kill(child, signal.SIGCONT)
            child = None
            for peer in peers:
                peer.header = peer.line()
                check(not peer.header.startswith(b'$EOF:'), 'disk length frame')
                payload = peer.snapshot()
                check(b'snapshot:0' in payload and b'wrapped-before-join' not in payload,
                      'original immutable snapshot boundary')
                commands = []
                while not commands or commands[-1][:2] != ['SET', 'after-join']:
                    args = read_command(peer.stream)
                    check(args is not None, 'retained command suffix')
                    commands.append(args)
                check(['SET', 'wrapped-before-join', suffix] in commands, 'wrapped suffix before late join')
                check(['SELECT', '5'] in commands, 'retained suffix DB selection')
                check(sum(a[:2] == ['SET', 'wrapped-before-join'] for a in commands) == 1,
                      'original suffix delivered exactly once')
            check(forks(c) == before + 1, 'no duplicate disk fork')
        finally:
            if child and child in children(proc.pid):
                os.kill(child, signal.SIGCONT)
            for peer in peers:
                peer.close()
        wait_for(lambda: info(c, 'connected_slaves') == '0', 'shared disk cleanup')
        wait_for(lambda: not children(proc.pid), 'shared child reaped')
        for rdb_only in (False, True):
            reject_incomplete_donor(c, port, proc, rdb_only)
        print(binary, 'late disk reuse with wrapped complete command suffix passed', flush=True)


if __name__ == '__main__':
    for binary in (reference(), PEADB):
        run(binary)

#!/usr/bin/env python3
"""Active-child exclusion, mixed cron capabilities and late pipe arrivals."""
import os
import signal
import time

from _replication_snapshot import PEADB, check, encode, info, reference, server, wait_for
from test_replication_pipe_relay import Peer
from test_rdb_child_parent_loss import children
from test_replication_admission import forks, finish


def run(binary):
    with server(binary) as (c, port, work, proc):
        for i in range(256):
            c('SET', f'key:{i}', 'x' * 256)
        c('CONFIG', 'SET', 'rdb-key-save-delay', 5000, 'repl-diskless-sync-delay', 0,
          'repl-ping-replica-period', 3600)
        before = forks(c)
        check(c('BGSAVE') == '+Background saving started', 'standalone child')
        original = children(proc.pid)
        check(len(original) == 1, 'owned disk child')
        # Both wait behind an unrelated BGSAVE; cron must intersect EOF caps.
        peers = [Peer(port, request=False), Peer(port, eof=False, request=False)]
        try:
            for peer in peers:
                peer.request()
            wait_for(lambda: info(c, 'connected_slaves') == '2', 'active-child waiters')
            check(children(proc.pid) == original and forks(c) == before + 1,
                  'no second child and no standalone BGSAVE reuse')
            for peer in peers:
                finish(peer, proc)
                check(not peer.header.startswith(b'$EOF:'), 'mixed cron capability intersection')
            check(forks(c) == before + 2, 'one standalone plus one compatible cohort fork')
        finally:
            for peer in peers:
                peer.close()
        wait_for(lambda: info(c, 'connected_slaves') == '0', 'mixed cohort cleanup')
        c('CONFIG', 'SET', 'rdb-key-save-delay', 0)
        c('SET', 'large', os.urandom(32 * 1024 * 1024))
        first = Peer(port)
        second = Peer(port, request=False)
        before = forks(c)
        try:
            first_child = children(proc.pid)
            check(len(first_child) == 1, 'blocked pipe child')
            second.request()
            wait_for(lambda: info(c, 'connected_slaves') == '2', 'late pipe waiter')
            c('SET', 'late-boundary', 'new-snapshot')
            time.sleep(0.15)
            check(children(proc.pid) == first_child and forks(c) == before, 'late pipe cannot reuse bytes')
            first.close()
            first = None
            second.read_header()
            check(second.header.startswith(b'$EOF:'), 'late peer fresh pipe')
            payload = second.snapshot()
            check(b'late-boundary' in payload and b'new-snapshot' in payload, 'new fork includes late write')
            wait_for(lambda: not children(proc.pid), 'late child reaped')
            check(forks(c) == before + 1, 'late peer exactly one new fork')
        finally:
            if first:
                first.close()
            second.close()
        wait_for(lambda: info(c, 'connected_slaves') == '0', 'late peer cleanup')
        # A disk-target child with no recipients has value only for configured saves.
        c('CONFIG', 'SET', 'repl-diskless-sync', 'no', 'rdb-key-save-delay', 5000)
        c('DEL', 'large')
        for save in ('', '3600 1'):
            c('CONFIG', 'SET', 'save', save)
            previous = (work / 'dump.rdb').read_bytes()
            c('SET', 'disk-usefulness', save or 'not-configured')
            peer = Peer(port, eof=False, request=False)
            try:
                peer.request()
                wait_for(lambda: len(children(proc.pid)) == 1, 'disk sync child')
                child = children(proc.pid)[0]
                # Stop only our verified child to keep the observation deterministic.
                os.kill(child, signal.SIGSTOP)
                peer.close()
                peer = None
                wait_for(lambda: info(c, 'connected_slaves') == '0', 'last disk recipient drops')
                if save:
                    check(children(proc.pid) == [child], 'configured save keeps useful disk child')
                    os.kill(child, signal.SIGCONT)
                    wait_for(lambda: not children(proc.pid), 'useful disk child completes')
                    check(info(c, 'rdb_last_bgsave_status', 'persistence') == 'ok', 'retained disk save succeeds')
                else:
                    # Redis's cancellation signal is handled cooperatively; a stopped
                    # child must resume before it can process SIGUSR1. PeaDB may
                    # already have synchronously reaped its SIGKILLed child.
                    if child in children(proc.pid):
                        os.kill(child, signal.SIGCONT)
                    wait_for(lambda: not children(proc.pid), 'unused disk child cancelled/reaped')
                    check((work / 'dump.rdb').read_bytes() == previous,
                          'cancelled unneeded child cannot publish its newer snapshot')
            finally:
                if peer:
                    peer.close()
                for owned in children(proc.pid):
                    os.kill(owned, signal.SIGCONT)
        # Both non-reading recipients time out in one shared pipe cohort. Removing
        # the first cursor must preserve the second, and the last must cancel/reap.
        c('CONFIG', 'SET', 'save', '', 'repl-diskless-sync', 'yes',
          'repl-diskless-sync-delay', 60, 'repl-diskless-sync-max-replicas', 2,
          'rdb-key-save-delay', 0)
        c('SET', 'timeout-cohort', os.urandom(32 * 1024 * 1024))
        peers = [Peer(port, request=False), Peer(port, request=False)]
        before = forks(c)
        try:
            for peer in peers:
                peer.request()
            for peer in peers:
                peer.read_header()
                check(peer.header.startswith(b'$EOF:'), 'all-timeout pipe cohort')
            check(forks(c) == before + 1, 'one all-timeout child')
            if os.path.isdir('/proc'):
                from test_replication_pipe_lifecycle import cpu_seconds
                cpu_before = cpu_seconds(proc.pid)
                time.sleep(0.6)
                check(cpu_seconds(proc.pid) - cpu_before < 0.25, 'multi-recipient suspended CPU bound')
            c('CONFIG', 'SET', 'repl-timeout', 1)
            wait_for(lambda: info(c, 'connected_slaves') == '0', 'all recipients timed out', timeout=5)
            wait_for(lambda: not children(proc.pid), 'all-timeout child reaped')
            check(not list(work.glob('temp-*.rdb')), 'all-timeout pipe cleanup')
        finally:
            for peer in peers:
                peer.close()
            c('CONFIG', 'SET', 'repl-timeout', 60)
        print(binary, 'active child/mixed cron/late pipe/disk usefulness/all-timeout passed', flush=True)


if __name__ == '__main__':
    for binary in (reference(), PEADB):
        run(binary)

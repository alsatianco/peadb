#!/usr/bin/env python3
"""Cron thresholds count only the oldest request-compatible cohort."""
import time

from _replication_snapshot import PEADB, check, info, reference, server, seed, wait_for
from test_replication_pipe_relay import Peer
from test_replication_snapshot_requests import request
from test_replication_admission import forks
from test_rdb_child_parent_loss import children


def run(binary):
    with server(binary) as (c, port, work, proc):
        seed(c)
        for i in range(256):
            c('SET', f'wait:{i}', 'x' * 128)
        c('CONFIG', 'SET', 'rdb-key-save-delay', 5000,
          'repl-diskless-sync-delay', 60, 'repl-diskless-sync-max-replicas', 2)
        c('BGSAVE')
        before = forks(c)
        first = Peer(port, request=False)
        second = Peer(port, eof=False, request=False)
        try:
            check(request(first, 'rdb-only', 1, 'rdb-filter-only', 'functions') == b'+OK\r\n',
                  'first filtered request')
            check(request(second, 'rdb-only', 1) == b'+OK\r\n', 'second full request')
            first.request()
            wait_for(lambda: info(c, 'connected_slaves') == '1', 'first waiter')
            second.request()
            wait_for(lambda: info(c, 'connected_slaves') == '2', 'different-request waiter')
            wait_for(lambda: not children(proc.pid), 'standalone save completes')
            time.sleep(0.2)
            check(forks(c) == before, 'incompatible requests must not combine toward threshold')
            c('CONFIG', 'SET', 'repl-diskless-sync-max-replicas', 1)
            first.read_header()
            check(first.header.startswith(b'$EOF:'), 'oldest cohort keeps its EOF capability')
            filtered = first.snapshot()
            check(b'wait:0' not in filtered and b'interop_f' in filtered, 'first cohort functions only')
            check(first.stream.read() == b'', 'first RDB-only completion')
            second.read_header()
            check(not second.header.startswith(b'$EOF:'), 'second full cohort has disk fallback')
            full = second.snapshot()
            check(b'wait:0' in full and b'interop_f' in full, 'second cohort full data/functions')
            check(second.stream.read() == b'', 'second RDB-only completion')
            wait_for(lambda: not children(proc.pid), 'all cohorts reaped')
            check(forks(c) == before + 2, 'exactly two request-cohort children')
        finally:
            first.close()
            second.close()
        print(binary, 'oldest-request threshold/capabilities/separate snapshot cohorts passed', flush=True)


if __name__ == '__main__':
    for binary in (reference(), PEADB):
        run(binary)

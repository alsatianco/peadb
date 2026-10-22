#!/usr/bin/env python3
"""Restarted master boundaries, expiry DELs and conservative metadata fallbacks (delta 47)."""
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

from test_rdb_restart_history import PEADB, Servers
from test_replica_offset_parity import REDIS, check, encode, info, stop, wait_for, wait_link
from test_replica_resync_parity import ports


def master_restart(binary, reader=None, expire=False):
    with tempfile.TemporaryDirectory(prefix='peadb-master-history-') as td:
        servers = Servers(td)
        p = ports(2)
        try:
            mp, m = servers.start(binary, p[0], 'master')
            m('SELECT', 5)
            m('SET', 'counter', 1)
            rp, r = servers.start(binary, p[1], 'replica', p[0])
            wait_link(r)
            m('INCR', 'counter')
            if expire:
                deadline = int(time.time() * 1000) + 800
                m('SET', 'expires-offline', 'v', 'PXAT', deadline)
            wait_for(lambda: info(m, 'master_repl_offset', 'replication') ==
                     info(r, 'slave_repl_offset', 'replication'), 'same snapshot point')
            oldid = info(m, 'master_replid', 'replication')
            offset = int(info(m, 'master_repl_offset', 'replication'))
            check(m('SAVE') == r('SAVE') == '+OK', 'save both ends')
            r.close(); stop(rp)
            m.close(); stop(mp)
            if expire:
                wait_for(lambda: int(time.time() * 1000) > deadline, 'offline expiry')
            _, m = servers.start(reader or binary, p[0], 'master')
            check(info(m, 'master_replid2', 'replication') == oldid and
                  info(m, 'master_replid', 'replication') != oldid and
                  int(info(m, 'second_repl_offset', 'replication')) == offset + 1 and
                  info(m, 'repl_backlog_active', 'replication') == '1', 'master secondary history')
            current = int(info(m, 'master_repl_offset', 'replication'))
            expected = len(encode('SELECT', 5) + encode('DEL', 'expires-offline')) if expire else 0
            check(current - offset == expected, 'only actual expiry bytes extend saved history')
            check(int(info(m, 'repl_backlog_histlen', 'replication')) == current - offset,
                  'no invented pre-restart backlog bytes')
            _, r = servers.start(binary, p[1], 'replica', p[0])
            wait_link(r)
            check(info(m, 'sync_partial_ok', 'stats') == '1' and
                  info(m, 'sync_full', 'stats') == '0', 'replica resumes at saved boundary')
            m('SELECT', 5); r('SELECT', 5)
            check(m('INCR', 'counter') == 3, 'post-restart write')
            wait_for(lambda: r('GET', 'counter') == '3', 'new stream starts with correct SELECT')
            if expire:
                wait_for(lambda: r('DEBUG', 'OBJECT', 'expires-offline').startswith('-ERR'),
                         'replica consumes startup expiry DEL')
            # Request a byte before the saved point: knowing the old ID is insufficient.
            with socket.create_connection(('127.0.0.1', p[0]), timeout=5) as sock:
                with sock.makefile('rb') as f:
                    sock.sendall(encode('PSYNC', oldid, offset))
                    check(f.readline().startswith(b'+FULLRESYNC '), 'older history needs full sync')
            print('PASS master restart:', Path(binary).name, Path(reader or binary).name,
                  'expiry=' + str(expire), flush=True)
        finally:
            servers.close()


def main():
    check(REDIS is not None, 'pinned Redis reference required')
    check(' v=7.2.5 ' in subprocess.check_output([REDIS, '--version'], text=True), 'pinned reference')
    selected = os.environ.get('PEADB_HISTORY_ENGINES', 'both')
    for binary in ([REDIS] if selected == 'redis' else [PEADB] if selected == 'peadb' else [REDIS, PEADB]):
        master_restart(binary)
        master_restart(binary, expire=True)
    if selected == 'both':
        master_restart(REDIS, PEADB, expire=True)
        master_restart(PEADB, REDIS, expire=True)


if __name__ == '__main__':
    main()

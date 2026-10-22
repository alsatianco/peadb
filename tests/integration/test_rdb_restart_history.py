#!/usr/bin/env python3
"""Redis-first RDB restart PSYNC, save paths and cross-engine snapshot history (delta 47)."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from _readiness import wait_until_ready
from test_replica_offset_parity import Conn, PEADB, REDIS, check, info, stop, wait_for, wait_link
from test_replica_resync_parity import ports

PEADB = os.environ.get('PEADB_HISTORY_BIN', PEADB)


class Servers:
    def __init__(self, root):
        self.root = Path(root)
        self.procs = []
        self.conns = []

    def start(self, binary, port, name, master=None, extra=()):
        work = self.root / name
        work.mkdir(exist_ok=True)
        args = [binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', str(work),
                '--save', '', '--shutdown-timeout', '0', '--repl-diskless-sync', 'no',
                '--repl-diskless-sync-delay', '0', '--repl-ping-replica-period', '3600',
                '--enable-debug-command', 'yes']
        if master:
            args += ['--replicaof', '127.0.0.1', str(master)]
        with (work / 'server.log').open('ab') as log:
            proc = subprocess.Popen(args + list(extra), stdout=log, stderr=subprocess.STDOUT)
        self.procs.append(proc)
        wait_until_ready(port, process=proc)
        conn = Conn(port)
        self.conns.append(conn)
        return proc, conn

    def close(self):
        if sys.exc_info()[0] is not None:
            for path in sorted(self.root.glob('*/server.log')):
                print(f'{path}:\n{path.read_text(errors="replace")[-12000:]}', file=sys.stderr)
        for conn in self.conns:
            conn.close()
        for proc in reversed(self.procs):
            stop(proc)


def aux_fields(data):
    """Decode initial AUX strings, including Redis's integer string encodings."""
    pos = 9

    def string():
        nonlocal pos
        first = data[pos]
        pos += 1
        kind, size = first >> 6, first & 63
        if kind == 3:
            check(size in (0, 1, 2), 'unexpected compressed AUX string')
            width = (1, 2, 4)[size]
            value = int.from_bytes(data[pos:pos + width], 'little', signed=True)
            pos += width
            return str(value).encode()
        if kind == 1:
            size = (size << 8) | data[pos]
            pos += 1
        elif kind == 2:
            width = 8 if first == 0x81 else 4
            size = int.from_bytes(data[pos:pos + width], 'big')
            pos += width
        value = data[pos:pos + size]
        pos += size
        return value

    fields = {}
    while data[pos] == 250:
        pos += 1
        key = string()
        fields[key] = string()
    return fields


def snapshot(c, proc, work, mode):
    if mode == 'BGSAVE':
        before = int(info(c, 'rdb_saves', 'persistence'))
        check(c('BGSAVE') == '+Background saving started', 'BGSAVE')
        wait_for(lambda: info(c, 'rdb_bgsave_in_progress', 'persistence') == '0' and
                 int(info(c, 'rdb_saves', 'persistence')) > before, 'BGSAVE complete')
    elif mode == 'cron':
        before = int(info(c, 'rdb_saves', 'persistence'))
        check(c('CONFIG', 'SET', 'save', '1 1') == '+OK', 'enable save point')
        wait_for(lambda: int(info(c, 'rdb_saves', 'persistence')) > before and
                 info(c, 'rdb_bgsave_in_progress', 'persistence') == '0', 'cron save')
        check(c('CONFIG', 'SET', 'save', '') == '+OK', 'disable save point')
    elif mode == 'shutdown':
        check(c('SHUTDOWN', 'SAVE') is None, 'SHUTDOWN SAVE closes connection')
        proc.wait(timeout=5)
        check(proc.returncode == 0, 'clean shutdown')
    else:
        check(c('SAVE') == '+OK', 'SAVE')
    return (work / 'dump.rdb').read_bytes()


def run(master_binary, source_binary, restart_binary, mode='SAVE', fallback=False):
    with tempfile.TemporaryDirectory(prefix='peadb-rdb-history-') as td:
        servers = Servers(td)
        p = ports(2)
        try:
            _, m = servers.start(master_binary, p[0], 'master', extra=('--repl-backlog-size', '16kb'))
            m('SELECT', 5)
            m('SET', 'counter', 1)
            proc, r = servers.start(source_binary, p[1], 'replica', p[0])
            wait_link(r)
            check(m('INCR', 'counter') == 2, 'write after initial full sync selects DB 5')
            wait_for(lambda: info(m, 'master_repl_offset', 'replication') ==
                     info(r, 'slave_repl_offset', 'replication'), 'applied offset')
            saved_id = info(r, 'master_replid', 'replication')
            saved_offset = int(info(r, 'slave_repl_offset', 'replication'))
            if mode == 'cached':
                check(r('CONFIG', 'SET', 'masterauth', 'wrong') == '+OK', 'hold reconnect')
                check(r('CLIENT', 'KILL', 'TYPE', 'master') == 1, 'cache master')
                wait_for(lambda: info(r, 'master_link_status', 'replication') == 'down', 'link down')
            data = snapshot(r, proc, Path(td) / 'replica', mode)
            fields = aux_fields(data)
            check(fields.get(b'repl-id') == saved_id.encode() and
                  fields.get(b'repl-offset') == str(saved_offset).encode() and
                  fields.get(b'repl-stream-db') == b'5', f'saved history: {fields}')
            r.close()
            stop(proc)
            full, partial = (int(info(m, key, 'stats')) for key in ('sync_full', 'sync_partial_ok'))
            if fallback:
                m('SET', 'large', b'x' * 65536)
            check(m('INCR', 'counter') == 3, 'offline write in remembered DB')
            _, r = servers.start(restart_binary, p[1], 'replica', p[0],
                                 extra=('--masterauth', 'hold') if mode == 'offline' else ())
            if mode == 'offline':
                check(info(r, 'master_replid', 'replication') == saved_id and
                      int(info(r, 'slave_repl_offset', 'replication')) == saved_offset and
                      info(r, 'repl_backlog_active', 'replication') == '0',
                      'offline startup restores cache without activating a downstream backlog')
                r('CONFIG', 'SET', 'masterauth', '')
            wait_link(r)
            r('SELECT', 5)
            wait_for(lambda: r('GET', 'counter') == '3', 'exactly-once offline replay in DB 5')
            r('SELECT', 0)
            check(r('GET', 'counter') is None, 'no replay into DB 0')
            check(int(info(m, 'sync_full', 'stats')) - full == int(fallback) and
                  int(info(m, 'sync_partial_ok', 'stats')) - partial == int(not fallback),
                  'restart must use partial sync unless backlog missed')
            print('PASS RDB restart:', mode, 'fallback=' + str(fallback),
                  Path(master_binary).name, Path(source_binary).name, Path(restart_binary).name,
                  flush=True)
        finally:
            servers.close()


def main():
    check(REDIS is not None, 'Redis 7.2.5 reference required')
    check(' v=7.2.5 ' in subprocess.check_output([REDIS, '--version'], text=True), 'pinned reference')
    selected = os.environ.get('PEADB_HISTORY_ENGINES', 'both')
    for binary in ([REDIS] if selected == 'redis' else [PEADB] if selected == 'peadb' else [REDIS, PEADB]):
        for mode in os.environ.get('PEADB_HISTORY_MODES', 'SAVE,BGSAVE,cron,shutdown,cached,offline').split(','):
            run(binary, binary, binary, mode)
        run(binary, binary, binary, fallback=True)
    if selected == 'both':
        run(REDIS, REDIS, PEADB)
        run(PEADB, PEADB, REDIS)
        run(REDIS, PEADB, PEADB)
        run(PEADB, REDIS, REDIS)


if __name__ == '__main__':
    main()

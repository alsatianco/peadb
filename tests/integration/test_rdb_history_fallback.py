#!/usr/bin/env python3
"""Missing/malformed RDB history and AOF precedence never authorize stale PSYNC (delta 47)."""
import os
from pathlib import Path
import subprocess
import tempfile

from test_rdb_restart_history import PEADB, Servers, aux_fields
from test_replica_offset_parity import REDIS, check, info, stop, wait_for, wait_link
from test_replica_resync_parity import ports


def rdb(fields):
    def string(value):
        check(len(value) < 64, 'small fixture string')
        return bytes([len(value)]) + value
    return (b'REDIS0011' + b''.join(b'\xfa' + string(k) + string(v) for k, v in fields) +
            b'\xfe\x05\x00\x07counter\x011\xff' + bytes(8))


def metadata_fallback(binary):
    # Redis shares these incomplete/-1 metadata fallbacks. Its atoi/strtoll parsing of
    # malformed values is intentionally not our authority for authorizing unsafe history.
    good = [(b'repl-id', b'a' * 40), (b'repl-offset', b'123'), (b'repl-stream-db', b'5')]
    cases = [[], good[:1], good[1:], good[:2],
             [(k, b'-1' if k == b'repl-offset' else v) for k, v in good],
             [(k, b'-1' if k == b'repl-stream-db' else v) for k, v in good],
             [(k, b'short' if k == b'repl-id' else v) for k, v in good]]
    if binary == PEADB:
        for key, value in [(b'repl-offset', b'123junk'), (b'repl-offset', b'9223372036854775808'),
                           (b'repl-offset', b'9223372036854775807'), (b'repl-offset', b'1\x002'),
                           (b'repl-stream-db', b'16'), (b'repl-stream-db', b'4294967296'),
                           (b'repl-stream-db', b'5\x00'), (b'repl-id', b'z' * 40)]:
            cases.append([(k, value if k == key else v) for k, v in good])
    with tempfile.TemporaryDirectory(prefix='peadb-metadata-fallback-') as td:
        servers = Servers(td)
        try:
            port = ports(1)[0]
            for i, fields in enumerate(cases):
                work = Path(td) / str(i)
                work.mkdir()
                (work / 'dump.rdb').write_bytes(rdb(fields))
                proc, c = servers.start(binary, port, str(i))
                check(info(c, 'master_replid2', 'replication') == '0' * 40 and
                      info(c, 'repl_backlog_active', 'replication') == '0',
                      f'no partial history for metadata {fields}')
                c('SELECT', 5)
                check(c('GET', 'counter') == '1', 'fallback still loads dataset')
                check(c('SAVE') == '+OK', 'fresh-history save')
                check(b'repl-id' not in aux_fields((work / 'dump.rdb').read_bytes()),
                      'fresh master does not write useless history')
                c.close(); stop(proc)
            print('PASS metadata fallback:', Path(binary).name, len(cases), flush=True)
        finally:
            servers.close()


def aof_fallback(binary):
    with tempfile.TemporaryDirectory(prefix='peadb-history-aof-') as td:
        servers = Servers(td)
        p = ports(2)
        try:
            _, m = servers.start(binary, p[0], 'master')
            proc, r = servers.start(binary, p[1], 'replica', p[0],
                                    extra=('--appendonly', 'yes', '--aof-use-rdb-preamble', 'no'))
            wait_link(r)
            m('SET', 'counter', 1)
            wait_for(lambda: r('GET', 'counter') == '1' and
                     info(r, 'aof_rewrite_in_progress', 'persistence') == '0' and
                     info(r, 'aof_rewrite_scheduled', 'persistence') == '0', 'durable AOF base')
            check(r('SAVE') == '+OK', 'stale RDB alongside AOF')
            oldid = info(r, 'master_replid', 'replication')
            r.close(); stop(proc)
            full = int(info(m, 'sync_full', 'stats'))
            _, r = servers.start(binary, p[1], 'replica', p[0],
                                extra=('--appendonly', 'yes', '--masterauth', 'hold'))
            check(r('GET', 'counter') == '1' and
                  info(r, 'master_replid', 'replication') != oldid,
                  'AOF dataset never borrows stale RDB history')
            r('CONFIG', 'SET', 'masterauth', '')
            wait_link(r)
            check(int(info(m, 'sync_full', 'stats')) == full + 1, 'AOF restart requires full sync')
            print('PASS AOF history fallback:', Path(binary).name, flush=True)
        finally:
            servers.close()


def main():
    check(REDIS is not None, 'pinned Redis reference required')
    check(' v=7.2.5 ' in subprocess.check_output([REDIS, '--version'], text=True), 'pinned reference')
    selected = os.environ.get('PEADB_HISTORY_ENGINES', 'both')
    for binary in ([REDIS] if selected == 'redis' else [PEADB] if selected == 'peadb' else [REDIS, PEADB]):
        metadata_fallback(binary)
        aof_fallback(binary)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Reject broken EOF transfers, preserve pre-load state, cancel superseded links."""
from _replication_snapshot import (MARK, Master, PEADB, REPLID, check, encode, info,
                                   reference, server, snapshot, wait_for, wait_link)


def short_transfers(binary, rdb):
    with server(binary) as (r, _, td, _), Master() as m:
        r('SET', 'preserved', 'old')
        r('FUNCTION', 'LOAD', "#!lua name=old\nredis.register_function{function_name='old', "
          "callback=function() return 'old-state' end, flags={'no-writes'}}")
        r('CONFIG', 'SET', 'repl-timeout', 1)
        r('REPLICAOF', '127.0.0.1', m.port)
        sock, _, _ = m.accept()
        for payload in (rdb[:100], rdb + MARK[:-1] + b'!', rdb + MARK[:39]):
            m.header(sock, b'$EOF:' + MARK)
            sock.sendall(payload)
            wait_for(lambda: info(r, 'master_sync_in_progress') == '1', 'incomplete transfer')
            check(info(r, 'master_link_status') == 'down', 'no premature load')
            # Keep the socket open: live inactivity timeout must end the attempt.
            next_sock, _, _ = m.accept()
            check(r('GET', 'preserved') == b'old' and
                  r('FCALL_RO', 'old', 0) == b'old-state', 'old keys/functions survive')
            wait_for(lambda: not list(td.glob('temp-*.rdb')), 'failed spool cleanup')
            sock = next_sock
        # Supersede while the final marker byte is missing, then try delivering it late.
        m.header(sock, b'$EOF:' + MARK)
        sock.sendall(rdb + MARK[:39])
        wait_for(lambda: info(r, 'master_sync_in_progress') == '1', 'superseded transfer')
        r('REPLICAOF', 'NO', 'ONE')
        r('SET', 'promoted', 'safe')
        try:
            sock.sendall(MARK[39:] + encode('SET', 'promoted', 'stale'))
        except OSError:
            pass
        wait_for(lambda: not list(td.glob('temp-*.rdb')), 'superseded spool cleanup')
        check(r('GET', 'promoted') == b'safe' and r('GET', 'preserved') == b'old',
              'late old-generation snapshot/commands cannot replace promoted data')
        check(r('EXISTS', 'binary') == 0, 'incomplete snapshot never published')
        print('PASS EOF short/mismatched/timeout/superseded:', binary, flush=True)


def invalid_snapshots(rdb):
    # Redis exits on a corrupted RDB. PeaDB's existing preflight keeps old state.
    with server(PEADB) as (r, _, td, _), Master() as m:
        r('SET', 'preserved', 'old')
        r('REPLICAOF', '127.0.0.1', m.port)
        sock, _, _ = m.accept()
        bad_crc = rdb[:-1] + bytes([rdb[-1] ^ 1])
        for header, payload in ((b'$EOF:short', b''),
                                (b'$EOF:' + MARK, rdb[:-10] + MARK),
                                (b'$EOF:' + MARK, bad_crc + MARK + encode("SET", "preserved", "bad"))):
            m.header(sock, header)
            if payload:
                sock.sendall(payload)
            sock, _, _ = m.accept()
            check(r('GET', 'preserved') == b'old' and r('EXISTS', 'binary') == 0,
                  'malformed header/truncated RDB/checksum refuses before replacement')
            check(not list(td.glob('temp-*.rdb')), 'invalid spool cleaned')
        r('REPLICAOF', 'NO', 'ONE')
        print('PASS EOF header/RDB/checksum refusals:', PEADB, flush=True)


if __name__ == '__main__':
    rdb, _ = snapshot()
    short_transfers(reference(), rdb)
    short_transfers(PEADB, rdb)
    invalid_snapshots(rdb)

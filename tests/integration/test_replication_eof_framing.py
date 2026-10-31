#!/usr/bin/env python3
"""EOF to disk, fragmented framing, binary data, functions, TTLs and applied offsets."""
from _replication_snapshot import (MARK, Master, PEADB, REPLID, check, encode, info,
                                   reference, server, snapshot, verify, wait_for, wait_link)
from test_replica_offset_parity import read_command


def run(binary, rdb, deadline, coalesced=False, length=False, env=None):
    with server(binary, env=env) as (r, _, td, _), Master() as m:
        r('REPLICAOF', '127.0.0.1', m.port)
        sock, stream, _ = m.accept()
        check(any(cmd[0] == 'REPLCONF' and 'eof' in cmd for cmd in m.commands),
              'replica advertises EOF only with a complete receiver')
        m.header(sock, f'${len(rdb)}'.encode() if length else b'$EOF:' + MARK)
        # Neither an incomplete snapshot nor an incomplete delimiter is a loaded dataset.
        sock.sendall(rdb[:-60])
        wait_for(lambda: info(r, 'master_sync_in_progress', 'replication') == '1', 'transfer')
        wait_for(lambda: int(info(r, 'master_sync_read_bytes')) == len(rdb) - 60,
                 'transfer byte accounting includes rolling suffix')
        total = len(rdb) if length else 0  # Redis uses zero for an unknown EOF total.
        check(int(info(r, 'master_sync_total_bytes')) == total and
              int(info(r, 'master_sync_left_bytes')) == total - (len(rdb) - 60),
              'Redis unknown-total/received/left INFO fields')
        check(info(r, 'master_link_status', 'replication') == 'down', 'not yet loaded')
        commands = encode('SELECT', 5) + encode('SET', 'tail', 'once') + encode('INCR', 'count')
        tail = rdb[-60:]
        if not length:
            sock.sendall(tail + MARK[:13])
            sock.sendall(MARK[13:39])
            tail = MARK[39:]
        sock.sendall(tail + (commands if coalesced else b''))
        wait_link(r)
        if not coalesced:
            # Redis's diskless master waits for this post-load ACK before sending commands.
            ack = read_command(stream)
            check(ack == ['REPLCONF', 'ACK', '0'], f'post-load ACK only applied offset: {ack}')
            sock.sendall(commands)
        r('SELECT', 5)
        wait_for(lambda: r('GET', 'tail') == b'once', 'first commands after trailer')
        check(r('GET', 'count') == b'1', 'read-ahead applied exactly once')
        wait_for(lambda: int(info(r, 'slave_repl_offset', 'replication')) == len(commands),
                 'offset excludes snapshot and trailer')
        r('SELECT', 0)
        verify(r, deadline)
        check((td / 'dump.rdb').read_bytes() == rdb, 'spool excludes delimiter and read-ahead')
        check(not list(td.glob('temp-*.rdb')), 'temporary spool cleaned')
        print('PASS snapshot framing', binary, 'coalesced=', coalesced, 'length=', length,
              flush=True)


if __name__ == '__main__':
    rdb, deadline = snapshot()
    run(reference(), rdb, deadline)
    run(PEADB, rdb, deadline)
    run(PEADB, rdb, deadline, coalesced=True)
    run(PEADB, rdb, deadline, coalesced=True, length=True)

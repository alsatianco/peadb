#!/usr/bin/env python3
"""All four engine pairings, length fallback, legacy SYNC and applied-offset ACKs."""
import socket

from _replication_snapshot import (PEADB, check, encode, info, reference, seed, server,
                                   verify, wait_for, wait_link)


def run(master, replica):
    with server(master) as (m, port, _, _), server(replica) as (r, _, _, _):
        deadline = seed(m)
        m('CONFIG', 'SET', 'repl-ping-replica-period', 60)
        r('REPLICAOF', '127.0.0.1', port)
        wait_link(r)
        verify(r, deadline)
        # Legacy SYNC without EOF capabilities must still receive length framing.
        with socket.create_connection(('127.0.0.1', port), timeout=8) as sock:
            with sock.makefile('rb') as stream:
                sock.sendall(encode('SYNC'))
                header = stream.readline()
                while header in (b'\n', b'\r\n'):
                    header = stream.readline()
                check(header.startswith(b'$') and not header.startswith(b'$EOF:'),
                      f'legacy SYNC length fallback: {header}')
                length = int(header[1:-2])
                body = stream.read(length)
                check(len(body) == length and body.startswith(b'REDIS') and body[-9] == 255,
                      'complete legacy SYNC RDB')
        # Select DB0 in the stream before pausing: SELECT itself is not a write and
        # Redis may apply it while holding the following SET.
        check(m('SET', 'barrier', 'ready') == '+OK' and m('WAIT', 1, 3000) == 1,
              'stream database barrier')
        wait_for(lambda: info(r, 'slave_repl_offset') == info(m, 'master_repl_offset'),
                 'all barrier/GETACK bytes applied')
        # Read bytes held by CLIENT PAUSE are not ACKed as applied, even after EOF load.
        r('CLIENT', 'PAUSE', 5000, 'WRITE')
        applied = int(info(r, 'slave_repl_offset'))
        check(m('SET', 'after', 'applied') == '+OK', 'post-snapshot write')
        wait_for(lambda: int(info(r, 'slave_read_repl_offset')) > applied, 'paused read')
        check(int(info(r, 'slave_repl_offset')) == applied, 'paused offset unchanged')
        check(m('WAIT', 1, 200) == 0, 'unapplied bytes not acknowledged')
        r('CLIENT', 'UNPAUSE')
        check(m('WAIT', 1, 3000) == 1 and r('GET', 'after') == b'applied', 'applied ACK')
        # Reconnect uses PSYNC2 and does not reload the snapshot.
        before = int(info(m, 'sync_partial_ok', 'stats'))
        check(m('CLIENT', 'KILL', 'TYPE', 'replica') == 1, 'disconnect only live replica')
        check(m('INCR', 'reconnected') == 1, 'write during reconnect')
        wait_for(lambda: int(info(m, 'sync_partial_ok', 'stats')) > before, 'partial reconnect')
        wait_for(lambda: r('GET', 'reconnected') == b'1', 'offline write replay')
        print('PASS full-sync pairing, legacy SYNC, offsets, reconnect:', master, replica,
              flush=True)


if __name__ == '__main__':
    redis = reference()
    for master, replica in ((redis, redis), (redis, PEADB), (PEADB, redis), (PEADB, PEADB)):
        run(master, replica)

#!/usr/bin/env python3
"""An aborted swap preserves chain/cache; validated publication replaces both together."""
from _replication_direct import (MARK, Master, PEADB, header, info, loading,
    reference, seed_old, server, snapshot, wait_for, wait_link)
from _replication_snapshot import VALUE


def master_client(c):
    return c('CLIENT', 'LIST', 'TYPE', 'master')


def run(binary, rdb):
    with (server(binary) as (r, port, _, _),
          server(reference()) as (chain, _, _, _), Master() as m):
        seed_old(r)
        assert chain('REPLICAOF', '127.0.0.1', port) == '+OK'
        wait_link(chain)
        assert chain('GET', 'old') == b'old-value'
        old_connection = master_client(chain)
        old_id, old_offset = info(r, 'master_replid'), info(r, 'master_repl_offset')
        old_syncs = int(info(r, 'sync_full', 'stats'))
        assert r('CONFIG', 'SET', 'repl-diskless-load', 'swapdb') == '+OK'
        assert r('CONFIG', 'SET', 'key-load-delay', 10000) == '+OK'
        assert r('CONFIG', 'SET', 'loading-process-events-interval-bytes', 1024) == '+OK'
        assert r('REPLICAOF', '127.0.0.1', m.port) == '+OK'
        sock, _, _ = m.accept()
        header(sock, old_id, rdb, False)
        sock.sendall(rdb + b'!' * 40)
        wait_for(lambda: loading(r, True), 'old chain during asynchronous decode')
        assert info(r, 'connected_slaves') == '1'
        assert info(chain, 'master_link_status') == 'up'
        assert chain('GET', 'old') == b'old-value'
        sock, _, request = m.accept()
        assert request[1:] == [old_id, str(int(old_offset) + 1)]
        assert info(r, 'master_replid') == old_id
        assert int(info(r, 'sync_full', 'stats')) == old_syncs
        assert info(chain, 'master_link_status') == 'up'
        # CLIENT LIST contains changing age/idle fields: the actual connection ID is stable.
        assert master_client(chain).split(b' ', 1)[0] == old_connection.split(b' ', 1)[0]
        assert chain('GET', 'old') == b'old-value'
        # A different snapshot of the same history must name a later offset. Reusing
        # offset zero would legitimately let the chain CONTINUE an unchanged history.
        header(sock, old_id, rdb, False, int(old_offset) + 1000)
        sock.sendall(rdb + MARK)
        wait_link(r)
        wait_for(lambda: chain('GET', 'binary') == VALUE, 'chain resnapshot after publication')
        assert chain('GET', 'old') is None
        assert chain('FCALL_RO', 'interop_f', 0) == b'from-lib'
        assert int(info(r, 'sync_full', 'stats')) > old_syncs
        assert master_client(chain).split(b' ', 1)[0] != old_connection.split(b' ', 1)[0]
        print('PASS abort cache/chain preservation and successful history replacement', binary, flush=True)


def main():
    rdb, _, _ = snapshot()
    for binary in [reference(), PEADB]:
        run(binary, rdb)


if __name__ == '__main__':
    main()

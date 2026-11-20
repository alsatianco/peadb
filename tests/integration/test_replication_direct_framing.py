#!/usr/bin/env python3
"""Length/fragmented EOF direct loads: old service, paired publication and exact suffix offsets."""
from _replication_direct import (MARK, Master, PEADB, REPLID, LOADING, check, encode,
    header, info, loading, reference, seed_old, server, snapshot, verify_snapshot,
    wait_for, wait_link)
from test_replica_offset_parity import read_command


def run(binary, rdb, deadline, stream, matching, length):
    with server(binary) as (r, _, td, _), Master() as m:
        seed_old(r)
        assert r('SAVE') == '+OK'
        original = (td / 'dump.rdb').read_bytes()
        lastsave = r('LASTSAVE')
        old_id = info(r, 'master_replid')
        assert r('CONFIG', 'SET', 'repl-diskless-load', 'swapdb') == '+OK'
        assert r('CONFIG', 'SET', 'key-load-delay', 10000) == '+OK'
        assert r('CONFIG', 'SET', 'loading-process-events-interval-bytes', 1024) == '+OK'
        assert r('REPLICAOF', '127.0.0.1', m.port) == '+OK'
        sock, source, _ = m.accept()
        header(sock, old_id if matching else REPLID, rdb, length)
        commands = encode('SELECT', 5) + encode('SET', 'tail', 'once') + encode('INCR', 'count')
        # Fragment the wire boundary; key delay exposes decoding without assuming Redis
        # services clients while blocked inside a socket read or its final trailer read.
        sock.sendall(rdb)
        if not length:
            sock.sendall(MARK[:13])
            sock.sendall(MARK[13:39])
            sock.sendall(MARK[39:] + (commands if binary == PEADB else b''))
        else:
            sock.sendall(commands)
        wait_for(lambda: loading(r, matching), 'production direct load before whole snapshot')
        assert not list(td.glob('temp-*.rdb')), 'direct reader must not spool'
        assert (td / 'dump.rdb').read_bytes() == original
        assert r('CONFIG', 'SET', 'appendonly', 'no') == LOADING
        assert r('REPLICAOF', 'NO', 'ONE') == LOADING
        if matching:
            assert r('FCALL_RO', 'old', 0) == 1
            assert r('FCALL_RO', 'old', 0) == 2
            assert r('MULTI') == '+OK'
            assert r('GET', 'old') == '+QUEUED'
            assert r('EXEC') == [b'old-value']
        wait_link(r)
        ack = read_command(source)
        while ack == []:
            ack = read_command(source)
        assert ack and ack[:2] == ['REPLCONF', 'ACK']
        assert int(ack[2]) <= len(commands), ack
        # Redis rioFreeConn(NULL) discards prefetched EOF bytes; its real master waits
        # for the post-load ACK. PeaDB additionally preserves an early coalesced suffix.
        if not length and binary != PEADB:
            sock.sendall(commands)
        assert r('SELECT', 5) == '+OK'
        wait_for(lambda: r('GET', 'count') == b'1', 'unread suffix applied once')
        assert r('GET', 'tail') == b'once'
        assert r('SELECT', 0) == '+OK'
        verify_snapshot(r, deadline, stream)
        wait_for(lambda: info(r, 'slave_repl_offset') == str(len(commands)), 'applied offset')
        assert (td / 'dump.rdb').read_bytes() == original
        assert r('LASTSAVE') == lastsave
        assert info(r, 'rdb_changes_since_last_save', 'persistence') == '3'
        assert int(info(r, 'rdb_last_load_keys_loaded', 'persistence')) == 69
        assert not list(td.glob('temp-*.rdb'))
        print('PASS direct framing', binary, matching, length, flush=True)


def main():
    rdb, deadline, stream = snapshot()
    for matching, length in [(False, False), (True, False), (False, True), (True, True)]:
        for binary in [reference(), PEADB]:
            run(binary, rdb, deadline, stream, matching, length)


if __name__ == '__main__':
    main()

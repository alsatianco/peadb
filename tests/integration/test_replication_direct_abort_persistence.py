#!/usr/bin/env python3
"""An aborted direct swap leaves old durable state; promotion rebuilds the suspended AOF."""
import socket

from _replication_direct import (Master, PEADB, header, info, reference, seed_old, server, snapshot)
from _replication_direct_persistence import aof_ready, restart


def run(binary, rdb, promote):
    with server(binary) as (r, port, td, proc), Master() as m:
        deadline = seed_old(r)
        assert r('SAVE') == '+OK'
        original = (td / 'dump.rdb').read_bytes()
        assert r('CONFIG', 'SET', 'appendfsync', 'always') == '+OK'
        assert r('CONFIG', 'SET', 'appendonly', 'yes') == '+OK'
        aof_ready(r)
        old_id = info(r, 'master_replid')
        assert r('CONFIG', 'SET', 'repl-diskless-load', 'swapdb') == '+OK'
        assert r('REPLICAOF', '127.0.0.1', m.port) == '+OK'
        sock, _, _ = m.accept()
        header(sock, old_id, rdb, False)
        sock.sendall(rdb[:-60])
        sock.shutdown(socket.SHUT_WR)
        m.accept()
        assert r('GET', 'old') == b'old-value'
        assert r('FCALL_RO', 'old', 0) == 1
        assert r('CONFIG', 'GET', 'appendonly') == [b'appendonly', b'yes']
        if promote:
            assert r('REPLICAOF', 'NO', 'ONE') == '+OK'
            aof_ready(r)
            assert r('SET', 'promoted', 'after-abort') == '+OK'
        assert (td / 'dump.rdb').read_bytes() == original
        proc.kill(); assert proc.wait(timeout=5) != 0

        def verify_restart(c):
            assert c('GET', 'old') == b'old-value'
            assert c('GET', 'binary') is None
            assert c('FCALL_RO', 'old', 0) == 1
            assert c('SELECT', 15) == '+OK'
            assert c('GET', 'local') == b'ttl' and c('PEXPIRETIME', 'local') == deadline
            assert c('SELECT', 0) == '+OK'
            assert c('GET', 'promoted') == (b'after-abort' if promote else None)
        restart(binary, port, td, verify_restart)
        print('PASS aborted direct AOF crash/restart', binary, promote, flush=True)


def main():
    rdb, _, _ = snapshot()
    for promote in [False, True]:
        for binary in [reference(), PEADB]:
            run(binary, rdb, promote)


if __name__ == '__main__':
    main()

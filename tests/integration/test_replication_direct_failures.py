#!/usr/bin/env python3
"""Aborted direct stages preserve keys/functions/history/files; corrupt input is never published."""
import socket

from _replication_direct import (MARK, Master, PEADB, REPLID, encode, header, info,
    reference, seed_old, server, snapshot)


def run(binary, rdb, matching, failure):
    with server(binary) as (r, _, td, _), Master() as m:
        deadline = seed_old(r)
        assert r('FCALL_RO', 'old', 0) == 1
        assert r('SAVE') == '+OK'
        original = (td / 'dump.rdb').read_bytes()
        old_id, old_offset = info(r, 'master_replid'), info(r, 'master_repl_offset')
        assert r('CONFIG', 'SET', 'repl-diskless-load', 'swapdb') == '+OK'
        assert r('REPLICAOF', '127.0.0.1', m.port) == '+OK'
        sock, _, _ = m.accept()
        payload = rdb
        if failure == 'function':
            code = b'#!lua name=broken\nthis is not valid lua'
            payload = b'REDIS0011\x00\x07partial\x03bad\xf5' + bytes([len(code)]) + code
            payload += b'\xff' + b'\x00' * 8
        elif failure == 'checksum':
            payload = rdb[:-1] + bytes([rdb[-1] ^ 1])
        header(sock, old_id if matching else REPLID, payload, False)
        if failure == 'short':
            sock.sendall(payload[:-60])
        else:
            sock.sendall(payload + (b'!' * 40 if failure == 'trailer' else MARK) +
                         encode('SET', 'old', 'must-not-run'))
        sock.shutdown(socket.SHUT_WR)
        # A fresh PSYNC proves the failed attempt unwound and its producer owns the socket
        # again. Leave the new handshake waiting, so no later attempt can hide data loss.
        _, _, request = m.accept()
        assert request[0] == 'PSYNC'
        assert r('GET', 'old') == b'old-value'
        assert r('EXISTS', 'partial', 'binary', 'tail') == 0
        assert r('FCALL_RO', 'old', 0) == 2, 'old closure identity must survive'
        assert r('FCALL_RO', 'interop_f', 0) == '-ERR Function not found'
        assert r('SELECT', 15) == '+OK'
        assert r('GET', 'local') == b'ttl' and r('PEXPIRETIME', 'local') == deadline
        assert r('SELECT', 0) == '+OK'
        assert info(r, 'master_replid') == old_id
        assert info(r, 'master_repl_offset') == old_offset
        assert request[1:] == [old_id, str(int(old_offset) + 1)]
        assert (td / 'dump.rdb').read_bytes() == original
        assert not list(td.glob('temp-*.rdb'))
        assert r('REPLICAOF', 'NO', 'ONE') == '+OK'
        assert r('GET', 'old') == b'old-value'
        assert r('FCALL_RO', 'old', 0) == 3
        print('PASS direct abort', binary, matching, failure, flush=True)


def empty_failure(binary, rdb):
    with server(binary) as (r, _, td, _), Master() as m:
        from _replication_direct import OLD_FUNCTION
        assert r('FUNCTION', 'LOAD', OLD_FUNCTION) == b'old'
        assert r('CONFIG', 'SET', 'repl-diskless-load', 'on-empty-db') == '+OK'
        assert r('REPLICAOF', '127.0.0.1', m.port) == '+OK'
        sock, _, _ = m.accept()
        header(sock, REPLID, rdb, False)
        sock.sendall(rdb[:-60])
        sock.shutdown(socket.SHUT_WR)
        _, _, request = m.accept()
        assert request[1:] == ['?', '-1']
        assert r('DBSIZE') == 0
        assert r('FCALL_RO', 'old', 0) == '-ERR Function not found'
        assert r('FUNCTION', 'LIST') == []
        assert not list(td.glob('temp-*.rdb'))
        print('PASS empty direct abort clears partial keys and old functions', binary, flush=True)


def main():
    rdb, _, _ = snapshot()
    for failure in ['short', 'trailer']:
        for matching in [False, True]:
            for binary in [reference(), PEADB]:
                run(binary, rdb, matching, failure)
    # Redis exits for corrupt RDB content; PeaDB's deliberate refusal keeps a healthy
    # process and the complete old state, as for its established file-preflight path.
    for failure in ['checksum', 'function']:
        run(PEADB, rdb, True, failure)
    for binary in [reference(), PEADB]:
        empty_failure(binary, rdb)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Request filters select separate fork cohorts; RDB-only never streams commands."""
import subprocess
import tempfile
from pathlib import Path

from _readiness import wait_until_ready
from test_replica_resync_parity import ports
from test_rdb_redis_interop import Conn

from _replication_snapshot import PEADB, check, encode, info, reference, server, seed, wait_for
from test_replication_pipe_relay import Peer
from test_rdb_child_parent_loss import children


def request(peer, *args):
    peer.sock.sendall(encode('REPLCONF', *args))
    return peer.line()


def run(binary):
    with server(binary) as (c, port, work, proc):
        seed(c)
        c('CONFIG', 'SET', 'repl-diskless-sync', 'no', 'repl-ping-replica-period', 3600)
        peer = Peer(port, eof=False, request=False)
        try:
            check(request(peer, 'rdb-filter-only', 'functions') == b'+OK\r\n', 'filter option')
            peer.request()
            check(peer.line() == b'-ERR Filtered replica requires EOF capability\r\n',
                  'filtered non-EOF sync rejected before attachment')
            check(info(c, 'connected_slaves') == '0', 'refused request is not a replica')
        finally:
            peer.close()
        # Filtered RDBs force a pipe even when diskless-sync is disabled. They
        # cannot overwrite a durable full snapshot with only functions.
        check(c('SAVE') == '+OK', 'durable snapshot control')
        previous = (work / 'dump.rdb').read_bytes()
        for filters, expected_functions in (('functions', 1), ('', 0)):
            peer = Peer(port, request=False)
            try:
                check(request(peer, 'rdb-only', 1, 'rdb-filter-only', filters) == b'+OK\r\n', 'request')
                peer.request()
                peer.read_header()
                check(peer.header.startswith(b'$EOF:'), 'filter forces socket target')
                c('SET', 'never-stream', 'suffix')
                payload = peer.snapshot()
                path = work / 'filtered.rdb'
                path.write_bytes(payload)
                result = subprocess.run([str(Path(reference()).with_name('redis-check-rdb')),
                                         str(path)], capture_output=True)
                check(result.returncode == 0, 'filtered RDB CRC/structure')
                # Cross-load through a real isolated engine, inspect both keys and libraries.
                with tempfile.TemporaryDirectory(prefix='peadb-filter-load-') as td:
                    Path(td, 'dump.rdb').write_bytes(payload)
                    load_port = ports(1)[0]
                    with open(Path(td, 'load.log'), 'wb') as log:
                        loader = subprocess.Popen([reference(), '--bind', '127.0.0.1', '--port',
                                                   str(load_port), '--dir', td, '--save', ''],
                                                  stdout=log, stderr=subprocess.STDOUT)
                    try:
                        wait_until_ready(load_port, process=loader)
                        loaded = Conn(load_port)
                        try:
                            check(loaded('DBSIZE') == 0, 'filter excludes data')
                            check(len(loaded('FUNCTION', 'LIST')) == expected_functions, 'function filter')
                            if expected_functions:
                                check(loaded('FCALL_RO', 'interop_f', 0) == b'from-lib', 'loaded callback')
                        finally:
                            loaded.close()
                    finally:
                        loader.terminate()
                        loader.wait(timeout=5)
                check(peer.stream.read() == b'', 'RDB-only closes without command suffix or ACK')
                wait_for(lambda: not children(proc.pid), 'RDB-only child reaped')
                check((work / 'dump.rdb').read_bytes() == previous, 'filtered sync preserves durable RDB')
            finally:
                peer.close()
        # Invalid option parsing cannot silently return success.
        for args, error in ((('rdb-only', 2), b'value is out of range'),
                            (('rdb-only', 'bad'), b'value is not an integer'),
                            (('rdb-filter-only', 'keys'), b'Unsupported rdb-filter-only option: keys'),
                            (('rdb-filter-only', '"'), b'Missing rdb-filter-only values'),
                            (('unknown', 1), b'Unrecognized REPLCONF option: unknown')):
            peer = Peer(port, request=False)
            try:
                reply = request(peer, *args)
                check(reply.startswith(b'-ERR ') and error in reply, f'option error {reply!r}')
            finally:
                peer.close()
        # Ordinary RDB-only requests work on disk too and do not retain post-fork bytes.
        peer = Peer(port, eof=False, request=False)
        try:
            check(request(peer, 'rdb-only', 1) == b'+OK\r\n', 'disk RDB-only request')
            peer.request()
            peer.read_header()
            check(not peer.header.startswith(b'$EOF:'), 'disk RDB-only length frame')
            check(peer.snapshot().startswith(b'REDIS'), 'disk RDB-only data')
            check(peer.stream.read() == b'', 'disk RDB-only disconnect')
        finally:
            peer.close()
        print(binary, 'filtered request/refusal/CRC/cross-load/RDB-only passed', flush=True)


if __name__ == '__main__':
    for binary in (reference(), PEADB):
        run(binary)

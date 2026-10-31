#!/usr/bin/env python3
"""An owned RDB child cannot publish an old snapshot after its server dies.

Redis detects parent loss through its child-info pipe. Exercise both BGSAVE and
replication's disk snapshot with a genuinely delayed serializer, then restart
against the same durable backup. Run through the integration supervisor on Linux
so adopted children are reaped by their owner; a zombie is already stopped.
"""
import os
import pathlib
import signal
import socket
import subprocess
import tempfile
import time

from _readiness import ObservedProcess, wait_until_ready
from test_replica_offset_parity import Conn, PEADB, REDIS, encode


def wait_for(predicate, message, timeout=6):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.01)
    raise AssertionError(message)


def children(parent):
    rows = subprocess.check_output(['ps', '-axo', 'pid=,ppid=,stat='], text=True)
    return [int(pid) for pid, ppid, state in (row.split() for row in rows.splitlines())
            if int(ppid) == parent and not state.startswith('Z')]


def stop(proc):
    if proc.poll() is None:
        proc.kill()
    proc.wait(timeout=5)


def run(binary, replication):
    with tempfile.TemporaryDirectory(prefix='peadb-rdb-parent-') as work:
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        argv = [binary, '--bind', '127.0.0.1', '--port', str(port), '--dir', work,
                '--save', '', '--dbfilename', 'owned.rdb', '--rdbcompression', 'no',
                '--repl-diskless-sync', 'no', '--shutdown-timeout', '0']
        proc, conn, peer, child = None, None, None, None
        with open(pathlib.Path(work) / 'server.log', 'wb') as log:
            try:
                proc = subprocess.Popen(argv, stdout=log, stderr=log)
                wait_until_ready(port, process=proc)
                conn = Conn(port)
                assert conn('SET', 'durable', 'before-fork') == '+OK'
                assert conn('SAVE') == '+OK'
                backup = pathlib.Path(work) / 'owned.rdb'
                original = backup.read_bytes()
                # More than Redis's 1024-key child-info sampling interval; natural
                # completion takes >16s, well beyond the bounded parent-loss wait.
                count = 8192
                conn.sock.sendall(b''.join(encode('SET', f'pending:{i}', 'x' * 256)
                                           for i in range(count)))
                for _ in range(count):
                    assert conn.read() == '+OK'
                assert conn('CONFIG', 'SET', 'rdb-key-save-delay', '2000') == '+OK'
                if replication:
                    peer = socket.create_connection(('127.0.0.1', port), timeout=5)
                    peer.sendall(encode('SYNC'))
                else:
                    assert conn('BGSAVE') == '+Background saving started'
                owned = wait_for(lambda: children(proc.pid), 'no owned RDB child')
                assert len(owned) == 1, owned
                child = owned[0]
                temp = pathlib.Path(work) / f'temp-{child}.rdb'
                wait_for(lambda: temp.exists() and temp.stat().st_size > 16384,
                         'child did not begin writing the delayed snapshot')
                assert backup.read_bytes() == original, 'snapshot published before parent loss'
                assert conn('PING') == '+PONG', 'parent must remain responsive during save'
                proc.kill()
                proc.wait(timeout=5)
                wait_for(lambda: ObservedProcess(child).poll() is not None,
                         f'{pathlib.Path(binary).name}: RDB child survived parent SIGKILL')
                assert backup.read_bytes() == original, 'orphan child replaced durable backup'
                if pathlib.Path(binary).name != 'redis-server':
                    assert not temp.exists(), 'cancelled RDB child left its temp file'
                conn.close()
                conn = None
                if peer:
                    peer.close()
                    peer = None
                # The old backup remains loadable and cannot resurrect the unsaved
                # dataset. A fresh ordinary BGSAVE still succeeds after restart.
                proc = subprocess.Popen(argv, stdout=log, stderr=log)
                wait_until_ready(port, process=proc)
                conn = Conn(port)
                assert conn('GET', 'durable') == 'before-fork'
                assert conn('DBSIZE') == 1
                assert conn('SET', 'after-restart', 'saved') == '+OK'
                assert conn('BGSAVE') == '+Background saving started'
                wait_for(lambda: 'rdb_bgsave_in_progress:0' in conn('INFO', 'persistence'),
                         'fresh child did not finish')
                assert 'rdb_last_bgsave_status:ok' in conn('INFO', 'persistence')
                assert backup.read_bytes() != original
                assert not children(proc.pid), 'successful child was not reaped'
                print(pathlib.Path(binary).name,
                      'SYNC' if replication else 'BGSAVE', 'parent loss/restart passed', flush=True)
            finally:
                if conn:
                    conn.close()
                if peer:
                    peer.close()
                if proc:
                    stop(proc)
                # Only our verified descendant, never a global process-name kill.
                if child and ObservedProcess(child).poll() is None:
                    os.kill(child, signal.SIGKILL)
                    wait_for(lambda: ObservedProcess(child).poll() is not None,
                             'owned fixture child did not stop')


if __name__ == '__main__':
    assert REDIS and 'v=7.2.5' in subprocess.check_output([REDIS, '--version'], text=True), \
        'pinned Redis 7.2.5 required'
    for binary in (REDIS, os.environ.get('PEADB_BIN', PEADB)):
        for replication in (False, True):
            run(binary, replication)

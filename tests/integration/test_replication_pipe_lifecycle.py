#!/usr/bin/env python3
"""Owned slow-pipe, child kill, disconnect, reconfiguration and FD-reuse regressions."""
import os
from pathlib import Path
import signal
import socket
import time

from _readiness import ObservedProcess
from _replication_snapshot import PEADB, check, encode, info, reference, server, wait_for
from test_rdb_child_parent_loss import children
from test_replication_pipe_relay import Peer


def cpu_seconds(pid):
    # Linux needs its own bound: upstream's `set os [catch {exec uname}]` never
    # takes the Linux branch. macOS exercises lifecycle without claiming CPU proof.
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')


def slow_peer(c, port, proc):
    peer = Peer(port)
    check(peer.header.startswith(b'$EOF:'), 'socket-target header')
    owned = children(proc.pid)
    check(len(owned) == 1, 'exact owned pipe child')
    # 32 MiB with no reads cannot fit in socket/pipe/shared chunk buffers.
    time.sleep(0.25)
    check(children(proc.pid) == owned, 'child must remain behind backpressure')
    return peer, owned[0]


def healthy(c, port, proc):
    c('CONFIG', 'SET', 'rdb-key-save-delay', 0)
    c('FLUSHALL')
    c('SET', 'healthy', 'after-cleanup')
    peer = Peer(port)
    try:
        check(peer.header.startswith(b'$EOF:'), 'fresh pipe after cleanup')
        check(peer.snapshot().startswith(b'REDIS'), 'fresh snapshot after cleanup')
        wait_for(lambda: not children(proc.pid), 'fresh child reaped')
        peer.sock.sendall(encode('REPLCONF', 'ACK', peer.offset))
        check(c('PING') == '+PONG', 'server alive after fresh transfer')
    finally:
        peer.close()
    wait_for(lambda: info(c, 'connected_slaves') == '0', 'fresh peer cleanup')


def run(binary):
    with server(binary) as (c, port, work, proc):
        c('CONFIG', 'SET', 'repl-ping-replica-period', 3600)
        c('CONFIG', 'SET', 'client-output-buffer-limit', 'replica 0 0 0')
        value = os.urandom(32 * 1024 * 1024)
        for action in ('kill-child', 'disconnect', 'flushall', 'kill-self', 'self-flush', 'self-role', 'role', 'timeout'):
            c('SET', 'large', value)
            baseline = int(info(c, 'used_memory', 'memory'))
            peer, child = slow_peer(c, port, proc)
            try:
                # The parent may hold kernel socket buffers plus one bounded user
                # chunk. It must not accumulate the serialized 32 MiB snapshot.
                check(int(info(c, 'used_memory', 'memory')) - baseline < 2 * 1024 * 1024,
                      'parent serialized snapshot memory must remain bounded')
                if Path('/proc').is_dir():
                    before = cpu_seconds(proc.pid)
                    time.sleep(0.6)
                    check(cpu_seconds(proc.pid) - before < 0.25, 'suspended pipe must not busy-spin')
                started = time.monotonic()
                check(c('PING') == '+PONG' and time.monotonic() - started < 0.5,
                      'ordinary PING under pipe backpressure')
                if action == 'kill-child':
                    os.kill(child, signal.SIGKILL)
                elif action == 'disconnect':
                    peer.close()
                    peer = None
                elif action == 'flushall':
                    check(c('FLUSHALL') == '+OK', 'FLUSHALL during relay')
                elif action == 'kill-self':
                    # Dispatch from a relay recipient itself: callback must defer
                    # its close, preserving the current command/session reference.
                    peer.sock.sendall(encode('CLIENT', 'KILL', 'TYPE', 'replica', 'SKIPME', 'no'))
                elif action == 'self-flush':
                    peer.sock.sendall(encode('FLUSHALL'))
                elif action == 'self-role':
                    with socket.socket() as listener:
                        listener.bind(('127.0.0.1', 0))
                        listener.listen()
                        peer.sock.sendall(encode('REPLICAOF', '127.0.0.1', listener.getsockname()[1]) +
                                          encode('REPLICAOF', 'NO', 'ONE'))
                elif action == 'role':
                    with socket.socket() as listener:
                        listener.bind(('127.0.0.1', 0))
                        listener.listen()
                        check(c('REPLICAOF', '127.0.0.1', listener.getsockname()[1]) == '+OK',
                              'role change during relay')
                    check(c('REPLICAOF', 'NO', 'ONE') == '+OK', 'promotion after cancellation')
                elif action == 'timeout':
                    c('CONFIG', 'SET', 'repl-timeout', 1)
                wait_for(lambda: not children(proc.pid), f'{action}: child reaped', timeout=5)
                wait_for(lambda: info(c, 'connected_slaves') == '0', f'{action}: recipients closed', timeout=5)
                check(not list(work.glob('temp-*.rdb')) and not (work / 'dump.rdb').exists(),
                      'pipe cancellation cannot publish an RDB')
            finally:
                if peer:
                    peer.close()
                c('CONFIG', 'SET', 'repl-timeout', 60)
            healthy(c, port, proc)
        # Parent loss during a blocked write must close every consumer/completion
        # descriptor inherited by the child, even when there are no further key checks.
        c('SET', 'large', value)
        peer, child = slow_peer(c, port, proc)
        try:
            proc.kill()
            proc.wait(timeout=5)
            wait_for(lambda: ObservedProcess(child).poll() is not None,
                     'blocked pipe child survived parent SIGKILL')
        finally:
            peer.close()
            if ObservedProcess(child).poll() is None:
                os.kill(child, signal.SIGKILL)
                wait_for(lambda: ObservedProcess(child).poll() is not None, 'owned child cleanup')
        print(Path(binary).name, 'slow/all-drop/kill/flush/self/role/timeout/parent passed', flush=True)



if __name__ == '__main__':
    for binary in (reference(), PEADB):
        run(binary)

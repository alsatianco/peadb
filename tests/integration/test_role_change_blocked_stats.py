#!/usr/bin/env python3
"""Role changes charge UNBLOCKED failures to each cancelled blocking invocation.

Pinned Redis 7.2.5 runs first. Cover all supported key-blocking families, WAIT,
multiple waiters, renamed list moves, both role-change aliases and successful /
timed-out / disconnected controls. Read real replies and eventual INFO counters;
do not count a deferred command a second time when it is cancelled.

Known boundaries remain explicit: PeaDB counts calls on initial dispatch and
uses renamed command names for stats. Redis counts a cancelled WAIT twice and
marks the successful REPLICAOF as failed through its global error counter. This
test requires one actual invocation per PeaDB call and no failure on its +OK.
"""
from collections import Counter
from contextlib import ExitStack
import os
from pathlib import Path
import subprocess
import tempfile

from _redis_path import redis_server_path
from test_legacy_write_propagation import Server, connect
from test_set_propagation_parity import encode, wait_for

ROOT = Path(__file__).resolve().parents[2]
UNBLOCKED = ('-UNBLOCKED force unblock from blocking operation, instance state changed '
             '(master -> replica?)')


def section(client, name):
    return dict(line.split(':', 1) for line in client('INFO', name).splitlines()
                if ':' in line and not line.startswith('#'))


def command_stats(client, command):
    raw = section(client, 'commandstats').get(f'cmdstat_{command.lower()}', '')
    fields = dict(item.split('=', 1) for item in raw.split(',') if '=' in item)
    return tuple(int(fields.get(key, 0))
                 for key in ('calls', 'rejected_calls', 'failed_calls'))


def error_count(client):
    return int(section(client, 'stats')['total_error_replies'])


def block(stack, client, port, command, count):
    waiter = connect(stack, port)
    waiter.sock.sendall(encode(*command))
    wait_for(lambda: int(section(client, 'clients')['blocked_clients']) == count,
             f'blocked {command[0]}')
    return waiter


def controls(stack, client, port):
    """Success, natural timeout and disconnect must not become role-change failures."""
    assert client('CONFIG', 'RESETSTAT') == '+OK'
    waiter = block(stack, client, port, ('BLPOP', 'served', 0), 1)
    assert client('RPUSH', 'served', 'value') == 1
    assert waiter.read() == ['served', 'value']
    timed = connect(stack, port)
    assert timed('BLPOP', 'timeout', 0.01) is None
    assert command_stats(client, 'blpop') == (2, 0, 0)
    abandoned = block(stack, client, port, ('BRPOP', 'abandoned', 0), 1)
    abandoned.close()
    # ExitStack would otherwise close the same makefile a second time (safe).
    wait_for(lambda: int(section(client, 'clients')['blocked_clients']) == 0,
             'disconnected waiter removal')
    assert error_count(client) == 0
    assert section(client, 'errorstats') == {}


def exercise(binary, label, renamed):
    with tempfile.TemporaryDirectory(prefix='peadb-role-stats-') as tmp, ExitStack() as stack:
        target = Server(stack, binary, Path(tmp) / 'target', renamed=renamed)
        master = Server(stack, binary, Path(tmp) / 'master')
        client = connect(stack, target.port)
        upstream = connect(stack, master.port)
        controls(stack, client, target.port)
        assert client('XGROUP', 'CREATE', 'stream', 'group', '0', 'MKSTREAM') == '+OK'
        assert upstream('SET', 'upstream-value', 'present') == '+OK'
        assert client('CONFIG', 'RESETSTAT') == '+OK'
        move = 'PULL' if renamed else 'BRPOPLPUSH'
        commands = [
            ('BLPOP', 'list-a', 0), ('BLPOP', 'list-b', 0),
            ('BRPOP', 'list-c', 0),
            ('BLMOVE', 'list-d', 'destination', 'LEFT', 'RIGHT', 0),
            (move, 'list-e', 'destination', 0),
            ('BLMPOP', 0, 2, 'list-f', 'list-g', 'LEFT', 'COUNT', 2),
            ('BZPOPMIN', 'zset-a', 0), ('BZPOPMAX', 'zset-b', 0),
            ('BZMPOP', 0, 1, 'zset-c', 'MIN', 'COUNT', 2),
            ('XREAD', 'BLOCK', 0, 'STREAMS', 'read-stream', '0'),
            ('XREADGROUP', 'GROUP', 'group', 'consumer', 'BLOCK', 0,
             'STREAMS', 'stream', '>'),
            ('WAIT', 1, 0),
        ]
        waiters = [block(stack, client, target.port, cmd, index)
                   for index, cmd in enumerate(commands, 1)]
        names = Counter(cmd[0].lower() for cmd in commands)
        if renamed and label == 'redis':
            names['brpoplpush'] = names.pop('pull')
        for name in names:
            _, rejected, failed = command_stats(client, name)
            assert (rejected, failed) == (0, 0), (name, rejected, failed)
        assert error_count(client) == 0
        change = 'SLAVEOF' if renamed else 'REPLICAOF'
        assert client(change, '127.0.0.1', master.port) == '+OK'
        for command, waiter in zip(commands, waiters):
            assert waiter.read() == UNBLOCKED, command
        assert int(section(client, 'clients')['blocked_clients']) == 0
        for name, count in names.items():
            calls = count * 2 if label == 'redis' and name == 'wait' else count
            actual = command_stats(client, name)
            assert actual == (calls, 0, count), (label, name, actual, (calls, 0, count))
        assert error_count(client) == len(commands)
        assert section(client, 'errorstats') == {
            'errorstat_UNBLOCKED': f'count={len(commands)}'}
        if label == 'peadb':
            # Errors sent to other sessions must not fail the role-changing command.
            assert command_stats(client, 'replicaof') == (1, 0, 0)
        wait_for(lambda: client('GET', 'upstream-value') == 'present',
                 'real full sync after cancellation')
        stable = {name: command_stats(client, name) for name in names}
        # A no-op same-master REPLICAOF and promotion cannot count old blocks again.
        assert client(change, '127.0.0.1', master.port).startswith('+OK')
        assert client('REPLICAOF', 'NO', 'ONE') == '+OK'
        assert {name: command_stats(client, name) for name in names} == stable
        assert error_count(client) == len(commands)
        assert section(client, 'errorstats') == {
            'errorstat_UNBLOCKED': f'count={len(commands)}'}
        print(f'{label}, renamed={renamed}: cancelled {len(commands)} calls, '
              'exact replies/counters and sync/control checks passed', flush=True)


def main():
    reference = redis_server_path()
    assert reference, 'pinned Redis 7.2.5 is required'
    version = subprocess.run([reference, '--version'], check=True,
                             capture_output=True, text=True).stdout
    assert ' v=7.2.5 ' in version, version
    for label, binary in [('redis', reference),
                          ('peadb', os.environ.get('PEADB_SERVER_BIN', str(ROOT / 'peadb-server')))]:
        for renamed in (False, True):
            exercise(binary, label, renamed)


if __name__ == '__main__':
    main()

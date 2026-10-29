#!/usr/bin/env python3
"""GETSET and both BRPOPLPUSH paths propagate Redis 7.2.5's canonical writes.

Compare the actual PSYNC stream and AOF, then verify values, absolute TTLs and
single application on real replicas and after AOF restart. BLMOVE must retain
LMOVE, including RIGHT LEFT: the command's identity determines the rewrite.
The pinned reference is mandatory and runs first; test all four engine pairings.
"""
from collections import Counter
from contextlib import ExitStack
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

from _readiness import wait_until_ready
from _redis_path import redis_server_path
from test_set_propagation_parity import (
    Conn, FakeReplica, aof_commands, aof_file, encode, info, stop, wait_for,
)

ROOT = Path(__file__).resolve().parents[2]


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class Server:
    def __init__(self, stack, binary, directory, *, aof=False, renamed=False):
        self.port = free_port()
        self.binary = binary
        self.directory = directory
        directory.mkdir(parents=True)
        self.log = stack.enter_context((directory / 'server.log').open('ab'))
        self.renamed = renamed
        self.aof = aof
        self.proc = None
        stack.callback(self.stop)
        self.start()

    def start(self):
        self.proc = subprocess.Popen([
            self.binary, '--bind', '127.0.0.1', '--port', str(self.port),
            '--dir', str(self.directory), '--save', '', '--loglevel', 'notice',
            '--shutdown-timeout', '0', '--repl-diskless-sync', 'no',
            '--repl-diskless-sync-delay', '0', '--appendonly', 'yes' if self.aof else 'no',
            '--appendfsync', 'always',
            *(['--rename-command', 'BRPOPLPUSH', 'PULL'] if self.renamed else []),
        ], stdout=self.log, stderr=subprocess.STDOUT)
        try:
            wait_until_ready(self.port, process=self.proc, timeout=15)
        except BaseException:
            self.stop()
            raise

    def stop(self):
        if self.proc is not None:
            stop(self.proc)

    def crash(self):
        self.proc.kill()
        self.proc.wait()


def connect(stack, port):
    client = Conn(port)
    stack.callback(client.close)
    return client


def snapshot(client):
    result = {}
    for db in (0, 9):
        assert client('SELECT', db) == '+OK'
        for key in sorted(client('KEYS', '*')):
            kind = client('TYPE', key)
            if kind == '+string':
                value = client('GET', key)
            else:
                assert kind == '+list', (key, kind)
                value = client('LRANGE', key, 0, -1)
            result[db, key] = (value, client('PEXPIRETIME', key))
    return result


def exercise(client, waiter, expected, move):
    """Record only mutations; all expected rewrites come from the pinned source."""
    deadline = int(time.time() * 1000) + 3600000

    def write(command, reply, propagated=None):
        actual = client(*command)
        assert actual == reply, (command, reply, actual)
        expected.extend([list(map(str, command))] if propagated is None else propagated)

    # GETSET removes the old TTL, even when the replacement equals the old value;
    # a nil old value still creates a key. Errors must emit nothing.
    write(('SET', 'string', 'old', 'PXAT', deadline), '+OK')
    write(('GETSET', 'string', 'new'), 'old', [['SET', 'string', 'new']])
    assert client('PEXPIRETIME', 'string') == -1
    write(('GETSET', 'absent', 'created'), None, [['SET', 'absent', 'created']])
    write(('GETSET', 'empty-string', ''), None, [['SET', 'empty-string', '']])
    write(('PEXPIREAT', 'string', deadline), 1)
    write(('GETSET', 'string', 'new'), 'new', [['SET', 'string', 'new']])
    write(('RPUSH', 'wrong', 'keep'), 1)
    wrongtype = '-WRONGTYPE Operation against a key holding the wrong kind of value'
    write(('GETSET', 'wrong', 'lost'), wrongtype, [])

    # Rotating a singleton into itself must not delete/recreate its TTL. The
    # canonical RPOPLPUSH on replicas/replay must preserve it just as BLMOVE does.
    write(('RPUSH', 'self-ttl', 'only'), 1)
    write(('PEXPIREAT', 'self-ttl', deadline), 1)
    write((move, 'self-ttl', 'self-ttl', 0), 'only',
          [['RPOPLPUSH', 'self-ttl', 'self-ttl']])
    assert client('PEXPIRETIME', 'self-ttl') == deadline, 'BRPOPLPUSH self TTL'
    write(('BLMOVE', 'self-ttl', 'self-ttl', 'LEFT', 'RIGHT', 0), 'only',
          [['LMOVE', 'self-ttl', 'self-ttl', 'LEFT', 'RIGHT']])
    assert client('PEXPIRETIME', 'self-ttl') == deadline, 'BLMOVE self TTL'

    # Immediate moves preserve surviving source/destination TTLs. Multiple source
    # elements make a duplicated move observable in replica/restart values.
    write(('RPUSH', 'source', 'a', 'b', 'c'), 3)
    write(('RPUSH', 'destination', 'base'), 1)
    write(('PEXPIREAT', 'source', deadline), 1)
    write(('PEXPIREAT', 'destination', deadline), 1)
    write((move, 'source', 'destination', 0), 'c',
          [['RPOPLPUSH', 'source', 'destination']])
    # A real BLMOVE of the same directions must remain LMOVE.
    write(('BLMOVE', 'source', 'destination', 'RIGHT', 'LEFT', 0), 'b',
          [['LMOVE', 'source', 'destination', 'RIGHT', 'LEFT']])
    assert client('PEXPIRETIME', 'source') == deadline
    assert client('PEXPIRETIME', 'destination') == deadline
    write((move, 'destination', 'destination', 0), 'base',
          [['RPOPLPUSH', 'destination', 'destination']])

    # Error and timeout paths do not propagate. A bad destination must not pop
    # from the source; the same applies when the source appears after blocking.
    write(('SET', 'bad-destination', 'not-a-list'), '+OK')
    write((move, 'source', 'bad-destination', 0), wrongtype, [])
    write((move, 'wrong-source', 'destination', 0.02), None, [])
    write((move, 'source', 'destination', -1),
          '-ERR timeout is negative', [])

    def wake(command, push, reply, effect, *, expiring=False):
        before = int(info(client, 'blocked_clients'))
        waiter.sock.sendall(encode(*command))
        wait_for(lambda: int(info(client, 'blocked_clients')) == before + 1,
                 f'blocked {command[0]}')
        if expiring:
            assert client('MULTI') == '+OK'
            assert client(*push) == '+QUEUED'
            assert client('PEXPIREAT', push[1], deadline) == '+QUEUED'
            assert client('EXEC') == [len(push) - 2, 1]
            expected.extend([['MULTI'], list(push),
                             ['PEXPIREAT', push[1], str(deadline)], ['EXEC']])
        else:
            write(push, len(push) - 2)
        assert waiter.read() == reply, command
        expected.extend(effect)
        wait_for(lambda: int(info(client, 'blocked_clients')) == before,
                 f'unblocked {command[0]}')

    wake((move, 'wake-source', 'destination', 0),
         ('RPUSH', 'wake-source', 'x', 'y', 'z'), 'z',
         [['RPOPLPUSH', 'wake-source', 'destination']])
    wake(('BLMOVE', 'bl-source', 'destination', 'LEFT', 'RIGHT', 0),
         ('RPUSH', 'bl-source', 'left', 'right'), 'left',
         [['LMOVE', 'bl-source', 'destination', 'LEFT', 'RIGHT']])
    wake(('BLMOVE', 'bl-right', 'destination', 'RIGHT', 'LEFT', 0),
         ('RPUSH', 'bl-right', 'first', 'last'), 'last',
         [['LMOVE', 'bl-right', 'destination', 'RIGHT', 'LEFT']])
    wake((move, 'wake-self', 'wake-self', 0),
         ('RPUSH', 'wake-self', 'first', 'last'), 'last',
         [['RPOPLPUSH', 'wake-self', 'wake-self']])
    # EXEC sets the TTL before waking the blocked singleton self-move.
    wake((move, 'wake-self-ttl', 'wake-self-ttl', 0),
         ('RPUSH', 'wake-self-ttl', 'only'), 'only',
         [['RPOPLPUSH', 'wake-self-ttl', 'wake-self-ttl']], expiring=True)
    assert client('PEXPIRETIME', 'wake-self-ttl') == deadline
    wake(('BLMOVE', 'bl-self-ttl', 'bl-self-ttl', 'LEFT', 'RIGHT', 0),
         ('RPUSH', 'bl-self-ttl', 'only'), 'only',
         [['LMOVE', 'bl-self-ttl', 'bl-self-ttl', 'LEFT', 'RIGHT']], expiring=True)
    assert client('PEXPIRETIME', 'bl-self-ttl') == deadline
    # Single-element source deletion, and a delayed destination error.
    wake((move, 'single', 'new-list', 0),
         ('LPUSH', 'single', 'only'), 'only', [['RPOPLPUSH', 'single', 'new-list']])
    wake((move, 'failed-wake', 'bad-destination', 0),
         ('RPUSH', 'failed-wake', 'first', 'last'), wrongtype, [])

    # Nested execution must use the same rewrites and preserve their order,
    # without an extra BLMOVE dispatch or duplicate propagation.
    for mode in ('exec', 'lua'):
        key = mode + '-string'
        write(('SET', key, 'before', 'PXAT', deadline), '+OK')
        commands = [('GETSET', key, 'after'),
                    (move, 'wake-source', 'destination', 0),
                    (move, 'empty', 'destination', 0),
                    ('BLMOVE', 'empty', 'destination', 'RIGHT', 'LEFT', 0)]
        if mode == 'exec':
            assert client('MULTI') == '+OK'
            for command in commands:
                assert client(*command) == '+QUEUED'
            assert client('EXEC') == ['before', 'y', None, None]
        else:
            script = ("return {redis.call('GETSET',KEYS[1],'after'),"
                      f"redis.call('{move}',KEYS[2],KEYS[3],0),"
                      f"redis.call('{move}',KEYS[4],KEYS[3],0),"
                      "redis.call('BLMOVE',KEYS[4],KEYS[3],'RIGHT','LEFT',0)}")
            assert client('EVAL', script, 4, key, 'wake-source', 'destination', 'empty') == [
                'before', 'x', None, None]
        expected.extend([['MULTI'], ['SET', key, 'after'],
                         ['RPOPLPUSH', 'wake-source', 'destination'], ['EXEC']])

    # Exercise the selected database of both immediate and blocked writes.
    assert client('SELECT', 9) == '+OK'
    assert waiter('SELECT', 9) == '+OK'
    expected.append(['SELECT', '9'])
    write(('GETSET', 'string', 'db9'), None, [['SET', 'string', 'db9']])
    wake((move, 'wake-source', 'destination', 0),
         ('RPUSH', 'wake-source', 'db9-first', 'db9-last'), 'db9-last',
         [['RPOPLPUSH', 'wake-source', 'destination']])
    write(('SET', 'end', '1'), '+OK')


def check_pair(master_name, master_binary, replica_name, replica_binary, *, renamed=False):
    label = f'{master_name}->{replica_name}' + (' (renamed BRPOPLPUSH)' if renamed else '')
    with tempfile.TemporaryDirectory(prefix='peadb-legacy-propagation-') as scratch:
        with ExitStack() as stack:
            master = Server(stack, master_binary, Path(scratch) / 'master', aof=True, renamed=renamed)
            replica = Server(stack, replica_binary, Path(scratch) / 'replica')
            client = connect(stack, master.port)
            reader = connect(stack, replica.port)
            assert reader('REPLICAOF', '127.0.0.1', master.port) == '+OK'
            wait_for(lambda: info(reader, 'master_link_status') == 'up', 'replica link', 15)
            fake = FakeReplica(master.port)
            stack.callback(fake.close)
            waiter = connect(stack, master.port)
            expected = [['SELECT', '0']]
            exercise(client, waiter, expected, 'PULL' if renamed else 'BRPOPLPUSH')
            stream = fake.commands_until(['SET', 'end', '1'])
            # PING ensures the previous command's AOF flush has completed too.
            assert client('PING') == '+PONG'
            aof = aof_commands(aof_file(master_name, str(master.directory)))
            mismatches = {name: actual for name, actual in [('stream', stream), ('AOF', aof)]
                          if actual != expected}
            assert not mismatches, (label, mismatches, 'expected', expected)
            assert reader('SELECT', 9) == '+OK'
            wait_for(lambda: reader('GET', 'end') == '1', 'replicated marker')
            before = snapshot(client)
            assert snapshot(reader) == before, (label, 'replica contents')
            counts = Counter(c[0].lower() for c in expected)
            stats = reader('INFO', 'commandstats')
            for name in ('set', 'rpoplpush', 'lmove'):
                assert f'cmdstat_{name}:calls={counts[name]},' in stats, (label, name, stats)
            assert 'cmdstat_getset:' not in stats and 'cmdstat_brpoplpush:' not in stats
            # Five actual and two empty BLMOVEs were submitted; BRPOPLPUSH is
            # not a second dispatch and must not add BLMOVE calls on the master.
            stats = client('INFO', 'commandstats')
            assert 'cmdstat_blmove:calls=7,' in stats, (label, stats)
            replica.stop()
            master.crash()
        # Close sockets before replacing the crashed master, then require actual
        # AOF readiness in a separate ownership scope.
        with ExitStack() as stack:
            # Discard any full-sync snapshot to prove replay comes from the AOF.
            (master.directory / 'dump.rdb').unlink(missing_ok=True)
            master.log = stack.enter_context((master.directory / 'server.log').open('ab'))
            stack.callback(master.stop)
            master.start()
            reloaded = connect(stack, master.port)
            assert snapshot(reloaded) == before, (label, 'AOF replay contents')
    print(f'{label}: stream, AOF, replica counters, values and TTLs passed', flush=True)


def main():
    reference = redis_server_path()
    assert reference, 'Redis 7.2.5 reference is required'
    version = subprocess.check_output([reference, '--version'], text=True)
    assert ' v=7.2.5 ' in version, version
    peadb = os.environ.get('PEADB_SERVER_BIN', str(ROOT / 'peadb-server'))
    for master, replica in [('redis', 'redis'), ('peadb', 'redis'),
                            ('redis', 'peadb'), ('peadb', 'peadb')]:
        binaries = {'redis': reference, 'peadb': peadb}
        check_pair(master, binaries[master], replica, binaries[replica])
    for name, binary in [('redis', reference), ('peadb', peadb)]:
        check_pair(name, binary, 'redis', reference, renamed=True)


if __name__ == '__main__':
    main()

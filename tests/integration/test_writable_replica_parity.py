#!/usr/bin/env python3
"""Writable replicas follow Redis 7.2.5 local-write and expiry ownership rules.

Run the pinned reference first, then every master/replica engine pairing. Local
writes are durable in AOF but do not enter the upstream stream or chained
replicas. Read lookups hide expired master keys; write lookups delete them.
Active expiry tracks only locally assigned TTLs, even after read-only returns.
Ordinary reconnect preserves local state; a full sync replaces it and functions.
"""
from contextlib import ExitStack
import os
from pathlib import Path
import subprocess
import tempfile
import time

from _readiness import wait_until_ready
from _redis_path import redis_server_path
from test_legacy_write_propagation import connect, free_port
from test_set_propagation_parity import aof_commands, aof_file, encode, stop, wait_for
from test_role_change_blocked_stats import section

ROOT = Path(__file__).resolve().parents[2]
READONLY = "-READONLY You can't write against a read only replica."


class Server:
    def __init__(self, stack, binary, directory, writable=False):
        self.binary, self.directory = binary, directory
        self.port = free_port()
        directory.mkdir()
        self.config = directory / 'server.conf'
        self.config.write_text('slave-read-only no\n' if writable else '')
        self.log = stack.enter_context((directory / 'server.log').open('ab'))
        self.proc = None
        stack.callback(self.stop)
        self.start()

    def start(self):
        self.proc = subprocess.Popen([
            self.binary, str(self.config), '--bind', '127.0.0.1', '--port', str(self.port),
            '--dir', str(self.directory), '--save', '', '--loglevel', 'notice',
            '--shutdown-timeout', '0', '--repl-diskless-sync', 'no',
            # Keep the upstream quiet while asserting that local writes add no offset bytes.
            '--repl-ping-replica-period', '3600',
            '--repl-diskless-sync-delay', '0', '--enable-debug-command', 'yes',
            '--appendfsync', 'always',
        ], stdout=self.log, stderr=subprocess.STDOUT)
        try:
            wait_until_ready(self.port, process=self.proc, timeout=15)
        except BaseException:
            self.log.flush()
            print((self.directory / 'server.log').read_text(errors='replace')[-12000:], flush=True)
            raise

    def stop(self):
        if self.proc is not None:
            stop(self.proc)


def number(client, name, group='stats'):
    return int(section(client, group)[name])


def attach(client, master, marker):
    assert client('REPLICAOF', '127.0.0.1', master.port) == '+OK'
    wait_for(lambda: section(client, 'replication')['master_link_status'] == 'up', marker)


def exercise(master_binary, replica_binary, reference, label, startup):
    with tempfile.TemporaryDirectory(prefix='peadb-writable-') as tmp, ExitStack() as stack:
        root = Path(tmp)
        master = Server(stack, master_binary, root / 'master')
        replica = Server(stack, replica_binary, root / 'replica', writable=startup)
        downstream = Server(stack, reference, root / 'downstream')
        m = connect(stack, master.port)
        r = connect(stack, replica.port)
        d = connect(stack, downstream.port)
        try:
            assert m('SET', 'from-master', 'initial') == '+OK'
            attach(r, master, 'initial full sync')
            assert r('GET', 'from-master') == 'initial'
            if not startup:
                assert r('SET', 'denied', 'x') == READONLY
                assert r('CONFIG', 'SET', 'replica-read-only', 'no') == '+OK'
            assert r('CONFIG', 'GET', 'slave-read-only') == ['slave-read-only', 'no']
            assert section(r, 'replication')['slave_read_only'] == '0'
            # Typed CONFIG SET stays atomic and aliases address the same live value.
            bad = r('CONFIG', 'SET', 'replica-read-only', 'yes', 'maxmemory', '-1')
            assert bad.startswith('-ERR'), bad
            assert r('CONFIG', 'GET', 'replica-read-only') == ['replica-read-only', 'no']
            assert r('CONFIG', 'REWRITE') == '+OK'
            assert 'replica-read-only no' in replica.config.read_text()
            attach(d, replica, 'chain initial full sync before local writes')
            assert r('CONFIG', 'SET', 'appendonly', 'yes') == '+OK'
            wait_for(lambda: number(r, 'aof_rewrite_in_progress', 'persistence') == 0 and
                     number(r, 'aof_rewrite_scheduled', 'persistence') == 0,
                     'replica AOF baseline')
            offset = number(r, 'slave_repl_offset', 'replication')
            # All data types, transactions, legacy and shebang scripts, cached scripts/functions.
            assert r('SET', 'local', 'value') == '+OK'
            assert r('RPUSH', 'local-list', 'a', 'b') == 2
            assert r('HSET', 'local-hash', 'field', 'value') == 1
            assert r('SADD', 'local-set', 'member') == 1
            assert r('ZADD', 'local-zset', 1, 'member') == 1
            assert r('XADD', 'local-stream', '1-0', 'field', 'value') == '1-0'
            assert r('LRANGE', 'local-list', 0, -1) == ['a', 'b']
            assert r('HGETALL', 'local-hash') == ['field', 'value']
            assert r('SMEMBERS', 'local-set') == ['member']
            assert r('ZRANGE', 'local-zset', 0, -1, 'WITHSCORES') == ['member', '1']
            assert r('XRANGE', 'local-stream', '-', '+') == [['1-0', ['field', 'value']]]
            waiter = connect(stack, replica.port)
            waiter.sock.sendall(encode('BLPOP', 'local-blocked', 0))
            wait_for(lambda: number(r, 'blocked_clients', 'clients') == 1, 'local blocked pop')
            assert r('RPUSH', 'local-blocked', 'served') == 1
            assert waiter.read() == ['local-blocked', 'served']
            assert r('MULTI') == '+OK'
            assert r('SET', 'transaction', '1') == '+QUEUED'
            assert r('INCR', 'transaction') == '+QUEUED'
            assert r('EXEC') == ['+OK', 2]
            assert r('EVAL', "return redis.call('incr',KEYS[1])", 1, 'script') == 1
            sha = r('SCRIPT', 'LOAD', "return redis.call('incr',KEYS[1])")
            assert r('EVALSHA', sha, 1, 'script') == 2
            assert r('EVAL', "#!lua\nreturn redis.call('set',KEYS[1],'yes')", 1,
                     'shebang') == '+OK'
            library = ("#!lua name=local_functions\nredis.register_function('local_write', "
                       "function(keys,args) return redis.call('incr',keys[1]) end)")
            assert r('FUNCTION', 'LOAD', library) == 'local_functions'
            assert r('FCALL', 'local_write', 1, 'function') == 1
            assert r('EVAL_RO', "return redis.call('set','readonly-script','bad')", 0).startswith('-ERR')
            assert r('GET', 'readonly-script') is None
            # No upstream byte accounting or downstream local deltas.
            assert number(r, 'slave_repl_offset', 'replication') == offset
            assert m('SET', 'barrier', 'after-local') == '+OK'
            wait_for(lambda: d('GET', 'barrier') == 'after-local', 'master bytes pass downstream')
            for c in (m, d):
                for key in ('local', 'local-list', 'local-hash', 'local-set', 'local-zset',
                            'local-stream', 'transaction', 'script', 'function', 'shebang'):
                    assert c('EXISTS', key) == 0, (label, key)
            # Live readonly toggles gate ordinary, queued and scripted writes, never master bytes.
            queued = connect(stack, replica.port)
            assert queued('MULTI') == '+OK'
            assert queued('SET', 'denied-at-exec', 'x') == '+QUEUED'
            assert r('CONFIG', 'SET', 'slave-read-only', 'yes') == '+OK'
            assert queued('EXEC').startswith('-EXECABORT')
            assert r('EXISTS', 'denied-at-exec') == 0
            assert section(r, 'replication')['slave_read_only'] == '1'
            assert r('MULTI') == '+OK'
            assert r('SET', 'denied', 'x') == READONLY
            assert r('EXEC').startswith('-EXECABORT')
            assert r('EVAL', "return redis.call('set','denied','x')", 0).startswith('-READONLY')
            assert r('FCALL', 'local_write', 1, 'function').startswith('-READONLY')
            assert m('SET', 'from-master', 'readonly-update') == '+OK'
            wait_for(lambda: r('GET', 'from-master') == 'readonly-update', 'readonly master application')
            assert r('CONFIG', 'SET', 'replica-read-only', 'no') == '+OK'
            observer = connect(stack, replica.port)
            assert r('CONFIG', 'SET', 'notify-keyspace-events', 'Ex') == '+OK'
            pattern = '__keyevent@*__:expired'
            assert observer('PSUBSCRIBE', pattern) == ['psubscribe', pattern, 1]
            expiry_checks(m, r)
            events = [observer.read() for _ in range(8)]
            expected_events = [('__keyevent@9__:expired', 'read-hidden'),
                               ('__keyevent@9__:expired', 'expired-list')]
            expected_events += [(f'__keyevent@{db}__:expired', key)
                                for db in (0, 9) for key in ('local-expire', 'renamed', 'copied')]
            assert sorted((event[2], event[3]) for event in events) == sorted(expected_events), events
            expiry_lookup_matrix(m, r)
            # Partial reconnect must preserve local state and expiry bookkeeping.
            full = number(m, 'sync_full')
            assert m('CLIENT', 'KILL', 'TYPE', 'replica') == 1
            assert m('SET', 'reconnect-barrier', 'done') == '+OK'
            wait_for(lambda: r('GET', 'reconnect-barrier') == 'done', 'partial reconnect')
            assert number(m, 'sync_full') == full
            assert r('GET', 'local') == 'value'
            # AOF stores local changes and local expiry DELs, independently of propagation.
            commands = aof_commands(aof_file('redis' if replica_binary == reference else 'peadb', replica.directory))
            assert ['SET', 'local', 'value'] in commands, commands
            assert ['DEL', 'read-hidden'] in commands, commands
            assert ['DEL', 'local-expire'] in commands, commands
            assert ['DEL', 'getdel-source'] in commands, commands
            # A different master forces real full replacement, including functions and tracker.
            replacement = Server(stack, master_binary, root / 'replacement')
            n = connect(stack, replacement.port)
            assert n('SET', 'replacement', 'ready') == '+OK'
            assert n('SET', 'owned-before-sync', 'master', 'PX', 3600000) == '+OK'
            assert r('SET', 'owned-before-sync', 'local', 'PX', 3600000) == '+OK'
            assert number(r, 'slave_expires_tracked_keys') > 0
            assert r('CONFIG', 'SET', 'replica-serve-stale-data', 'no') == '+OK'
            assert r('REPLICAOF', '127.0.0.1', free_port()) == '+OK'
            assert r('SET', 'stale-write', 'refused').startswith('-MASTERDOWN')
            assert r('CONFIG', 'SET', 'replica-serve-stale-data', 'yes') == '+OK'
            attach(r, replacement, 'full replacement')
            wait_for(lambda: r('GET', 'replacement') == 'ready', 'replacement values')
            for key in ('local', 'transaction', 'script', 'function', 'shebang', 'local-list'):
                assert r('EXISTS', key) == 0, key
            assert r('FUNCTION', 'LIST') == []
            assert r('GET', 'owned-before-sync') == 'master'
            assert number(r, 'slave_expires_tracked_keys') == 0
            # Promotion is writable regardless of the retained read-only setting.
            assert r('CONFIG', 'SET', 'replica-read-only', 'yes') == '+OK'
            assert r('REPLICAOF', 'NO', 'ONE') == '+OK'
            assert r('SET', 'promoted', 'yes') == '+OK'
            # Crash/AOF reload keeps local durable writes; the TTL ownership table is transient.
            wait_for(lambda: number(r, 'aof_rewrite_in_progress', 'persistence') == 0 and
                     number(r, 'aof_rewrite_scheduled', 'persistence') == 0,
                     'replacement AOF rewrite completed before durability check')
            assert r('SET', 'durable-local', 'present') == '+OK'
            assert ['SET', 'durable-local', 'present'] in aof_commands(
                aof_file('redis' if replica_binary == reference else 'peadb', replica.directory))
            replica.proc.kill()
            replica.proc.wait()
            replica.config.write_text('appendonly yes\nreplica-read-only no\n')
            replica.start()
            rr = connect(stack, replica.port)
            assert rr('GET', 'durable-local') == 'present'
            assert rr('GET', 'replacement') == 'ready'
            assert rr('GET', 'local') is None
            assert number(rr, 'slave_expires_tracked_keys') == 0
            print(f'{label}, startup={startup}: local writes/expiry/AOF/chain/reconnect/full sync passed',
                  flush=True)
        except BaseException:
            for path in root.glob('*/server.log'):
                print(f'--- {path.parent.name} log ---\n{path.read_text(errors="replace")[-12000:]}',
                      flush=True)
            raise


def expiry_checks(m, r):
    assert m('DEBUG', 'SET-ACTIVE-EXPIRE', 0) == '+OK'
    assert r('DEBUG', 'SET-ACTIVE-EXPIRE', 0) == '+OK'
    # Isolate in DB 9; master owns all these expiries, active local tracking must stay empty.
    assert m('SELECT', 9) == '+OK'
    assert r('SELECT', 9) == '+OK'
    deadline = int(time.time() * 1000) + 1500
    assert m('SET', 'read-hidden', '41', 'PXAT', deadline) == '+OK'
    assert m('RPUSH', 'expired-list', 'a') == 1
    assert m('PEXPIREAT', 'expired-list', deadline) == 1
    assert m('SET', 'untouched-master', 'keep', 'PXAT', deadline) == '+OK'
    assert m('SET', 'inherited-ttl', '41', 'PXAT', deadline) == '+OK'
    assert m('RPUSH', 'inherited-self', 'only') == 1
    assert m('PEXPIREAT', 'inherited-self', deadline) == 1
    assert m('SET', 'expiry-barrier', 'ready') == '+OK'
    wait_for(lambda: r('GET', 'expiry-barrier') == 'ready', 'expiry setup replicated')
    assert r('INCR', 'inherited-ttl') == 42
    assert r('LMOVE', 'inherited-self', 'inherited-self', 'LEFT', 'RIGHT') == 'only'
    assert number(r, 'slave_expires_tracked_keys') == 0
    before = number(r, 'expired_keys')
    size = r('DBSIZE')
    wait_for(lambda: int(time.time() * 1000) > deadline, 'master TTL passed')
    assert r('GET', 'read-hidden') is None
    assert r('DBSIZE') == size
    # A read followed by a write in the SAME script must still delete the old physical key.
    assert r('EVAL', "local x=redis.call('get',KEYS[1]); "
             "return redis.call('incr',KEYS[1])", 1, 'read-hidden') == 1
    assert r('LPOP', 'expired-list') is None
    assert number(r, 'expired_keys') == before + 2
    assert r('DBSIZE') == size - 1
    # Locally assigned expiries are tracked across databases, explicit expiry transfers and
    # read-only toggles. No active deletion of the untouched master-owned TTL is allowed.
    assert r('DEBUG', 'SET-ACTIVE-EXPIRE', 1) == '+OK'
    for db in (0, 9):
        assert r('SELECT', db) == '+OK'
        assert r('SET', 'local-expire', 'value', 'PX', 300) == '+OK'
        assert r('SET', 'renamed-expire', 'value', 'PX', 300) == '+OK'
        assert r('RENAME', 'renamed-expire', 'renamed') == '+OK'
        assert r('COPY', 'renamed', 'copied') == 1
        assert r('SET', 'persisted', 'keep', 'PX', 300) == '+OK'
        assert r('PERSIST', 'persisted') == 1
    assert r('CONFIG', 'SET', 'slave-read-only', 'yes') == '+OK'
    wait_for(lambda: number(r, 'slave_expires_tracked_keys') == 0, 'local active expiry reclaimed')
    for db in (0, 9):
        assert r('SELECT', db) == '+OK'
        assert r('GET', 'local-expire') is None
        assert r('GET', 'renamed') is None
        assert r('GET', 'copied') is None
        assert r('GET', 'persisted') == 'keep'
    assert number(r, 'expired_keys') == before + 8
    # The master-owned key is hidden but still physically counted on the replica.
    assert r('GET', 'untouched-master') is None
    assert r('DBSIZE') == 6  # three inherited/untouched TTLs, read-hidden, barrier, persisted
    assert r('CONFIG', 'SET', 'replica-read-only', 'no') == '+OK'
    assert m('SELECT', 0) == '+OK'
    assert r('SELECT', 0) == '+OK'


def expiry_lookup_matrix(m, r):
    """A write lookup expires even when the resulting mutation is a no-op."""
    assert m('SELECT', 15) == '+OK'
    assert r('SELECT', 15) == '+OK'
    assert r('DEBUG', 'SET-ACTIVE-EXPIRE', 0) == '+OK'
    cases = [
        (('SET', '{key}', 'new', 'XX'), None),
        (('MSETNX', '{key}', 'new', 'live', 'changed'), 0),
        (('LPUSHX', '{key}', 'new'), 0),
        (('RPOP', '{key}'), None),
        (('LREM', '{key}', 1, 'new'), 0),
        (('LTRIM', '{key}', 0, 1), '+OK'),
        (('LINSERT', '{key}', 'BEFORE', 'a', 'b'), 0),
        (('LMPOP', 1, '{key}', 'LEFT'), None),
        (('BLMOVE', '{key}', 'destination', 'LEFT', 'RIGHT', 0.01), None),
        (('ZADD', '{key}', 'XX', 1, 'member'), 0),
        (('ZREMRANGEBYRANK', '{key}', 0, -1), 0),
        (('ZREMRANGEBYSCORE', '{key}', '-inf', '+inf'), 0),
        (('ZREMRANGEBYLEX', '{key}', '-', '+'), 0),
        (('ZMPOP', 1, '{key}', 'MIN'), None),
        (('BZMPOP', 0.01, 1, '{key}', 'MIN'), None),
        (('XADD', '{key}', 'NOMKSTREAM', '*', 'field', 'value'), None),
        (('XTRIM', '{key}', 'MAXLEN', 0), 0),
        (('SMOVE', '{key}', 'destination', 'member'), 0),
    ]
    deadline = int(time.time() * 1000) + 1500
    for index in range(len(cases)):
        assert m('SET', f'matrix-{index}', 'expired', 'PXAT', deadline) == '+OK'
    for key in ('copy-source', 'sort-source', 'set-source', 'zset-source', 'getex-source', 'xack-source', 'getdel-source'):
        assert m('SET', key, 'expired', 'PXAT', deadline) == '+OK'
    assert m('SET', 'live', 'keep') == '+OK'
    wait_for(lambda: r('GET', 'live') == 'keep', 'lookup matrix replicated')
    wait_for(lambda: int(time.time() * 1000) > deadline, 'lookup matrix TTL passed')
    expired = number(r, 'expired_keys')
    for index, (command, expected) in enumerate(cases):
        args = tuple(f'matrix-{index}' if arg == '{key}' else arg for arg in command)
        actual = r(*args)
        assert actual == expected, (args, expected, actual)
        assert number(r, 'expired_keys') == expired + index + 1, args
    # Write commands with read-only source lookups must leave those physical entries alone.
    for command, expected in [(('GETDEL', 'getdel-source'), None),
                              (('GETEX', 'getex-source'), None),
                              (('XACK', 'xack-source', 'group', '1-0'), 0),
                              (('COPY', 'copy-source', 'copy-dest'), 0),
                              (('SORT', 'sort-source', 'STORE', 'sort-dest'), 0),
                              (('SUNIONSTORE', 'set-dest', 'set-source'), 0),
                              (('ZUNIONSTORE', 'zset-dest', 1, 'zset-source'), 0)]:
        assert r(*command) == expected, command
        assert number(r, 'expired_keys') == expired + len(cases), command
    assert r('DEBUG', 'SET-ACTIVE-EXPIRE', 1) == '+OK'
    assert m('SELECT', 0) == '+OK'
    assert r('SELECT', 0) == '+OK'


def main():
    reference = redis_server_path()
    assert reference, 'pinned Redis 7.2.5 is required'
    version = subprocess.run([reference, '--version'], check=True, capture_output=True,
                             text=True).stdout
    assert ' v=7.2.5 ' in version, version
    peadb = os.environ.get('PEADB_SERVER_BIN', str(ROOT / 'peadb-server'))
    for master, replica, label in [(reference, reference, 'Redis -> Redis'),
                                   (reference, peadb, 'Redis -> PeaDB'),
                                   (peadb, reference, 'PeaDB -> Redis'),
                                   (peadb, peadb, 'PeaDB -> PeaDB')]:
        exercise(master, replica, reference, label, startup=label.endswith('PeaDB'))


if __name__ == '__main__':
    main()

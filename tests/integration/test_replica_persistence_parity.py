#!/usr/bin/env python3
"""Replica full-sync files, AOF effects and configured roles survive restart (delta 45).

Run the disk-backed Redis 7.2.5 cases first, then PeaDB, including a real Redis
master feeding PeaDB. Kill the replica without saving so an in-memory round trip
cannot hide missing persistence. Runtime REPLICAOF alone does not edit config.
"""
import os
import pathlib
import subprocess
import sys
import tempfile
import time

from test_replica_gates_parity import (
    Conn, PEADB, REDIS, READONLY, info_section, start, stop, wait_for,
)

PORT = 6738
LIB = "#!lua name=persist\nredis.register_function{function_name='answer', callback=function() return 42 end, flags={'no-writes'}}"


def kill(proc):
    proc.kill()
    proc.wait(timeout=10)


def ready(c):
    wait_for(lambda: info_section(c, 'replication')['master_link_status'] == 'up', 'full sync')


def aof_ready(c):
    wait_for(lambda: info_section(c, 'persistence')['aof_rewrite_in_progress'] == '0'
             and info_section(c, 'persistence')['aof_rewrite_scheduled'] == '0', 'AOF rewrite')
    assert info_section(c, 'persistence')['aof_last_bgrewrite_status'] == 'ok'


def seed(c, streams=False):
    assert c('SET', 'initial', 'snapshot') == '+OK'
    assert c('SET', 'ttl', 'kept', 'PX', 60000) == '+OK'
    assert c('FUNCTION', 'LOAD', LIB) == 'persist'
    assert c('SELECT', 3) == '+OK'
    assert c('SET', 'counter', 10) == '+OK'
    assert c('RPUSH', 'list', 'a', 'b') == 2
    assert c('HSET', 'hash', 'f', 'v') == 1
    assert c('SADD', 'set', 'a', 'b') == 2
    assert c('ZADD', 'zset', 1, 'a', 2, 'b') == 2
    if streams:  # PeaDB's RDB stream encoding is still an explicit cross-engine delta.
        assert c('XADD', 'stream', '1-0', 'f', 'one') == '1-0'
        assert c('XGROUP', 'CREATE', 'stream', 'g', '0') == '+OK'
        assert c('XREADGROUP', 'GROUP', 'g', 'alice', 'STREAMS', 'stream', '>')
    assert c('SELECT', 0) == '+OK'


def verify(c, streamed=False, streams=False):
    actual = c('GET', 'initial')
    assert actual == 'snapshot', actual
    assert c('GET', 'obsolete') is None
    assert c('GET', 'ttl') == 'kept'
    assert 0 < c('PTTL', 'ttl') < 60000
    assert c('FCALL_RO', 'answer', 0) == 42
    assert c('SELECT', 3) == '+OK'
    assert c('GET', 'counter') == ('13' if streamed else '10')
    assert c('LRANGE', 'list', 0, -1) == (['b', 'c', 'd'] if streamed else ['a', 'b'])
    assert c('HGET', 'hash', 'f') == ('v2' if streamed else 'v')
    assert sorted(c('SMEMBERS', 'set')) == ['a', 'b']
    assert c('ZRANGE', 'zset', 0, -1, 'WITHSCORES') == ['a', '1', 'b', '2']
    if streams:
        assert c('XLEN', 'stream') == (2 if streamed else 1)
        assert c('XPENDING', 'stream', 'g')[0] == (0 if streamed else 1)
        stream_info = c('XINFO', 'STREAM', 'stream')
        stream_info = dict(zip(stream_info[::2], stream_info[1::2]))
        assert stream_info['entries-added'] == (2 if streamed else 1)
    assert c('SELECT', 0) == '+OK'


def check(binary, master_binary, work, mode):
    streams = binary == master_binary and binary != REDIS
    master_dir = str(pathlib.Path(work, 'master'))
    replica_dir = str(pathlib.Path(work, 'replica'))
    master = start(master_binary, PORT, master_dir,
                   ('--repl-diskless-sync', 'no', '--requirepass', 'sync-password'))
    replica = None
    m = r = None
    extra = ('--masterauth', 'sync-password', '--dbfilename', 'received.rdb')
    if mode == 'aof':
        extra += ('--appendonly', 'yes', '--appendfsync', 'always')
    try:
        m = Conn(PORT)
        assert m('AUTH', 'sync-password') == '+OK'
        seed(m, streams)
        startup = ('--slaveof', '127.0.0.1', str(PORT)) if mode == 'runtime-aof' else ()
        replica = start(binary, PORT + 1, replica_dir, (*extra, *startup))
        r = Conn(PORT + 1)
        if not startup:
            # An old save child must be stopped before the received dump replaces its file.
            for i in range(100):
                assert r('SET', f'obsolete:{i}', 'old-local-data') == '+OK'
            assert r('SET', 'obsolete', 'old-local-data') == '+OK'
            assert r('SAVE') == '+OK'
            assert r('CONFIG', 'SET', 'rdb-key-save-delay', 200000) == '+OK'
            assert r('BGSAVE').startswith('+Background saving')
            assert info_section(r, 'persistence')['rdb_bgsave_in_progress'] == '1'
            assert r('REPLICAOF', '127.0.0.1', PORT) == '+OK'
        ready(r)
        wait_for(lambda: info_section(r, 'persistence')['rdb_bgsave_in_progress'] == '0',
                 'old RDB child reaped')
        verify(r, streams=streams)
        assert pathlib.Path(replica_dir, 'received.rdb').is_file(), 'full-sync RDB was deleted'
        if mode == 'runtime-aof':
            assert r('CONFIG', 'SET', 'rdb-key-save-delay', 200000) == '+OK'
            assert r('CONFIG', 'SET', 'appendonly', 'yes') == '+OK'
            extra += ('--appendonly', 'yes', '--appendfsync', 'always')
        if mode != 'rdb':
            # Redis may schedule the rewrite until its killed RDB child is reaped.
            wait_for(lambda: info_section(r, 'persistence')['aof_rewrite_in_progress'] == '1',
                     'replacement AOF rewrite started')
            assert m('SELECT', 3) == '+OK'
            assert m('MULTI') == '+OK'
            for command in [('INCRBY', 'counter', 2), ('LPOP', 'list'),
                            ('RPUSH', 'list', 'c'), ('HSET', 'hash', 'f', 'v2')]:
                assert m(*command) == '+QUEUED'
            assert m('EXEC') == [12, 'a', 2, 0]
            assert m('EVAL', "redis.call('incr',KEYS[1]); return redis.call('rpush',KEYS[2],'d')",
                     2, 'counter', 'list') == 3
            if streams:
                assert m('XADD', 'stream', '2-0', 'f', 'two') == '2-0'
                assert m('XACK', 'stream', 'g', '1-0') == 1
            assert m('WAIT', 1, 5000) == 1
            verify(r, True, streams)
            aof_ready(r)  # the writes made during the rewrite must join its diff buffer
            # Save points do not replace the AOF. Promotion must keep the entire received history.
            assert r('REPLICAOF', 'NO', 'ONE') == '+OK'
            assert r('SET', 'promoted', 'local') == '+OK'
        kill(replica)
        r.close()
        r = None
        stop(master)

        m.close()
        m = None
        # A runtime role change alone is not persisted by Redis either.
        replica = start(binary, PORT + 1, replica_dir, extra)
        r = Conn(PORT + 1)
        assert r('ROLE')[0] == 'master'
        verify(r, mode != 'rdb', streams)
        if mode != 'rdb':
            assert r('GET', 'promoted') == 'local'
        kill(replica)
        r.close()
        r = None
        # The configured role must hold even when the master is unavailable, and even if
        # REPLICAOF was renamed/disabled: startup must not dispatch a public command name.
        alias = 'slaveof' if mode == 'runtime-aof' else 'replicaof'
        config = pathlib.Path(replica_dir, 'replica.conf')
        config.write_text(f'{alias} 127.0.0.1 {PORT}\nrename-command REPLICAOF ""\n')
        replica = start(binary, PORT + 1, replica_dir,
                        (*extra, '--include', str(config)))
        r = Conn(PORT + 1)
        assert r('ROLE')[0] == 'slave'
        assert r('CONFIG', 'GET', 'replicaof') == ['replicaof', f'127.0.0.1 {PORT}']
        assert r('SET', 'forbidden', 'write') == READONLY
        verify(r, mode != 'rdb', streams)
    finally:
        for c in (m, r):
            if c:
                c.close()
        if replica:
            stop(replica)
        stop(master)


def check_expired_startup(binary, work):
    """RDB loading on a configured replica keeps expired keys until the master's DEL."""
    proc = start(binary, PORT + 1, work)
    c = Conn(PORT + 1)
    try:
        assert c('SET', 'expired', 'v', 'PX', 1000) == '+OK'
        assert c('SAVE') == '+OK'
        kill(proc)
        c.close()
        time.sleep(1.05)
        proc = start(binary, PORT + 1, work, ('--replicaof', '127.0.0.1', '0'))
        c = Conn(PORT + 1)
        assert c('ROLE')[0] == 'slave'
        assert c('DBSIZE') == 1, 'replica role was applied after the RDB load'
        assert c('GET', 'expired') is None
        assert c('DBSIZE') == 1, 'replica deleted an expired key without its master'
    finally:
        c.close()
        stop(proc)


def check_rename_failure(binary, master_binary, work):
    """A filesystem failure refuses the sync; promotion restarts the suspended AOF."""
    replica_dir = pathlib.Path(work, 'replica')
    replica_dir.mkdir(parents=True)
    (replica_dir / 'blocked.rdb').mkdir()
    master = start(master_binary, PORT, str(pathlib.Path(work, 'master')),
                   ('--repl-diskless-sync', 'no'))
    replica = None
    m = r = None
    extra = ('--appendonly', 'yes', '--appendfsync', 'always', '--dbfilename', 'blocked.rdb')
    try:
        m = Conn(PORT)
        assert m('SET', 'incoming', 'new') == '+OK'
        replica = start(binary, PORT + 1, str(replica_dir), extra)
        r = Conn(PORT + 1)
        assert r('SET', 'old', 'kept') == '+OK'
        assert r('REPLICAOF', '127.0.0.1', PORT) == '+OK'
        wait_for(lambda: 'Failed trying to rename the temp DB' in
                 (replica_dir / 'server.log').read_text(), 'failed sync rename')
        assert info_section(r, 'replication')['master_link_status'] == 'down'
        assert r('CONFIG', 'GET', 'appendonly') == ['appendonly', 'yes']
        wait_for(lambda: r('GET', 'old') == 'kept', 'preserved old dataset')
        assert r('GET', 'incoming') is None
        assert (replica_dir / 'blocked.rdb').is_dir()
        assert r('REPLICAOF', 'NO', 'ONE') == '+OK'
        aof_ready(r)
        kill(replica)
        r.close()
        r = None
        replica = start(binary, PORT + 1, str(replica_dir), extra)
        r = Conn(PORT + 1)
        assert r('GET', 'old') == 'kept'
        assert r('GET', 'incoming') is None
    finally:
        for c in (m, r):
            if c:
                c.close()
        if replica:
            stop(replica)
        stop(master)


def main():
    target = os.environ.get('PEADB_SERVER_BIN', PEADB)
    selected = sys.argv[1:] or ['rdb', 'aof', 'runtime-aof']
    if REDIS:
        version = subprocess.check_output([REDIS, '--version'], text=True)
        assert 'v=7.2.5 ' in version, version
    with tempfile.TemporaryDirectory(prefix='peadb-replica-persistence-') as tmp:
        for mode in selected:
            if REDIS:
                check(REDIS, REDIS, str(pathlib.Path(tmp, 'redis-' + mode)), mode)
                print('PASS Redis 7.2.5:', mode, flush=True)
            check(target, REDIS or target, str(pathlib.Path(tmp, 'peadb-' + mode)), mode)
            print('PASS PeaDB:', mode, flush=True)
        if not sys.argv[1:]:
            if REDIS:
                check_expired_startup(REDIS, str(pathlib.Path(tmp, 'redis-expired')))
            check_expired_startup(target, str(pathlib.Path(tmp, 'peadb-expired')))
            check(target, target, str(pathlib.Path(tmp, 'peadb-master')), 'aof')
            check_rename_failure(target, REDIS or target, str(pathlib.Path(tmp, 'rename-failure')))
            print('PASS configured replica expiry and PeaDB master persistence', flush=True)


if __name__ == '__main__':
    main()

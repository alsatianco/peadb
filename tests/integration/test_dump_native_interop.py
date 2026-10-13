#!/usr/bin/env python3
"""Redis 7.2.5 compact DUMP/RESTORE/MIGRATE and stream snapshot interoperability.

Run Redis first, then PeaDB. Compare values, absolute TTLs and stream history,
consumer groups, empty consumers and pending entries (including deleted IDs).
Native stream payloads also survive RDB reload, real full sync and AOF replay.
"""
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time

from test_rdb_redis_interop import Conn, REDIS, crc64
from test_restore_propagation_parity import start, stop

ROOT = pathlib.Path(__file__).resolve().parents[2]
PEADB = sys.argv[1] if len(sys.argv) > 1 else str(ROOT / 'peadb-server')


def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def payload(body):
    body += b'\x0b\x00'
    return body + crc64(body).to_bytes(8, 'little')


def wait_for(test, timeout=8):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if test():
            return
        time.sleep(.02)
    raise AssertionError('condition did not become true')


def fields(reply):
    return dict(zip(reply[::2], reply[1::2]))


def stream_state(c, key):
    info = fields(c('XINFO', 'STREAM', key, 'FULL', 'COUNT', 0))
    # Macro nodes and allocation size depend on the in-memory encoding (delta 16).
    for k in (b'radix-tree-keys', b'radix-tree-nodes'):
        info.pop(k, None)
    groups = []
    for raw in info[b'groups']:
        group = fields(raw)
        consumers = []
        for rawc in group[b'consumers']:
            consumer = fields(rawc)
            # These are absolute timestamps, unlike the idle times in XINFO CONSUMERS.
            consumers.append(consumer)
        group[b'consumers'] = sorted(consumers, key=lambda x: x[b'name'])
        groups.append(group)
    info[b'groups'] = sorted(groups, key=lambda x: x[b'name'])
    return info


def populate(c):
    assert c('FLUSHALL') == '+OK'
    c('SET', 'str', b'\x00\xff' + b'abc' * 100)
    c('RPUSH', 'list', '1', '-32768', b'\x00\xff', 'x' * 5000)
    c('SADD', 'ints16', '-32768', '32767', '0')
    c('SADD', 'ints32', '-2147483648', '2147483647')
    c('SADD', 'ints64', '-9223372036854775808', '9223372036854775807')
    c('SADD', 'strings', 'one', 'two', b'\x00\xff')
    c('HSET', 'hash', 'one', '1', 'binary', b'\x00\xff')
    c('ZADD', 'zset', '-inf', 'low', '5e-324', 'tiny', '+inf', 'high')
    c('CONFIG', 'SET', 'list-max-listpack-size', 1)
    c('RPUSH', 'nodes', *range(20))
    c('CONFIG', 'SET', 'list-max-listpack-size', -2)
    for i in range(1, 105):
        if i % 2:
            c('XADD', 'stream', f'{i}-0', 'f', i, 'binary', b'\x00\xff')
        else:
            c('XADD', 'stream', f'{i}-0', 'other', i, 'other', 'duplicate field')
    c('XGROUP', 'CREATE', 'stream', 'g', '0', 'ENTRIESREAD', 0)
    c('XREADGROUP', 'GROUP', 'g', 'alice', 'COUNT', 3, 'STREAMS', 'stream', '>')
    c('XCLAIM', 'stream', 'g', 'bob', 0, '2-0', 'TIME', 1234, 'RETRYCOUNT', 7)
    c('XDEL', 'stream', '1-0', '104-0')
    c('XGROUP', 'CREATECONSUMER', 'stream', 'g', 'empty')
    c('XGROUP', 'CREATE', 'stream', 'unknown', '$', 'ENTRIESREAD', -1)
    c('XGROUP', 'CREATE', 'stream', b'\x00\xff', '0')
    c('XGROUP', 'CREATECONSUMER', 'stream', b'\x00\xff', b'\x00\xff')
    c('XGROUP', 'CREATE', 'empty', '', '0', 'MKSTREAM')
    c('XGROUP', 'CREATECONSUMER', 'empty', '', '')
    c('XADD', 'huge', '18446744073709551614-18446744073709551615', 'f', 'a')
    c('XADD', 'huge', '18446744073709551615-0', 'f', 'b')
    c('XADD', 'wide', '1-1', 'f', 'a')
    c('XADD', 'wide', '9223372036854775809-0', 'f', 'b')
    c('XADD', 'wide', '18446744073709551615-18446744073709551615', 'f', 'c')
    c('XADD', 'longstream', '1-0', 'f' * 5000, b'\x00\xff' * 3000)
    c('XADD', 'trimmed', '1-0', 'f', 'a')
    c('XDEL', 'trimmed', '1-0')
    for key in ('str', 'list', 'stream', 'empty'):
        assert c('PEXPIREAT', key, 2000000000000) == 1
    return ['str', 'list', 'ints16', 'ints32', 'ints64', 'strings', 'hash', 'zset', 'nodes',
            'stream', 'empty', 'huge', 'wide', 'longstream', 'trimmed']


def state(c, key):
    t = c('TYPE', key)
    if t == '+stream':
        value = stream_state(c, key)
    elif t == '+string':
        value = c('GET', key)
    elif t == '+list':
        value = c('LRANGE', key, 0, -1)
    elif t == '+set':
        value = sorted(c('SMEMBERS', key))
    elif t == '+hash':
        value = fields(c('HGETALL', key))
    elif t == '+zset':
        raw = c('ZRANGE', key, 0, -1, 'WITHSCORES')
        value = list(zip(raw[::2], map(float, raw[1::2])))
    else:
        raise AssertionError((key, t))
    return t, value, c('PEXPIRETIME', key)


def migrated_state(actual, expected):
    # MIGRATE sends a relative TTL; network/command elapsed time shifts its absolute deadline.
    return (actual[:2] == expected[:2] and
            (actual[2] == expected[2] or (actual[2] > 0 and expected[2] > 0 and
                                        abs(actual[2] - expected[2]) <= 100)))


def check(binary, name, work):
    rp, pp = port(), port()
    source = start(REDIS, rp, os.path.join(work, name + '-source'))
    target_dir = os.path.join(work, name + '-target')
    target = start(binary, pp, target_dir)
    r, p = Conn(rp), Conn(pp)
    try:
        keys = populate(r)
        expected = {k: state(r, k) for k in keys}
        for k in keys:
            dump = r('DUMP', k)
            assert p('RESTORE', k, 2000000000000 if expected[k][2] > 0 else 0,
                     dump, 'ABSTTL') == '+OK', (name, k, dump[0])
            assert state(p, k) == expected[k], (name, k, state(p, k), expected[k])
            # Go back to Redis with PeaDB's native DUMP (streams formerly returned nil).
            back = p('DUMP', k)
            assert isinstance(back, bytes), (name, k, back)
            assert r('RESTORE', 'copy:' + k, expected[k][2] if expected[k][2] > 0 else 0,
                     back, 'ABSTTL') == '+OK'
            assert state(r, 'copy:' + k) == expected[k], (name, k, 'return trip')
        assert p('SAVE') == '+OK'
        checker = pathlib.Path(REDIS).with_name('redis-check-rdb')
        checked = subprocess.run([str(checker), os.path.join(target_dir, 'dump.rdb')],
                                 capture_output=True, text=True)
        assert checked.returncode == 0, checked.stdout + checked.stderr
        # Load the target's file with both engines, not just its DUMP payloads.
        p.close(); stop(target)
        target = start(binary, pp, target_dir); p = Conn(pp)
        for k in keys:
            assert state(p, k) == expected[k], (name, k, 'restart')
        r.close(); stop(source)
        source_dir = os.path.join(work, name + '-source')
        shutil.copyfile(os.path.join(target_dir, 'dump.rdb'), os.path.join(source_dir, 'dump.rdb'))
        source = start(REDIS, rp, source_dir); r = Conn(rp)
        for k in keys:
            assert state(r, k) == expected[k], (name, k, 'Redis RDB load')
        # Actual MIGRATE in both directions, including groups and binary fields.
        for k in keys:
            assert r('MIGRATE', '127.0.0.1', pp, k, 0, 3000, 'REPLACE') == '+OK', (name, k)
            assert r('EXISTS', k) == 0
            assert migrated_state(state(p, k), expected[k]), (name, k, state(p, k), expected[k])
            assert p('MIGRATE', '127.0.0.1', rp, k, 0, 3000, 'REPLACE') == '+OK', (name, k)
            assert p('EXISTS', k) == 0
            assert migrated_state(state(r, k), expected[k]), (name, k, state(r, k), expected[k])
        # Refusing an unsupported or incomplete value must preserve REPLACE's old key.
        for body in (b'\xff', b'\x10\x01\x00', b'\x0b\x08' + b'\x00' * 8,
                     r('DUMP', 'stream')[:-11]):
            assert p('SET', 'keep', 'old') == '+OK'
            assert p('RESTORE', 'keep', 0, payload(body), 'REPLACE') == '-ERR Bad data format'
            assert p('GET', 'keep') == b'old'
        print(name + ': compact and stream DUMP/RESTORE/MIGRATE, RDB checks passed', flush=True)
    finally:
        r.close(); p.close(); stop(source); stop(target)


def check_replication(master_binary, replica_binary, name, work):
    mp, rp = port(), port()
    master = start(master_binary, mp, os.path.join(work, name + '-master'),
                   '--repl-diskless-sync', 'no', '--repl-diskless-sync-delay', '0')
    replica = start(replica_binary, rp, os.path.join(work, name + '-replica'))
    m, r = Conn(mp), Conn(rp)
    try:
        keys = populate(m)
        expected = {k: state(m, k) for k in keys}
        assert r('REPLICAOF', '127.0.0.1', mp) == '+OK'
        wait_for(lambda: b'master_link_status:up' in r('INFO', 'replication'))
        for k in keys:
            assert state(r, k) == expected[k], (name, k, 'full sync')
        assert m('XADD', 'stream', '105-0', 'f', 'live') == b'105-0'
        wait_for(lambda: r('XLEN', 'stream') == m('XLEN', 'stream'))
        assert r('XRANGE', 'stream', '-', '+') == m('XRANGE', 'stream', '-', '+')
        print(name + ': native stream full sync and live writes passed', flush=True)
    finally:
        m.close(); r.close(); stop(replica); stop(master)


def check_aof(binary, name, work):
    sp, tp = port(), port()
    source = start(REDIS, sp, os.path.join(work, name + '-aof-source'))
    target_dir = os.path.join(work, name + '-aof')
    target = start(binary, tp, target_dir, '--appendonly', 'yes', '--appendfsync', 'always')
    s, t = Conn(sp), Conn(tp)
    try:
        keys = populate(s)
        expected = {k: state(s, k) for k in keys}
        for k in keys:
            assert t('RESTORE', k, max(0, expected[k][2]), s('DUMP', k), 'ABSTTL') == '+OK'
        t.close(); stop(target)
        target = start(binary, tp, target_dir, '--appendonly', 'yes'); t = Conn(tp)
        for k in keys:
            assert state(t, k) == expected[k], (name, k, 'RESTORE AOF replay')
        print(name + ': native RESTORE AOF replay passed', flush=True)
    finally:
        s.close(); t.close(); stop(source); stop(target)


def main():
    if not REDIS or " v=7.2.5 " not in subprocess.check_output([REDIS, "--version"], text=True):
        raise RuntimeError('Redis 7.2.5 is required for native RDB interoperability')
    with tempfile.TemporaryDirectory(prefix='peadb-native-dump-') as work:
        check(REDIS, 'redis', work)
        check(PEADB, 'peadb', work)
        check_aof(REDIS, 'redis', work)
        check_aof(PEADB, 'peadb', work)
        for mb, rb, name in ((REDIS, REDIS, 'redis-redis'),
                             (REDIS, PEADB, 'redis-peadb'),
                             (PEADB, REDIS, 'peadb-redis'),
                             (PEADB, PEADB, 'peadb-peadb')):
            check_replication(mb, rb, name, work)


if __name__ == '__main__':
    main()

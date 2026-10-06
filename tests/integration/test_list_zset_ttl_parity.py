#!/usr/bin/env python3
"""LSET, LREM, LINSERT, LTRIM, ZREM, ZREMRANGEBYSCORE/RANK/LEX and INCRBYFLOAT keep the key's TTL (compat/delta.md item 31).

Each command runs on a key with a TTL on peadb-server and on the pinned redis-server 7.2.x, and both must give the expected reply and contents and keep the TTL; a command that empties the key deletes it. The same runs check the keyspace events (Redis's order, with "del" when the key becomes empty), the order in which these commands and the shared sorted-set range parser check their arguments, a key whose TTL has passed while active expiry is off, a replica of each engine and across the engines both ways (the replicated key keeps its TTL, an emptied key stays deleted) and a restart from the AOF. PeaDB alone: the edits change large collections in place, so 300 ZREM, LSET and LTRIM calls on 200,000 items take well under the bound (the old copy-and-rebuild took seconds). Without a pinned Redis 7.2.x, only the PeaDB side runs.
"""
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _redis_path import redis_server_path  # noqa: E402

PEADB = str(ROOT / 'peadb-server')
PORTS = {'peadb': 6596, 'redis': 6597, 'peadb-replica': 6598, 'redis-replica': 6599}
TTL_MS = 100000


def redis_7_2():
    """The redis-server to compare with, only if it is Redis 7.2.x (the version PeaDB follows)."""
    path = redis_server_path()
    if not path:
        return None
    version = subprocess.run([path, '--version'], capture_output=True, text=True).stdout
    return path if ' v=7.2.' in version else None


REDIS = redis_7_2()

LIST = ('RPUSH', 'k', 'a', 'b', 'c', 'b', 'd')
ZSET = ('ZADD', 'k', '1', 'a', '2', 'b', '3', 'c', '4', 'd')
LEX = ('ZADD', 'k', '0', 'a', '0', 'b', '0', 'c', '0', 'd')
LRANGE = ('LRANGE', 'k', '0', '-1')
ZRANGE = ('ZRANGE', 'k', '0', '-1', 'WITHSCORES')

# (setup, command, reply, read command, contents after) — the key keeps its TTL.
TTL_CASES = [
    (LIST, ('LSET', 'k', '1', 'x'), '+OK', LRANGE, ['a', 'x', 'c', 'b', 'd']),
    (LIST, ('LSET', 'k', '-1', 'x'), '+OK', LRANGE, ['a', 'b', 'c', 'b', 'x']),
    (LIST, ('LREM', 'k', '1', 'b'), 1, LRANGE, ['a', 'c', 'b', 'd']),
    (LIST, ('LREM', 'k', '-1', 'b'), 1, LRANGE, ['a', 'b', 'c', 'd']),
    (LIST, ('LREM', 'k', '0', 'b'), 2, LRANGE, ['a', 'c', 'd']),
    (LIST, ('LREM', 'k', '-9223372036854775808', 'b'), 2, LRANGE, ['a', 'c', 'd']),
    (LIST, ('LREM', 'k', '0', 'zz'), 0, LRANGE, ['a', 'b', 'c', 'b', 'd']),
    (LIST, ('LINSERT', 'k', 'BEFORE', 'c', 'y'), 6, LRANGE, ['a', 'b', 'y', 'c', 'b', 'd']),
    (LIST, ('LINSERT', 'k', 'AFTER', 'b', 'y'), 6, LRANGE, ['a', 'b', 'y', 'c', 'b', 'd']),
    (LIST, ('LINSERT', 'k', 'AFTER', 'zz', 'y'), -1, LRANGE, ['a', 'b', 'c', 'b', 'd']),
    (LIST, ('LTRIM', 'k', '1', '-2'), '+OK', LRANGE, ['b', 'c', 'b']),
    (LIST, ('LTRIM', 'k', '-2', '99'), '+OK', LRANGE, ['b', 'd']),
    (LIST, ('LTRIM', 'k', '0', '-1'), '+OK', LRANGE, ['a', 'b', 'c', 'b', 'd']),
    (ZSET, ('ZREM', 'k', 'a', 'zz'), 1, ZRANGE, ['b', '2', 'c', '3', 'd', '4']),
    (ZSET, ('ZREMRANGEBYSCORE', 'k', '(1', '2'), 1, ZRANGE, ['a', '1', 'c', '3', 'd', '4']),
    (ZSET, ('ZREMRANGEBYSCORE', 'k', '', '2'), 2, ZRANGE, ['c', '3', 'd', '4']),
    (ZSET, ('ZREMRANGEBYRANK', 'k', '0', '1'), 2, ZRANGE, ['c', '3', 'd', '4']),
    (ZSET, ('ZREMRANGEBYRANK', 'k', '-1', '-1'), 1, ZRANGE, ['a', '1', 'b', '2', 'c', '3']),
    (LEX, ('ZREMRANGEBYLEX', 'k', '[b', '(d'), 2, ZRANGE, ['a', '0', 'd', '0']),
    (('SET', 'k', '1.5'), ('INCRBYFLOAT', 'k', '1'), '2.5', ('GET', 'k'), '2.5'),
]

# (setup, command, reply) — the command empties the key, so the key is deleted.
EMPTY_CASES = [
    (('RPUSH', 'k', 'b', 'b'), ('LREM', 'k', '0', 'b'), 2),
    (LIST, ('LTRIM', 'k', '5', '9'), '+OK'),
    (LIST, ('LTRIM', 'k', '-1', '0'), '+OK'),
    (('ZADD', 'k', '1', 'a'), ('ZREM', 'k', 'a', 'b'), 1),
    (ZSET, ('ZREMRANGEBYSCORE', 'k', '-inf', '+inf'), 4),
    (ZSET, ('ZREMRANGEBYRANK', 'k', '0', '-1'), 4),
    (LEX, ('ZREMRANGEBYLEX', 'k', '-', '+'), 4),
]

# (setup, command, reply) — the key's TTL has passed but active expiry is off, so the command
# must expire the key itself and act as on a missing key.
EXPIRED_CASES = [
    (LIST, ('LSET', 'k', '0', 'x'), '-ERR no such key'),
    (LIST, ('LREM', 'k', '0', 'b'), 0),
    (LIST, ('LINSERT', 'k', 'BEFORE', 'a', 'x'), 0),
    (LIST, ('LTRIM', 'k', '0', '1'), '+OK'),
    (ZSET, ('ZREM', 'k', 'a'), 0),
    (ZSET, ('ZREMRANGEBYSCORE', 'k', '-inf', '+inf'), 0),
    (ZSET, ('ZREMRANGEBYRANK', 'k', '0', '-1'), 0),
    (LEX, ('ZREMRANGEBYLEX', 'k', '-', '+'), 0),
]

# (setup, command, keyevent names in order).
EVENT_CASES = [
    (LIST, ('LSET', 'k', '1', 'x'), ['lset']),
    (LIST, ('LSET', 'k', '9', 'x'), []),
    ((), ('LSET', 'k', '0', 'x'), []),
    (LIST, ('LREM', 'k', '0', 'b'), ['lrem']),
    (LIST, ('LREM', 'k', '0', 'zz'), []),
    (('RPUSH', 'k', 'b', 'b'), ('LREM', 'k', '0', 'b'), ['lrem', 'del']),
    (LIST, ('LINSERT', 'k', 'AFTER', 'c', 'y'), ['linsert']),
    (LIST, ('LINSERT', 'k', 'AFTER', 'zz', 'y'), []),
    ((), ('LINSERT', 'k', 'AFTER', 'zz', 'y'), []),
    (LIST, ('LTRIM', 'k', '0', '-1'), ['ltrim']),
    (LIST, ('LTRIM', 'k', '5', '9'), ['ltrim', 'del']),
    ((), ('LTRIM', 'k', '0', '1'), []),
    (ZSET, ('ZREM', 'k', 'a', 'zz'), ['zrem']),
    (ZSET, ('ZREM', 'k', 'zz'), []),
    (('ZADD', 'k', '1', 'a'), ('ZREM', 'k', 'a', 'b'), ['zrem', 'del']),
    (ZSET, ('ZREMRANGEBYSCORE', 'k', '1', '2'), ['zremrangebyscore']),
    (ZSET, ('ZREMRANGEBYSCORE', 'k', '7', '8'), []),
    (ZSET, ('ZREMRANGEBYSCORE', 'k', '-inf', '+inf'), ['zremrangebyscore', 'del']),
    (ZSET, ('ZREMRANGEBYRANK', 'k', '0', '1'), ['zremrangebyrank']),
    (ZSET, ('ZREMRANGEBYRANK', 'k', '0', '-1'), ['zremrangebyrank', 'del']),
    (LEX, ('ZREMRANGEBYLEX', 'k', '[a', '[b'), ['zremrangebylex']),
    (LEX, ('ZREMRANGEBYLEX', 'k', '-', '+'), ['zremrangebylex', 'del']),
    (('SET', 'k', '1'), ('INCRBYFLOAT', 'k', '1'), ['incrbyfloat']),
]

WRONGTYPE = '-WRONGTYPE Operation against a key holding the wrong kind of value'
NOT_INT = '-ERR value is not an integer or out of range'
NOT_FLOAT = '-ERR min or max is not a float'
NOT_LEX = '-ERR min or max not valid string range item'
# (command, reply) with s = a string, l = [a b a c a], z = {a:1 b:2 c:3 d:4}.
ARG_CASES = [
    (('LTRIM', 's', 'a', '1'), NOT_INT),
    (('LREM', 's', 'a', 'x'), NOT_INT),
    (('LREM', 's', '1', 'x'), WRONGTYPE),
    (('LREM', 'l', '-9223372036854775808', 'a'), 3),
    (('LINSERT', 'nokey', 'BAD', 'a', 'b'), '-ERR syntax error'),
    (('LINSERT', 's', 'BAD', 'a', 'b'), '-ERR syntax error'),
    (('LINSERT', 's', 'BEFORE', 'a', 'b'), WRONGTYPE),
    (('LINSERT', 'nokey', 'before', 'a', 'b'), 0),
    (('LSET', 'nokey', 'a', 'b'), '-ERR no such key'),
    (('LSET', 's', 'a', 'b'), WRONGTYPE),
    (('LSET', 'l', 'a', 'b'), NOT_INT),
    (('LSET', 'l', '-9', 'b'), '-ERR index out of range'),
    (('LSET', 'l', '5', 'b'), '-ERR index out of range'),
    (('ZREM', 's', 'a'), WRONGTYPE),
    (('ZREM', 'nokey', 'a'), 0),
    (('ZREMRANGEBYSCORE', 's', 'a', 'b'), NOT_FLOAT),
    (('ZREMRANGEBYSCORE', 's', '1', '2'), WRONGTYPE),
    (('ZREMRANGEBYSCORE', 'z', '1abc', '2'), NOT_FLOAT),
    (('ZREMRANGEBYSCORE', 'z', '2 ', '3'), NOT_FLOAT),
    (('ZREMRANGEBYSCORE', 'z', 'nan', '3'), NOT_FLOAT),
    (('ZREMRANGEBYSCORE', 'z', '(', '(3'), 2),
    (('ZREMRANGEBYRANK', 's', 'a', 'b'), NOT_INT),
    (('ZREMRANGEBYRANK', 's', '0', '1'), WRONGTYPE),
    (('ZREMRANGEBYRANK', 'z', '2', '1'), 0),
    (('ZREMRANGEBYLEX', 's', 'a', 'b'), NOT_LEX),
    (('ZREMRANGEBYLEX', 'z', '', '+'), NOT_LEX),
    (('ZREMRANGEBYLEX', 'z', '+x', '+'), NOT_LEX),
    (('ZREMRANGEBYLEX', 'z', '(', '+'), 4),
    (('ZCOUNT', 's', 'x', '5'), NOT_FLOAT),
    (('ZCOUNT', 'z', '1abc', '5'), NOT_FLOAT),
    (('ZCOUNT', 'z', '', '0x2'), 2),
    (('ZRANGEBYSCORE', 's', 'x', '5', 'BAD'), '-ERR syntax error'),
    (('ZREVRANGEBYSCORE', 'z', '5', '1abc'), NOT_FLOAT),
    (('ZRANGE', 's', 'a', 'b', 'BYSCORE'), NOT_FLOAT),
    (('ZRANGE', 'z', '0', '-1', 'LIMIT', '0', '-1'), ['a', 'b', 'c', 'd']),
    (('ZRANGE', 'z', '1', '3', 'BYSCORE', 'BYSCORE'), '-ERR syntax error'),
    (('ZRANGE', 'z', '0', '1', 'REV', 'REV'), '-ERR syntax error'),
    (('ZRANGEBYLEX', 'z', '-', '+', 'BYLEX'), '-ERR syntax error'),
    (('ZLEXCOUNT', 's', 'a', 'b'), NOT_LEX),
]


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=10)
        self.f = self.sock.makefile('rb')

    def close(self):
        self.f.close()
        self.sock.close()

    def read(self):
        line = self.f.readline()
        if not line:
            raise ConnectionError('connection closed')
        kind, rest = line[:1], line[1:-2]
        if kind in (b'+', b'-'):
            return (kind + rest).decode()
        if kind == b':':
            return int(rest)
        if kind == b'$':
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2].decode()
        if kind == b'*':
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise ValueError(f'unexpected reply {line!r}')

    def send(self, *args):
        out = f'*{len(args)}\r\n'.encode()
        for a in args:
            b = str(a).encode()
            out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
        self.sock.sendall(out)

    def __call__(self, *args):
        self.send(*args)
        return self.read()


def start(binary, port, workdir, *extra):
    os.makedirs(workdir, exist_ok=True)
    log = open(os.path.join(workdir, 'server.log'), 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--save', '',
                             '--dir', workdir, '--loglevel', 'warning', *extra],
                            stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 15
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'{binary} exited: {open(os.path.join(workdir, "server.log")).read()}')
        try:
            c = Conn(port)
            if c('PING') == '+PONG':
                c.close()
                return proc
            c.close()
        except (OSError, ConnectionError):
            pass
        time.sleep(0.05)
    proc.kill()
    raise RuntimeError(f'{binary} did not start on port {port}')


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def rename(args, key):
    return tuple(key if a == 'k' else a for a in args)


def check_ttl_kept(c, name):
    for setup, cmd, reply, read, contents in TTL_CASES:
        c('FLUSHALL')
        c(*setup)
        assert c('PEXPIRE', 'k', TTL_MS) == 1
        got = c(*cmd)
        assert got == reply, (name, cmd, got, reply)
        assert c(*read) == contents, (name, cmd, c(*read), contents)
        pttl = c('PTTL', 'k')
        assert TTL_MS - 10000 < pttl <= TTL_MS, (name, cmd, 'TTL lost', pttl)
    for setup, cmd, reply in EMPTY_CASES:
        c('FLUSHALL')
        c(*setup)
        assert c('PEXPIRE', 'k', TTL_MS) == 1
        got = c(*cmd)
        assert got == reply, (name, cmd, got, reply)
        assert c('EXISTS', 'k') == 0 and c('PTTL', 'k') == -2, (name, cmd, 'key not deleted')
    assert c('DEBUG', 'SET-ACTIVE-EXPIRE', '0') == '+OK'
    try:
        for setup, cmd, reply in EXPIRED_CASES:
            c('FLUSHALL')
            c(*setup)
            assert c('PEXPIRE', 'k', 1) == 1
            time.sleep(0.01)
            got = c(*cmd)
            assert got == reply, (name, 'expired key', cmd, got, reply)
            assert c('DBSIZE') == 0, (name, 'expired key', cmd, 'key still there')
        c('FLUSHALL')
        c('SET', 'k', '1.5', 'PX', 1)
        time.sleep(0.01)
        assert c('INCRBYFLOAT', 'k', '1') == '1', (name, 'INCRBYFLOAT on an expired key')
        assert c('PTTL', 'k') == -1, (name, 'INCRBYFLOAT on an expired key kept the old TTL')
    finally:
        c('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
    # The key really expires after an edit: the capped-list (LPUSH + LTRIM) and sorted-set
    # window (ZADD + ZREMRANGEBYSCORE) patterns.
    c('FLUSHALL')
    c('RPUSH', 'capped', *[str(i) for i in range(20)])
    c('ZADD', 'window', *sum(([str(i), f'm{i}'] for i in range(20)), []))
    c('PEXPIRE', 'capped', 300)
    c('PEXPIRE', 'window', 300)
    for i in range(5):
        c('LPUSH', 'capped', f'new{i}')
        assert c('LTRIM', 'capped', '0', '9') == '+OK'
        c('ZADD', 'window', str(100 + i), f'new{i}')
        assert c('ZREMRANGEBYSCORE', 'window', '-inf', str(i)) == 1
    assert c('LLEN', 'capped') == 10 and c('ZCARD', 'window') == 20
    time.sleep(0.6)
    assert c('EXISTS', 'capped', 'window') == 0, (name, 'edited keys did not expire')


def check_events(port, name):
    """Each case in db 0 with all classes, without the generic class ("del" is generic), with only
    the generic class, then in db 3 inside MULTI/EXEC and in db 5 from a script."""
    c = Conn(port)
    sub = Conn(port)
    sub.send('PSUBSCRIBE', '__keyevent@*__:*')
    sub.read()
    sub.send('SUBSCRIBE', 'mark')
    sub.read()
    passes = [
        ('KEA', 0, 'plain', lambda e: True),
        ('Elz$', 0, 'plain', lambda e: e != 'del'),
        ('Eg', 0, 'plain', lambda e: e == 'del'),
        ('KEA', 3, 'multi', lambda e: True),
        ('KEA', 5, 'script', lambda e: True),
    ]
    try:
        for flags, db, how, keep in passes:
            assert c('CONFIG', 'SET', 'notify-keyspace-events', flags) == '+OK'
            for setup, cmd, expected in EVENT_CASES:
                c('FLUSHALL')
                assert c('SELECT', db) == '+OK'
                if setup:
                    c(*setup)
                c('PUBLISH', 'mark', 'start')
                if how == 'multi':
                    c('MULTI')
                    c(*cmd)
                    c('EXEC')
                elif how == 'script':
                    c('EVAL', 'return redis.pcall(unpack(ARGV))', '0', *cmd)
                else:
                    c(*cmd)
                c('PUBLISH', 'mark', 'end')
                events, started = [], False
                while True:
                    m = sub.read()
                    if m[0] == 'message':
                        if m[2] == 'end':
                            break
                        started = True
                    elif started:
                        assert m[3] == 'k', (name, cmd, m)
                        events.append(m[2])
                want = [f'__keyevent@{db}__:{e}' for e in expected if keep(e)]
                assert events == want, (name, flags, how, cmd, events, want)
    finally:
        c('CONFIG', 'SET', 'notify-keyspace-events', '')
        sub.close()
        c.close()


def check_argument_order(c, name):
    for cmd, expected in ARG_CASES:
        c('FLUSHALL')
        c('SET', 's', 'v')
        c('RPUSH', 'l', 'a', 'b', 'a', 'c', 'a')
        c('ZADD', 'z', '1', 'a', '2', 'b', '3', 'c', '4', 'd')
        got = c(*cmd)
        assert got == expected, (name, cmd, got, expected)


def run_all_cases(c):
    """Every TTL case on its own key k0, k1, ... and every emptying case on e0, e1, ...; returns
    {key: (read command, contents)}, with None for a key that must be gone."""
    out = {}
    for i, (setup, cmd, reply, read, contents) in enumerate(TTL_CASES):
        key = f'k{i}'
        c(*rename(setup, key))
        assert c('PEXPIRE', key, TTL_MS) == 1
        assert c(*rename(cmd, key)) == reply
        out[key] = (rename(read, key), contents)
    for i, (setup, cmd, reply) in enumerate(EMPTY_CASES):
        key = f'e{i}'
        c(*rename(setup, key))
        assert c('PEXPIRE', key, TTL_MS) == 1
        assert c(*rename(cmd, key)) == reply
        out[key] = None
    return out


def check_keys(c, expected, name, what):
    for key, want in expected.items():
        if want is None:
            assert c('EXISTS', key) == 0, (name, what, key, 'emptied key is back')
            continue
        read, contents = want
        assert c(*read) == contents, (name, what, key, c(*read), contents)
        pttl = c('PTTL', key)
        assert pttl > TTL_MS - 30000, (name, what, key, 'TTL lost', pttl)


def wait_for(cond, what, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.05)
    raise AssertionError(f'timed out waiting for {what}')


def check_replica(master_bin, replica_bin, master_port, replica_port, workdir, name):
    master = start(master_bin, master_port, os.path.join(workdir, 'master'))
    replica = start(replica_bin, replica_port, os.path.join(workdir, 'replica'))
    try:
        m, r = Conn(master_port), Conn(replica_port)
        assert r('REPLICAOF', '127.0.0.1', master_port) == '+OK'
        wait_for(lambda: 'master_link_status:up' in r('INFO', 'replication'), f'{name} replica link')
        expected = run_all_cases(m)
        m('SET', 'last', 'x')
        wait_for(lambda: r('GET', 'last') == 'x', f'{name} replica catch-up')
        check_keys(r, expected, name, 'replica')
        m.close()
        r.close()
    finally:
        stop(replica)
        stop(master)


def check_aof_restart(binary, port, workdir, name):
    proc = start(binary, port, workdir, '--appendonly', 'yes', '--appendfsync', 'always')
    try:
        c = Conn(port)
        expected = run_all_cases(c)
        try:
            c('SHUTDOWN')
        except ConnectionError:
            pass
        proc.wait(timeout=15)
    finally:
        stop(proc)
    proc = start(binary, port, workdir, '--appendonly', 'yes')
    try:
        c = Conn(port)
        check_keys(c, expected, name, 'AOF restart')
        c.close()
    finally:
        stop(proc)


def check_large_in_place(c):
    """PeaDB only: the edits no longer copy and rebuild the collection."""
    c('FLUSHALL')
    n = 200000
    for base in range(0, n, 10000):
        c('RPUSH', 'biglist', *[f'e{i}' for i in range(base, base + 10000)])
        c('ZADD', 'bigzset', *sum(([str(i), f'm{i}'] for i in range(base, base + 10000)), []))
    c('PEXPIRE', 'biglist', TTL_MS)
    c('PEXPIRE', 'bigzset', TTL_MS)
    t0 = time.time()
    for i in range(300):
        assert c('ZREM', 'bigzset', f'm{i * 600}') == 1
        assert c('LSET', 'biglist', str(i * 600), 'x') == '+OK'
        assert c('LTRIM', 'biglist', '1', '-1') == '+OK'
    elapsed = time.time() - t0
    assert c('ZCARD', 'bigzset') == n - 300 and c('LLEN', 'biglist') == n - 300
    assert c('PTTL', 'bigzset') > 0 and c('PTTL', 'biglist') > 0
    # Each old ZREM copied and rebuilt all 200,000 members (tens of ms per call): the old code
    # took about 10 s here, the new about 0.04 s (macOS).
    assert elapsed < 5.0, f'900 edits on 200,000-item collections took {elapsed:.2f} s'
    print(f'  in-place edits: 900 calls on 200,000 items in {elapsed:.3f} s')


def run_engine(name, binary, port, replica_port, base):
    proc = start(binary, port, os.path.join(base, f'{name}-main'), '--enable-debug-command', 'yes')
    try:
        c = Conn(port)
        check_ttl_kept(c, name)
        check_events(port, name)
        check_argument_order(c, name)
        if name == 'peadb':
            check_large_in_place(c)
        c.close()
    finally:
        stop(proc)
    check_replica(binary, binary, port, replica_port, os.path.join(base, f'{name}-repl'), name)
    check_aof_restart(binary, port, os.path.join(base, f'{name}-aof'), name)
    print(f'  {name}: TTL, deleted keys, events, argument order, replica and AOF restart OK')


def main():
    base = tempfile.mkdtemp(prefix='peadb-ttl-')
    try:
        # The Redis side runs first: the expectations are Redis 7.2.5's behaviour.
        if REDIS:
            run_engine('redis', REDIS, PORTS['redis'], PORTS['redis-replica'], base)
        else:
            print('  no pinned Redis 7.2.x found: PeaDB side only')
        run_engine('peadb', PEADB, PORTS['peadb'], PORTS['peadb-replica'], base)
        if REDIS:
            # Replication across the engines, both ways.
            check_replica(PEADB, REDIS, PORTS['peadb'], PORTS['redis-replica'],
                          os.path.join(base, 'peadb-to-redis'), 'PeaDB master, Redis replica')
            check_replica(REDIS, PEADB, PORTS['redis'], PORTS['peadb-replica'],
                          os.path.join(base, 'redis-to-peadb'), 'Redis master, PeaDB replica')
            print('  cross-engine replication both ways OK')
    finally:
        shutil.rmtree(base, ignore_errors=True)
    print('PASS test_list_zset_ttl_parity')


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""SCAN, HSCAN, SSCAN and ZSCAN behave like Redis 7.2.5's scanGenericCommand() (compat/delta.md items 32 and 55).

The same checks run on peadb-server and on the pinned redis-server (Redis side first, only when it is Redis 7.2.x):

- the SCAN guarantee: every key present for the whole scan is returned at least once, when the keyspace grows a lot in the middle of a scan (100,000 keys, then 300,000 more), when it grows a little after every call (from a few keys to thousands, many rounds), and when most keys are deleted in the middle of a scan (Redis shrinks its table then); the same for the fields of a hash, the members of a set and of a sorted set that grow or shrink during an HSCAN, SSCAN or ZSCAN.
- COUNT bounds the work of one call: a MATCH or a TYPE that matches nothing returns an empty page and a non-zero cursor instead of walking the whole database; a full scan with such a MATCH needs many calls.
- a small hash, set or sorted set (listpack or intset encoding) is returned whole in one call with cursor 0, whatever the cursor and COUNT, and MATCH filters it; an intset comes in ascending order and a small sorted set in score order, as Redis keeps them.
- argument checks in Redis's order and text: the cursor is read like strtoul() (`-1` and `+5` are cursors, ` 1`, `1x` and 2^64 are `invalid cursor`), COUNT must be an integer of at least 1, an option without its value is a syntax error, TYPE is only for SCAN, NOVALUES is not a Redis 7.2 option; HSCAN/SSCAN/ZSCAN check the cursor, then the key (a missing key answers an empty page before any option is read, a key of another type WRONGTYPE), then the options.
- SCAN with TYPE looks each key up like a read, so it counts keyspace hits and misses (Redis's lookupKeyReadWithFlags()); without TYPE it counts none.
- RANDOMKEY returns every key of a database of two keys, also when both are in one bucket chain (keys are hashed with a random key, so that happens in about one round of four).

Without a pinned Redis 7.2.x, only the PeaDB side runs.
"""
import os
import pathlib
import random
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


def redis_7_2():
    """The redis-server to compare with, only if it is Redis 7.2.x (replies differ in other versions)."""
    path = redis_server_path()
    if not path:
        return None
    version = subprocess.run([path, '--version'], capture_output=True, text=True).stdout
    return path if ' v=7.2.' in version else None


REDIS = redis_7_2()
PORTS = {'peadb': 6660, 'redis': 6661}


def encode(*args):
    out = b'*%d\r\n' % len(args)
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode()
        out += b'$%d\r\n' % len(b) + b + b'\r\n'
    return out


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=30)
        self.f = self.sock.makefile('rb')

    def close(self):
        self.f.close()
        self.sock.close()

    def read(self):
        line = self.f.readline()
        if not line:
            return None  # connection closed
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

    def __call__(self, *args):
        self.sock.sendall(encode(*args))
        return self.read()

    def pipeline(self, commands):
        """Sends every command at once and returns the replies."""
        self.sock.sendall(b''.join(encode(*cmd) for cmd in commands))
        return [self.read() for _ in commands]


def start(binary, port, workdir, *extra):
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    log = open(log_path, 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                             '--loglevel', 'notice', '--save', '', '--enable-debug-command', 'yes', *extra],
                            stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'{binary} exited: {open(log_path).read()}')
        try:
            socket.create_connection(('127.0.0.1', port), timeout=0.2).close()
            return proc
        except OSError:
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError(f'{binary} did not start on port {port}')


def stop(proc):
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def info_field(c, section, field):
    for line in c('INFO', section).split('\r\n'):
        if line.startswith(field + ':'):
            return line.split(':', 1)[1]
    raise AssertionError(f'no {field} in INFO {section}')


def scan_all(c, cmd, key=None, count=10, between=None, match=None):
    """Runs a whole scan and returns (elements, calls). `between(call_number)` runs after each call that did not end the scan."""
    seen = []
    cursor = '0'
    calls = 0
    while True:
        args = [cmd] + ([key] if key is not None else []) + [cursor, 'COUNT', count]
        if match is not None:
            args += ['MATCH', match]
        reply = c(*args)
        assert isinstance(reply, list) and len(reply) == 2, (args, reply)
        cursor, page = reply
        calls += 1
        if cmd in ('HSCAN', 'ZSCAN'):
            seen.extend(page[0::2])
        else:
            seen.extend(page)
        if cursor == '0':
            return seen, calls
        if between:
            between(calls)


def check_keyspace_growth(name, c):
    # One big growth in the middle of the scan: the table grows twice (100,000 -> 400,000 keys).
    assert c('FLUSHALL') == '+OK'
    assert c('DEBUG', 'POPULATE', 100000) == '+OK'
    original = {f'key:{i}' for i in range(100000)}

    def grow(call):
        if call == 300:
            assert c('DEBUG', 'POPULATE', 300000, 'grow') == '+OK'
            assert c('DBSIZE') == 400000, name

    seen, calls = scan_all(c, 'SCAN', count=100, between=grow)
    assert calls > 300, (name, calls)
    missed = original - set(seen)
    assert not missed, (name, 'big growth', len(missed), sorted(missed)[:5])

    # Growth after every call, from a few keys: like the upstream "SCAN guarantees check under write load", with more
    # rounds and a table that starts small. The new names come from a bounded pool (upstream: 1,000), so the scan ends.
    rng = random.Random(7)
    for rnd in range(20):
        assert c('FLUSHALL') == '+OK'
        n = rng.randint(5, 200)
        kept = [f'k{rnd}:{i}' for i in range(n)]
        c.pipeline([('SET', k, 'v') for k in kept])
        pool = rng.choice((300, 1000, 4000))

        def write_load(call):
            c.pipeline([('SET', f'added:{rng.randrange(pool)}', 'v') for _ in range(rng.randint(1, 40))])

        seen, _ = scan_all(c, 'SCAN', count=rng.randint(1, 20), between=write_load)
        missed = set(kept) - set(seen)
        assert not missed, (name, 'write load', rnd, n, pool, sorted(missed)[:5])


def check_keyspace_shrink(name, c):
    # Most keys are deleted in the middle of the scan; Redis's cron then shrinks the table (fill under 10%).
    assert c('FLUSHALL') == '+OK'
    assert c('DEBUG', 'POPULATE', 60000) == '+OK'
    kept = [f'keep:{i}' for i in range(2000)]
    c.pipeline([('SET', k, 'v') for k in kept])

    def delete_most(call):
        if call == 50:
            dels = [('DEL', *[f'key:{i}' for i in range(j, j + 1000)]) for j in range(0, 60000, 1000)]
            assert sum(c.pipeline(dels)) == 60000, name
            time.sleep(0.5)  # a few cron runs: the shrink happens there

    seen, _ = scan_all(c, 'SCAN', count=100, between=delete_most)
    missed = set(kept) - set(seen)
    assert not missed, (name, 'shrink', len(missed), sorted(missed)[:5])


def check_collection_growth_and_shrink(name, c):
    def fill(kind, key, members):
        if kind == 'hash':
            args = ['HSET', key]
            for m in members:
                args += [m, '1']
        elif kind == 'set':
            args = ['SADD', key] + members
        else:
            args = ['ZADD', key]
            for i, m in enumerate(members):
                args += [i, m]
        assert isinstance(c(*args), int), (name, kind)

    def remove(kind, key, members):
        cmd = {'hash': 'HDEL', 'set': 'SREM', 'zset': 'ZREM'}[kind]
        assert c(cmd, key, *members) == len(members), (name, kind)

    scan_cmd = {'hash': 'HSCAN', 'set': 'SSCAN', 'zset': 'ZSCAN'}
    big_encoding = {'hash': 'hashtable', 'set': 'hashtable', 'zset': 'skiplist'}
    for kind in ('hash', 'set', 'zset'):
        key = f'{kind}-grow'
        c('DEL', key)
        original = [f'm:{i}' for i in range(1000)]
        fill(kind, key, original)
        assert c('OBJECT', 'ENCODING', key) == big_encoding[kind], (name, kind)

        def grow(call, kind=kind, key=key):
            if call == 20:
                fill(kind, key, [f'grow:{i}' for i in range(20000)])

        seen, _ = scan_all(c, scan_cmd[kind], key=key, count=10, between=grow)
        missed = set(original) - set(seen)
        assert not missed, (name, kind, 'growth', len(missed), sorted(missed)[:5])

        # Most members removed in the middle of the scan: Redis shrinks the table at once (fill under 10%).
        key = f'{kind}-shrink'
        c('DEL', key)
        kept = [f'keep:{i}' for i in range(150)]
        dropped = [f'drop:{i}' for i in range(5000)]
        fill(kind, key, kept + dropped)
        assert c('OBJECT', 'ENCODING', key) == big_encoding[kind], (name, kind)

        def shrink(call, kind=kind, key=key):
            if call == 10:
                remove(kind, key, dropped)
                assert c('OBJECT', 'ENCODING', key) == big_encoding[kind], (name, kind)

        seen, _ = scan_all(c, scan_cmd[kind], key=key, count=10, between=shrink)
        missed = set(kept) - set(seen)
        assert not missed, (name, kind, 'shrink', len(missed), sorted(missed)[:5])


def check_count_bounds_work(name, c):
    assert c('FLUSHALL') == '+OK'
    assert c('DEBUG', 'POPULATE', 100000) == '+OK'
    for args in (('SCAN', 0, 'MATCH', 'nomatch:*', 'COUNT', 10), ('SCAN', 0, 'TYPE', 'list', 'COUNT', 10),
                 ('SCAN', 0, 'TYPE', 'nosuchtype', 'COUNT', 5)):
        cursor, page = c(*args)
        assert cursor != '0' and page == [], (name, args, cursor, len(page))
    seen, calls = scan_all(c, 'SCAN', count=1000, match='key:1234*')
    assert sorted(set(seen)) == sorted({'key:1234'} | {f'key:1234{i}' for i in range(10)}), (name, sorted(set(seen)))
    assert calls > 10, (name, calls)

    fields = []
    for i in range(2000):
        fields += [f'f:{i}', 'v']
    assert c('HSET', 'bighash', *fields) == 2000
    cursor, page = c('HSCAN', 'bighash', 0, 'MATCH', 'nomatch*', 'COUNT', 10)
    assert cursor != '0' and page == [], (name, cursor, page)
    seen, calls = scan_all(c, 'HSCAN', key='bighash', count=10, match='f:19*')
    assert sorted(set(seen)) == sorted({'f:19'} | {f'f:19{i}' for i in range(10)} | {f'f:19{i:02d}' for i in range(100)}), name
    assert calls > 10, (name, calls)


def check_small_collections(name, c):
    assert c('FLUSHALL') == '+OK'
    fields = []
    for i in range(30):
        fields += [f'key:{i}', str(i)]
    assert c('HSET', 'h', *fields) == 30
    assert c('OBJECT', 'ENCODING', 'h') == 'listpack'
    assert c('SADD', 'si', *range(100)) == 100
    assert c('OBJECT', 'ENCODING', 'si') == 'intset'
    assert c('SADD', 'sl', *[f'ele:{i}' for i in range(100)]) == 100
    assert c('OBJECT', 'ENCODING', 'sl') == 'listpack'
    members = []
    for i in range(30):
        members += [i, f'key:{i}']
    assert c('ZADD', 'z', *members) == 30
    assert c('OBJECT', 'ENCODING', 'z') == 'listpack'
    for cursor in (0, 12345, 18446744073709551615):
        reply = c('HSCAN', 'h', cursor, 'COUNT', 1)
        assert reply[0] == '0' and sorted(zip(reply[1][0::2], reply[1][1::2])) == sorted(
            (f'key:{i}', str(i)) for i in range(30)), (name, cursor, reply)
        reply = c('SSCAN', 'si', cursor, 'COUNT', 1)
        assert reply[0] == '0' and sorted(reply[1], key=int) == [str(i) for i in range(100)], (name, cursor, reply)
        reply = c('SSCAN', 'sl', cursor, 'COUNT', 1)
        assert reply[0] == '0' and sorted(reply[1]) == sorted(f'ele:{i}' for i in range(100)), (name, cursor, reply)
        reply = c('ZSCAN', 'z', cursor, 'COUNT', 1)
        assert reply[0] == '0' and sorted(zip(reply[1][0::2], reply[1][1::2])) == sorted(
            (f'key:{i}', str(i)) for i in range(30)), (name, cursor, reply)
    # The order Redis keeps them in: an intset ascending, a listpack sorted set by score, then member.
    assert c('SADD', 'iorder', 10, 3, -7, 1, 5, 300) == 6
    assert c('SSCAN', 'iorder', 0) == ['0', ['-7', '1', '3', '5', '10', '300']], name
    assert c('ZADD', 'zorder', 3, 'c', 1, 'b', 2, 'x', 1, 'a', -1, 'z') == 5
    assert c('ZSCAN', 'zorder', 0, 'COUNT', 1) == ['0', ['z', '-1', 'a', '1', 'b', '1', 'x', '2', 'c', '3']], name
    reply = c('HSCAN', 'h', 0, 'MATCH', 'key:1*', 'COUNT', 1)
    assert reply[0] == '0' and sorted(reply[1][0::2]) == sorted(['key:1'] + [f'key:1{i}' for i in range(10)]), (name, reply)
    reply = c('SSCAN', 'si', 0, 'MATCH', '9*')
    assert reply[0] == '0' and sorted(reply[1]) == sorted(['9'] + [f'9{i}' for i in range(10)]), (name, reply)
    reply = c('ZSCAN', 'z', 0, 'MATCH', '*2*')
    assert reply[0] == '0' and sorted(reply[1][0::2]) == sorted(['key:2', 'key:12', 'key:20', 'key:21', 'key:22', 'key:23',
                                                                'key:24', 'key:25', 'key:26', 'key:27', 'key:28',
                                                                'key:29']), (name, reply)


def check_arguments(name, c):
    assert c('FLUSHALL') == '+OK'
    assert c('DEBUG', 'POPULATE', 1000) == '+OK'
    assert c('HSET', 'h', 'a', '1') == 1
    assert c('SADD', 's', 'a') == 1
    assert c('ZADD', 'z', 1, 'a') == 1
    assert c('SET', 'str', 'v') == '+OK'
    invalid = '-ERR invalid cursor'
    syntax = '-ERR syntax error'
    not_int = '-ERR value is not an integer or out of range'
    wrongtype = '-WRONGTYPE Operation against a key holding the wrong kind of value'
    empty = ['0', []]
    exact = [
        (('SCAN', ' 1'), invalid), (('SCAN', '1x'), invalid),
        (('SCAN', '18446744073709551616'), invalid), (('SCAN', '0x10'), invalid), (('SCAN', '1 '), invalid),
        (('SCAN', 0, 'COUNT', 0), syntax), (('SCAN', 0, 'COUNT', -1), syntax), (('SCAN', 0, 'COUNT', 'abc'), not_int),
        (('SCAN', 0, 'COUNT', '9223372036854775808'), not_int), (('SCAN', 0, 'COUNT', '1.5'), not_int),
        (('SCAN', 0, 'COUNT', ' 5'), not_int), (('SCAN', 0, 'MATCH'), syntax), (('SCAN', 0, 'TYPE'), syntax),
        (('SCAN', 0, 'COUNT'), syntax), (('SCAN', 0, 'FOO', 'bar'), syntax), (('SCAN', 0, 'COUNT', 5, 'MATCH'), syntax),
        (('SCAN', 'x', 'COUNT', 0), invalid),
        (('HSCAN', 'h', 0, 'TYPE', 'string'), syntax), (('HSCAN', 'h', 0, 'NOVALUES'), syntax),
        (('HSCAN', 'h', 0, 'COUNT', 0), syntax), (('SSCAN', 's', 0, 'COUNT', -5), syntax),
        (('ZSCAN', 'z', 0, 'COUNT', 'x'), not_int), (('ZSCAN', 'z', 0, 'MATCH'), syntax),
        (('HSCAN', 'missing', 0, 'badopt'), empty), (('SSCAN', 'missing', 0, 'COUNT', 0), empty),
        (('ZSCAN', 'missing', 0, 'MATCH'), empty), (('HSCAN', 'missing', 'x'), invalid),
        (('HSCAN', 'str', 0, 'badopt'), wrongtype), (('SSCAN', 'str', 0, 'COUNT', 0), wrongtype),
        (('ZSCAN', 'str', 0, 'TYPE', 'x'), wrongtype), (('SSCAN', 'str', 'x'), invalid),
        (('ZSCAN', 'h', 0), wrongtype), (('HSCAN', 'z', '1x'), invalid),
        (('SCAN',), "-ERR wrong number of arguments for 'scan' command"),
        (('HSCAN', 'h'), "-ERR wrong number of arguments for 'hscan' command"),
    ]
    for args, want in exact:
        got = c(*args)
        assert got == want, (name, args, got, want)
    # strtoul() reads a sign: -1 is the largest cursor (one bucket, then 0), +5 is cursor 5; an empty cursor is 0.
    reply = c('SCAN', '', 'COUNT', 1000)
    assert reply == c('SCAN', 0, 'COUNT', 1000), (name, reply)
    reply = c('SCAN', '-1')
    assert isinstance(reply, list) and reply[0] == '0', (name, reply)
    reply = c('SCAN', '18446744073709551615')
    assert isinstance(reply, list) and reply[0] == '0', (name, reply)
    reply = c('SCAN', '+5', 'COUNT', 1)
    assert isinstance(reply, list) and reply == c('SCAN', 5, 'COUNT', 1) and reply[0] != '0', (name, reply)
    # TYPE is case-insensitive, options can repeat (the last one wins) and come in any order.
    seen, _ = scan_all(c, 'SCAN', count=100)
    assert len(set(seen)) == 1004, (name, len(set(seen)))
    strings = set()
    cursor = '0'
    while True:
        cursor, page = c('SCAN', cursor, 'TYPE', 'STRING', 'COUNT', 1, 'MATCH', '*', 'COUNT', 50)
        strings.update(page)
        if cursor == '0':
            break
    assert strings == {f'key:{i}' for i in range(1000)} | {'str'}, (name, len(strings))
    for typ, key in (('hash', 'h'), ('set', 's'), ('zset', 'z')):
        found = set()
        cursor = '0'
        while True:
            cursor, page = c('SCAN', cursor, 'COUNT', 100, 'TYPE', typ)
            found.update(page)
            if cursor == '0':
                break
        assert found == {key}, (name, typ, found)


def check_type_lookup_stats(name, c):
    assert c('FLUSHALL') == '+OK'
    assert c('DEBUG', 'SET-ACTIVE-EXPIRE', 0) == '+OK'
    c.pipeline([('SET', f'k{i}', 'v') for i in range(20)])
    assert c('HSET', 'hh', 'f', 'v') == 1
    assert c('SET', 'gone', 'v', 'PX', 1) == '+OK'
    time.sleep(0.01)

    def stats():
        return int(info_field(c, 'stats', 'keyspace_hits')), int(info_field(c, 'stats', 'keyspace_misses'))

    h0, m0 = stats()
    cursor, page = c('SCAN', 0, 'COUNT', 1000)
    assert cursor == '0' and len(page) == 21, (name, len(page))
    h1, m1 = stats()
    assert (h1 - h0, m1 - m0) == (0, 0), (name, h1 - h0, m1 - m0)
    assert c('SET', 'gone2', 'v', 'PX', 1) == '+OK'
    time.sleep(0.01)
    cursor, page = c('SCAN', 0, 'TYPE', 'string', 'COUNT', 1000)
    assert cursor == '0' and sorted(page) == sorted(f'k{i}' for i in range(20)), (name, page)
    h2, m2 = stats()
    assert (h2 - h1, m2 - m1) == (21, 1), (name, h2 - h1, m2 - m1)
    assert c('DBSIZE') == 21, name
    assert c('DEBUG', 'SET-ACTIVE-EXPIRE', 1) == '+OK'


def check_randomkey_chain(name, c):
    # Two keys share a bucket of the 4-bucket table about once in four rounds; RANDOMKEY must still return both (Redis's
    # dictGetRandomKey() picks a random element of the chain). The upstream RANDOMKEY test in unit/keyspace has one round.
    assert c('SELECT', 9) == '+OK'
    for rnd in range(40):  # new names each round: the hash key is fixed for the life of the process
        a, b = f'foo{rnd}', f'bar{rnd}'
        assert c('FLUSHDB') == '+OK'
        assert c('SET', a, 'x') == '+OK' and c('SET', b, 'y') == '+OK'
        seen = set(c.pipeline([('RANDOMKEY',)] * 100))
        assert seen == {a, b}, (name, rnd, seen)
    assert c('FLUSHDB') == '+OK' and c('SELECT', 0) == '+OK'


def check_engine(name, binary, work):
    proc = start(binary, PORTS[name], os.path.join(work, name))
    try:
        c = Conn(PORTS[name])
        check_arguments(name, c)
        check_small_collections(name, c)
        check_count_bounds_work(name, c)
        check_type_lookup_stats(name, c)
        check_randomkey_chain(name, c)
        check_collection_growth_and_shrink(name, c)
        check_keyspace_shrink(name, c)
        check_keyspace_growth(name, c)
        c.close()
    finally:
        stop(proc)


def main():
    work = tempfile.mkdtemp(prefix='peadb-scan-guarantee-')
    try:
        if REDIS:
            check_engine('redis', REDIS, work)
        check_engine('peadb', PEADB, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print('ok')


if __name__ == '__main__':
    main()

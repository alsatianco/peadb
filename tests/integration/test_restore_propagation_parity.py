#!/usr/bin/env python3
"""RESTORE reaches replicas and the AOF as Redis 7.2.5 sends it, and changes only what Redis changes (compat/delta.md item 42).

The same checks run on peadb-server and on the pinned redis-server (Redis side first, only when it is Redis 7.2.x):

- propagation: a RESTORE with a relative TTL goes out as `RESTORE key <absolute ms> <payload> <its options> ABSTTL` with the payload unchanged (PeaDB wrapped the payload in braces, so every replica and every AOF load refused it, and dropped REPLACE, IDLETIME and FREQ); a RESTORE without a TTL or with ABSTTL goes out as it came; a RESTORE whose absolute TTL has passed sends nothing, counts as no change and sends no event, unless REPLACE deleted a key, which goes out as DEL with a `del` event; REPLACE erases a key whose TTL has passed as it is (no DEL or `expired` event of its own); inside MULTI/EXEC and scripts too.
- a real replica and an AOF reload hold the same keys, values and expire times as the master.
- RESTORE ... REPLACE with a payload that cannot be read keeps the old key (PeaDB deleted it first), and a payload with an empty collection, duplicate members or fields, or a NaN score is refused like Redis's rdbLoadObject() refuses it, and an LZF string that claims gigabytes from a few bytes is refused at once.
- the arguments are checked in Redis's order with Redis's errors (BUSYKEY before the TTL, `Invalid TTL value, must be >= 0`, strict integers, IDLETIME and FREQ not together).
- a restored key starts with a fresh LRU clock or LFU counter, and IDLETIME applies only under an LRU policy and FREQ only under an LFU policy, as in Redis's objectSetLRUOrLFU(), and OBJECT IDLETIME reads it back on Redis's 24-bit seconds clock.

Without a pinned Redis 7.2.x, only the PeaDB side runs.
"""
import math
import os
import pathlib
import shutil
import socket
import struct
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
PORTS = {'peadb': 6694, 'redis': 6695, 'peadb-replica': 6696, 'redis-replica': 6697}
PAST_MS = 1000  # an absolute time long gone


def encode(*args):
    """RESP for a command; text is sent as latin-1, so a DUMP payload read back as text goes out unchanged."""
    out = f'*{len(args)}\r\n'.encode()
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode('latin-1')
        out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
    return out


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
            return None  # connection closed
        kind, rest = line[:1], line[1:-2]
        if kind in (b'+', b'-'):
            return (kind + rest).decode()
        if kind == b':':
            return int(rest)
        if kind == b'$':
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2].decode('latin-1')
        if kind == b'*':
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise ValueError(f'unexpected reply {line!r}')

    def __call__(self, *args):
        self.sock.sendall(encode(*args))
        return self.read()


class FakeReplica:
    """A replica that sends PSYNC ? -1, skips the RDB, then reads the commands its master sends."""

    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=10)
        self.f = self.sock.makefile('rb')
        self.sock.sendall(encode('PSYNC', '?', '-1'))
        line = self.f.readline()
        assert line.startswith(b'+FULLRESYNC'), line
        while True:  # Redis sends newlines while it prepares the RDB
            b = self.f.read(1)
            if b == b'$':
                break
            assert b == b'\n', b
        n = int(self.f.readline()[:-2])
        self.f.read(n)

    def command(self):
        line = self.f.readline()
        assert line.startswith(b'*'), line
        args = []
        for _ in range(int(line[1:-2])):
            n = int(self.f.readline()[1:-2])
            args.append(self.f.read(n + 2)[:-2].decode('latin-1'))
        return args

    def commands_until(self, last):
        out = []
        while True:
            cmd = self.command()
            if cmd == ['PING'] or cmd[:2] == ['REPLCONF', 'GETACK']:
                continue
            out.append(cmd)
            if cmd == last:
                return out

    def close(self):
        self.f.close()
        self.sock.close()


def start(binary, port, workdir, *extra):
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    log = open(log_path, 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                             '--loglevel', 'notice', '--save', '', '--shutdown-timeout', '0', *extra],
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
        proc.wait()


def crash(proc):
    proc.kill()
    proc.wait()


def info(c, field):
    for line in c('INFO', 'all').splitlines():
        if line.startswith(field + ':'):
            return line.split(':', 1)[1]
    raise KeyError(field)


def wait_for(cond, what, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.02)
    raise AssertionError(f'timed out waiting for {what}')


def aof_file(name, workdir):
    """The file the server appends to: PeaDB's single file, or Redis's last INCR file."""
    if name == 'peadb':
        return os.path.join(workdir, 'appendonly.aof')
    d = os.path.join(workdir, 'appendonlydir')
    with open(os.path.join(d, 'appendonly.aof.manifest')) as f:
        incr = [ln.split()[1] for ln in f if ln.split() and ln.split()[-1] == 'i']
    return os.path.join(d, incr[-1])


def aof_commands(path):
    data = open(path, 'rb').read()
    out = []
    pos = 0
    while pos < len(data):
        eol = data.index(b'\r\n', pos)
        argc = int(data[pos + 1:eol])
        pos = eol + 2
        args = []
        for _ in range(argc):
            eol = data.index(b'\r\n', pos)
            n = int(data[pos + 1:eol])
            args.append(data[eol + 2:eol + 2 + n].decode('latin-1'))
            pos = eol + 2 + n + 2
        out.append(args)
    return out


def crc64(data):
    table = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ 0x95ac9329ac4bc9b5 if c & 1 else c >> 1
        table.append(c)
    c = 0
    for b in data:
        c = table[(c ^ b) & 0xff] ^ (c >> 8)
    return c


def payload(body):
    """A DUMP payload with a good RDB 11 footer around `body` (the type byte and the value)."""
    body += b'\x0b\x00'
    return (body + crc64(body).to_bytes(8, 'little')).decode('latin-1')


def enc_str(s):
    b = s.encode()
    if len(b) < 64:
        return bytes([len(b)]) + b
    return bytes([0x40 | (len(b) >> 8), len(b) & 0xff]) + b


def value(c, key):
    """A key's type and value, the same whatever order the server keeps a hash or set in."""
    kind = c('TYPE', key)
    if kind == '+string':
        return kind, c('GET', key)
    if kind == '+list':
        return kind, c('LRANGE', key, 0, -1)
    if kind == '+hash':
        return kind, sorted(c('HGETALL', key))
    if kind == '+set':
        return kind, sorted(c('SMEMBERS', key))
    if kind == '+zset':
        return kind, c('ZRANGE', key, 0, -1, 'WITHSCORES')
    return kind, None


def snapshot(c):
    return {k: (value(c, k), c('PEXPIRETIME', k)) for k in sorted(c('KEYS', '*'))}


def read_events(events, last):
    out = []
    while True:
        msg = events.read()
        out.append((msg[2].split(':', 1)[1], msg[3]))
        if out[-1] == last:
            return out


def run_restores(c, events):
    """Runs the RESTOREs; returns what the master must propagate and the events it must send."""
    expected = [['SELECT', '0']]
    for cmd in (('SET', 's:str', 'hello'), ('RPUSH', 's:list', 'a', 'b', 'c'),
                ('HSET', 's:hash', 'f1', 'v1', 'f2', 'v2'), ('SADD', 's:set', 'm1', 'm2'),
                ('ZADD', 's:zset', 1, 'a', 2.5, 'b')):
        c(*cmd)
        expected.append([str(a) for a in cmd])
    p = {kind: c('DUMP', 's:' + kind) for kind in ('str', 'list', 'hash', 'set', 'zset')}
    assert c('SET', 'mark1', 1) == '+OK'
    expected.append(['SET', 'mark1', '1'])
    read_events(events, ('set', 'mark1'))
    want_events = []

    def restore(*args, reply='+OK', changes=1, propagated=None, evs=()):
        dirty = int(info(c, 'rdb_changes_since_last_save'))
        assert c('RESTORE', *args) == reply, args
        assert int(info(c, 'rdb_changes_since_last_save')) == dirty + changes, args
        if propagated == 'abs':  # a relative TTL goes out as an absolute one, with ABSTTL last
            expected.append(['RESTORE', args[0], str(c('PEXPIRETIME', args[0])), *map(str, args[2:]), 'ABSTTL'])
        elif propagated is not None:
            expected.append(propagated)
        want_events.extend(evs)

    # Without a TTL, or with ABSTTL: as it came.
    restore('r:str', 0, p['str'], propagated=['RESTORE', 'r:str', '0', p['str']],
            evs=[('new', 'r:str'), ('restore', 'r:str')])
    far = int(time.time() * 1000) + 3600_000
    restore('r:abs', far, p['list'], 'absttl', 'REPLACE',
            propagated=['RESTORE', 'r:abs', str(far), p['list'], 'absttl', 'REPLACE'],
            evs=[('new', 'r:abs'), ('restore', 'r:abs')])
    # A relative TTL: the absolute time, the payload unchanged, every option kept, ABSTTL last.
    restore('r:list', 100000, p['list'], propagated='abs', evs=[('new', 'r:list'), ('restore', 'r:list')])
    restore('r:hash', 200000, p['hash'], 'IDLETIME', 7, propagated='abs',
            evs=[('new', 'r:hash'), ('restore', 'r:hash')])
    restore('r:set', 300000, p['set'], 'FREQ', 9, propagated='abs', evs=[('new', 'r:set'), ('restore', 'r:set')])
    restore('r:zset', 400000, p['zset'], propagated='abs', evs=[('new', 'r:zset'), ('restore', 'r:zset')])
    # REPLACE of a key that exists: deleted and added again, so `new` comes before `restore`.
    restore('r:list', 50000, p['str'], 'replace', 'IDLETIME', 3, propagated='abs',
            evs=[('new', 'r:list'), ('restore', 'r:list')])
    restore('r:str', 0, p['zset'], 'REPLACE', propagated=['RESTORE', 'r:str', '0', p['zset'], 'REPLACE'],
            evs=[('new', 'r:str'), ('restore', 'r:str')])
    # An absolute TTL that has passed: nothing stored, nothing sent, no change...
    restore('r:gone', PAST_MS, p['str'], 'ABSTTL', changes=0)
    restore('r:gone', PAST_MS, p['str'], 'ABSTTL', 'REPLACE', changes=0)
    # ...unless REPLACE deleted a key: then a DEL and a `del` event.
    assert c('SET', 'r:old', 'x') == '+OK'
    expected.append(['SET', 'r:old', 'x'])
    want_events += [('new', 'r:old'), ('set', 'r:old')]
    restore('r:old', PAST_MS, p['str'], 'REPLACE', 'ABSTTL', propagated=['DEL', 'r:old'], evs=[('del', 'r:old')])
    # REPLACE of a key whose TTL has passed but that is not deleted yet (active expiry is off): Redis's
    # dbDelete() erases it as it is, with no `expired` event and no DEL of its own.
    assert c('DEBUG', 'SET-ACTIVE-EXPIRE', 0) == '+OK'
    for key in ('r:exp1', 'r:exp2'):
        assert c('SET', key, 'x') == '+OK' and c('PEXPIRE', key, 50) == 1
        expected += [['SET', key, 'x'], ['PEXPIREAT', key, str(c('PEXPIRETIME', key))]]
        want_events += [('new', key), ('set', key), ('expire', key)]
    time.sleep(0.1)
    restore('r:exp1', 0, p['str'], 'REPLACE', propagated=['RESTORE', 'r:exp1', '0', p['str'], 'REPLACE'],
            evs=[('new', 'r:exp1'), ('restore', 'r:exp1')])
    restore('r:exp2', PAST_MS, p['str'], 'REPLACE', 'ABSTTL', propagated=['DEL', 'r:exp2'],
            evs=[('del', 'r:exp2')])

    # In a transaction or a script: the same forms; one write goes out without MULTI/EXEC.
    assert c('MULTI') == '+OK'
    assert c('RESTORE', 'r:multi', 100000, p['str']) == '+QUEUED'
    assert c('RESTORE', 'r:gone', PAST_MS, p['str'], 'ABSTTL') == '+QUEUED'
    assert c('EXEC') == ['+OK', '+OK']
    expected.append(['RESTORE', 'r:multi', str(c('PEXPIRETIME', 'r:multi')), p['str'], 'ABSTTL'])
    want_events += [('new', 'r:multi'), ('restore', 'r:multi')]
    assert c('EVAL', "return redis.call('restore', KEYS[1], ARGV[1], ARGV[2])", 1, 'r:lua', 100000,
             p['set']) == '+OK'
    expected.append(['restore', 'r:lua', str(c('PEXPIRETIME', 'r:lua')), p['set'], 'ABSTTL'])
    want_events += [('new', 'r:lua'), ('restore', 'r:lua')]

    assert c('SET', 'end', 1) == '+OK'
    expected.append(['SET', 'end', '1'])
    want_events += [('new', 'end'), ('set', 'end')]
    got = read_events(events, ('set', 'end'))
    assert got == want_events, got
    return expected


def check_propagation(name, binary, work):
    port = PORTS[name]
    rport = PORTS[name + '-replica']
    workdir = os.path.join(work, name, 'master')
    proc = start(binary, port, workdir, '--appendonly', 'yes', '--appendfsync', 'always',
                 '--notify-keyspace-events', 'EAn', '--repl-diskless-sync-delay', '0',
                 '--enable-debug-command', 'yes')
    replica_proc = start(binary, rport, os.path.join(work, name, 'replica'))
    try:
        r = Conn(rport)
        assert r('REPLICAOF', '127.0.0.1', port) == '+OK'
        wait_for(lambda: info(r, 'master_link_status') == 'up', 'the replica link')
        fake = FakeReplica(port)
        events = Conn(port)
        assert events('PSUBSCRIBE', '__keyevent@0__:*') == ['psubscribe', '__keyevent@0__:*', 1]
        c = Conn(port)
        expected = run_restores(c, events)
        events.close()
        stream = fake.commands_until(['SET', 'end', '1'])
        fake.close()
        assert stream == expected, (name, stream)
        assert aof_commands(aof_file(name, workdir)) == expected, (name, aof_commands(aof_file(name, workdir)))

        # The master, the replica and an AOF reload hold the same keys, values and expire times.
        wait_for(lambda: r('GET', 'end') == '1', 'the replicated writes')
        before = snapshot(c)
        assert set(before) == {'s:str', 's:list', 's:hash', 's:set', 's:zset', 'mark1', 'r:str', 'r:abs', 'r:list',
                               'r:hash', 'r:set', 'r:zset', 'r:multi', 'r:lua', 'r:exp1', 'end'}, (name, before)
        assert before['r:list'][0] == ('+string', 'hello') and before['r:list'][1] > 0, (name, before['r:list'])
        assert before['r:str'] == (('+zset', ['a', '1', 'b', '2.5']), -1), (name, before['r:str'])
        assert snapshot(r) == before, (name, snapshot(r))
        r.close()
        stop(replica_proc)
        c.close()
        crash(proc)
        proc = start(binary, port, workdir, '--appendonly', 'yes')
        c = Conn(port)
        assert snapshot(c) == before, (name, snapshot(c))
        c.close()
    finally:
        stop(replica_proc)
        stop(proc)


SYNTAX = '-ERR syntax error'
NOT_INT = '-ERR value is not an integer or out of range'
BUSY = '-BUSYKEY Target key name already exists.'
BAD = '-ERR Bad data format'
CHECKSUM = '-ERR DUMP payload version or checksum are wrong'


def check_payloads_and_args(name, binary, work):
    port = PORTS[name]
    proc = start(binary, port, os.path.join(work, name, 'args'))
    try:
        c = Conn(port)
        assert c('SET', 'src', 'hello') == '+OK'
        good = c('DUMP', 'src')
        nan = struct.pack('<d', math.nan)
        bad_payloads = [
            payload(b'\x63junk'),  # a type byte no Redis writes
            payload(b'\x00'),  # a string cut short
            payload(b'\x01\x00'),  # a list, a set, a zset, a hash with no element
            payload(b'\x02\x00'),
            payload(b'\x05\x00'),
            payload(b'\x04\x00'),
            payload(b'\x02\x02' + enc_str('a') + enc_str('a')),  # the same set member twice
            # A field twice, in a hash too big for a listpack (a value over hash-max-listpack-value): Redis
            # 7.2.5 with sanitize-dump-payload no takes a small one as a broken listpack; PeaDB refuses both.
            payload(b'\x04\x02' + enc_str('f') + enc_str('x' * 65) + enc_str('f') + enc_str('2')),
            payload(b'\x05\x02' + enc_str('m') + struct.pack('<d', 1) + enc_str('m') + struct.pack('<d', 2)),
            payload(b'\x05\x01' + enc_str('m') + nan),  # a NaN score
            # An LZF string that claims 2 GiB from 3 bytes (PeaDB used to zero-fill all of it first).
            payload(b'\x00\xc3\x03\x80\x80\x00\x00\x00\x02abc'),
        ]
        # REPLACE with a payload that cannot be read keeps the key that is there.
        assert c('RPUSH', 'keep', 'a', 'b') == 2
        assert c('EXPIRE', 'keep', 1000) == 1
        for p in bad_payloads:
            assert c('RESTORE', 'keep', 0, p, 'REPLACE') == BAD, (name, p)
            assert c('RESTORE', 'fresh', 0, p) == BAD, (name, p)
        if name == 'peadb':
            small_dup = payload(b'\x04\x02' + enc_str('f') + enc_str('1') + enc_str('f') + enc_str('2'))
            assert c('RESTORE', 'keep', 0, small_dup, 'REPLACE') == BAD, name
        assert c('RESTORE', 'keep', 0, good[:-1] + 'x', 'REPLACE') == CHECKSUM, name
        assert c('RESTORE', 'keep', PAST_MS, '\x00' * 9, 'ABSTTL', 'REPLACE') == CHECKSUM, name
        assert c('LRANGE', 'keep', 0, -1) == ['a', 'b'] and c('TTL', 'keep') > 990, name
        assert c('EXISTS', 'fresh') == 0, name
        # A payload that can be read is checked before an absolute TTL that has passed.
        assert c('RESTORE', 'keep', PAST_MS, bad_payloads[0], 'ABSTTL', 'REPLACE') == BAD, name
        assert c('EXISTS', 'keep') == 1, name

        assert c('SET', 'busy', 'v') == '+OK'
        for args, reply in [
            (('busy', -1, good), BUSY),  # the key is checked before the TTL and the payload
            (('busy', 'abc', 'junk'), BUSY),
            (('busy', 0, good, 'BOGUS'), SYNTAX),  # the options before the key
            (('busy', 0, good, 'IDLETIME', 'x'), NOT_INT),
            (('nokey', -1, good), '-ERR Invalid TTL value, must be >= 0'),
            (('nokey', 'abc', good), NOT_INT),
            (('nokey', ' 5', good), NOT_INT),
            (('nokey', '05', good), NOT_INT),
            (('nokey', '+5', good), NOT_INT),
            (('nokey', -1, 'junk'), '-ERR Invalid TTL value, must be >= 0'),  # the TTL before the payload
            (('nokey', 0, good, 'IDLETIME', 'abc'), NOT_INT),
            (('nokey', 0, good, 'IDLETIME', '01'), NOT_INT),
            (('nokey', 0, good, 'IDLETIME', -1), '-ERR Invalid IDLETIME value, must be >= 0'),
            (('nokey', 0, good, 'FREQ', 'x'), NOT_INT),
            (('nokey', 0, good, 'FREQ', 256), '-ERR Invalid FREQ value, must be >= 0 and <= 255'),
            (('nokey', 0, good, 'FREQ', -1), '-ERR Invalid FREQ value, must be >= 0 and <= 255'),
            (('nokey', 0, good, 'IDLETIME', 1, 'FREQ', 1), SYNTAX),
            (('nokey', 0, good, 'FREQ', 1, 'IDLETIME', 1), SYNTAX),
            (('nokey', 0, good, 'IDLETIME'), SYNTAX),
            (('nokey', 0, good, 'FREQ'), SYNTAX),
            (('nokey', 0, 'junk'), CHECKSUM),
        ]:
            assert c('RESTORE', *args) == reply, (name, args, c('RESTORE', *args))
        assert c('EXISTS', 'nokey') == 0, name
        assert c('RESTORE', 'nokey', 0, good, 'IDLETIME', 1, 'IDLETIME', 2) == '+OK', name
        assert c('RESTORE', 'nokey', 0, good, 'REPLACE', 'FREQ', 1, 'freq', 2, 'replace') == '+OK', name
        check_lru_lfu(name, c, good)
        c.close()
    finally:
        stop(proc)


def check_lru_lfu(name, c, good):
    """A restored key starts fresh; IDLETIME counts under an LRU policy, FREQ under an LFU policy."""
    assert c('CONFIG', 'SET', 'maxmemory-policy', 'allkeys-lru') == '+OK'
    assert c('RESTORE', 'lru1', 0, good) == '+OK'
    assert c('RESTORE', 'lru2', 0, good, 'FREQ', 100) == '+OK'
    assert c('RESTORE', 'lru3', 0, good, 'IDLETIME', 5000) == '+OK'
    assert c('OBJECT', 'IDLETIME', 'lru1') <= 1, (name, c('OBJECT', 'IDLETIME', 'lru1'))
    assert c('OBJECT', 'IDLETIME', 'lru2') <= 1, (name, c('OBJECT', 'IDLETIME', 'lru2'))
    assert 5000 <= c('OBJECT', 'IDLETIME', 'lru3') <= 5001, (name, c('OBJECT', 'IDLETIME', 'lru3'))
    # Redis's access clock counts seconds in 24 bits (about 194 days): longer idle times wrap.
    assert c('RESTORE', 'lru4', 0, good, 'IDLETIME', 1000000) == '+OK'
    assert c('RESTORE', 'lru5', 0, good, 'IDLETIME', 20000000) == '+OK'
    assert 1000000 <= c('OBJECT', 'IDLETIME', 'lru4') <= 1000001, (name, c('OBJECT', 'IDLETIME', 'lru4'))
    assert 3222784 <= c('OBJECT', 'IDLETIME', 'lru5') <= 3222786, (name, c('OBJECT', 'IDLETIME', 'lru5'))
    assert c('CONFIG', 'SET', 'maxmemory-policy', 'allkeys-lfu') == '+OK'
    assert c('RESTORE', 'lfu1', 0, good) == '+OK'
    assert c('RESTORE', 'lfu2', 0, good, 'FREQ', 100) == '+OK'
    assert c('RESTORE', 'lfu3', 0, good, 'IDLETIME', 5000) == '+OK'
    assert [c('OBJECT', 'FREQ', k) for k in ('lfu1', 'lfu2', 'lfu3')] == [5, 100, 5], name
    assert c('CONFIG', 'SET', 'maxmemory-policy', 'noeviction') == '+OK'


def check_engine(name, binary, work):
    check_propagation(name, binary, work)
    check_payloads_and_args(name, binary, work)
    print(f'{name}: ok')


def main():
    work = tempfile.mkdtemp(prefix='peadb-restoreprop-')
    try:
        if REDIS:
            check_engine('redis', REDIS, work)
        else:
            print('no pinned Redis 7.2.x: skipping the Redis side')
        check_engine('peadb', sys.argv[1] if len(sys.argv) > 1 else PEADB, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()

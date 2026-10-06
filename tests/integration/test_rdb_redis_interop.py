#!/usr/bin/env python3
"""PeaDB and the pinned Redis 7.2.5 read each other's RDB data (compat/delta.md item 27).

Both engines must use Redis's CRC-64 (Jones polynomial, reflected, crc64("123456789") = 0xe9c6d914c4b8d9ca) for the 8-byte checksum at the end of an RDB file, of a DUMP payload and of a FUNCTION DUMP payload. The checks:

- Each engine's dump.rdb loads in the other one with every key, value, TTL, database and function library intact (Redis's file also as written with an LRU and an LFU maxmemory-policy, which adds per-key opcodes), and redis-check-rdb 7.2.5 says "Checksum OK" for PeaDB's file.
- A DUMP payload of one engine RESTOREs in the other (from Redis only the types PeaDB's RESTORE reads today; the listpack, intset and quicklist encodings are item 51), FUNCTION DUMP / FUNCTION RESTORE work across engines, and MIGRATE from PeaDB moves keys into a real Redis.
- A real Redis replica of a PeaDB master and a PeaDB replica of a real Redis master finish their full sync with the same data. A PeaDB replica that gets a full sync with a bad checksum from a fake master keeps its keys and libraries and syncs again (PeaDB only: Redis exits there).
- Run on both engines with the same expected results: the checksum written is the one computed here in Python; a wrong checksum (including one computed with the table older PeaDB used) stops startup, `--rdbchecksum no` loads it anyway and saves a zero checksum, a zero checksum loads with `rdbchecksum yes`, CONFIG SET rdbchecksum is refused (immutable), a file cut short is refused even with `--rdbchecksum no`, a file or a DUMP payload with a newer RDB version than 11 is refused, RESTORE and FUNCTION RESTORE refuse a wrong checksum with Redis's errors, RESTORE of an unreadable body answers "Bad data format", and DEBUG SET-SKIP-CHECKSUM-VALIDATION 1 lets RESTORE take a wrong checksum.

Without a Redis 7.2.x redis-server (the pinned build is found first, see _redis_path.py), only the PeaDB side of the checks runs.
"""
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _redis_path import redis_server_path  # noqa: E402

PEADB = str(ROOT / 'peadb-server')


def redis_7_2():
    """The redis-server to compare with, only if it is Redis 7.2.x: other versions write and accept other RDB versions."""
    path = redis_server_path()
    if not path:
        return None
    version = subprocess.run([path, '--version'], capture_output=True, text=True).stdout
    return path if ' v=7.2.' in version else None


REDIS = redis_7_2()
REDIS_CHECK_RDB = os.path.join(os.path.dirname(REDIS), 'redis-check-rdb') if REDIS else None
PORTS = {'peadb': 6586, 'redis': 6587, 'fake-master': 6588}

# no-writes, so FCALL also runs on a read-only replica.
LIB = ("#!lua name=interoplib\nredis.register_function{function_name='interop_f', "
       "callback=function(keys, args) return 'from-lib' end, flags={'no-writes'}}")


# ── CRC-64 oracle (Redis's crc64.c: Jones polynomial, reflected in and out, init 0) ──
def _table(poly):
    table = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ poly if c & 1 else c >> 1
        table.append(c)
    return table


REDIS_TABLE = _table(0x95ac9329ac4bc9b5)   # Jones polynomial 0xad93d23594c935a9, bit-reflected
OLD_PEADB_TABLE = _table(0xad93d23594c935a9)  # what PeaDB used before the fix (unreflected)


def crc64(data, table=REDIS_TABLE):
    c = 0
    for b in data:
        c = table[(c ^ b) & 0xff] ^ (c >> 8)
    return c


assert crc64(b'123456789') == 0xe9c6d914c4b8d9ca


def with_checksum(body, table=REDIS_TABLE):
    return body + crc64(body, table).to_bytes(8, 'little')


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
            return None if n < 0 else self.f.read(n + 2)[:-2]
        if kind == b'*':
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise ValueError(f'unexpected reply {line!r}')

    def __call__(self, *args):
        out = f'*{len(args)}\r\n'.encode()
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode()
            out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
        self.sock.sendall(out)
        return self.read()


STARTED = []  # every server this test started, stopped in main() even when a check fails


def start(binary, port, workdir, *extra):
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    log = open(log_path, 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                             '--loglevel', 'notice', '--save', '', '--enable-debug-command', 'yes',
                             *extra],
                            stdout=log, stderr=subprocess.STDOUT)
    STARTED.append(proc)
    deadline = time.time() + 15
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'{binary} exited: {open(log_path, errors="replace").read()}')
        try:
            c = Conn(port)
            reply = c('PING')
            c.close()
            if reply == '+PONG':
                return proc
        except OSError:
            pass
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


def read_log(workdir):
    return open(os.path.join(workdir, 'server.log'), errors='replace').read()


def expect_start_fails(name, binary, port, workdir, *extra):
    """Starts the server and expects it to exit with an error; returns the new log text."""
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    before = os.path.getsize(log_path) if os.path.exists(log_path) else 0
    with open(log_path, 'ab') as log:
        proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                                 '--loglevel', 'notice', '--save', '', *extra],
                                stdout=log, stderr=subprocess.STDOUT)
    STARTED.append(proc)
    try:
        rc = proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        stop(proc)
        raise AssertionError(f'{name}: started although it should not ({extra})')
    assert rc != 0, (name, rc)
    return read_log(workdir)[before:]


def ok(reply, what):
    assert reply == '+OK', (what, reply)


def populate(c):
    """Every type and encoding both engines write (no streams: PeaDB writes them as its own type 200, item 5)."""
    ok(c('FLUSHALL'), 'flushall')
    c('FUNCTION', 'FLUSH')
    ok(c('SET', 'str', 'hello world'), 'set')
    ok(c('SET', 'int', '12345'), 'set int')
    ok(c('SET', 'neg', '-7'), 'set neg')
    ok(c('SET', 'zip', 'abc' * 200), 'set zip')  # Redis writes this LZF-compressed
    ok(c('SET', 'bin', bytes(range(256))), 'set bin')
    ok(c('SET', 'ttl', 'v', 'PX', 3600000), 'set ttl')
    c('RPUSH', 'list_small', 'a', 'b', 'c', '1', '2')
    c('RPUSH', 'list_big', *[f'item{i}' for i in range(1000)])
    c('RPUSH', 'list_long', *['y' * 100 + str(i) for i in range(10)])
    c('SADD', 'set_int', 1, 2, 3, -5, 1000000)
    c('SADD', 'set_small', 'a', 'b', 'c')
    c('SADD', 'set_big', *[f'm{i}' for i in range(600)])
    c('HSET', 'hash_small', 'f1', 'v1', 'f2', '2')
    fields = []
    for i in range(600):
        fields += [f'f{i}', f'v{i}']
    c('HSET', 'hash_big', *fields)
    c('HSET', 'hash_ttl', 'a', '1')
    assert c('EXPIRE', 'hash_ttl', 100000) == 1
    c('ZADD', 'zset_small', 1, 'a', 1.5, 'b', -0.25, 'c')
    members = []
    for i in range(200):
        members += [i * 0.5, f'z{i}']
    c('ZADD', 'zset_big', *members, 'inf', 'pinf', '-inf', 'ninf')
    ok(c('SELECT', 3), 'select 3')
    ok(c('SET', 'db3key', 'in db 3'), 'set db3')
    c('RPUSH', 'db3list', 'x')
    ok(c('SELECT', 0), 'select 0')
    assert c('FUNCTION', 'LOAD', LIB) == b'interoplib'


def value_of(c, key):
    kind = c('TYPE', key)
    if kind == '+string':
        value = c('GET', key)
    elif kind == '+list':
        value = c('LRANGE', key, 0, -1)
    elif kind == '+set':
        value = sorted(c('SMEMBERS', key))
    elif kind == '+hash':
        flat = c('HGETALL', key)
        value = sorted(zip(flat[::2], flat[1::2]))
    elif kind == '+zset':
        flat = c('ZRANGE', key, 0, -1, 'WITHSCORES')
        value = [(m, float(s)) for m, s in zip(flat[::2], flat[1::2])]
    else:
        raise AssertionError(f'unexpected type {kind} for {key!r}')
    pttl = c('PTTL', key)
    return kind, value, 'ttl' if pttl > 0 else pttl


def state(c):
    """Every key in every database, with its type, value and whether it has a TTL, plus the function libraries."""
    out = {}
    for db in range(16):
        ok(c('SELECT', db), f'select {db}')
        for key in c('KEYS', '*'):
            out[(db, key)] = value_of(c, key)
    ok(c('SELECT', 0), 'select 0')
    libs = c('FUNCTION', 'LIST')
    out['libraries'] = sorted(lib[1] for lib in libs)  # [library_name, <name>, engine, ...]
    out['fcall'] = c('FCALL', 'interop_f', 0) if out['libraries'] else None
    return out


def wait_for(cond, what, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.1)
    raise AssertionError(f'timed out waiting for {what}')


def write_dataset_and_save(binary, port, workdir, *extra):
    proc = start(binary, port, workdir, *extra)
    c = Conn(port)
    populate(c)
    expected = state(c)
    ok(c('SAVE'), 'save')
    c.close()
    stop(proc)
    return expected


def load_and_compare(binary, port, workdir, expected, what):
    proc = start(binary, port, workdir)
    try:
        c = Conn(port)
        got = state(c)
        c.close()
        diff = [k for k in set(got) | set(expected) if got.get(k) != expected.get(k)]
        assert not diff, (what, [(k, expected.get(k), got.get(k)) for k in sorted(diff, key=str)[:5]])
    finally:
        stop(proc)


# ── Cross-engine checks (need the pinned Redis) ──

def check_redis_file_in_peadb(work):
    # With an LRU or LFU maxmemory-policy, Redis writes each key's idle time (opcode 248) or
    # access frequency (249) before it.
    for policy in ('noeviction', 'allkeys-lru', 'allkeys-lfu'):
        rdir = os.path.join(work, f'x-redis-file-{policy}')
        expected = write_dataset_and_save(REDIS, PORTS['redis'], rdir, '--maxmemory-policy', policy)
        pdir = os.path.join(work, f'x-peadb-loads-redis-{policy}')
        os.makedirs(pdir)
        shutil.copy(os.path.join(rdir, 'dump.rdb'), os.path.join(pdir, 'dump.rdb'))
        load_and_compare(PEADB, PORTS['peadb'], pdir, expected, f'redis dump.rdb ({policy}) in peadb')


def check_peadb_file_in_redis(work):
    pdir = os.path.join(work, 'x-peadb-file')
    expected = write_dataset_and_save(PEADB, PORTS['peadb'], pdir)
    dump = os.path.join(pdir, 'dump.rdb')
    check = subprocess.run([REDIS_CHECK_RDB, dump], capture_output=True, text=True)
    assert check.returncode == 0 and 'Checksum OK' in check.stdout and 'RDB looks OK' in check.stdout, \
        check.stdout + check.stderr
    rdir = os.path.join(work, 'x-redis-loads-peadb')
    os.makedirs(rdir)
    shutil.copy(dump, os.path.join(rdir, 'dump.rdb'))
    load_and_compare(REDIS, PORTS['redis'], rdir, expected, 'peadb dump.rdb in redis')


# Redis DUMP types PeaDB's RESTORE reads today; the listpack, intset and quicklist ones are item 51.
REDIS_TO_PEADB_KEYS = ['str', 'int', 'neg', 'zip', 'bin', 'set_big', 'hash_big', 'zset_big']
PEADB_TO_REDIS_KEYS = ['str', 'int', 'neg', 'zip', 'bin', 'list_small', 'list_big', 'list_long',
                       'set_int', 'set_small', 'set_big', 'hash_small', 'hash_big', 'zset_small',
                       'zset_big']


def check_dump_restore_across(work):
    rproc = start(REDIS, PORTS['redis'], os.path.join(work, 'x-dump-redis'))
    pproc = start(PEADB, PORTS['peadb'], os.path.join(work, 'x-dump-peadb'))
    try:
        r, p = Conn(PORTS['redis']), Conn(PORTS['peadb'])
        for src, dst, keys, what in ((r, p, REDIS_TO_PEADB_KEYS, 'redis->peadb'),
                                     (p, r, PEADB_TO_REDIS_KEYS, 'peadb->redis')):
            populate(src)
            ok(dst('FLUSHALL'), 'flushall')
            dst('FUNCTION', 'FLUSH')
            for key in keys:
                payload = src('DUMP', key)
                assert payload[-8:] == crc64(payload[:-8]).to_bytes(8, 'little'), (what, key)
                reply = dst('RESTORE', key, 0, payload)
                assert reply == '+OK', (what, key, reply)
                assert value_of(dst, key) == value_of(src, key), (what, key)
            payload = src('FUNCTION', 'DUMP')
            assert payload[-8:] == crc64(payload[:-8]).to_bytes(8, 'little'), (what, 'function dump')
            ok(dst('FUNCTION', 'RESTORE', payload), (what, 'function restore'))
            assert dst('FCALL', 'interop_f', 0) == b'from-lib', what
        r.close()
        p.close()
    finally:
        stop(rproc)
        stop(pproc)


def check_migrate_to_redis(work):
    """MIGRATE from PeaDB moves keys into a real Redis (it sends PeaDB's DUMP payloads)."""
    rproc = start(REDIS, PORTS['redis'], os.path.join(work, 'x-migrate-redis'))
    pproc = start(PEADB, PORTS['peadb'], os.path.join(work, 'x-migrate-peadb'))
    try:
        r, p = Conn(PORTS['redis']), Conn(PORTS['peadb'])
        populate(p)
        for key in PEADB_TO_REDIS_KEYS + ['ttl', 'hash_ttl']:
            want = value_of(p, key)
            ok(p('MIGRATE', '127.0.0.1', PORTS['redis'], key, 0, 5000), ('migrate', key))
            assert p('EXISTS', key) == 0 and value_of(r, key) == want, ('migrate', key)
        r.close()
        p.close()
    finally:
        stop(rproc)
        stop(pproc)


def check_full_sync(work, master_name, master_bin, replica_name, replica_bin):
    what = f'{replica_name} replica of {master_name} master'
    mport, rport = PORTS[master_name], PORTS[replica_name]
    mproc = start(master_bin, mport, os.path.join(work, f'x-sync-{master_name}-m'))
    rproc = start(replica_bin, rport, os.path.join(work, f'x-sync-{replica_name}-r'))
    try:
        m, r = Conn(mport), Conn(rport)
        populate(m)
        expected = state(m)
        ok(r('REPLICAOF', '127.0.0.1', mport), what)

        def synced():
            info = r('INFO', 'replication')
            return b'master_link_status:up' in info and state(r) == expected

        wait_for(synced, what)
        ok(m('SET', 'after-sync', 'streamed'), what)
        wait_for(lambda: r('GET', 'after-sync') == b'streamed', what + ' (stream)')
        m.close()
        r.close()
    finally:
        stop(rproc)
        stop(mproc)


# ── Checks run on both engines with the same expected results ──

def check_checksum_rules(name, binary, work):
    port = PORTS[name]
    workdir = os.path.join(work, name, 'rules')
    proc = start(binary, port, workdir)
    c = Conn(port)
    ok(c('SET', 'k', 'v'), name)
    c('RPUSH', 'l', 'a', 'b')
    ok(c('SAVE'), name)
    # CONFIG SET rdbchecksum is refused: it is immutable in Redis.
    assert c('CONFIG', 'GET', 'rdbchecksum') == [b'rdbchecksum', b'yes'], name
    assert c('CONFIG', 'SET', 'rdbchecksum', 'no') == \
        "-ERR CONFIG SET failed (possibly related to argument 'rdbchecksum') - can't set immutable config", name
    # DUMP payloads carry Redis's checksum; RESTORE refuses a wrong one or a newer RDB version.
    payload = c('DUMP', 'k')
    assert payload[-8:] == crc64(payload[:-8]).to_bytes(8, 'little'), name
    bad_crc = payload[:-1] + bytes([payload[-1] ^ 0x01])
    assert c('RESTORE', 'k2', 0, bad_crc) == '-ERR DUMP payload version or checksum are wrong', name
    old_crc = with_checksum(payload[:-8], OLD_PEADB_TABLE)
    assert c('RESTORE', 'k2', 0, old_crc) == '-ERR DUMP payload version or checksum are wrong', name
    v12 = with_checksum(payload[:-10] + (12).to_bytes(2, 'little'))
    assert c('RESTORE', 'k2', 0, v12) == '-ERR DUMP payload version or checksum are wrong', name
    v11 = with_checksum(payload[:-10] + (11).to_bytes(2, 'little'))
    ok(c('RESTORE', 'k11', 0, v11), name)
    assert c('GET', 'k11') == b'v', name
    # A good checksum over a body that cannot be read (no value; a string or a set whose length
    # points past the end of the payload): Redis's "Bad data format".
    for body in (b'', b'\x00\x40\xffab', b'\x02\x40\x64\x01a'):
        reply = c('RESTORE', 'k3', 0, with_checksum(body + (10).to_bytes(2, 'little')))
        assert reply == '-ERR Bad data format', (name, body, reply)
    ok(c('DEBUG', 'SET-SKIP-CHECKSUM-VALIDATION', 1), name)
    ok(c('RESTORE', 'k2', 0, bad_crc), name)
    ok(c('DEBUG', 'SET-SKIP-CHECKSUM-VALIDATION', 0), name)
    assert c('GET', 'k2') == b'v', name
    # FUNCTION DUMP is a DUMP-style payload too.
    assert c('FUNCTION', 'LOAD', LIB) == b'interoplib', name
    fpayload = c('FUNCTION', 'DUMP')
    assert fpayload[0] == 0xF5 and fpayload[-8:] == crc64(fpayload[:-8]).to_bytes(8, 'little'), name
    fbad = fpayload[:-1] + bytes([fpayload[-1] ^ 0x01])
    assert c('FUNCTION', 'RESTORE', fbad, 'REPLACE') == '-ERR DUMP payload version or checksum are wrong', name
    assert c('FUNCTION', 'RESTORE', fpayload, 'MERGE') == \
        '-ERR Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.', name
    not_function = with_checksum(b'\x00\x03abc' + (11).to_bytes(2, 'little'))
    assert c('FUNCTION', 'RESTORE', not_function, 'REPLACE') == '-ERR given type is not a function', name
    c.close()
    stop(proc)

    dump = os.path.join(workdir, 'dump.rdb')
    data = open(dump, 'rb').read()
    assert data[-8:] == crc64(data[:-8]).to_bytes(8, 'little'), (name, 'file checksum')

    def write(body):
        with open(dump, 'wb') as f:
            f.write(body)

    def loads(*extra):
        proc = start(binary, port, workdir, *extra)
        c = Conn(port)
        got = (c('GET', 'k'), c('LRANGE', 'l', 0, -1))
        c.close()
        stop(proc)
        assert got == (b'v', [b'a', b'b']), (name, extra, got)

    # A wrong checksum stops startup with Redis's log line, also for the table older PeaDB used.
    for bad in (data[:-1] + bytes([data[-1] ^ 0x01]), with_checksum(data[:-8], OLD_PEADB_TABLE)):
        write(bad)
        log = expect_start_fails(name, binary, port, workdir)
        stored = int.from_bytes(bad[-8:], 'little')
        assert f'Wrong RDB checksum expected: ({crc64(data[:-8]):x}) but got ({stored:x}). Aborting now.' in log, \
            (name, log[-1500:])
        # rdbchecksum no loads it anyway, as in Redis (the way to keep a file written by older PeaDB).
        loads('--rdbchecksum', 'no')
    # With rdbchecksum no, SAVE writes a zero checksum ...
    proc = start(binary, port, workdir, '--rdbchecksum', 'no')
    c = Conn(port)
    assert c('CONFIG', 'GET', 'rdbchecksum') == [b'rdbchecksum', b'no'], name
    ok(c('SAVE'), name)
    c.close()
    stop(proc)
    zero = open(dump, 'rb').read()
    assert zero[-8:] == bytes(8) and zero[:-8] != b'', name
    # ... which loads with rdbchecksum yes, as "saved with checksum disabled".
    before = len(read_log(workdir))
    loads()
    assert 'RDB file was saved with checksum disabled: no check performed.' in read_log(workdir)[before:], name
    # A file cut short (here after its first key) is refused even when the checksum is not checked.
    cut = data[:data.index(b'\x00\x01k\x01v') + 5]
    write(cut)
    log = expect_start_fails(name, binary, port, workdir, '--rdbchecksum', 'no')
    assert 'Short read or OOM loading DB. Unrecoverable error, aborting now.' in log, (name, log[-1500:])
    # A newer RDB version than Redis 7.2.5's 11 is refused.
    write(with_checksum(b'REDIS0012' + data[9:-8]))
    log = expect_start_fails(name, binary, port, workdir)
    assert "Can't handle RDB format version 12" in log, (name, log[-1500:])
    print(f'{name}: checksum rules ok')


class FakeMaster:
    """Answers a replica's handshake and sends one full-sync RDB per connection: `rdbs[i]` to the i-th connection (the last one again after that)."""

    def __init__(self, port, rdbs):
        self.rdbs = rdbs
        self.connections = 0
        self.socks = []
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(('127.0.0.1', port))
        self.listener.listen(4)
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while True:
            try:
                sock, _ = self.listener.accept()
            except OSError:
                return
            self.socks.append(sock)
            rdb = self.rdbs[min(self.connections, len(self.rdbs) - 1)]
            self.connections += 1
            threading.Thread(target=self._handshake, args=(sock, rdb), daemon=True).start()

    def _handshake(self, sock, rdb):
        f = sock.makefile('rb')
        try:
            while True:
                line = f.readline()
                if not line:
                    return
                args = []
                for _ in range(int(line[1:-2])):
                    n = int(f.readline()[1:-2])
                    args.append(f.read(n + 2)[:-2].decode().upper())
                if args[0] == 'PING':
                    sock.sendall(b'+PONG\r\n')
                elif args[0] == 'PSYNC':
                    sock.sendall(b'+FULLRESYNC ' + b'f' * 40 + b' 0\r\n$' + str(len(rdb)).encode() + b'\r\n' + rdb)
                else:
                    sock.sendall(b'+OK\r\n')
        except (OSError, ValueError):
            return

    def close(self):
        self.listener.close()
        for sock in self.socks:
            sock.close()


def check_replica_refuses_bad_checksum(work):
    """A PeaDB replica checks the checksum of the RDB its master sends: on a mismatch it keeps its keys and function libraries, logs Redis's line and syncs again. (Redis 7.2.5 runs redis-check-rdb and exits there instead.)"""
    body = b'REDIS0011' + b'\xfe\x00' + b'\x00\x06synced\x03yes' + b'\xff'
    good = with_checksum(body)
    bad = good[:-1] + bytes([good[-1] ^ 0x01])
    port = PORTS['peadb']
    workdir = os.path.join(work, 'peadb', 'bad-sync')
    master = FakeMaster(PORTS['fake-master'], [bad, good])
    proc = start(PEADB, port, workdir)
    try:
        c = Conn(port)
        ok(c('SET', 'mine', 'kept'), 'bad sync')
        assert c('FUNCTION', 'LOAD', LIB) == b'interoplib'
        ok(c('REPLICAOF', '127.0.0.1', PORTS['fake-master']), 'bad sync')
        wait_for(lambda: 'Wrong RDB checksum expected' in read_log(workdir), 'the checksum error')
        assert c('GET', 'mine') == b'kept' and c('GET', 'synced') is None, 'keys kept after a bad sync'
        assert c('FCALL', 'interop_f', 0) == b'from-lib', 'libraries kept after a bad sync'
        wait_for(lambda: c('GET', 'synced') == b'yes', 'the second sync')
        assert master.connections >= 2 and c('GET', 'mine') is None and c('FUNCTION', 'LIST') == []
        ok(c('REPLICAOF', 'NO', 'ONE'), 'bad sync')
        c.close()
    finally:
        stop(proc)
        master.close()
    print('peadb: replica refuses a full sync with a bad checksum ok')


def main():
    work = tempfile.mkdtemp(prefix='peadb-rdbinterop-')
    try:
        if REDIS:
            check_checksum_rules('redis', REDIS, work)
        else:
            print('no Redis 7.2.x redis-server: skipping the Redis side')
        check_checksum_rules('peadb', PEADB, work)
        check_replica_refuses_bad_checksum(work)
        if REDIS:
            check_redis_file_in_peadb(work)
            check_peadb_file_in_redis(work)
            print('dump.rdb both ways: ok')
            check_dump_restore_across(work)
            print('DUMP/RESTORE and FUNCTION DUMP/RESTORE both ways: ok')
            check_migrate_to_redis(work)
            print('MIGRATE to redis: ok')
            check_full_sync(work, 'peadb', PEADB, 'redis', REDIS)
            check_full_sync(work, 'redis', REDIS, 'peadb', PEADB)
            print('full sync both ways: ok')
    finally:
        for proc in STARTED:
            stop(proc)
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()

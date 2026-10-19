#!/usr/bin/env python3
"""Redis 7.2.5-checked pre-7.0 RDB encodings and database-range refusal (delta 53).

Fixtures follow Redis's ziplist.c/zipmap.c layouts; Redis no longer writes them.
Every valid fixture is RESTOREd, loaded, saved and checked by the pinned Redis
before PeaDB is tested. Invalid RESTOREs must preserve REPLACE's existing key.
Full-sync validation must preserve the live keys, libraries and disk snapshot.
"""
import os
import pathlib
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

from test_rdb_redis_interop import Conn, REDIS, with_checksum
from test_dump_native_interop import payload, port, wait_for

ROOT = pathlib.Path(__file__).resolve().parents[2]
PEADB = sys.argv[1] if len(sys.argv) > 1 else str(ROOT / 'peadb-server')
RANGE_ERROR = ('FATAL: Data file was created with a Redis server configured to handle '
               'more than 16 databases. Exiting')
LIB = ("#!lua name=kept\nredis.register_function{function_name='kept', "
       "callback=function() return 'old' end, flags={'no-writes'}}")


def length(n):
    if n < 64:
        return bytes([n])
    if n < 16384:
        return bytes([0x40 | (n >> 8), n & 255])
    return (b'\x80' + struct.pack('>I', n) if n <= 0xffffffff
            else b'\x81' + struct.pack('>Q', n))


def string(b):
    return length(len(b)) + b


def ziplist(items, unknown=False):
    """bytes = string; (integer, width) explicitly exercises each integer encoding."""
    out = bytearray(b'\x00' * 10)
    previous, tail = 0, 10
    for item in items:
        tail = len(out)
        out += bytes([previous]) if previous < 254 else b'\xfe' + struct.pack('<I', previous)
        if isinstance(item, bytes):
            n = len(item)
            out += (bytes([n]) if n < 64 else
                    bytes([0x40 | (n >> 8), n & 255]) if n < 16384 else
                    b'\x80' + struct.pack('>I', n))
            out += item
        else:
            n, width = item
            out += bytes([0xf1 + n]) if width == 0 else bytes([
                {1: 0xfe, 2: 0xc0, 3: 0xf0, 4: 0xd0, 8: 0xe0}[width]]) + n.to_bytes(
                    width, 'little', signed=True)
        previous = len(out) - tail
    out += b'\xff'
    out[:10] = struct.pack('<IIH', len(out), tail, 65535 if unknown else min(65535, len(items)))
    return bytes(out)


def zipmap(pairs, unknown=False):
    def zlen(n):
        return bytes([n]) if n < 254 else b'\xfe' + struct.pack('<I', n)
    out = bytes([254 if unknown else len(pairs)])
    for key, value in pairs:
        # Three free bytes after each value, as left by a shrinking update.
        out += zlen(len(key)) + key + zlen(len(value)) + b'\x03' + value + b'\x00' * 3
    return out + b'\xff'


def fixtures():
    integers = [(0, 0), (12, 0), (-128, 1), (127, 1), (-32768, 2), (32767, 2),
                (-8388608, 3), (8388607, 3), (-2147483648, 4), (2147483647, 4),
                (-9223372036854775808, 8), (9223372036854775807, 8)]
    items = [b'', b'a\x00\xff', b'x' * 63, b'x' * 64, b'x' * 253, b'x' * 254,
             b'x' * 16383, b'x' * 16384] + integers
    logical = [i if isinstance(i, bytes) else str(i[0]).encode() for i in items]
    fields = [(b'', b''), (b'bin\x00\xff', b'v\x00\xff'), (b'k' * 254, b'v' * 300)]
    hash_items = [part for pair in fields for part in pair] + [b'int', (-8388608, 3)]
    scores = [b'low', b'-inf', b'negative', b'-1.25', b'zero', (0, 0),
              b'tiny', b'5e-324', b'high', b'inf']
    return [
        (b'zipmap', b'\x09' + string(zipmap(fields)), 'hash', dict(fields)),
        (b'zipmap-unknown', b'\x09' + string(zipmap(fields, True)), 'hash', dict(fields)),
        (b'list', b'\x0a' + string(ziplist(items)), 'list', logical),
        (b'list-unknown', b'\x0a' + string(ziplist(items, True)), 'list', logical),
        (b'list-many', b'\x0a' + string(ziplist([b'a'] * 65536)), 'list', [b'a'] * 65536),
        (b'hash', b'\x0d' + string(ziplist(hash_items)), 'hash',
         dict(fields + [(b'int', b'-8388608')])),
        (b'zset', b'\x0c' + string(ziplist(scores)), 'zset', None),
        (b'quicklist', b'\x0e' + length(3) + string(ziplist(items[:5])) +
         string(ziplist(items[5:])) + string(ziplist([])), 'list', logical),
    ]


def state(c, key, kind):
    if kind == 'list':
        return c('LRANGE', key, 0, -1)
    if kind == 'hash':
        a = c('HGETALL', key)
        return dict(zip(a[::2], a[1::2]))
    a = c('ZRANGE', key, 0, -1, 'WITHSCORES')
    return [(a[i], float(a[i+1])) for i in range(0, len(a), 2)]


def rdb(cases):
    out = b'REDIS0009\xfe\x01'
    for key, body, _, _ in cases:
        out += b'\xfc' + struct.pack('<Q', 4102444800000) + body[:1] + string(key) + body[1:]
    return with_checksum(out + b'\xff')


def start(binary, p, work, *extra):
    os.makedirs(work, exist_ok=True)
    logpath = pathlib.Path(work) / 'server.log'
    with logpath.open('ab') as log:
        proc = subprocess.Popen([binary, '--port', str(p), '--bind', '127.0.0.1', '--dir', str(work),
                                 '--save', '', '--loglevel', 'notice', '--shutdown-timeout', '0',
                                 '--enable-debug-command', 'yes', *extra], stdout=log, stderr=log)
    try:
        def ready():
            if proc.poll() is not None:
                raise AssertionError(logpath.read_text(errors='replace'))
            try:
                c = Conn(p)
                try:
                    return c('PING') == '+PONG'
                finally:
                    c.close()
            except OSError:
                return False
        wait_for(ready, 10)
        return proc
    except BaseException:
        stop(proc)
        raise


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    proc.wait()


def refuses(binary, p, work, data):
    work.mkdir(parents=True, exist_ok=True)
    (work / 'dump.rdb').write_bytes(data)
    with (work / 'server.log').open('wb') as log:
        proc = subprocess.Popen([binary, '--port', str(p), '--bind', '127.0.0.1', '--dir', str(work),
                                 '--save', '', '--loglevel', 'notice'], stdout=log, stderr=log)
    try:
        rc = proc.wait(timeout=5)
        assert rc != 0, rc
    except subprocess.TimeoutExpired:
        c = Conn(p)
        try:
            c('SELECT', 1)
            observed = c('GET', 'same')
        finally:
            c.close()
        raise AssertionError(f'accepted invalid database; previous DB key overwritten with {observed!r}')
    finally:
        stop(proc)
    return (work / 'server.log').read_text(errors='replace')


def check_legacy(binary, name, work):
    p = port()
    d = work / name
    cases = fixtures()
    d.mkdir()
    (d / 'dump.rdb').write_bytes(rdb(cases))
    # Check and load the fixture before any PeaDB expectation is used.
    subprocess.run([os.path.join(os.path.dirname(REDIS), 'redis-check-rdb'), str(d / 'dump.rdb')],
                   check=True, stdout=subprocess.DEVNULL)
    proc = start(binary, p, d, '--sanitize-dump-payload', 'yes')
    c = Conn(p)
    try:
        assert c('SELECT', 1) == '+OK'
        for key, body, kind, expected in cases:
            if expected is None:
                expected = [(b'low', float('-inf')), (b'negative', -1.25), (b'zero', 0.0),
                            (b'tiny', 5e-324), (b'high', float('inf'))]
            assert state(c, key, kind) == expected, (name, key, 'RDB')
            assert c('PEXPIRETIME', key) == 4102444800000, (name, key, 'TTL')
            assert c('RESTORE', key, 4102444800000, payload(body), 'ABSTTL', 'REPLACE') == '+OK', (name, key)
            assert state(c, key, kind) == expected, (name, key, 'RESTORE')
        # Well-framed, checksum-valid but structurally invalid encodings.
        invalid = [b'\x09' + string(zipmap([])),
                   b'\x09' + string(zipmap([(b'f', b'v')])[:-1]),
                   b'\x0a' + string(ziplist([b'a'])[:-1]),
                   b'\x0a' + string(ziplist([b'a'])[:10] + b'\x01' + ziplist([b'a'])[11:]),
                   b'\x0d' + string(ziplist([b'f'])),
                   b'\x0e\x01' + string(b'broken'),
                   b'\x09' + string(zipmap([(b'f', b'v'), (b'f', b'other')])),
                   b'\x0d' + string(ziplist([b'f', b'v', b'f', b'other'])),
                   b'\x0c' + string(ziplist([b'm', b'1', b'm', b'2']))]
        zl = ziplist([b'a'])
        invalid += [b'\x0a' + string(zl[:offset] + changed + zl[offset+len(changed):])
                    for offset, changed in [(0, b'\xff' * 4), (4, b'\x00' * 4),
                                            (8, b'\x02\x00'), (11, b'\xff')]]
        if name == 'peadb':
            # Redis's legacy zset conversion accepts NaN even with sanitization;
            # PeaDB fails loudly, as its existing compact readers do (item 51).
            invalid.append(b'\x0c' + string(ziplist([b'm', b'nan'])))
        for body in invalid:
            assert c('SET', 'keep', 'old') == '+OK'
            assert c('RESTORE', 'keep', 0, payload(body), 'REPLACE') == '-ERR Bad data format', (name, body)
            assert c('GET', 'keep') == b'old'
        assert c('SAVE') == '+OK'
    finally:
        c.close(); stop(proc)
    subprocess.run([os.path.join(os.path.dirname(REDIS), 'redis-check-rdb'), str(d / 'dump.rdb')],
                   check=True, stdout=subprocess.DEVNULL)
    proc = start(REDIS, p, d)
    c = Conn(p)
    try:
        assert c('SELECT', 1) == '+OK'
        for key, _, kind, expected in cases:
            if expected is not None:
                assert state(c, key, kind) == expected, (name, key, 'Redis reload')
        assert state(c, b'zset', 'zset') == [(b'low', float('-inf')), (b'negative', -1.25),
                                           (b'zero', 0.0), (b'tiny', 5e-324), (b'high', float('inf'))]
    finally:
        c.close(); stop(proc)
    print(name + ': legacy RESTORE/RDB, integer and string widths, TTL and Redis reload passed', flush=True)


def database_fixture(work):
    p = port()
    d = work / 'redis-32'
    proc = start(REDIS, p, d, '--databases', '32')
    c = Conn(p)
    try:
        for db, value in [(1, 'valid'), (31, 'out-of-range')]:
            assert c('SELECT', db) == '+OK'
            assert c('SET', 'same', value) == '+OK'
        assert c('SAVE') == '+OK'
        return (d / 'dump.rdb').read_bytes()
    finally:
        c.close(); stop(proc)


class Master:
    """One owned thread; holds the next full sync until the test permits it."""
    def __init__(self, bad, good):
        self.bad, self.good = bad, good
        self.p = port()
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', self.p)); self.listener.listen(4)
        self.listener.settimeout(.1)
        self.done, self.resume = threading.Event(), threading.Event()
        self.sock = None
        self.error = None
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def run(self):
        try:
            first = True
            while not self.done.is_set():
                try:
                    sock, _ = self.listener.accept()
                except socket.timeout:
                    continue
                self.sock = sock
                with sock, sock.makefile('rb') as f:
                    c = Conn.__new__(Conn); c.sock = sock; c.f = f
                    while not self.done.is_set():
                        args = c.read()
                        if args is None:
                            break
                        cmd = args[0].upper()
                        if cmd == b'PSYNC':
                            if not first:
                                while not self.resume.wait(.05) and not self.done.is_set():
                                    pass
                            if self.done.is_set():
                                break
                            data = self.bad if first else self.good
                            first = False
                            sock.sendall(b'+FULLRESYNC ' + b'f' * 40 + b' 0\r\n$' +
                                         str(len(data)).encode() + b'\r\n' + data)
                        elif cmd == b'PING':
                            sock.sendall(b'+PONG\r\n')
                        elif cmd == b'REPLCONF' and args[1].upper() == b'ACK':
                            pass
                        else:
                            sock.sendall(b'+OK\r\n')
                self.sock = None
        except OSError as e:
            if not self.done.is_set():
                self.error = e
        except BaseException as e:
            self.error = e

    def close(self):
        self.done.set(); self.resume.set()
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.thread.join(timeout=3)
        self.listener.close()
        assert not self.thread.is_alive(), 'master thread was not reaped'
        assert self.error is None, self.error


def check_sync(bad, tag, work):
    good = rdb(fixtures()[:1])
    master = Master(bad, good)
    p = port(); d = work / ('sync-' + tag)
    proc, c = None, None
    try:
        proc = start(PEADB, p, d, '--appendonly', 'yes', '--appendfsync', 'always')
        c = Conn(p)
        wait_for(lambda: b'aof_rewrite_in_progress:0' in c('INFO', 'persistence'))
        assert c('SELECT', 1) == '+OK'
        assert c('SET', 'same', 'kept') == '+OK'
        assert c('FUNCTION', 'LOAD', LIB) == b'kept'
        assert c('SAVE') == '+OK'
        snapshot = (d / 'dump.rdb').read_bytes()
        aof = (d / 'appendonly.aof').read_bytes()
        assert c('REPLICAOF', '127.0.0.1', master.p) == '+OK'
        wait_for(lambda: 'rdb_load on event loop failed:' in (d / 'server.log').read_text())
        assert c('GET', 'same') == b'kept', (tag, 'live data replaced')
        assert c('FCALL_RO', 'kept', 0) == b'old', (tag, 'libraries replaced')
        assert (d / 'dump.rdb').read_bytes() == snapshot, (tag, 'disk snapshot replaced')
        assert (d / 'appendonly.aof').read_bytes() == aof, (tag, 'AOF changed')
        master.resume.set()
        wait_for(lambda: b'master_link_status:up' in c('INFO', 'replication'))
        assert c('GET', 'same') is None
        assert state(c, b'zipmap', 'hash') == fixtures()[0][3]
        assert c('FUNCTION', 'LIST') == []
        assert c('REPLICAOF', 'NO', 'ONE') == '+OK'
    except BaseException:
        if (d / 'server.log').exists():
            print((d / 'server.log').read_text(errors='replace')[-8 * 1024:], flush=True)
        raise
    finally:
        if c:
            c.close()
        if proc:
            stop(proc)
        master.close()
    print('peadb: ' + tag + ' full sync refused before replacement, valid retry passed', flush=True)


def main():
    if not REDIS or ' v=7.2.5 ' not in subprocess.check_output([REDIS, '--version'], text=True):
        raise RuntimeError('Pinned Redis 7.2.5 is required')
    phase = os.environ.get('RDB_LEGACY_PHASE', 'all')
    with tempfile.TemporaryDirectory(prefix='peadb-rdb-legacy-') as temp:
        work = pathlib.Path(temp)
        if phase in ('all', 'legacy'):
            check_legacy(REDIS, 'redis', work)
            check_legacy(PEADB, 'peadb', work)
        if phase in ('all', 'database', 'sync'):
            data = database_fixture(work)
            if phase != 'sync':
                for binary, name in [(REDIS, 'redis'), (PEADB, 'peadb')]:
                    assert RANGE_ERROR in refuses(binary, port(), work / (name + '-range'), data)
                    print(name + ': real Redis DB 31 snapshot refused', flush=True)
            if phase != 'database':
                check_sync(data, 'database-31', work)
            # These IDs must be checked BEFORE narrowing to int (2**32 aliases DB 0).
            for db in (16, 2**31, 2**32, 2**64 - 1):
                bad = with_checksum(b'REDIS0011\xfe\x01\x00' + string(b'same') + string(b'valid') +
                                    b'\xfe' + length(db) + b'\x00' + string(b'same') +
                                    string(b'overwritten') + b'\xff')
                if phase != 'sync':
                    assert RANGE_ERROR in refuses(PEADB, port(), work / str(db), bad)
                if phase != 'database' and db == 2**32:
                    check_sync(bad, 'wide-database', work)
            if phase != 'database':
                check_sync(with_checksum(b'REDIS0011\xfe\x00\x0a' + string(b'bad') +
                                         string(b'broken') + b'\xff'), 'invalid-ziplist', work)


if __name__ == '__main__':
    main()

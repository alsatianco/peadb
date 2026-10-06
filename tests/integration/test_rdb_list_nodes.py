#!/usr/bin/env python3
"""Lists of any length survive RDB files and full syncs, split into nodes like Redis 7.2.5 (compat/delta.md item 29).

PeaDB used to write a whole list as one listpack, whose entry count is 16 bits, so a list of 65,536 or more items came back with N mod 65,536 items after SAVE, BGSAVE, DEBUG RELOAD or a full sync (65,536 items: the key was gone). It also used other backlen sizes than Redis for an entry of exactly 16,383 or 2,097,151 bytes, so each engine misread the other's listpack there. The checks:

- PeaDB only: lists of 65,536 and 100,000 items, a list with a TTL in database 2, and a list with elements on each listpack length limit (16,377/16,378/16,379 and 2,097,145/2,097,146/2,097,147 bytes, 8 KB, empty, binary, numbers) come back the same after SAVE + restart, BGSAVE + restart, DEBUG RELOAD and a full sync to a PeaDB replica; in PeaDB's dump.rdb every node of every list keeps to the list-max-listpack-size limit and its entry count is right.
- Both engines (needs a Redis 7.2.x redis-server): each engine's dump.rdb with those lists loads in the other one (redis-check-rdb says the PeaDB file is OK), a full sync works both ways, and for each list-max-listpack-size value tried PeaDB splits a list built by RPUSH into the same nodes as Redis does (Redis saving with rdbcompression no, so the nodes can be read here).
- Hand-built files, run on both engines with the same expected result: set, hash and zset listpacks whose entry count says 65535 ("unknown", 70,000 entries) load every entry; a PLAIN node loads as one element, also when its bytes would read as a listpack; a node container other than 1 or 2, and a list, set, hash or zset listpack whose total-bytes field is wrong or that has no 0xFF at the end, stop startup with Redis's "Quicklist integrity check failed." / "Listpack integrity check failed." (or the set, hash, zset text).
- PeaDB only: a list node of 70,000 entries with count "unknown" loads them all (Redis keeps N mod 65,536: its in-memory node count is 16 bits; Redis never writes such a node); a count that does not match the entries, as older PeaDB wrote for long lists, loads every entry, so such a dump.rdb comes back whole (Redis checks the count only with sanitize-dump-payload yes); a hash or zset listpack with an odd number of entries stops startup; and the encodings of Redis before 7.0 (types 9, 10, 12, 13, 14), which PeaDB cannot read yet (item 53), stop startup with a clear error instead of loading wrong data.
"""
import hashlib
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
    """The redis-server to compare with, only if it is Redis 7.2.x: other versions write other RDB versions."""
    path = redis_server_path()
    if not path:
        return None
    version = subprocess.run([path, '--version'], capture_output=True, text=True).stdout
    return path if ' v=7.2.' in version else None


REDIS = redis_7_2()
REDIS_CHECK_RDB = os.path.join(os.path.dirname(REDIS), 'redis-check-rdb') if REDIS else None
PORTS = {'peadb': 6592, 'redis': 6593, 'replica': 6594}


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=30)
        self.f = self.sock.makefile('rb', buffering=1 << 20)

    def close(self):
        self.f.close()
        self.sock.close()

    def read(self):
        line = self.f.readline()
        if not line:
            return None
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
        out = [f'*{len(args)}\r\n'.encode()]
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode()
            out.append(f'${len(b)}\r\n'.encode() + b + b'\r\n')
        self.sock.sendall(b''.join(out))
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
            raise RuntimeError(f'{binary} exited: {open(log_path, errors="replace").read()[-2000:]}')
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


def expect_start_fails(what, binary, port, workdir):
    """Starts the server on workdir's dump.rdb and expects it to exit with an error; returns the new log text."""
    log_path = os.path.join(workdir, 'server.log')
    before = os.path.getsize(log_path) if os.path.exists(log_path) else 0
    with open(log_path, 'ab') as log:
        proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                                 '--loglevel', 'notice', '--save', ''],
                                stdout=log, stderr=subprocess.STDOUT)
    STARTED.append(proc)
    try:
        rc = proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        stop(proc)
        raise AssertionError(f'{what}: started although it should not')
    assert rc != 0, (what, rc)
    return open(log_path, errors='replace').read()[before:]


def ok(reply, what):
    assert reply == '+OK', (what, reply)


def wait_for(cond, what, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.1)
    raise AssertionError(f'timed out waiting for {what}')


# ── The dataset ──

def mixed_items():
    """Elements on each listpack length limit: string headers of 1/2/5 bytes (63/64, 4095/4096), backlens of 2/3 and 3/4 bytes (entry length 16,383 and 2,097,151 with the 5-byte header: Redis's limits, not 16,384 and 2,097,152), a node of its own (8 KB and more), and empty, binary and number-like strings (kept as written)."""
    items = [b'', b'a', b'12345', b'-7', b'007', b'3.14', bytes(range(256))]
    for n in (63, 64, 4095, 4096, 8192, 16377, 16378, 16379, 2097145, 2097146, 2097147):
        items += [bytes([97 + n % 26]) * n, f'after-{n}'.encode()]
    return items


LISTS = {  # (db, key) -> items
    (0, 'l65536'): [f'e{i}'.encode() for i in range(65536)],  # 65,536 mod 65,536 = 0: the key was gone
    (0, 'l100000'): [f'item{i}'.encode() for i in range(100000)],  # came back with 34,464 items
    (0, 'lmixed'): mixed_items(),
    (2, 'lttl'): [f't{i}'.encode() for i in range(70000)],
}


def populate(c):
    ok(c('FLUSHALL'), 'flushall')
    for (db, key), items in LISTS.items():
        ok(c('SELECT', db), 'select')
        for i in range(0, len(items), 20000):
            assert c('RPUSH', key, *items[i:i + 20000]) == min(i + 20000, len(items)), key
    ok(c('SELECT', 2), 'select')
    assert c('PEXPIRE', 'lttl', 3600000) == 1
    ok(c('SELECT', 0), 'select')


def digest(items):
    h = hashlib.sha1()
    for it in items:
        h.update(len(it).to_bytes(4, 'little') + it)
    return h.hexdigest()


EXPECTED = {key: (len(items), digest(items)) for key, items in LISTS.items()}


def check_lists(c, what):
    """Every list has its items in order; lttl still has its TTL; nothing else is there."""
    for (db, key), (n, d) in EXPECTED.items():
        ok(c('SELECT', db), 'select')
        got = c('LRANGE', key, 0, -1)
        assert len(got) == n and digest(got) == d, (what, key, n, len(got))
    ok(c('SELECT', 2), 'select')
    assert c('PTTL', 'lttl') > 0, (what, 'ttl')
    assert c('DBSIZE') == 1, what
    ok(c('SELECT', 0), 'select')
    assert c('DBSIZE') == 3, what


# ── Reading the list nodes of a dump.rdb (no LZF: PeaDB never compresses, Redis runs with rdbcompression no) ──

def rdb_len(b, i):
    kind = b[i] >> 6
    if kind == 0:
        return b[i] & 0x3f, i + 1, False
    if kind == 1:
        return ((b[i] & 0x3f) << 8) | b[i + 1], i + 2, False
    if kind == 3:
        return b[i] & 0x3f, i + 1, True
    if b[i] == 0x80:
        return int.from_bytes(b[i + 1:i + 5], 'big'), i + 5, False
    return int.from_bytes(b[i + 1:i + 9], 'big'), i + 9, False


def rdb_str(b, i):
    n, i, encoded = rdb_len(b, i)
    if not encoded:
        return b[i:i + n], i + n
    size = {0: 1, 1: 2, 2: 4}[n]  # an integer; 3 (LZF) is not expected here
    return str(int.from_bytes(b[i:i + size], 'little', signed=True)).encode(), i + size


def listpack_entries(lp):
    """Entries of a listpack, checked like Redis's lpValidateIntegrity()."""
    assert int.from_bytes(lp[:4], 'little') == len(lp) and lp[-1] == 0xff
    out, i = [], 6
    while lp[i] != 0xff:
        e = lp[i]
        if e < 0x80:
            hdr, n = 1, 0
        elif e & 0xc0 == 0x80:
            hdr, n = 1, e & 0x3f
        elif e & 0xe0 == 0xc0:
            hdr, n = 2, 0
        elif e & 0xf0 == 0xe0:
            hdr, n = 2, ((e & 0x0f) << 8) | lp[i + 1]
        elif e == 0xf0:
            hdr, n = 5, int.from_bytes(lp[i + 1:i + 5], 'little')
        else:
            hdr, n = {0xf1: 3, 0xf2: 4, 0xf3: 5, 0xf4: 9}[e], 0
        size = hdr + n
        out.append(lp[i + hdr:i + size])
        i += size + len(backlen(size))
        assert lp[i - len(backlen(size)):i] == backlen(size), 'backlen'
    assert i == len(lp) - 1
    count = int.from_bytes(lp[4:6], 'little')
    assert count == 65535 or count == len(out), (count, len(out))
    return out


def list_nodes(path):
    """{key: [(container, element count, listpack bytes or element size), ...]} for every list in the file."""
    b = open(path, 'rb').read()
    assert b[:5] == b'REDIS'
    i, out = 9, {}
    while True:
        op = b[i]
        i += 1
        if op == 0xff:
            return out
        if op == 0xfa:  # AUX
            _, i = rdb_str(b, i)
            _, i = rdb_str(b, i)
        elif op == 0xfe:  # SELECTDB
            _, i, _ = rdb_len(b, i)
        elif op == 0xfb:  # RESIZEDB
            _, i, _ = rdb_len(b, i)
            _, i, _ = rdb_len(b, i)
        elif op == 0xfc:  # EXPIRETIME_MS
            i += 8
        elif op == 18:  # QUICKLIST_2
            key, i = rdb_str(b, i)
            count, i, _ = rdb_len(b, i)
            nodes = []
            for _ in range(count):
                container, i, _ = rdb_len(b, i)
                data, i = rdb_str(b, i)
                if container == 2:
                    nodes.append((2, len(listpack_entries(data)), len(data)))
                else:
                    nodes.append((container, 1, len(data)))
            out[key.decode()] = nodes
        else:
            raise AssertionError(f'unexpected opcode {op} at {i - 1}')


def node_limit_ok(nodes, fill):
    """Every PACKED node of more than one element keeps to Redis's limit for `fill`."""
    for container, count, size in nodes:
        if container != 2 or count == 1:
            continue
        if fill < 0:
            assert size <= (4096, 8192, 16384, 32768, 65536)[min(-fill - 1, 4)], (fill, count, size)
        else:
            assert size <= 8192 and count <= max(fill, 1), (fill, count, size)


# ── PeaDB round trips ──

def check_peadb_round_trips(work):
    port, rport = PORTS['peadb'], PORTS['replica']
    workdir = os.path.join(work, 'peadb')
    dump = os.path.join(workdir, 'dump.rdb')
    proc = start(PEADB, port, workdir)
    c = Conn(port)
    populate(c)
    check_lists(c, 'before saving')

    ok(c('SAVE'), 'save')
    for key, nodes in list_nodes(dump).items():
        assert sum(n for _, n, _ in nodes) == next(len(v) for (_, k), v in LISTS.items() if k == key), key
        node_limit_ok(nodes, -2)
    c.close()
    stop(proc)
    proc = start(PEADB, port, workdir)
    c = Conn(port)
    check_lists(c, 'SAVE + restart')

    os.remove(dump)
    assert c('BGSAVE') == '+Background saving started'
    wait_for(lambda: b'rdb_bgsave_in_progress:0' in c('INFO', 'persistence') and os.path.exists(dump), 'bgsave')
    assert b'rdb_last_bgsave_status:ok' in c('INFO', 'persistence')
    c.close()
    stop(proc)
    proc = start(PEADB, port, workdir)
    c = Conn(port)
    check_lists(c, 'BGSAVE + restart')

    ok(c('DEBUG', 'RELOAD'), 'debug reload')
    check_lists(c, 'DEBUG RELOAD')

    rproc = start(PEADB, rport, os.path.join(work, 'peadb-replica'))
    r = Conn(rport)
    ok(r('REPLICAOF', '127.0.0.1', port), 'replicaof')
    wait_for(lambda: b'master_link_status:up' in r('INFO', 'replication'), 'peadb replica link')
    wait_for(lambda: r('DBSIZE') == 3, 'peadb replica data')
    check_lists(r, 'full sync to a PeaDB replica')
    r.close()
    c.close()
    stop(rproc)
    stop(proc)
    print('peadb: SAVE, BGSAVE, DEBUG RELOAD and full sync keep every list ok')


# ── Both engines ──

def check_files_across(work):
    for src_name, src, dst_name, dst in (('peadb', PEADB, 'redis', REDIS), ('redis', REDIS, 'peadb', PEADB)):
        sdir = os.path.join(work, f'x-{src_name}-file')
        proc = start(src, PORTS[src_name], sdir)
        c = Conn(PORTS[src_name])
        populate(c)
        ok(c('SAVE'), 'save')
        c.close()
        stop(proc)
        dump = os.path.join(sdir, 'dump.rdb')
        if src_name == 'peadb':
            check = subprocess.run([REDIS_CHECK_RDB, dump], capture_output=True, text=True)
            assert check.returncode == 0 and 'Checksum OK' in check.stdout and 'RDB looks OK' in check.stdout, \
                check.stdout + check.stderr
        ddir = os.path.join(work, f'x-{dst_name}-loads-{src_name}')
        os.makedirs(ddir)
        shutil.copy(dump, os.path.join(ddir, 'dump.rdb'))
        proc = start(dst, PORTS[dst_name], ddir)
        c = Conn(PORTS[dst_name])
        check_lists(c, f'{src_name} dump.rdb in {dst_name}')
        c.close()
        stop(proc)
    print('dump.rdb with long lists both ways: ok')


def check_full_sync_across(work):
    for master_name, master, replica_name, replica in (('peadb', PEADB, 'redis', REDIS),
                                                       ('redis', REDIS, 'peadb', PEADB)):
        what = f'{replica_name} replica of {master_name} master'
        mport, rport = PORTS[master_name], PORTS[replica_name]
        mproc = start(master, mport, os.path.join(work, f'x-sync-{master_name}-m'))
        rproc = start(replica, rport, os.path.join(work, f'x-sync-{replica_name}-r'))
        m, r = Conn(mport), Conn(rport)
        populate(m)
        ok(r('REPLICAOF', '127.0.0.1', mport), what)
        wait_for(lambda: b'master_link_status:up' in r('INFO', 'replication'), what)
        wait_for(lambda: r('DBSIZE') == 3, what)
        check_lists(r, what)
        m.close()
        r.close()
        stop(rproc)
        stop(mproc)
    print('full sync with long lists both ways: ok')


def check_same_nodes_as_redis(work):
    """For each list-max-listpack-size, PeaDB's file holds the list in the same nodes as Redis's (no number-like items: Redis stores those as integers, which takes fewer bytes)."""
    # Seeded random lengths, so node ends also fall inside the 8 bytes of SIZE_ESTIMATE_OVERHEAD.
    rng = random.Random(29)
    sizes = [rng.randint(1, 120) if rng.random() < 0.99 else rng.choice((rng.randint(4090, 4100), 9000))
             for _ in range(5000)]
    items = [bytes([97 + i % 26]) * n for i, n in enumerate(sizes)]
    servers = {}
    for name, binary in (('peadb', PEADB), ('redis', REDIS)):
        workdir = os.path.join(work, f'x-nodes-{name}')
        servers[name] = (start(binary, PORTS[name], workdir, '--rdbcompression', 'no'), Conn(PORTS[name]),
                         os.path.join(workdir, 'dump.rdb'))
    for fill in (-2, -1, -3, -5, -9, 0, 1, 7, 128, 100000):
        layouts = {}
        for name, (_, c, dump) in servers.items():
            ok(c('FLUSHALL'), 'flushall')
            ok(c('CONFIG', 'SET', 'list-max-listpack-size', fill), (name, fill))
            for i in range(0, len(items), 1000):
                c('RPUSH', 'nodes', *items[i:i + 1000])
            ok(c('SAVE'), 'save')
            layouts[name] = list_nodes(dump)['nodes']
        node_limit_ok(layouts['peadb'], fill)
        assert layouts['peadb'] == layouts['redis'], (fill, len(layouts['peadb']), len(layouts['redis']),
                                                      layouts['peadb'][:5], layouts['redis'][:5])
    for proc, c, _ in servers.values():
        c.close()
        stop(proc)
    print('the same list nodes as redis for each list-max-listpack-size: ok')


# ── Hand-built files ──

def backlen(size):
    if size <= 127:
        return bytes([size])
    if size < 16383:
        return bytes([size >> 7, (size & 127) | 128])
    if size < 2097151:
        return bytes([size >> 14, ((size >> 7) & 127) | 128, (size & 127) | 128])
    if size < 268435455:
        return bytes([size >> 21, ((size >> 14) & 127) | 128, ((size >> 7) & 127) | 128, (size & 127) | 128])
    return bytes([size >> 28, ((size >> 21) & 127) | 128, ((size >> 14) & 127) | 128,
                  ((size >> 7) & 127) | 128, (size & 127) | 128])


def listpack(items, count=None):
    body = []
    for it in items:
        n = len(it)
        hdr = bytes([0x80 | n]) if n < 64 else bytes([0xe0 | (n >> 8), n & 0xff]) if n < 4096 \
            else b'\xf0' + n.to_bytes(4, 'little')
        body.append(hdr + it + backlen(len(hdr) + n))
    body = b''.join(body)
    count = min(len(items), 65535) if count is None else count
    return (4 + 2 + len(body) + 1).to_bytes(4, 'little') + count.to_bytes(2, 'little') + body + b'\xff'


def enc_len(n):
    if n < 64:
        return bytes([n])
    if n < 16384:
        return bytes([0x40 | (n >> 8), n & 0xff])
    return b'\x80' + n.to_bytes(4, 'big')


def enc_str(b):
    return enc_len(len(b)) + b


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


def rdb_file(type_byte, key, value):
    body = b'REDIS0011\xfe\x00' + bytes([type_byte]) + enc_str(key) + value + b'\xff'
    return body + crc64(body).to_bytes(8, 'little')


def quicklist(nodes):
    """nodes: [(container, data)]."""
    return enc_len(len(nodes)) + b''.join(enc_len(c) + enc_str(d) for c, d in nodes)


def check_hand_built_files(name, binary, work):
    port = PORTS[name]
    workdir = os.path.join(work, f'{name}-built')
    os.makedirs(workdir)
    dump = os.path.join(workdir, 'dump.rdb')

    def write(data):
        with open(dump, 'wb') as f:
            f.write(data)

    def loads(data, want):
        write(data)
        proc = start(binary, port, workdir)
        c = Conn(port)
        got = c('LRANGE', 'k', 0, -1)
        c.close()
        stop(proc)
        assert got == want, (name, len(got), len(want))

    def run(data, *commands):
        """Loads the file and returns the reply to each command."""
        write(data)
        proc = start(binary, port, workdir)
        c = Conn(port)
        got = [c(*cmd) for cmd in commands]
        c.close()
        stop(proc)
        return got

    # Listpacks whose entry count is 65535 ("unknown": Redis stores that from 65,535 entries on, for
    # example in a hash with hash-max-listpack-entries raised): every entry up to the 0xFF counts.
    unknown = [f'u{i}'.encode() for i in range(70000)]
    assert run(rdb_file(20, b's', enc_str(listpack(unknown))), ('SCARD', 's'), ('SISMEMBER', 's', 'u69999')) \
        == [70000, 1], name
    assert run(rdb_file(16, b'h', enc_str(listpack(unknown))), ('HLEN', 'h'), ('HGET', 'h', 'u69998')) \
        == [35000, b'u69999'], name
    pairs = []
    for i in range(35000):
        pairs += [f'm{i}'.encode(), f'{i}.5'.encode()]
    assert run(rdb_file(17, b'z', enc_str(listpack(pairs))), ('ZCARD', 'z'), ('ZSCORE', 'z', 'm34999')) \
        == [35000, b'34999.5'], name
    if name == 'peadb':
        # The same in a list node. Redis never writes a node that big, and keeps only N mod 65,536 of
        # its items (its in-memory node count is 16 bits); PeaDB keeps them all.
        loads(rdb_file(18, b'k', quicklist([(2, listpack(unknown))])), unknown)
        # A count that does not match the entries, as PeaDB wrote before item 29 was fixed (the
        # count cut to 16 bits): the entries count, so such a dump.rdb loads with all its items.
        # Redis checks the count only with sanitize-dump-payload yes, and otherwise trusts it.
        loads(rdb_file(18, b'k', quicklist([(2, listpack(unknown, count=70000 % 65536))])), unknown)
        loads(rdb_file(18, b'k', quicklist([(2, listpack(unknown[:65536], count=0))])), unknown[:65536])
    # A PLAIN node is one element as it is, even when its bytes would read as a listpack.
    plain = listpack([b'x', b'y'])
    loads(rdb_file(18, b'k', quicklist([(2, listpack([b'a', b'b'])), (1, plain), (2, listpack([b'c']))])),
          [b'a', b'b', plain, b'c'])
    good = listpack([b'x', b'y'])
    bad_total = (len(good) + 1).to_bytes(4, 'little') + good[4:]
    no_end = good[:-1] + b'\xfe'
    for what, data, text in (
            ('container 3', rdb_file(18, b'k', quicklist([(3, good)])), 'Quicklist integrity check failed.'),
            ('wrong total bytes', rdb_file(18, b'k', quicklist([(2, bad_total)])), 'Listpack integrity check failed.'),
            ('no 0xFF at the end', rdb_file(18, b'k', quicklist([(2, no_end)])), 'Listpack integrity check failed.'),
            ('set: wrong total bytes', rdb_file(20, b'k', enc_str(bad_total)), 'Set listpack integrity check failed.'),
            ('hash: wrong total bytes', rdb_file(16, b'k', enc_str(bad_total)), 'Hash listpack integrity check failed.'),
            ('zset: no 0xFF at the end', rdb_file(17, b'k', enc_str(no_end)), 'Zset listpack integrity check failed.')):
        write(data)
        log = expect_start_fails(f'{name}: {what}', binary, port, workdir)
        assert text in log, (name, what, log[-1500:])
    if name == 'peadb':
        # A hash or zset listpack with an odd number of entries (Redis checks that only with
        # sanitize-dump-payload yes).
        for type_byte, text in ((16, 'Hash listpack integrity check failed.'),
                                (17, 'Zset listpack integrity check failed.')):
            write(rdb_file(type_byte, b'k', enc_str(listpack([b'a', b'1', b'b']))))
            log = expect_start_fails(f'peadb: odd entries, type {type_byte}', binary, port, workdir)
            assert text in log, (type_byte, log[-1500:])
        # Encodings of Redis before 7.0: refused until PeaDB can read them (item 53).
        for type_byte in (9, 10, 12, 13, 14):
            value = enc_len(1) + enc_str(b'\x00' * 12) if type_byte == 14 else enc_str(b'\x00' * 12)
            write(rdb_file(type_byte, b'k', value))
            log = expect_start_fails(f'peadb: type {type_byte}', binary, port, workdir)
            assert f'unsupported RDB type {type_byte} (an encoding written by Redis before 7.0' in log, \
                (type_byte, log[-1500:])
    print(f'{name}: hand-built list files ok')


def main():
    work = tempfile.mkdtemp(prefix='peadb-listnodes-')
    try:
        check_peadb_round_trips(work)
        check_hand_built_files('peadb', PEADB, work)
        if REDIS:
            check_hand_built_files('redis', REDIS, work)
            check_files_across(work)
            check_full_sync_across(work)
            check_same_nodes_as_redis(work)
        else:
            print('no Redis 7.2.x redis-server: skipping the Redis side')
    finally:
        for proc in STARTED:
            stop(proc)
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()

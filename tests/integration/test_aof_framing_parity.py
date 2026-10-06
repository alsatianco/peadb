#!/usr/bin/env python3
"""The AOF and the replicas get each transaction and script whole, as in Redis 7.2.5 (compat/delta.md items 36, 3 and 43).

The same checks run on peadb-server and on the pinned redis-server (Redis side first, only when it is Redis 7.2.x):

- framing: a MULTI/EXEC, a script and a function that write more than once reach the AOF and a replica (a fake one that sends PSYNC) wrapped in MULTI/EXEC, with each SELECT inside; a single write, and a transaction with one write, are not wrapped; a script that drops a target with redis.set_repl() still sends that target an empty MULTI/EXEC, as Redis does.
- torn units: the AOF of two money transfers (one MULTI/EXEC, one script) cut inside a transfer loads without half of it.
- eviction: a key evicted in database 3 while the AOF is in database 0 is deleted in database 3 on replay, and the key of the same name in database 0 stays.
- rewrite scheduling: BGREWRITEAOF and CONFIG SET appendonly yes inside MULTI/EXEC, or while a BGSAVE runs, start the rewrite after it, with Redis's replies and log lines, and nothing is applied twice.
- the AOF and the master stream are never refused: an AOF with transactions loads under maxmemory or min-replicas-to-write, and a replica with either setting applies every write its master sends.
- a woken XREADGROUP is one unit; BRPOPLPUSH and BLMOVE served at once move one element on replay (PeaDB sent BRPOPLPUSH twice); PUBLISH goes to the replicas only; SCAN's expiry DELs are not wrapped.
- PeaDB only: a key evicted in the middle of a transaction keeps its place in the streams, and a replica drops a MULTI cut off by a broken link when the next full sync comes.

Without a pinned Redis 7.2.x, only the PeaDB side runs.
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
    """The redis-server to compare with, only if it is Redis 7.2.x (log lines and replies differ in other versions)."""
    path = redis_server_path()
    if not path:
        return None
    version = subprocess.run([path, '--version'], capture_output=True, text=True).stdout
    return path if ' v=7.2.' in version else None


REDIS = redis_7_2()
PORTS = {'peadb': 6640, 'redis': 6641, 'peadb-replica': 6642, 'redis-replica': 6643, 'fake-master': 6644}

FUNCTION_CODE = ("#!lua name=framinglib\n"
                 "redis.register_function('two_writes', function(keys, args) "
                 "redis.call('set', keys[1], '1'); redis.call('set', keys[2], '2') end)")


def encode(*args):
    out = f'*{len(args)}\r\n'.encode()
    for a in args:
        b = str(a).encode()
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
            return None if n < 0 else self.f.read(n + 2)[:-2].decode()
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
            args.append(self.f.read(n + 2)[:-2].decode())
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
                             '--loglevel', 'notice', '--save', '', *extra],
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


def read_log(workdir):
    return open(os.path.join(workdir, 'server.log'), errors='replace').read()


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


def wait_rewrite_done(c, forks_before, count=1):
    wait_for(lambda: int(info(c, 'total_forks')) >= forks_before + count and
             info(c, 'aof_rewrite_in_progress') == '0' and info(c, 'aof_rewrite_scheduled') == '0' and
             info(c, 'rdb_bgsave_in_progress') == '0', 'the AOF rewrite')


def aof_file(name, workdir):
    """The file the server appends to: PeaDB's single file, or Redis's last INCR file."""
    if name == 'peadb':
        return os.path.join(workdir, 'appendonly.aof')
    d = os.path.join(workdir, 'appendonlydir')
    with open(os.path.join(d, 'appendonly.aof.manifest')) as f:
        incr = [ln.split()[1] for ln in f if ln.split() and ln.split()[-1] == 'i']
    return os.path.join(d, incr[-1])


def aof_commands(path):
    """Each command in an AOF as (args, start offset, end offset)."""
    data = open(path, 'rb').read()
    out = []
    pos = 0
    while pos < len(data):
        start = pos
        eol = data.index(b'\r\n', pos)
        argc = int(data[pos + 1:eol])
        pos = eol + 2
        args = []
        for _ in range(argc):
            eol = data.index(b'\r\n', pos)
            n = int(data[pos + 1:eol])
            args.append(data[eol + 2:eol + 2 + n].decode())
            pos = eol + 2 + n + 2
        out.append((args, start, pos))
    return out


def run_units(c):
    """Writes in several databases from MULTI/EXEC, scripts and a function."""
    for reply, cmd in [('+OK', ('MULTI',)), ('+QUEUED', ('SET', 'a', '1')), ('+QUEUED', ('SELECT', '3')),
                       ('+QUEUED', ('INCR', 'b'))]:
        assert c(*cmd) == reply, cmd
    assert c('EXEC') == ['+OK', '+OK', 1]
    assert c('SELECT', 0) == '+OK'
    c('EVAL', "redis.call('set','x','1'); redis.call('select','2'); redis.call('incr','y')", 0)
    c('EVAL', "redis.call('set','single','1')", 0)
    assert c('FUNCTION', 'LOAD', 'REPLACE', FUNCTION_CODE) == 'framinglib'
    c('FCALL', 'two_writes', 2, 'k1', 'k2')
    assert c('MULTI') == '+OK'
    c('SET', 'only', '1')
    c('GET', 'only')
    assert c('EXEC') == ['+OK', '1']
    c('EVAL', "redis.set_repl(redis.REPL_AOF); redis.call('set','ra','1'); redis.call('set','rb','2')", 0)
    c('EVAL', "redis.set_repl(redis.REPL_REPLICA); redis.call('set','rc','1'); redis.call('set','rd','2')", 0)
    assert c('SET', 'end', '1') == '+OK'


def expected_units(stream):
    common = [
        ['MULTI'], ['SELECT', '0'], ['SET', 'a', '1'], ['SELECT', '3'], ['INCR', 'b'], ['EXEC'],
        ['MULTI'], ['SELECT', '0'], ['set', 'x', '1'], ['SELECT', '2'], ['incr', 'y'], ['EXEC'],
        ['SELECT', '0'], ['set', 'single', '1'],
        ['FUNCTION', 'LOAD', 'REPLACE', FUNCTION_CODE],
        ['MULTI'], ['set', 'k1', '1'], ['set', 'k2', '2'], ['EXEC'],
        ['SET', 'only', '1'],
    ]
    if stream:
        # The REPL_AOF script reaches the replicas as an empty transaction, the REPL_REPLICA one whole.
        tail = [['MULTI'], ['EXEC'], ['MULTI'], ['set', 'rc', '1'], ['set', 'rd', '2'], ['EXEC']]
    else:
        tail = [['MULTI'], ['set', 'ra', '1'], ['set', 'rb', '2'], ['EXEC'], ['MULTI'], ['EXEC']]
    return common + tail + [['SET', 'end', '1']]


def check_framing(name, binary, work):
    port = PORTS[name]
    workdir = os.path.join(work, name, 'framing')
    proc = start(binary, port, workdir, '--appendonly', 'yes', '--appendfsync', 'always')
    try:
        replica = FakeReplica(port)
        c = Conn(port)
        run_units(c)
        stream = replica.commands_until(['SET', 'end', '1'])
        replica.close()
        assert stream == expected_units(True), (name, stream)
        cmds = [args for args, _, _ in aof_commands(aof_file(name, workdir))]
        assert cmds == expected_units(False), (name, cmds)
        c.close()
    finally:
        stop(proc)


def normalized(cmds):
    """Commands with the forms that differ between the engines made the same: an XCLAIM's delivery time (PeaDB sends 0, compat/delta.md item 48) and RPOPLPUSH, which PeaDB sends as the LMOVE it is."""
    out = []
    for cmd in cmds:
        if cmd[0] == 'XCLAIM' and 'TIME' in cmd:
            cmd = list(cmd)
            cmd[cmd.index('TIME') + 1] = '*'
        if cmd[0] == 'RPOPLPUSH':
            cmd = ['LMOVE', cmd[1], cmd[2], 'RIGHT', 'LEFT']
        out.append(cmd)
    return out


def check_woken_and_moves(name, binary, work):
    """A woken XREADGROUP is one unit; BRPOPLPUSH and BLMOVE served at once move one element; PUBLISH goes to the replicas only; SCAN sends the DELs of the expired keys it meets without MULTI/EXEC."""
    port = PORTS[name]
    workdir = os.path.join(work, name, 'woken')
    aof = ('--appendonly', 'yes', '--appendfsync', 'always')
    proc = start(binary, port, workdir, *aof, '--enable-debug-command', 'yes')
    try:
        replica = FakeReplica(port)
        c = Conn(port)
        assert c('XGROUP', 'CREATE', 's', 'g', '$', 'MKSTREAM') == '+OK'
        b = Conn(port)
        b.sock.sendall(encode('XREADGROUP', 'GROUP', 'g', 'c1', 'COUNT', 2, 'BLOCK', 0, 'STREAMS', 's', '>'))
        wait_for(lambda: info(c, 'blocked_clients') == '1', 'the blocked XREADGROUP')
        assert c('MULTI') == '+OK'
        c('XADD', 's', '1-1', 'f', 'v')
        c('XADD', 's', '1-2', 'f', 'v')
        assert c('EXEC') == ['1-1', '1-2']
        assert b.read() == [['s', [['1-1', ['f', 'v']], ['1-2', ['f', 'v']]]]]
        b.close()
        assert c('RPUSH', 'src', 'a', 'b', 'c') == 3
        assert c('BRPOPLPUSH', 'src', 'dst', 0) == 'c'
        assert c('BLMOVE', 'src', 'dst', 'LEFT', 'RIGHT', 0) == 'a'
        assert c('MULTI') == '+OK'
        c('PUBLISH', 'ch', 'm1')
        c('PUBLISH', 'ch', 'm2')
        assert c('EXEC') == [0, 0]
        assert c('DEBUG', 'SET-ACTIVE-EXPIRE', 0) == '+OK'
        for i in range(3):
            assert c('SET', f'e{i}', 1, 'PX', 1) == '+OK'
        time.sleep(0.05)
        assert sorted(c('SCAN', 0, 'COUNT', 1000)[1]) == ['dst', 's', 'src'], name
        assert c('SET', 'end', 1) == '+OK'
        c.close()
        units = [
            ['SELECT', '0'], ['XGROUP', 'CREATE', 's', 'g', '$', 'MKSTREAM'],
            ['MULTI'], ['XADD', 's', '1-1', 'f', 'v'], ['XADD', 's', '1-2', 'f', 'v'], ['EXEC'],
            ['MULTI'],
            ['XCLAIM', 's', 'g', 'c1', '0', '1-1', 'TIME', '*', 'RETRYCOUNT', '1', 'FORCE', 'JUSTID', 'LASTID', '1-1'],
            ['XCLAIM', 's', 'g', 'c1', '0', '1-2', 'TIME', '*', 'RETRYCOUNT', '1', 'FORCE', 'JUSTID', 'LASTID', '1-2'],
            ['EXEC'],
            ['RPUSH', 'src', 'a', 'b', 'c'], ['LMOVE', 'src', 'dst', 'RIGHT', 'LEFT'],
            ['LMOVE', 'src', 'dst', 'LEFT', 'RIGHT'],
        ]
        stream = normalized(replica.commands_until(['SET', 'end', '1']))
        replica.close()
        publish = [['MULTI'], ['PUBLISH', 'ch', 'm1'], ['PUBLISH', 'ch', 'm2'], ['EXEC']]
        assert stream[:len(units) + 4] == units + publish, (name, stream)
        rest = stream[len(units) + 4:]
        assert [cmd[:2] for cmd in rest[:3]] == [['SET', f'e{i}'] for i in range(3)], (name, rest)
        assert sorted(rest[3:-1]) == [['DEL', f'e{i}'] for i in range(3)], (name, rest)
        cmds = normalized([args for args, _, _ in aof_commands(aof_file(name, workdir))])
        # The AOF gets the empty transaction of the two PUBLISHes, as in Redis.
        assert cmds[:len(units) + 2] == units + [['MULTI'], ['EXEC']], (name, cmds)
    finally:
        crash(proc)
    proc = start(binary, port, workdir, *aof)
    try:
        c = Conn(port)
        assert (c('LRANGE', 'src', 0, -1), c('LRANGE', 'dst', 0, -1)) == (['b'], ['c', 'a']), name
        assert c('XPENDING', 's', 'g')[0] == 2, name
        c.close()
    finally:
        stop(proc)


def check_eviction_in_unit(work):
    """PeaDB only: a key evicted between two writes of a transaction keeps its place in the streams.

    Redis evicts only before a command runs; PeaDB also evicts in the OOM check of each command
    that EXEC runs (compat/delta.md item 39), and the DEL then goes inside the transaction.
    """
    port = PORTS['peadb']
    workdir = os.path.join(work, 'peadb', 'evict-unit')
    aof = ('--appendonly', 'yes', '--appendfsync', 'always')
    proc = start(PEADB, port, workdir, *aof, '--maxmemory-policy', 'volatile-lru',
                 '--enable-debug-command', 'yes')
    try:
        replica = FakeReplica(port)
        c = Conn(port)
        assert c('SELECT', 3) == '+OK'
        assert c('SET', 'victim', 'x' * 200000, 'EX', 1000) == '+OK'
        assert c('SELECT', 0) == '+OK'
        time.sleep(0.15)  # used_memory is recomputed at most every 100 ms
        assert c('CONFIG', 'SET', 'maxmemory', int(info(c, 'used_memory')) + 50000) == '+OK'
        assert c('MULTI') == '+OK'
        c('SET', 'big', 'y' * 100000)
        c('DEBUG', 'SLEEP', '0.15')
        c('SET', 'small', '1')
        assert c('EXEC') == ['+OK', '+OK', '+OK']
        assert c('CONFIG', 'SET', 'maxmemory', 0) == '+OK'
        assert c('SET', 'end', 1) == '+OK'
        c.close()
        unit = [['MULTI'], ['SELECT', '0'], ['SET', 'big', 'y' * 100000], ['SELECT', '3'],
                ['DEL', 'victim'], ['SELECT', '0'], ['SET', 'small', '1'], ['EXEC'], ['SET', 'end', '1']]
        assert replica.commands_until(['SET', 'end', '1'])[-len(unit):] == unit
        replica.close()
        assert [args for args, _, _ in aof_commands(aof_file('peadb', workdir))][-len(unit):] == unit
    finally:
        crash(proc)
    proc = start(PEADB, port, workdir, *aof)
    try:
        c = Conn(port)
        assert (c('STRLEN', 'big'), c('GET', 'small')) == (100000, '1')
        assert c('SELECT', 3) == '+OK' and c('DBSIZE') == 0
        c.close()
    finally:
        stop(proc)


def check_replica_drops_open_multi(work):
    """PeaDB only: a MULTI whose EXEC never came before the link broke is dropped by the next full sync."""
    rdb = b'REDIS0011' + b'\xff' + b'\x00' * 8  # an empty RDB saved without a checksum
    streams = [encode('SELECT', '0') + encode('SET', 'pre', '1') + encode('MULTI') + encode('SET', 'a', '1'),
               encode('SELECT', '0') + encode('SET', 'b', '1')]
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('127.0.0.1', PORTS['fake-master']))
    srv.listen(2)
    syncs = []

    def serve(conn, stream, close_after):
        f = conn.makefile('rb')
        while True:
            line = f.readline()
            if not line:
                return
            args = []
            for _ in range(int(line[1:-2])):
                n = int(f.readline()[1:-2])
                args.append(f.read(n + 2)[:-2].decode())
            if args[0].upper() == 'PSYNC':
                syncs.append(args)
                conn.sendall(b'+FULLRESYNC ' + b'a' * 40 + b' 0\r\n$' + str(len(rdb)).encode() + b'\r\n' + rdb +
                             stream)
                if close_after:
                    time.sleep(0.3)
                    conn.close()
                    return
            elif args[0].upper() == 'PING':
                conn.sendall(b'+PONG\r\n')
            elif args[0].upper() == 'REPLCONF' and args[1].upper() != 'ACK':
                conn.sendall(b'+OK\r\n')

    def accept():
        for i, stream in enumerate(streams):
            conn, _ = srv.accept()
            threading.Thread(target=serve, args=(conn, stream, i == 0), daemon=True).start()

    threading.Thread(target=accept, daemon=True).start()
    port = PORTS['peadb-replica']
    proc = start(PEADB, port, os.path.join(work, 'peadb', 'open-multi'))
    try:
        r = Conn(port)
        assert r('REPLICAOF', '127.0.0.1', PORTS['fake-master']) == '+OK'
        wait_for(lambda: r('GET', 'b') == '1', 'the second full sync', timeout=20)
        assert len(syncs) == 2 and r('GET', 'a') is None and r('GET', 'pre') is None
        assert r('SET', 'x', 1).startswith('-READONLY')
        r.close()
    finally:
        stop(proc)
        srv.close()


def check_torn_units(name, binary, work):
    """An AOF cut inside a transfer loads without either half of it."""
    port = PORTS[name]
    workdir = os.path.join(work, name, 'torn')
    proc = start(binary, port, workdir, '--appendonly', 'yes', '--appendfsync', 'always')
    c = Conn(port)
    assert c('SET', 'a', 100) == '+OK' and c('SET', 'b', 0) == '+OK'
    assert c('MULTI') == '+OK'
    c('DECRBY', 'a', 30)
    c('INCRBY', 'b', 30)
    assert c('EXEC') == [70, 30]
    c('EVAL', "redis.call('decrby',KEYS[1],ARGV[1]); redis.call('incrby',KEYS[2],ARGV[1])", 2, 'a', 'b', 20)
    c.close()
    crash(proc)
    path = aof_file(name, workdir)
    cmds = aof_commands(path)
    starts = [i for i, (args, _, _) in enumerate(cmds) if args == ['MULTI']]
    assert len(starts) == 2, (name, [a for a, _, _ in cmds])
    original = open(path, 'rb').read()
    # (where the file is cut, the values a restart must find): inside each transfer after its
    # debit, in the middle of its credit, and the whole file.
    cuts = []
    for i, values in zip(starts, [('100', '0'), ('70', '30')]):
        debit = i + 1
        while cmds[debit][0][0].upper() == 'SELECT':
            debit += 1
        assert cmds[debit][0][0].upper() == 'DECRBY', (name, cmds[debit])
        cuts.append((cmds[debit][2], values))
        cuts.append((cmds[debit + 1][2] - 5, values))
    cuts.append((len(original), ('50', '50')))
    for cut, (a, b) in cuts:
        with open(path, 'wb') as f:
            f.write(original[:cut])
        proc = start(binary, port, workdir, '--appendonly', 'yes')
        try:
            c = Conn(port)
            assert (c('GET', 'a'), c('GET', 'b')) == (a, b), (name, cut, c('GET', 'a'), c('GET', 'b'))
            c.close()
        finally:
            crash(proc)


def check_eviction_select(name, binary, work):
    """An evicted key's DEL replays in the key's database."""
    port = PORTS[name]
    workdir = os.path.join(work, name, 'evict')
    aof = ('--appendonly', 'yes', '--appendfsync', 'always')
    proc = start(binary, port, workdir, *aof, '--maxmemory-policy', 'volatile-lru')
    try:
        c = Conn(port)
        assert c('SELECT', 3) == '+OK'
        assert c('SET', 'k', 'x' * 1000, 'EX', 1000) == '+OK'  # the only key eviction can take
        assert c('SELECT', 0) == '+OK'
        assert c('SET', 'k', 'db0') == '+OK'  # the AOF is in database 0 now
        assert c('CONFIG', 'SET', 'maxmemory', 1) == '+OK'
        assert c('SELECT', 3) == '+OK'
        wait_for(lambda: c('DBSIZE') == 0, 'the eviction')
        assert c('CONFIG', 'SET', 'maxmemory', 0) == '+OK'
        assert c('SELECT', 0) == '+OK'
        assert c('GET', 'k') == 'db0'
        c.close()
    finally:
        crash(proc)
    cmds = [args for args, _, _ in aof_commands(aof_file(name, workdir))]
    assert cmds[-2:] == [['SELECT', '3'], ['DEL', 'k']], (name, cmds)
    proc = start(binary, port, workdir, *aof)
    try:
        c = Conn(port)
        assert c('GET', 'k') == 'db0', name
        assert c('SELECT', 3) == '+OK' and c('DBSIZE') == 0, name
        c.close()
    finally:
        stop(proc)


def slow_bgsave(c):
    """Starts a BGSAVE that runs about a second."""
    assert c('CONFIG', 'SET', 'rdb-key-save-delay', 200000) == '+OK'
    for i in range(5):
        assert c('SET', f'slow{i}', i) == '+OK'
    assert c('BGSAVE') == '+Background saving started'
    assert c('CONFIG', 'SET', 'rdb-key-save-delay', 0) == '+OK'


def check_rewrite_scheduling(name, binary, work):
    port = PORTS[name]
    workdir = os.path.join(work, name, 'rewrite')
    aof = ('--appendonly', 'yes', '--appendfsync', 'always')
    proc = start(binary, port, workdir, *aof)
    try:
        c = Conn(port)
        forks = int(info(c, 'total_forks'))
        assert c('MULTI') == '+OK'
        c('INCR', 'c')
        c('BGREWRITEAOF')
        c('INCR', 'c')
        assert c('EXEC') == [1, '+Background append only file rewriting scheduled', 2], name
        wait_rewrite_done(c, forks)
        assert info(c, 'aof_last_bgrewrite_status') == 'ok'
        forks = int(info(c, 'total_forks'))
        slow_bgsave(c)
        assert c('BGREWRITEAOF') == '+Background append only file rewriting scheduled', name
        assert info(c, 'aof_rewrite_scheduled') == '1' and info(c, 'aof_rewrite_in_progress') == '0'
        assert c('INCR', 'c') == 3
        wait_rewrite_done(c, forks, 2)  # the BGSAVE, then the rewrite
        c.close()
    finally:
        crash(proc)
    proc = start(binary, port, workdir, *aof)
    try:
        c = Conn(port)
        assert c('GET', 'c') == '3', name  # nothing applied twice
        c.close()
    finally:
        stop(proc)

    # CONFIG SET appendonly yes inside EXEC, then during a BGSAVE.
    for case in ('exec', 'bgsave'):
        workdir = os.path.join(work, name, 'appendonly-' + case)
        proc = start(binary, port, workdir)
        try:
            c = Conn(port)
            forks = int(info(c, 'total_forks'))
            if case == 'exec':
                assert c('MULTI') == '+OK'
                c('SET', 'foo', 'bar')
                c('CONFIG', 'SET', 'appendonly', 'yes')
                c('INCR', 'n')
                assert c('EXEC') == ['+OK', '+OK', 1], name
                line = 'AOF was enabled during a transaction. An AOF background was scheduled to start when possible.'
                count = 1
            else:
                slow_bgsave(c)
                assert c('SET', 'foo', 'bar') == '+OK'
                assert c('CONFIG', 'SET', 'appendonly', 'yes') == '+OK'
                assert info(c, 'aof_rewrite_scheduled') == '1'
                line = ('AOF was enabled but there is already another background operation. An AOF '
                        'background was scheduled to start when possible.')
                count = 2
            assert line in read_log(workdir), (name, case)
            wait_rewrite_done(c, forks, count)
            assert info(c, 'aof_enabled') == '1'
            assert c('INCR', 'n') == (2 if case == 'exec' else 1)
            c.close()
        finally:
            crash(proc)
        proc = start(binary, port, workdir, '--appendonly', 'yes')
        try:
            c = Conn(port)
            assert (c('GET', 'foo'), c('GET', 'n')) == ('bar', '2' if case == 'exec' else '1'), (name, case)
            c.close()
        finally:
            stop(proc)


def check_obeyed_streams(name, binary, work):
    """The AOF and the master stream are applied whatever maxmemory and min-replicas-to-write say."""
    port = PORTS[name]
    workdir = os.path.join(work, name, 'obey')
    proc = start(binary, port, workdir, '--appendonly', 'yes', '--appendfsync', 'always')
    c = Conn(port)
    assert c('SET', 'big', 'x' * 100000) == '+OK'
    assert c('MULTI') == '+OK'
    c('INCR', 't')
    c('INCR', 't')
    assert c('EXEC') == [1, 2]
    c.close()
    crash(proc)
    for extra, refusal in [(('--maxmemory', '1'), '-OOM'), (('--min-replicas-to-write', '1'), '-NOREPLICAS')]:
        proc = start(binary, port, workdir, '--appendonly', 'yes', *extra)
        try:
            c = Conn(port)
            assert c('GET', 't') == '2' and c('STRLEN', 'big') == 100000, (name, extra)
            assert c('SET', 'new', 1).startswith(refusal), (name, extra)
            c.close()
        finally:
            crash(proc)

    # A replica with either setting applies everything its master sends, transactions included.
    rport = PORTS[name + '-replica']
    master = start(binary, port, os.path.join(work, name, 'obey-master'), '--repl-diskless-sync-delay', '0')
    try:
        for extra in (('--maxmemory', '1'), ('--min-replicas-to-write', '1')):
            replica = start(binary, rport, os.path.join(work, name, 'obey-replica'), *extra)
            try:
                m = Conn(port)
                assert m('FLUSHALL') == '+OK' and m('SET', 'pre', 1) == '+OK'
                r = Conn(rport)
                assert r('REPLICAOF', '127.0.0.1', port) == '+OK'
                # `pre` comes with the full sync: everything after it must come in the stream.
                wait_for(lambda: r('GET', 'pre') == '1', 'the full sync')
                assert m('SET', 'big', 'y' * 100000) == '+OK'
                assert m('MULTI') == '+OK'
                m('INCR', 'u')
                m('INCR', 'u')
                assert m('EXEC') == [1, 2]
                m('EVAL', "redis.call('set','s1','1'); redis.call('set','s2','2')", 0)
                assert m('SET', 'last', 1) == '+OK'
                wait_for(lambda: r('GET', 'last') == '1', 'the replicated writes')
                assert (r('GET', 'u'), r('GET', 's2'), r('STRLEN', 'big')) == ('2', '2', 100000), (name, extra)
                m.close()
                r.close()
            finally:
                stop(replica)
    finally:
        stop(master)


def check_engine(name, binary, work):
    check_framing(name, binary, work)
    check_woken_and_moves(name, binary, work)
    check_torn_units(name, binary, work)
    check_eviction_select(name, binary, work)
    check_rewrite_scheduling(name, binary, work)
    check_obeyed_streams(name, binary, work)
    if name == 'peadb':
        check_eviction_in_unit(work)
        check_replica_drops_open_multi(work)
    print(f'{name}: ok')


def main():
    work = tempfile.mkdtemp(prefix='peadb-aofframing-')
    try:
        if REDIS:
            check_engine('redis', REDIS, work)
        else:
            print('no pinned Redis 7.2.x: skipping the Redis side')
        check_engine('peadb', PEADB, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()

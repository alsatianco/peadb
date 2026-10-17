#!/usr/bin/env python3
"""Expired keys are deleted as Redis 7.2.5 deletes them (compat/delta.md item 37).

The same checks run on peadb-server and on the pinned redis-server (Redis side first, only when it is Redis 7.2.x):

- lazy expiry on a master: a command that meets an expired key (EXISTS, HGET, TTL, TYPE, GET, MGET, INCR, APPEND, DEL, a script that turned replication off with redis.set_repl(), a key in another database) deletes it, sends the `expired` event and writes a DEL to the AOF and to a replica (a fake one that sends PSYNC), inside MULTI/EXEC with the command's own write; SCAN and RANDOMKEY send their DELs without MULTI/EXEC; INFO expired_keys counts each key. A SET with an expire time in the past keeps the key until a command meets it, while EXPIRE and GETEX PXAT with a time in the past delete it at once, with one DEL and a `del` event; EXPIRE and GETEX on a key that has already expired send only its DEL; MOVE into a database whose key has expired deletes that key inside the MOVE's MULTI/EXEC; a BLMOVE that blocks leaves an expired destination alone, and its wakeup sends MULTI, DEL, LMOVE, EXEC.
- walks that do not delete: KEYS skips expired keys, and SAVE, a full sync, WATCH/EXEC, MEMORY USAGE and DEBUG OBJECT keep them (DBSIZE still counts them; a WATCHed key that expires before EXEC aborts it); CLUSTER COUNTKEYSINSLOT and GETKEYSINSLOT count and list them; a BLPOP that SWAPDB would wake with a key that has already expired stays blocked and sends no DEL.
- replay: an AOF written while a key was alive (SET with PX, then INCR; RPUSH, PEXPIRE, RPUSH; HSET, PEXPIRE, HINCRBY) loads after the TTL has passed without bringing the key back as a new key; an RDB saved with an expired key (also one that a lookup hid during CLIENT PAUSE, counted by the pinned redis-check-rdb) loads without it; an AOF with a RESTORE whose expire time has passed, then an APPEND, loads without bringing the key back; an AOF rewrite writes expired keys with their expire time and deletes nothing.
- events: GET of an expired key of another type answers nil (PeaDB answered WRONGTYPE), EXPIRE of a missing key sends no event (PeaDB sent `expire`), and EXPIRE with a time in the past sends `del`.
- a client blocked in one database is not served by a push to the same key in another database (PeaDB served it from the database the last command used).
- replica: a replica keeps an expired key that its full sync brought, hides expired keys from its clients without deleting them (DBSIZE counts them, expired_keys does not), applies a write its master made before the TTL passed to the kept key even when it arrives after (a paused replica), deletes the keys when the master's DELs come, answers RANDOMKEY with an expired key when every key of the database has expired, and deletes its kept expired keys once promoted with REPLICAOF NO ONE.

Without a pinned Redis 7.2.x, only the PeaDB side runs.
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


def redis_7_2():
    """The redis-server to compare with, only if it is Redis 7.2.x (replies and streams differ in other versions)."""
    path = redis_server_path()
    if not path:
        return None
    version = subprocess.run([path, '--version'], capture_output=True, text=True).stdout
    return path if ' v=7.2.' in version else None


REDIS = redis_7_2()
PORTS = {'peadb': 6650, 'redis': 6651, 'peadb-replica': 6652, 'redis-replica': 6653}
# A Redis master starts a full sync at once, from a disk save (PeaDB warns about these and ignores them).
SYNC_NOW = ('--repl-diskless-sync', 'no', '--repl-diskless-sync-delay', '0')


def encode(*args):
    out = f'*{len(args)}\r\n'.encode()
    for a in args:
        b = str(a).encode()
        out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
    return out


def encode_bytes(*args):
    out = b'*%d\r\n' % len(args)
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode()
        out += b'$%d\r\n' % len(b) + b + b'\r\n'
    return out


def check_rdb_keys(path):
    """The `N keys read` count of the pinned redis-check-rdb, or None when there is no pinned Redis."""
    if not REDIS:
        return None
    tool = os.path.join(os.path.dirname(REDIS), 'redis-check-rdb')
    out = subprocess.run([tool, path], capture_output=True, text=True).stdout
    return next(int(line.split()[1]) for line in out.splitlines() if line.endswith(' keys read'))


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

    def dump(self, key):
        """DUMP's payload as bytes."""
        self.sock.sendall(encode('DUMP', key))
        n = int(self.f.readline()[1:-2])
        return self.f.read(n + 2)[:-2]


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
        """The commands up to `last`, without pings, GETACKs and the PUBLISHes the event marks send."""
        out = []
        while True:
            cmd = self.command()
            if cmd == ['PING'] or cmd[:2] == ['REPLCONF', 'GETACK'] or cmd[0].upper() == 'PUBLISH':
                continue
            out.append(cmd)
            if cmd == last:
                return out

    def close(self):
        self.f.close()
        self.sock.close()


class Events:
    """A client subscribed to the `expired` events of every database."""

    def __init__(self, port):
        self.c = Conn(port)
        self.c.sock.sendall(encode('PSUBSCRIBE', '__keyevent@*__:expired'))
        assert self.c.read() == ['psubscribe', '__keyevent@*__:expired', 1]
        self.marks = 0

    def until_mark(self, c):
        """(channel, key) of each event since the last call: `c` publishes a mark on the same pattern, which comes after them."""
        self.marks += 1
        mark = f'mark{self.marks}'
        assert c('PUBLISH', '__keyevent@0__:expired', mark) == 1
        out = []
        while True:
            msg = self.c.read()
            assert msg[0] == 'pmessage', msg
            if msg[3] == mark:
                return out
            out.append((msg[2], msg[3]))


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
            c = Conn(port)
            try:
                if c('PING') == '+PONG':
                    return proc
            finally:
                c.close()
        except OSError:
            pass
        time.sleep(0.05)
    proc.kill()
    proc.wait()
    raise RuntimeError(f'{binary} did not start on port {port}: {open(log_path).read()}')


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


def aof_file(name, workdir, kind='i'):
    """The file the server appends to (`i`) or the base a rewrite wrote (`b`): PeaDB's single file, or Redis's file of that kind."""
    if name == 'peadb':
        return os.path.join(workdir, 'appendonly.aof')
    d = os.path.join(workdir, 'appendonlydir')
    with open(os.path.join(d, 'appendonly.aof.manifest')) as f:
        names = [ln.split()[1] for ln in f if ln.split() and ln.split()[-1] == kind]
    return os.path.join(d, names[-1])


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
            args.append(data[eol + 2:eol + 2 + n].decode())
            pos = eol + 2 + n + 2
        out.append(args)
    return out


def after(cmds, mark):
    """The commands after `mark`."""
    return cmds[cmds.index(mark) + 1:]


def masked(cmds):
    """Commands with the absolute expire times made the same (they differ from run to run)."""
    out = []
    for cmd in cmds:
        if cmd[0].upper() == 'PEXPIREAT':
            cmd = [cmd[0], cmd[1], '*']
        elif cmd[0].upper() == 'SET' and 'PXAT' in cmd:
            cmd = cmd[:cmd.index('PXAT') + 1] + ['*']
        out.append(cmd)
    return out


def no_leading_select(cmds):
    """A new replica's stream may start with a SELECT of the database the master is in."""
    return cmds[1:] if cmds and cmds[0][0] == 'SELECT' else cmds


def without_repeated_selects(cmds, db='0'):
    """The commands without a SELECT of the database already selected."""
    out = []
    for cmd in cmds:
        if cmd[0] == 'SELECT':
            if cmd[1] == db:
                continue
            db = cmd[1]
        out.append(cmd)
    return out


def ev(db, *keys):
    return [(f'__keyevent@{db}__:expired', k) for k in keys]


# A command, its reply, and what it sends to the AOF and the replicas.
LAZY_PATHS = [
    (('EXISTS', 'e1'), 0, [['DEL', 'e1']]),
    (('HGET', 'h1', 'f'), None, [['DEL', 'h1']]),
    (('TTL', 't1'), -2, [['DEL', 't1']]),
    (('TYPE', 'y1'), '+none', [['DEL', 'y1']]),
    (('GET', 'g1'), None, [['DEL', 'g1']]),
    (('MGET', 'm1', 'm2'), [None, None], [['MULTI'], ['DEL', 'm1'], ['DEL', 'm2'], ['EXEC']]),
    (('INCR', 'i1'), 1, [['MULTI'], ['DEL', 'i1'], ['INCR', 'i1'], ['EXEC']]),
    (('APPEND', 'a1', 'x'), 1, [['MULTI'], ['DEL', 'a1'], ['APPEND', 'a1', 'x'], ['EXEC']]),
    (('DEL', 'd1'), 0, [['DEL', 'd1']]),
    # Redis's propagateDeletion() sends the DEL whatever redis.set_repl() says.
    (('EVAL', "redis.set_repl(redis.REPL_NONE); return redis.call('get', KEYS[1])", 1, 's1'), None, [['DEL', 's1']]),
]
LAZY_KEYS = ['e1', 'h1', 't1', 'y1', 'g1', 'm1', 'm2', 'i1', 'a1', 'd1', 's1']


LAZY_TTL_MS = 250


def expire_soon(c, *keys):
    """Arm expiry after setup finishes, including the two-command hash setup under ASAN."""
    for k in keys:
        if k == 'h1':
            assert c('HSET', k, 'f', 'v') == 1
            assert c('PEXPIRE', k, LAZY_TTL_MS) == 1
        else:
            assert c('SET', k, 'v', 'PX', LAZY_TTL_MS) == '+OK'


def check_lazy(name, binary, work):
    port = PORTS[name]
    workdir = os.path.join(work, name, 'lazy')
    proc = start(binary, port, workdir, '--appendonly', 'yes', '--appendfsync', 'always',
                 '--notify-keyspace-events', 'Ex', *SYNC_NOW)
    try:
        c = Conn(port)
        assert c('DEBUG', 'SET-ACTIVE-EXPIRE', '0') == '+OK'
        replica = FakeReplica(port)
        events = Events(port)
        stream = []  # what the replica gets after end1; the AOF must hold the same

        def upto(last):
            got = masked(replica.commands_until(last))
            stream.extend(got)
            return got

        # Every command that meets an expired key deletes it and propagates the DEL.
        assert c('SELECT', 2) == '+OK'
        expire_soon(c, 'x2')
        assert c('SELECT', 0) == '+OK'
        expire_soon(c, *LAZY_KEYS)
        assert c('SET', 'end1', '1') == '+OK'
        replica.commands_until(['SET', 'end1', '1'])
        time.sleep(LAZY_TTL_MS / 1000 + 0.02)
        expected = []
        for cmd, reply, sent in LAZY_PATHS:
            assert c(*cmd) == reply, (name, cmd)
            expected += sent
        assert c('SELECT', 2) == '+OK'
        assert c('EXISTS', 'x2') == 0
        assert c('SELECT', 0) == '+OK'
        assert c('SET', 'end2', '1') == '+OK'
        expected += [['SELECT', '2'], ['DEL', 'x2'], ['SELECT', '0'], ['SET', 'end2', '1']]
        got = upto(['SET', 'end2', '1'])
        assert got == expected, (name, got)
        assert events.until_mark(c) == ev(0, *LAZY_KEYS) + ev(2, 'x2'), name

        # A SET with an expire time in the past keeps the key until a command meets it; EXPIRE and
        # GETEX with a time in the past delete it at once (Redis's checkAlreadyExpired()), with one DEL
        # and a `del` event, not `expired`.
        assert c('SET', 'p1', 'v', 'PXAT', 1) == '+OK'
        assert c('EXISTS', 'p1') == 0
        assert c('SET', 'q1', 'v') == '+OK'
        assert c('EXPIRE', 'q1', -1) == 1
        assert c('EXISTS', 'q1') == 0
        assert c('SET', 'q2', 'v') == '+OK'
        assert c('GETEX', 'q2', 'PXAT', 1) == 'v'
        assert c('EXISTS', 'q2') == 0
        assert c('SET', 'q3', 'v') == '+OK'
        assert c('PEXPIREAT', 'q3', 1) == 1
        assert c('SET', 'end2b', '1') == '+OK'
        got = upto(['SET', 'end2b', '1'])
        assert got == [['SET', 'p1', 'v', 'PXAT', '*'], ['DEL', 'p1'], ['SET', 'q1', 'v'], ['DEL', 'q1'],
                       ['SET', 'q2', 'v'], ['DEL', 'q2'], ['SET', 'q3', 'v'], ['DEL', 'q3'], ['SET', 'end2b', '1']], \
            (name, got)
        assert events.until_mark(c) == ev(0, 'p1'), name

        # EXPIRE and GETEX on a key that has already expired send only its DEL; MEMORY USAGE and DEBUG OBJECT
        # read an expired key raw and delete nothing; MOVE into a database whose key has expired deletes that
        # key first, inside the MOVE's MULTI/EXEC; a BLMOVE that blocks does not touch its destination.
        expire_soon(c, 'u1', 'u2', 'u3')
        assert c('SELECT', 3) == '+OK'
        expire_soon(c, 'mv')
        assert c('SELECT', 0) == '+OK'
        assert c('SET', 'mv', 'live') == '+OK'
        assert c('RPUSH', 'dst9', 'old') == 1
        assert c('PEXPIRE', 'dst9', LAZY_TTL_MS) == 1
        assert c('SET', 'end2c', '1') == '+OK'
        upto(['SET', 'end2c', '1'])
        time.sleep(LAZY_TTL_MS / 1000 + 0.02)
        assert c('EXPIRE', 'u1', 100) == 0
        assert c('GETEX', 'u2', 'PX', 100) is None
        assert isinstance(c('MEMORY', 'USAGE', 'u3'), int), name
        assert c('DEBUG', 'OBJECT', 'u3').startswith('+Value at:'), name
        assert c('MOVE', 'mv', 3) == 1
        b9 = Conn(port)
        b9.sock.sendall(encode('BLMOVE', 'src9', 'dst9', 'LEFT', 'LEFT', 0))
        wait_for(lambda: info(c, 'blocked_clients') == '1', 'the BLMOVE to block')
        assert c('SET', 'end2d', '1') == '+OK'
        got = upto(['SET', 'end2d', '1'])
        assert got == [['DEL', 'u1'], ['DEL', 'u2'], ['MULTI'], ['SELECT', '3'], ['DEL', 'mv'], ['SELECT', '0'],
                       ['MOVE', 'mv', '3'], ['EXEC'], ['SET', 'end2d', '1']], (name, got)
        assert c('RPUSH', 'src9', 'x') == 1
        assert b9.read() == 'x'
        b9.close()
        assert c('SET', 'end2e', '1') == '+OK'
        got = upto(['SET', 'end2e', '1'])
        assert got == [['RPUSH', 'src9', 'x'], ['MULTI'], ['DEL', 'dst9'], ['LMOVE', 'src9', 'dst9', 'LEFT', 'LEFT'],
                       ['EXEC'], ['SET', 'end2e', '1']], (name, got)
        assert c('LRANGE', 'dst9', 0, -1) == ['x']
        # dst9 is deleted, mv is in database 3 now, and u3 has expired (its DEL comes first, from the lookup).
        assert c('DEL', 'dst9', 'mv', 'u3') == 1
        assert c('SET', 'end2f', '1') == '+OK'
        got = upto(['SET', 'end2f', '1'])
        assert got == [['MULTI'], ['DEL', 'u3'], ['DEL', 'dst9', 'mv', 'u3'], ['EXEC'], ['SET', 'end2f', '1']], \
            (name, got)
        assert events.until_mark(c) == ev(0, 'u1', 'u2') + ev(3, 'mv') + ev(0, 'dst9', 'u3'), name

        # KEYS, SAVE, a full sync and WATCH/EXEC leave expired keys in place.
        expire_soon(c, 'k1', 'k2', 'k3', 'w1')
        assert c('SET', 'end3', '1') == '+OK'
        time.sleep(LAZY_TTL_MS / 1000 + 0.02)
        live = ['a1', 'end1', 'end2', 'end2b', 'end2c', 'end2d', 'end2e', 'end2f', 'end3', 'i1']
        assert sorted(c('KEYS', '*')) == live, name
        assert c('DBSIZE') == len(live) + 4, name
        assert c('SAVE') == '+OK'
        replica2 = FakeReplica(port)
        assert c('WATCH', 'w1') == '+OK'
        assert c('MULTI') == '+OK'
        assert c('SET', 'wx', '1') == '+QUEUED'
        assert c('EXEC') == ['+OK'], name
        assert c('SET', 'wl', 'v', 'PX', 100) == '+OK'
        assert c('WATCH', 'wl') == '+OK'
        time.sleep(0.15)
        assert c('MULTI') == '+OK'
        assert c('SET', 'wy', '1') == '+QUEUED'
        assert c('EXEC') is None, name
        assert c('DBSIZE') == len(live) + 6, name
        assert c('SET', 'end4', '1') == '+OK'
        got = upto(['SET', 'end4', '1'])
        # The new replica makes the master send a SELECT again (Redis's replicationSetupSlaveForFullResync()).
        assert got == [['SET', k, 'v', 'PXAT', '*'] for k in ('k1', 'k2', 'k3', 'w1')] + \
            [['SET', 'end3', '1'], ['SELECT', '0'], ['SET', 'wx', '1'], ['SET', 'wl', 'v', 'PXAT', '*'],
             ['SET', 'end4', '1']], (name, got)
        assert events.until_mark(c) == [], name

        # SCAN deletes the expired keys it meets, each DEL on its own (no MULTI/EXEC).
        cursor, seen = '0', []
        while True:
            cursor, keys = c('SCAN', cursor, 'COUNT', 100)
            seen += keys
            if cursor == '0':
                break
        assert sorted(set(seen)) == sorted(live + ['end4', 'wx']), (name, seen)
        assert c('SET', 'end5', '1') == '+OK'
        dels = [['DEL', k] for k in ('k1', 'k2', 'k3', 'w1', 'wl')]
        got = upto(['SET', 'end5', '1'])
        assert sorted(got[:-1]) == dels and got[-1] == ['SET', 'end5', '1'], (name, got)
        # The replica that synced after SAVE got the same DELs: the master kept the keys.
        got2 = masked(no_leading_select(replica2.commands_until(['SET', 'end5', '1'])))
        assert got2[:3] == [['SET', 'wx', '1'], ['SET', 'wl', 'v', 'PXAT', '*'], ['SET', 'end4', '1']] and \
            got2[3:] == got, (name, got2)
        replica2.close()
        assert sorted(events.until_mark(c)) == ev(0, 'k1', 'k2', 'k3', 'w1', 'wl'), name

        # RANDOMKEY deletes the expired keys it meets, without MULTI/EXEC.
        assert c('SELECT', 5) == '+OK'
        expire_soon(c, 'r1', 'r2')
        assert c('SELECT', 0) == '+OK'
        assert c('SET', 'end6', '1') == '+OK'
        upto(['SET', 'end6', '1'])
        time.sleep(LAZY_TTL_MS / 1000 + 0.02)
        assert c('SELECT', 5) == '+OK'
        assert c('RANDOMKEY') is None, name
        assert c('SELECT', 0) == '+OK'
        assert c('SET', 'end7', '1') == '+OK'
        got = upto(['SET', 'end7', '1'])
        assert got[0] == ['SELECT', '5'] and sorted(got[1:3]) == [['DEL', 'r1'], ['DEL', 'r2']] and \
            got[3:] == [['SELECT', '0'], ['SET', 'end7', '1']], (name, got)
        assert sorted(events.until_mark(c)) == ev(5, 'r1', 'r2'), name

        # A BLPOP that SWAPDB would wake with a key that has already expired stays blocked, and no DEL
        # goes out until a command meets the key (Redis checks a woken key without deleting it).
        b = Conn(port)
        bid = b('CLIENT', 'ID')
        assert b('SELECT', 9) == '+OK'
        b.sock.sendall(encode('BLPOP', 'bl', 0))
        wait_for(lambda: info(c, 'blocked_clients') == '1', 'the BLPOP to block')
        assert c('SELECT', 1) == '+OK'
        assert c('RPUSH', 'bl', 'a') == 1
        assert c('PEXPIRE', 'bl', 50) == 1
        time.sleep(0.1)
        assert c('SWAPDB', 1, 9) == '+OK'
        time.sleep(0.05)
        assert info(c, 'blocked_clients') == '1', name
        assert c('SELECT', 0) == '+OK'
        assert c('SET', 'end8', '1') == '+OK'
        got = upto(['SET', 'end8', '1'])
        assert got == [['SELECT', '1'], ['RPUSH', 'bl', 'a'], ['PEXPIREAT', 'bl', '*'], ['SWAPDB', '1', '9'],
                       ['SELECT', '0'], ['SET', 'end8', '1']], (name, got)
        assert c('CLIENT', 'UNBLOCK', bid) == 1
        assert b.read() is None
        b.close()
        assert c('SELECT', 9) == '+OK'
        assert c('EXISTS', 'bl') == 0
        assert c('SELECT', 0) == '+OK'
        assert c('SET', 'end9', '1') == '+OK'
        got = upto(['SET', 'end9', '1'])
        assert got == [['SELECT', '9'], ['DEL', 'bl'], ['SELECT', '0'], ['SET', 'end9', '1']], (name, got)
        assert events.until_mark(c) == ev(9, 'bl'), name

        # EXPIRE and GETEX in the past are deletions, not expiries (Redis counts only the latter).
        assert info(c, 'expired_keys') == str(len(LAZY_KEYS) + 1 + 1 + 5 + 5 + 2 + 1), name
        # The AOF holds what the replica got.
        aof = masked(after(aof_commands(aof_file(name, workdir)), ['SET', 'end1', '1']))
        assert aof == without_repeated_selects(stream), (name, aof, stream)
        replica.close()
        c.close()
    finally:
        stop(proc)


def check_cluster_slots(name, binary, work):
    port = PORTS[name]
    workdir = os.path.join(work, name, 'cluster')
    proc = start(binary, port, workdir, '--cluster-enabled', 'yes')
    try:
        c = Conn(port)
        assert c('CLUSTER', 'ADDSLOTSRANGE', 0, 16383) == '+OK'
        wait_for(lambda: 'cluster_state:ok' in c('CLUSTER', 'INFO'), 'the cluster state')
        assert c('DEBUG', 'SET-ACTIVE-EXPIRE', '0') == '+OK'
        assert c('SET', '{t}gone', 'v', 'PX', 1) == '+OK'
        assert c('SET', '{t}keep', '1') == '+OK'
        time.sleep(0.02)
        slot = c('CLUSTER', 'KEYSLOT', '{t}gone')
        # Redis counts and lists the keys of its slot index, expired ones too, and deletes nothing.
        assert c('CLUSTER', 'COUNTKEYSINSLOT', slot) == 2, name
        assert sorted(c('CLUSTER', 'GETKEYSINSLOT', slot, 10)) == ['{t}gone', '{t}keep'], name
        assert c('DBSIZE') == 2, name
        assert c('GET', '{t}gone') is None
        assert c('CLUSTER', 'COUNTKEYSINSLOT', slot) == 1, name
        c.close()
    finally:
        stop(proc)


def check_replay(name, binary, work):
    port = PORTS[name]
    # An AOF written while the keys were alive, loaded after their TTL passed: no key comes back.
    workdir = os.path.join(work, name, 'replay')
    args = ('--appendonly', 'yes', '--appendfsync', 'always')
    proc = start(binary, port, workdir, *args)
    try:
        c = Conn(port)
        # No active expiry before the crash, so the AOF holds only what was written while the keys lived.
        assert c('DEBUG', 'SET-ACTIVE-EXPIRE', '0') == '+OK'
        assert c('SET', 'k', '5', 'PX', 400) == '+OK'
        assert c('INCR', 'k') == 6
        assert c('RPUSH', 'l', 'a') == 1
        assert c('PEXPIRE', 'l', 400) == 1
        assert c('RPUSH', 'l', 'b') == 2
        assert c('HSET', 'h', 'f', '1') == 1
        assert c('PEXPIRE', 'h', 400) == 1
        assert c('HINCRBY', 'h', 'f', 1) == 2
        assert c('SET', 'keep', '1') == '+OK'
        c.close()
        time.sleep(0.45)
        crash(proc)
        proc = start(binary, port, workdir, *args)
        c = Conn(port)
        assert c('GET', 'k') is None, name
        assert c('LRANGE', 'l', 0, -1) == [], name
        assert c('HGET', 'h', 'f') is None, name
        assert c('GET', 'keep') == '1'
        wait_for(lambda: c('DBSIZE') == 1, 'the expired keys to go')
        # The DELs reach the new AOF.
        cmds = aof_commands(aof_file(name, workdir))
        assert ['DEL', 'k'] in cmds, (name, cmds)
        c.close()
    finally:
        stop(proc)

    # An RDB saved with an expired key: SAVE keeps it, loading on a master leaves it out.
    workdir = os.path.join(work, name, 'rdb')
    proc = start(binary, port, workdir)
    try:
        c = Conn(port)
        assert c('DEBUG', 'SET-ACTIVE-EXPIRE', '0') == '+OK'
        assert c('SET', 'gone', 'v', 'PX', 1) == '+OK'
        assert c('SET', 'keep', '1') == '+OK'
        time.sleep(0.02)
        assert c('SAVE') == '+OK'
        assert c('DBSIZE') == 2, name
        assert check_rdb_keys(os.path.join(workdir, 'dump.rdb')) in (2, None), name
        # A key that a lookup hid during CLIENT PAUSE is written too.
        assert c('SET', 'gone2', 'v', 'PX', 1) == '+OK'
        time.sleep(0.02)
        assert c('CLIENT', 'PAUSE', 3000, 'WRITE') == '+OK'
        assert c('GET', 'gone2') is None
        assert c('SAVE') == '+OK'
        assert c('CLIENT', 'UNPAUSE') == '+OK'
        assert check_rdb_keys(os.path.join(workdir, 'dump.rdb')) in (3, None), name
        payload = c.dump('keep')
        c.close()
        stop(proc)
        proc = start(binary, port, workdir)
        c = Conn(port)
        assert c('DBSIZE') == 1, name
        assert c('GET', 'keep') == '1'
        c.close()
    finally:
        stop(proc)

    # An AOF with a RESTORE whose expire time has passed, then a write to the key: as with SET, the key is
    # restored with its past expire time, so the write applies to it and the key then expires (PeaDB dropped
    # the RESTORE, and the APPEND made a new key without a TTL).
    workdir = os.path.join(work, name, 'restore')
    os.makedirs(workdir)
    with open(os.path.join(workdir, 'appendonly.aof'), 'wb') as f:
        f.write(encode_bytes('SELECT', 0) +
                encode_bytes('RESTORE', 'r', int(time.time() * 1000) - 10000, payload, 'ABSTTL') +
                encode_bytes('APPEND', 'r', 'x'))
    proc = start(binary, port, workdir, '--appendonly', 'yes')
    try:
        c = Conn(port)
        assert c('GET', 'r') is None, name
        wait_for(lambda: c('DBSIZE') == 0, 'the restored key to expire')
        c.close()
    finally:
        stop(proc)

    # An AOF rewrite writes an expired key with its expire time (Redis 7.2's rewriteAppendOnlyFileRio()), and
    # deletes nothing.
    workdir = os.path.join(work, name, 'rewrite')
    proc = start(binary, port, workdir, '--appendonly', 'yes', '--aof-use-rdb-preamble', 'no')
    try:
        c = Conn(port)
        assert c('DEBUG', 'SET-ACTIVE-EXPIRE', '0') == '+OK'
        assert c('SET', 'gone', 'v', 'PX', 1) == '+OK'
        assert c('SET', 'keep', '1') == '+OK'
        time.sleep(0.02)
        forks = int(info(c, 'total_forks'))
        assert c('BGREWRITEAOF').startswith('+Background append only file rewriting')
        wait_for(lambda: int(info(c, 'total_forks')) > forks and info(c, 'aof_rewrite_in_progress') == '0',
                 'the AOF rewrite')
        cmds = masked(aof_commands(aof_file(name, workdir, 'b')))
        assert ['SET', 'gone', 'v'] in cmds and ['PEXPIREAT', 'gone', '*'] in cmds and ['SET', 'keep', '1'] in cmds, \
            (name, cmds)
        assert c('DBSIZE') == 2, name
        c.close()
    finally:
        stop(proc)


def check_replica(name, binary, work):
    mport, rport = PORTS[name], PORTS[name + '-replica']
    master = start(binary, mport, os.path.join(work, name, 'master'), *SYNC_NOW)
    replica = start(binary, rport, os.path.join(work, name, 'replica'))
    try:
        m, r = Conn(mport), Conn(rport)
        assert m('DEBUG', 'SET-ACTIVE-EXPIRE', '0') == '+OK'
        assert m('SET', 'old', 'v', 'PX', 1) == '+OK'
        time.sleep(0.02)
        assert r('REPLICAOF', '127.0.0.1', mport) == '+OK'
        wait_for(lambda: info(r, 'master_link_status') == 'up', 'the replica to sync')
        # The full sync kept and sent the expired key; the replica keeps it hidden.
        assert m('DBSIZE') == 1, name
        assert r('GET', 'old') is None, name
        assert r('DBSIZE') == 1, name

        assert m('SET', 'k', '5', 'PX', 500) == '+OK'
        assert m('SET', 'sync1', '1') == '+OK'
        wait_for(lambda: r('GET', 'sync1') == '1', 'sync1 on the replica')
        assert r('CLIENT', 'PAUSE', 5000, 'WRITE') == '+OK'
        assert m('INCR', 'k') == 6
        time.sleep(0.55)
        assert r('GET', 'k') is None, name
        assert r('EXISTS', 'k') == 0, name
        assert r('DBSIZE') == 3, name
        assert r('CLIENT', 'UNPAUSE') == '+OK'
        assert m('SET', 'sync2', '1') == '+OK'
        wait_for(lambda: r('GET', 'sync2') == '1', 'sync2 on the replica')
        # The INCR the master made while k was alive applied to the kept key, so k is still expired.
        assert r('GET', 'k') is None, name
        assert r('DBSIZE') == 4, name
        assert m('GET', 'k') is None and m('GET', 'old') is None
        assert m('SET', 'sync3', '1') == '+OK'
        wait_for(lambda: r('GET', 'sync3') == '1', 'sync3 on the replica')
        assert r('DBSIZE') == 3, name
        assert info(r, 'expired_keys') == '0', name
        assert info(m, 'expired_keys') == '2', name
        # A replica's RANDOMKEY returns an expired key when every key of the database has expired.
        assert m('SELECT', 5) == '+OK'
        assert m('SET', 'r5', 'v', 'PX', 300) == '+OK'
        assert m('SELECT', 0) == '+OK'
        assert m('SET', 'sync4', '1') == '+OK'
        wait_for(lambda: r('GET', 'sync4') == '1', 'sync4 on the replica')
        time.sleep(0.35)
        assert r('SELECT', 5) == '+OK'
        assert r('RANDOMKEY') == 'r5', name
        assert r('DBSIZE') == 1, name
        # Once promoted it deletes the expired keys it kept, as a master does.
        assert r('REPLICAOF', 'NO', 'ONE') == '+OK'
        assert r('GET', 'r5') is None
        assert r('DBSIZE') == 0, name
        assert info(r, 'expired_keys') == '1', name
        m.close()
        r.close()
    finally:
        stop(replica)
        stop(master)


def check_wakeup_db(name, binary, work):
    """A client blocked in one database is not served by a push to the same key in another one."""
    port = PORTS[name]
    proc = start(binary, port, os.path.join(work, name, 'wakeup-db'))
    try:
        c, b = Conn(port), Conn(port)
        assert b('SELECT', 9) == '+OK'
        b.sock.sendall(encode('BLPOP', 'bl', 0))
        wait_for(lambda: info(c, 'blocked_clients') == '1', 'the BLPOP to block')
        assert c('SELECT', 1) == '+OK'
        assert c('RPUSH', 'bl', 'a') == 1
        time.sleep(0.05)
        assert info(c, 'blocked_clients') == '1' and c('LLEN', 'bl') == 1, name
        assert c('SELECT', 9) == '+OK'
        assert c('RPUSH', 'bl', 'b') == 1
        assert b.read() == ['bl', 'b'], name
        assert c('SELECT', 1) == '+OK'
        assert c('LRANGE', 'bl', 0, -1) == ['a'], name
        b.close()
        c.close()
    finally:
        stop(proc)


def check_events(name, binary, work):
    """GET of an expired key of another type answers nil (PeaDB answered WRONGTYPE); EXPIRE of a missing key sends
    no event (PeaDB sent `expire`), and EXPIRE with a time in the past sends `del`."""
    port = PORTS[name]
    proc = start(binary, port, os.path.join(work, name, 'events'), '--notify-keyspace-events', 'KEA')
    try:
        c, sub = Conn(port), Conn(port)
        assert c('DEBUG', 'SET-ACTIVE-EXPIRE', '0') == '+OK'
        sub.sock.sendall(encode('PSUBSCRIBE', '__keyevent@*__:*'))
        assert sub.read() == ['psubscribe', '__keyevent@*__:*', 1]
        assert c('HSET', 'h', 'f', 'v') == 1
        assert c('PEXPIRE', 'h', LAZY_TTL_MS) == 1
        time.sleep(LAZY_TTL_MS / 1000 + 0.02)
        assert c('GET', 'h') is None, name
        assert c('EXPIRE', 'missing', 10) == 0
        assert c('SET', 'k', 'v') == '+OK'
        assert c('EXPIRE', 'k', -1) == 1
        assert c('PUBLISH', '__keyevent@0__:mark', 'end') == 1
        got = []
        while True:
            msg = sub.read()
            if msg[2] == '__keyevent@0__:mark':
                break
            got.append((msg[2].split(':', 1)[1], msg[3]))
        assert got == [('hset', 'h'), ('expire', 'h'), ('expired', 'h'), ('set', 'k'), ('del', 'k')], (name, got)
        sub.close()
        c.close()
    finally:
        stop(proc)


def check_engine(name, binary, work):
    check_lazy(name, binary, work)
    check_events(name, binary, work)
    check_wakeup_db(name, binary, work)
    check_cluster_slots(name, binary, work)
    check_replay(name, binary, work)
    check_replica(name, binary, work)


def main():
    work = tempfile.mkdtemp(prefix='peadb-lazy-expire-')
    try:
        if REDIS:
            check_engine('redis', REDIS, work)
        check_engine('peadb', PEADB, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print('ok')


if __name__ == '__main__':
    main()

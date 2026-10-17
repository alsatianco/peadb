#!/usr/bin/env python3
"""A SET that NX or XX stops, and a SETNX or MSETNX answered 0, change nothing anywhere, as in Redis 7.2.5 (compat/delta.md item 41).

The same checks run on peadb-server and on the pinned redis-server (Redis side first, only when it is Redis 7.2.x):

- propagation: the AOF and a replica (a fake one that sends PSYNC) get nothing for a SET stopped by NX or XX (with or without a TTL, with or without GET), a SETNX or MSETNX answered 0, a GETEX, GETDEL, SETRANGE, PERSIST or SORT that changes nothing, or such a write inside MULTI/EXEC or a script (only the DEL of a key whose TTL passed goes out); a SET that did run goes out as Redis sends it (EX, PX and EXAT as `SET key value PXAT <ms>`, PXAT as it came, GET left out).
- the lock race of the review: a real replica and an AOF reload keep the value of the client that holds the lock, with its TTL.
- a stopped SET sends no keyspace event and does not count as a change (`rdb_changes_since_last_save`).
- SET's options are read like Redis's parseExtendedStringArgumentsOrReply(): conflicting options are a syntax error before any value is checked, and EX, PX, EXAT and PXAT must be positive (GETEX too, which looks up its key before it checks the value).
- MGET, MSETNX and SORT's BY and GET patterns leave a key of another type as it is (PeaDB turned it into an empty string).

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
    """The redis-server to compare with, only if it is Redis 7.2.x (replies differ in other versions)."""
    path = redis_server_path()
    if not path:
        return None
    version = subprocess.run([path, '--version'], capture_output=True, text=True).stdout
    return path if ' v=7.2.' in version else None


REDIS = redis_7_2()
PORTS = {'peadb': 6690, 'redis': 6691, 'peadb-replica': 6692, 'redis-replica': 6693}
FAR_S = int(time.time()) + 86400  # a time in the future for EXAT and PXAT
FAR_MS = FAR_S * 1000


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
                             '--loglevel', 'notice', '--save', '', '--shutdown-timeout', '0', *extra],
                            stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'{binary} exited: {open(log_path).read()}')
        try:
            c = Conn(port)
            try:
                # A connected socket can still receive LOADING while Redis replays its AOF.
                if c('PING') == '+PONG':
                    return proc
            finally:
                c.close()
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
            args.append(data[eol + 2:eol + 2 + n].decode())
            pos = eol + 2 + n + 2
        out.append(args)
    return out


# Each SET, SETNX or MSETNX here changes nothing: (command, reply).
STOPPED = [
    (('SET', 'lock', 'v2', 'NX', 'PX', 30000), None),
    (('SET', 'lock', 'v3', 'NX', 'EX', 100), None),
    (('SET', 'lock', 'v4', 'NX'), None),
    (('SET', 'lock', 'v5', 'NX', 'GET'), 'v1'),
    (('SET', 'lock', 'v6', 'NX', 'GET', 'EXAT', FAR_S), 'v1'),
    (('SET', 'lock', 'v7', 'nx', 'pxat', FAR_MS), None),
    (('SET', 'absent', 'v', 'XX'), None),
    (('SET', 'absent', 'v', 'XX', 'EX', 100), None),
    (('SET', 'absent', 'v', 'XX', 'GET'), None),
    (('SET', 'absent', 'v', 'XX', 'KEEPTTL'), None),
    (('SETNX', 'lock', 'v8'), 0),
    (('MSETNX', 'fresh', 'a', 'lock', 'v9'), 0),
    # Other writes that change nothing are not propagated or counted either.
    (('GETEX', 'mark1'), '1'),
    (('GETEX', 'mark1', 'PERSIST'), '1'),
    (('GETEX', 'absent', 'EX', 100), None),
    (('GETDEL', 'absent'), None),
    (('SETRANGE', 'absent', 0, ''), 0),
    (('SETRANGE', 'mark1', 3, ''), 1),
    (('PERSIST', 'mark1'), 0),
    (('PERSIST', 'absent'), 0),
    (('SORT', 'slist'), ['1', '2']),
    (('SORT', 'slist', 'BY', 'w_*', 'GET', 'w_*'), [None, None]),
    (('SORT', 'nolist', 'STORE', 'absent2'), 0),
]


def pxat(c, key):
    return ['SET', key, c('GET', key), 'PXAT', str(c('PEXPIRETIME', key))]


def run_sets(c, events):
    """Runs the SETs and returns what the master must propagate for them."""
    expected = [['SELECT', '0']]
    assert c('DEBUG', 'SET-ACTIVE-EXPIRE', 0) == '+OK'
    assert c('SET', 'lock', 'v1', 'PX', 100000) == '+OK'
    expected.append(pxat(c, 'lock'))
    assert c('RPUSH', 'slist', 2, 1) == 2
    expected.append(['RPUSH', 'slist', '2', '1'])

    # Nothing between the two marks changes data, sends an event or counts as a change.
    assert c('SET', 'mark1', 1) == '+OK'
    expected.append(['SET', 'mark1', '1'])
    while events.read() != ['pmessage', '__keyevent@0__:*', '__keyevent@0__:set', 'mark1']:
        pass  # the events of the first SET
    dirty = int(info(c, 'rdb_changes_since_last_save'))
    for cmd, reply in STOPPED:
        assert c(*cmd) == reply, cmd
    assert int(info(c, 'rdb_changes_since_last_save')) == dirty
    assert c('SET', 'mark2', 1) == '+OK'
    expected.append(['SET', 'mark2', '1'])
    assert events.read() == ['pmessage', '__keyevent@0__:*', '__keyevent@0__:set', 'mark2']

    # A transaction or a script whose only write is stopped sends nothing.
    assert c('MULTI') == '+OK'
    assert c('SET', 'lock', 'x', 'NX') == '+QUEUED'
    assert c('EXEC') == [None]
    assert c('EVAL', "return redis.call('set', KEYS[1], 'x', 'NX')", 1, 'lock') is None
    # A key whose TTL has passed but that is not deleted yet (active expiry is off): a SET that XX
    # then stops sends only the key's DEL; a SETNX that runs goes out with the DEL in MULTI/EXEC.
    for key in ('exp1', 'exp2'):
        assert c('SET', key, 'v', 'PX', 50) == '+OK'
        expected.append(pxat(c, key))
    time.sleep(0.1)
    assert c('SET', 'exp1', 'w', 'XX') is None
    expected.append(['DEL', 'exp1'])
    assert c('SETNX', 'exp2', 'w') == 1
    expected += [['MULTI'], ['DEL', 'exp2'], ['SETNX', 'exp2', 'w'], ['EXEC']]

    # A stopped SET in a transaction or a script: only the other write goes out, without MULTI/EXEC.
    assert c('MULTI') == '+OK'
    assert c('SET', 'lock', 'v10', 'NX', 'EX', 10) == '+QUEUED'
    assert c('INCR', 'n') == '+QUEUED'
    assert c('EXEC') == [None, 1]
    expected.append(['INCR', 'n'])
    assert c('EVAL', "redis.call('set', KEYS[1], 'x', 'NX', 'EX', 10); return redis.call('incr', 'n')",
             1, 'lock') == 2
    expected.append(['incr', 'n'])

    # SETs that ran go out as Redis sends them.
    assert c('SET', 'nx1', 'a', 'NX', 'EX', 100) == '+OK'
    expected.append(pxat(c, 'nx1'))
    assert c('SET', 'nx2', 'a', 'nx', 'exat', FAR_S) == '+OK'
    expected.append(['SET', 'nx2', 'a', 'PXAT', str(FAR_MS)])
    assert c('SET', 'px1', 'v', 'PXAT', FAR_MS, 'NX') == '+OK'
    expected.append(['SET', 'px1', 'v', 'PXAT', str(FAR_MS), 'NX'])
    assert c('SET', 'px2', 'v', 'pxat', FAR_MS, 'GET') is None
    expected.append(['SET', 'px2', 'v', 'pxat', str(FAR_MS), 'GET'])
    assert c('SET', 'ex1', 'v', 'EX', 100, 'GET') is None
    expected.append(pxat(c, 'ex1'))
    assert c('SET', 'g1', 'v', 'GET') is None
    expected.append(['SET', 'g1', 'v'])
    assert c('SET', 'g2', 'v', 'get', 'NX', 'GET') is None
    expected.append(['SET', 'g2', 'v', 'NX'])
    assert c('SET', 'nx1', 'b', 'XX', 'KEEPTTL') == '+OK'
    expected.append(['SET', 'nx1', 'b', 'XX', 'KEEPTTL'])
    assert c('SET', 'g1', 'w', 'XX', 'GET') == 'v'
    expected.append(['SET', 'g1', 'w', 'XX'])
    assert c('SETNX', 'fresh2', 1) == 1
    expected.append(['SETNX', 'fresh2', '1'])
    assert c('MSETNX', 'm1', 'a', 'm2', 'b') == 1
    expected.append(['MSETNX', 'm1', 'a', 'm2', 'b'])
    assert c('SET', 'end', 1) == '+OK'
    expected.append(['SET', 'end', '1'])
    return expected


def snapshot(c):
    return {k: (c('GET', k), c('PEXPIRETIME', k)) for k in sorted(c('KEYS', '*'))}


def check_propagation(name, binary, work):
    port = PORTS[name]
    rport = PORTS[name + '-replica']
    workdir = os.path.join(work, name, 'master')
    proc = start(binary, port, workdir, '--appendonly', 'yes', '--appendfsync', 'always',
                 '--notify-keyspace-events', 'KEA', '--repl-diskless-sync-delay', '0',
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
        expected = run_sets(c, events)
        events.close()
        stream = fake.commands_until(['SET', 'end', '1'])
        fake.close()
        assert stream == expected, (name, stream)
        assert aof_commands(aof_file(name, workdir)) == expected, (name, aof_commands(aof_file(name, workdir)))

        # The lock still holds v1 with its TTL on the master, on the replica and after an AOF reload.
        wait_for(lambda: r('GET', 'end') == '1', 'the replicated writes')
        before = snapshot(c)
        assert before['lock'][0] == 'v1' and before['lock'][1] > 0, (name, before['lock'])
        assert not {'absent', 'absent2', 'fresh', 'exp1'} & set(before), (name, before)
        assert before['exp2'] == ('w', -1), (name, before['exp2'])
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
INVALID_SET = "-ERR invalid expire time in 'set' command"
NOT_INT = '-ERR value is not an integer or out of range'
WRONGTYPE = '-WRONGTYPE Operation against a key holding the wrong kind of value'


def check_parser(name, binary, work):
    port = PORTS[name]
    proc = start(binary, port, os.path.join(work, name, 'parser'))
    try:
        c = Conn(port)
        for cmd, reply in [
            (('SET', 'k', 'v', 'NX', 'XX'), SYNTAX),
            (('SET', 'k', 'v', 'xx', 'nx'), SYNTAX),
            (('SET', 'k', 'v', 'KEEPTTL', 'EX', 10), SYNTAX),
            (('SET', 'k', 'v', 'EX', 10, 'KEEPTTL'), SYNTAX),
            (('SET', 'k', 'v', 'EX', 10, 'PX', 100), SYNTAX),
            (('SET', 'k', 'v', 'PXAT', 1, 'EXAT', 1), SYNTAX),
            (('SET', 'k', 'v', 'EX'), SYNTAX),
            (('SET', 'k', 'v', 'NXX'), SYNTAX),
            (('SET', 'k', 'v', 'EX', 'abc', 'NX', 'XX'), SYNTAX),
            (('SET', 'k', 'v', 'EX', 'abc'), NOT_INT),
            (('SET', 'k', 'v', 'EX', '05'), NOT_INT),
            (('SET', 'k', 'v', 'EX', '+5'), NOT_INT),
            (('SET', 'k', 'v', 'EX', ' 5'), NOT_INT),
            (('SET', 'k', 'v', 'PX', '-0'), NOT_INT),
            (('SET', 'k', 'v', 'EX', 0), INVALID_SET),
            (('SET', 'k', 'v', 'PX', -1), INVALID_SET),
            (('SET', 'k', 'v', 'EXAT', 0), INVALID_SET),
            (('SET', 'k', 'v', 'PXAT', -5), INVALID_SET),
            (('SET', 'k', 'v', 'EX', 9223372036854775807), INVALID_SET),
            # Not checked: PX near LLONG_MAX, where Redis's overflow test is undefined behaviour (the
            # macOS build answers OK and stores 0); PeaDB refuses it, as that test means to.
        ]:
            assert c(*cmd) == reply, (name, cmd, c(*cmd))
        assert c('EXISTS', 'k') == 0, name
        # GETEX: the options, then the key (nil, WRONGTYPE), then the expire value.
        invalid_getex = "-ERR invalid expire time in 'getex' command"
        assert c('GETEX', 'k', 'EXAT', 0) is None, name
        assert c('GETEX', 'k', 'EX', 10, 'PERSIST') == SYNTAX, name
        assert c('SET', 'gk', 'v') == '+OK'
        for cmd, reply in [
            (('GETEX', 'gk', 'EXAT', 0), invalid_getex),
            (('GETEX', 'gk', 'PXAT', -1), invalid_getex),
            (('GETEX', 'gk', 'EX', 'abc'), NOT_INT),
            (('GETEX', 'gk', 'EX', '05'), NOT_INT),
            (('GETEX', 'gk', 'NX'), SYNTAX),
            (('GETEX', 'gk', 'KEEPTTL'), SYNTAX),
            (('GETEX', 'gk', 'GET'), SYNTAX),
            (('GETEX', 'gk', 'EX'), SYNTAX),
            (('GETEX', 'gk', 'PERSIST', 'EX', 10), SYNTAX),
            (('GETEX', 'gk', 'EX', 10, 'PX', 10), SYNTAX),
        ]:
            assert c(*cmd) == reply, (name, cmd, c(*cmd))
        assert c('TTL', 'gk') == -1, name
        assert c('GETEX', 'gk', 'EX', 10, 'EX', 20) == 'v', name
        assert 19 <= c('TTL', 'gk') <= 20, name
        assert c('GETEX', 'gk', 'PERSIST', 'PERSIST') == 'v', name
        assert c('TTL', 'gk') == -1, name
        assert c('SET', 'k', 'v', 'EX', 10, 'EX', 20) == '+OK', name
        assert 19 <= c('TTL', 'k') <= 20, name
        assert c('SET', 'k', 'v', 'GET', 'GET', 'KEEPTTL', 'KEEPTTL') == 'v', name
        assert 19 <= c('TTL', 'k') <= 20, name
        assert c('HSET', 'h', 'f', 'v') == 1
        assert c('GETEX', 'h', 'EX', 0) == WRONGTYPE, name  # the type is checked first
        assert c('SET', 'h', 'v', 'NX', 'GET') == WRONGTYPE, name
        assert c('SET', 'h', 'v', 'GET') == WRONGTYPE, name
        assert c('SET', 'h', 'v', 'GET', 'EX', 0) == INVALID_SET, name  # the expire is checked first
        assert c('SET', 'h', 'v', 'NX') is None, name
        assert c('TYPE', 'h') == '+hash', name
        assert c('SET', 'h', 'v') == '+OK', name
        assert c('TYPE', 'h') == '+string', name
        check_other_types(name, c)
        c.close()
    finally:
        stop(proc)


def check_other_types(name, c):
    """MGET, MSETNX and SORT's BY and GET patterns leave keys of other types as they are (PeaDB turned them into empty strings)."""
    assert c('HSET', 'oh', 'f', 'v') == 1 and c('RPUSH', 'ol', 'a') == 1 and c('SADD', 'os', 'm') == 1
    assert c('ZADD', 'oz', 1, 'm') == 1
    assert c('MGET', 'oh', 'ol', 'os', 'oz', 'nokey') == [None] * 5, name
    assert c('MSETNX', 'ol', 'x', 'onew', 'y') == 0, name
    assert c('MSETNX', 'onew', 'y', 'oh', 'x') == 0, name
    assert c('EXISTS', 'onew') == 0, name
    assert c('RPUSH', 'onames', 'oh', 'ol', 'os', 'oz') == 4
    assert c('SORT', 'onames', 'BY', 'nosort', 'GET', '*', 'GET', '#') == [
        None, 'oh', None, 'ol', None, 'os', None, 'oz'], name
    # Every weight is nil, so the order of the result is not defined: only the keys matter here.
    assert sorted(c('SORT', 'onames', 'BY', '*', 'ALPHA')) == ['oh', 'ol', 'os', 'oz'], name
    assert [c('TYPE', k) for k in ('oh', 'ol', 'os', 'oz')] == ['+hash', '+list', '+set', '+zset'], name
    assert (c('HGET', 'oh', 'f'), c('LRANGE', 'ol', 0, -1), c('SMEMBERS', 'os'), c('ZSCORE', 'oz', 'm')) == (
        'v', ['a'], ['m'], '1'), name


def check_engine(name, binary, work):
    check_propagation(name, binary, work)
    check_parser(name, binary, work)
    print(f'{name}: ok')


def main():
    work = tempfile.mkdtemp(prefix='peadb-setprop-')
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

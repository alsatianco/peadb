#!/usr/bin/env python3
"""maxmemory counts real memory and refuses only the commands Redis 7.2.5 refuses (compat/delta.md items 38 and 39).

The same checks run on peadb-server and on the pinned redis-server (Redis side first, only when it is Redis 7.2.x):

- accounting: `used_memory` counts what the server really allocated, so with `maxmemory 16mb` and `allkeys-lru` a load of 200,000 small lists is evicted down to the limit and RSS stays near it (PeaDB counted about one third of real memory, and on macOS its RSS reached 13 times the limit with no eviction); after FLUSHALL `used_memory` is back near where it started.
- speed: with 1,500,000 keys and `maxmemory` set, writes run at full speed and INFO answers at once (PeaDB walked every key on each write once one walk took 100 ms: 3 to 9 writes a second at 2M keys); lowering `maxmemory` 60 MB under the used memory answers at once, logs Redis's warning, and evicts in short steps (500 us each, maxmemory-eviction-tenacity 10), so a client's PING is answered within 100 ms while it evicts (about 2 ms on both engines) (PeaDB froze for minutes).
- the OOM rules (Redis's `processCommand()`, `scriptVerifyOOM()`): with `noeviction` and the memory over the limit, only `denyoom` commands are refused (SET, RPUSH, …), and DEL, UNLINK, EXPIRE, LPOP, HDEL and other commands that free memory run (PeaDB refused every write); inside MULTI every command but EXEC, DISCARD, QUIT and RESET is refused (WATCH too, before its own error); an EXEC is refused when its transaction holds a `denyoom` command and memory went over after queueing; the commands an EXEC runs are not checked again; a script without a shebang is refused at its first `denyoom` write only if memory was over when it started and it has not written yet; a script or function with declared flags is refused before it runs unless it has `allow-oom` or `no-writes`; a script whose own writes go over the limit is not stopped half done (PeaDB refused its next write).
- eviction inside MULTI (upstream `unit/maxmemory` "propagation with eviction in MULTI"): `CONFIG SET maxmemory 1` inside a transaction does not evict inside it; the key is evicted after EXEC by the eviction timer that CONFIG SET started, with no other command sent, and a replica gets MULTI, INCR, INCR, EXEC, then DEL.
- memory not counted for eviction: a replica that does not read makes its output buffer grow, which is not counted against `maxmemory` (Redis's `freeMemoryGetNotCountedMemory()`), so no key is evicted for it; INFO reports it in `mem_not_counted_for_evict`. A 64 MB `repl-backlog-size` with no replica uses almost nothing at start and does not use up a 32 MB `maxmemory` when 50 MB pass through the replication stream (Redis creates its backlog with the first replica; PeaDB allocated and counted the whole ring); a 16 KB backlog that wraps a dozen times still serves a partial PSYNC from near its end with exactly the bytes a replica attached from the start received.

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
PORTS = {'peadb': 6670, 'redis': 6671}
OOM = "-OOM command not allowed when used memory > 'maxmemory'."


def encode(*args):
    out = b'*%d\r\n' % len(args)
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode()
        out += b'$%d\r\n' % len(b) + b + b'\r\n'
    return out


class Conn:
    def __init__(self, port, timeout=10):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=timeout)
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

    def pipeline(self, cmds):
        self.sock.sendall(b''.join(encode(*c) for c in cmds))
        return [self.read() for _ in cmds]


class FakeReplica:
    """A replica that sends PSYNC ? -1, skips the RDB, then reads the commands its master sends (or reads nothing)."""

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
        """The commands up to `last`, without pings and GETACKs."""
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
        proc.wait()


def info(c, field, section='all'):
    for line in c('INFO', section).splitlines():
        if line.startswith(field + ':'):
            return line.split(':', 1)[1]
    raise KeyError(field)


def rss_bytes(pid):
    out = subprocess.run(['ps', '-o', 'rss=', '-p', str(pid)], capture_output=True, text=True).stdout
    return int(out.strip()) * 1024


def wait_for(cond, what, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.01)
    raise AssertionError(f'timed out waiting for {what}')


def check_accounting(name, binary, work):
    """used_memory follows real memory, so maxmemory bounds the process."""
    limit = 16 * 1024 * 1024
    proc = start(binary, PORTS[name], os.path.join(work, f'{name}-accounting'),
                 '--maxmemory', '16mb', '--maxmemory-policy', 'allkeys-lru')
    try:
        c = Conn(PORTS[name])
        start_used = int(info(c, 'used_memory', 'memory'))
        start_rss = rss_bytes(proc.pid)
        assert start_used < 16 * 1024 * 1024, (name, start_used)
        for batch in range(200):
            cmds = [('RPUSH', f'list:{i}', 'a', 'b', 'c', 'd', 'e') for i in range(batch * 1000, batch * 1000 + 1000)]
            replies = c.pipeline(cmds)
            assert all(r == 5 for r in replies), (name, [r for r in replies if r != 5][:3])
        used = int(info(c, 'used_memory', 'memory'))
        evicted = int(info(c, 'evicted_keys', 'stats'))
        rss = rss_bytes(proc.pid)
        assert evicted > 0, (name, 'no key evicted', used, rss)
        assert used <= limit + 1024 * 1024, (name, used)
        # Allow for allocator fragmentation, but not for an RSS that grows to many times the limit.
        assert rss - start_rss < 2 * limit, (name, 'rss', rss, 'start rss', start_rss, 'used', used, 'evicted', evicted)
        assert c('CONFIG', 'SET', 'maxmemory', 0) == '+OK'
        assert c('FLUSHALL') == '+OK'
        after = int(info(c, 'used_memory', 'memory'))
        assert after < start_used + 2 * 1024 * 1024, (name, 'after FLUSHALL', after, 'start', start_used)
        c.close()
    finally:
        stop(proc)


def check_large_keyspace(name, binary, work):
    """With many keys and maxmemory set, writes and INFO stay fast and lowering maxmemory evicts in short steps."""
    workdir = os.path.join(work, f'{name}-large')
    proc = start(binary, PORTS[name], workdir)
    try:
        c = Conn(PORTS[name], timeout=20)
        assert c('DEBUG', 'POPULATE', 1500000, 'key', 100) == '+OK'
        assert c('CONFIG', 'SET', 'maxmemory', '16gb') == '+OK'
        for policy in ('allkeys-lru', 'noeviction'):
            assert c('CONFIG', 'SET', 'maxmemory-policy', policy) == '+OK'
            n = 0
            t0 = time.time()
            while time.time() - t0 < 0.5:
                assert c('SET', f'w:{n}', 'v') == '+OK'
                n += 1
            assert n >= 500, (name, policy, 'writes in 0.5 s', n)
            t0 = time.time()
            info(c, 'used_memory', 'memory')
            assert time.time() - t0 < 0.05, (name, policy, 'INFO memory took', time.time() - t0)
        # Lower maxmemory by about 60 MB: Redis evicts in short steps (maxmemory-eviction-tenacity 10: 500 us),
        # from the CONFIG SET's eviction timer and before each command, so a client is answered while it evicts.
        assert c('CONFIG', 'SET', 'maxmemory-policy', 'allkeys-random') == '+OK'
        used = int(info(c, 'used_memory', 'memory')) - int(info(c, 'mem_not_counted_for_evict', 'memory'))
        target = used - 60 * 1024 * 1024
        t0 = time.time()
        assert c('CONFIG', 'SET', 'maxmemory', target) == '+OK'
        assert time.time() - t0 < 0.2, (name, 'CONFIG SET maxmemory took', time.time() - t0)
        worst = 0.0
        deadline = time.time() + 15
        while True:
            t0 = time.time()
            assert c('PING') == '+PONG'
            worst = max(worst, time.time() - t0)
            # What eviction compares with maxmemory: used_memory less mem_not_counted_for_evict. Each
            # command allocates a little after the eviction that ran before it, so allow some slack.
            mem = c('INFO', 'memory')
            fields = dict(line.split(':', 1) for line in mem.splitlines() if ':' in line)
            if int(fields['used_memory']) - int(fields['mem_not_counted_for_evict']) <= target + 64 * 1024:
                break
            assert time.time() < deadline, (name, 'used memory did not reach the limit')
        evicted = int(info(c, 'evicted_keys', 'stats'))
        assert evicted > 150000, (name, 'evicted', evicted)
        # About 2 ms on both engines here; one eviction of all 60 MB at once takes about 290 ms.
        assert worst < 0.1, (name, 'slowest PING while evicting', worst)
        log = open(os.path.join(workdir, 'server.log')).read()
        assert (f'WARNING: the new maxmemory value set via CONFIG SET ({target}) is smaller than the current memory '
                'usage (') in log, (name, log[-2000:])
        assert ('This will result in key eviction and/or the inability to accept new write commands depending on '
                'the maxmemory-policy.') in log, name
        c.close()
    finally:
        stop(proc)


def check_oom_rules(name, binary, work):
    """Which commands -OOM refuses, as in processCommand() and scriptVerifyOOM()."""
    proc = start(binary, PORTS[name], os.path.join(work, f'{name}-rules'))
    try:
        c = Conn(PORTS[name])
        other = Conn(PORTS[name])
        assert c('FUNCTION', 'LOAD', "#!lua name=lib\n"
                 "redis.register_function('plain', function(keys, args) return redis.call('set', keys[1], 'v') end)\n"
                 "redis.register_function{function_name='oomok', callback=function(keys, args) "
                 "return redis.call('set', keys[1], 'v') end, flags={'allow-oom'}}\n"
                 "redis.register_function{function_name='ro', callback=function(keys, args) "
                 "return redis.call('get', keys[1]) end, flags={'no-writes'}}\n") == 'lib'
        setup = [('SET', 's', 'v'), ('SET', 's2', 'v'), ('HSET', 'h', 'f', 'v', 'g', 'w'), ('RPUSH', 'l', 'a', 'b', 'c'),
                 ('SADD', 'st', 'a', 'b'), ('ZADD', 'z', 1, 'a', 2, 'b'), ('SET', 'e', 'v'), ('SET', 'u', 'v'),
                 ('SET', 'n', 1)]
        for cmd in setup:
            assert not str(c(*cmd)).startswith('-'), (name, cmd)
        assert c('CONFIG', 'SET', 'maxmemory', 1) == '+OK'
        refused = [('SET', 'x', 'v'), ('RPUSH', 'l', 'd'), ('LPUSH', 'l', 'd'), ('HSET', 'h', 'f2', 'v'),
                   ('SADD', 'st', 'c'), ('ZADD', 'z', 3, 'c'), ('INCR', 'n'), ('APPEND', 's', 'x'),
                   ('SETRANGE', 's', 0, 'x'), ('MSET', 'a', 1, 'b', 2), ('XADD', 'xs', '*', 'f', 'v'),
                   ('PFADD', 'hll', 'a'), ('GEOADD', 'g', 0, 0, 'm'), ('COPY', 's', 'scopy'),
                   ('SUNIONSTORE', 'su', 'st'), ('LMOVE', 'l', 'l2', 'LEFT', 'RIGHT'), ('HINCRBY', 'h', 'c', 1),
                   ('RESTORE', 'r', 0, 'x'), ('BITOP', 'AND', 'bo', 's')]
        for cmd in refused:
            assert c(*cmd) == OOM, (name, cmd, c(*cmd))
        # Reads and commands that free memory run.
        allowed = [(('GET', 's'), 'v'), (('EXPIRE', 'e', 100), 1), (('PERSIST', 'e'), 1), (('PEXPIRE', 'e', 100000), 1),
                   (('HDEL', 'h', 'g'), 1), (('LPOP', 'l'), 'a'), (('RPOP', 'l'), 'c'), (('SREM', 'st', 'b'), 1),
                   (('ZREM', 'z', 'b'), 1), (('LTRIM', 'l', 0, 0), '+OK'), (('DEL', 's2'), 1), (('UNLINK', 'u'), 1),
                   (('GETDEL', 'e'), 'v'), (('DBSIZE',), 6), (('PING',), '+PONG'), (('EVAL', 'return 1', 0), 1),
                   (('EVAL', "return redis.call('get', 's')", 0), 'v'),
                   (('EVAL', "#!lua flags=no-writes\nreturn redis.call('get', 's')", 0), 'v'),
                   (('FCALL_RO', 'ro', 1, 's'), 'v'), (('FUNCTION', 'DELETE', 'nolib'), '-ERR Library not found')]
        for cmd, want in allowed:
            assert c(*cmd) == want, (name, cmd, c(*cmd))
        # Scripts and functions.
        r = c('EVAL', "return redis.call('set', 'x', 'v')", 0)
        assert isinstance(r, str) and r.startswith(OOM), (name, r)
        assert c('EVAL', "redis.call('del', 'n') return redis.call('set', 'x', 'v')", 0) == '+OK', name
        assert c('EVAL', '#!lua\nreturn 1', 0) == OOM, (name, c('EVAL', '#!lua\nreturn 1', 0))
        assert c('EVAL', "#!lua flags=allow-oom\nreturn redis.call('set', 'y', 'v')", 0) == '+OK', name
        assert c('FCALL', 'plain', 1, 'x2') == OOM, (name, c('FCALL', 'plain', 1, 'x2'))
        assert c('FCALL', 'oomok', 1, 'x3') == '+OK', name
        # MULTI: every command but EXEC, DISCARD, QUIT and RESET is refused while memory is over.
        assert c('MULTI') == '+OK'
        assert c('GET', 's') == OOM, name
        assert c('DEL', 's') == OOM, name
        assert c('WATCH', 's') == OOM, name
        assert c('EXEC') == '-EXECABORT Transaction discarded because of previous errors.', name
        assert c('MULTI') == '+OK'
        assert c('DISCARD') == '+OK'
        # A transaction queued while memory was under the limit.
        assert c('CONFIG', 'SET', 'maxmemory', 0) == '+OK'
        assert c('MULTI') == '+OK'
        assert c('SET', 'q', 'v') == '+QUEUED'
        assert other('CONFIG', 'SET', 'maxmemory', 1) == '+OK'
        assert c('EXEC') == '-EXECABORT Transaction discarded because of: ' + OOM[1:], name
        assert c('EXISTS', 'q') == 0
        assert other('CONFIG', 'SET', 'maxmemory', 0) == '+OK'
        assert c('MULTI') == '+OK'
        assert c('DEL', 's') == '+QUEUED'
        assert c('SET', 'q', 'v') == '+QUEUED'
        assert other('CONFIG', 'SET', 'maxmemory', 1) == '+OK'
        assert c('EXEC') == '-EXECABORT Transaction discarded because of: ' + OOM[1:], name
        assert other('CONFIG', 'SET', 'maxmemory', 0) == '+OK'
        assert c('MULTI') == '+OK'
        assert c('DEL', 'x') == '+QUEUED'
        assert c('EXPIRE', 'y', 100) == '+QUEUED'
        assert other('CONFIG', 'SET', 'maxmemory', 1) == '+OK'
        assert c('EXEC') == [1, 1], name
        assert c('FLUSHALL') == '+OK'
        assert c('SET', 'x', 'v') == OOM, name
        assert c('CONFIG', 'SET', 'maxmemory', 0) == '+OK'
        assert c('SET', 'x', 'v') == '+OK'
        errs = c('INFO', 'errorstats')
        assert 'errorstat_OOM:count=' in errs and 'errorstat_EXECABORT:count=' in errs, (name, errs)
        # A script whose own writes go over the limit runs to its end; the next write is refused.
        used = int(info(c, 'used_memory', 'memory'))
        assert c('CONFIG', 'SET', 'maxmemory', used + 300 * 1024) == '+OK'
        script = "for i = 1, 10 do redis.call('set', 'big' .. i, string.rep('x', 100000)) end return 10"
        assert c('EVAL', script, 0) == 10, name
        assert c('EXISTS', *[f'big{i}' for i in range(1, 11)]) == 10, name
        assert c('SET', 'after', 'v') == OOM, name
        assert c('DEL', *[f'big{i}' for i in range(1, 11)]) == 10, name
        assert c('CONFIG', 'SET', 'maxmemory', 0) == '+OK'
        c.close()
        other.close()
    finally:
        stop(proc)


def check_multi_eviction(name, binary, work):
    """Upstream unit/maxmemory "propagation with eviction in MULTI": no eviction inside EXEC, the DEL comes after it."""
    proc = start(binary, PORTS[name], os.path.join(work, f'{name}-multi'))
    try:
        c = Conn(PORTS[name])
        rep = FakeReplica(PORTS[name])
        assert c('CONFIG', 'SET', 'maxmemory-policy', 'allkeys-lru') == '+OK'
        assert c('MULTI') == '+OK'
        assert c('INCR', 'x') == '+QUEUED'
        assert c('CONFIG', 'SET', 'maxmemory', 1) == '+QUEUED'
        assert c('INCR', 'x') == '+QUEUED'
        assert c('EXEC') == [1, '+OK', 2], name
        # No command is sent until the DEL arrives: the eviction timer that CONFIG SET maxmemory
        # started evicts by itself (Redis's startEvictionTimeProc()).
        got = rep.commands_until(['DEL', 'x'])
        assert c('DBSIZE') == 0, name
        got = [cmd for cmd in got if cmd[0].upper() != 'SELECT']
        assert [[a.lower() if i == 0 else a for i, a in enumerate(cmd)] for cmd in got] == \
            [['multi'], ['incr', 'x'], ['incr', 'x'], ['exec'], ['del', 'x']], (name, got)
        assert c('CONFIG', 'SET', 'maxmemory', 0) == '+OK'
        rep.close()
        c.close()
    finally:
        stop(proc)


def check_not_counted(name, binary, work):
    """A replica's output buffer is not counted against maxmemory, so it evicts no key."""
    proc = start(binary, PORTS[name], os.path.join(work, f'{name}-notcounted'))
    try:
        c = Conn(PORTS[name])
        for i in range(100):
            assert c('SET', f'keep:{i}', 'v') == '+OK'
        rep = FakeReplica(PORTS[name])  # it reads nothing more, so its output buffer grows
        assert c('CONFIG', 'SET', 'maxmemory-policy', 'allkeys-lru') == '+OK'
        used = int(info(c, 'used_memory', 'memory'))
        # Room for Redis's 1 MB backlog, which fills now and is counted (PeaDB's is allocated at start).
        assert c('CONFIG', 'SET', 'maxmemory', used + 3 * 1024 * 1024) == '+OK'
        # Values well under the 1 MB backlog (a bigger write never reaches a PeaDB replica: delta 40).
        value = 'x' * (100 * 1024)
        for i in range(400):
            assert c('SET', 'big', value) == '+OK'
        not_counted = int(info(c, 'mem_not_counted_for_evict', 'memory'))
        assert not_counted > 8 * 1024 * 1024, (name, not_counted)  # far more than the 3 MB of room
        assert int(info(c, 'evicted_keys', 'stats')) == 0, (name, info(c, 'evicted_keys', 'stats'))
        assert c('DBSIZE') == 101, name
        assert c('CONFIG', 'SET', 'maxmemory', 0) == '+OK'
        assert int(info(c, 'evicted_clients', 'stats')) == 0
        int(info(c, 'total_eviction_exceeded_time', 'stats'))
        int(info(c, 'current_eviction_exceeded_time', 'stats'))
        int(info(c, 'lazyfree_pending_objects', 'memory'))
        rep.close()
        c.close()
    finally:
        stop(proc)


def check_backlog(name, binary, work):
    """A large repl-backlog-size with no replica does not use up maxmemory (Redis creates its backlog with the first replica)."""
    proc = start(binary, PORTS[name], os.path.join(work, f'{name}-backlog'), '--repl-backlog-size', '64mb')
    try:
        c = Conn(PORTS[name])
        used = int(info(c, 'used_memory', 'memory'))
        assert used < 16 * 1024 * 1024, (name, 'used_memory with an empty 64 MB backlog', used)
        assert c('CONFIG', 'SET', 'maxmemory', '32mb') == '+OK'
        value = 'x' * (500 * 1024)
        for i in range(100):  # 50 MB through the replication stream, while the data stays 500 KB
            assert c('SET', 'k', value) == '+OK', name
        assert c('SET', 'a', 'b') == '+OK', name
        assert c('CONFIG', 'SET', 'maxmemory', 0) == '+OK'
        c.close()
    finally:
        stop(proc)


def check_backlog_ring(name, binary, work):
    """The backlog ring keeps the stream exactly as it wraps: a partial PSYNC from an offset near its end returns the same bytes a replica attached from the start received."""
    proc = start(binary, PORTS[name], os.path.join(work, f'{name}-ring'), '--repl-backlog-size', '16kb')
    try:
        c = Conn(PORTS[name])
        whole = socket.create_connection(('127.0.0.1', PORTS[name]), timeout=10)
        f = whole.makefile('rb')
        whole.sendall(encode('PSYNC', '?', '-1'))
        line = f.readline()
        assert line.startswith(b'+FULLRESYNC'), (name, line)
        replid, start_offset = line.split()[1].decode(), int(line.split()[2])
        while True:  # Redis sends newlines while it prepares the RDB
            b = f.read(1)
            if b == b'$':
                break
        f.read(int(f.readline()[:-2]))
        for batch in range(40):  # about 200 KB in 3 KB passes: the 16 KB ring wraps a dozen times
            replies = c.pipeline([('SET', f'ring:{batch}:{i}', 'v' * 100) for i in range(25)])
            assert all(r == '+OK' for r in replies), name
        end_offset = int(info(c, 'master_repl_offset', 'replication'))
        received = b''
        deadline = time.time() + 10
        while len(received) < end_offset - start_offset and time.time() < deadline:
            received += f.read1(65536)
        assert len(received) == end_offset - start_offset, (name, len(received), end_offset - start_offset)
        tail = 8000
        part = socket.create_connection(('127.0.0.1', PORTS[name]), timeout=10)
        pf = part.makefile('rb')
        # A replica that holds the stream up to offset R asks for R + 1, the first byte it lacks
        # (Redis's replicas send cached_master->reploff + 1).
        part.sendall(encode('PSYNC', replid, end_offset - tail + 1))
        assert pf.readline().startswith(b'+CONTINUE'), name
        got = pf.read(tail)
        assert got == received[-tail:], (name, 'partial resync bytes differ from the stream')
        # Close the file objects too, or the sockets stay open and the shutdown waits
        # shutdown-timeout seconds for these two replicas, which never ACK.
        for s_ in (pf, part, f, whole):
            s_.close()
        c.close()
    finally:
        stop(proc)


def check_engine(name, binary, work):
    check_oom_rules(name, binary, work)
    check_multi_eviction(name, binary, work)
    check_not_counted(name, binary, work)
    check_backlog(name, binary, work)
    check_backlog_ring(name, binary, work)
    check_accounting(name, binary, work)
    check_large_keyspace(name, binary, work)


def main():
    work = tempfile.mkdtemp(prefix='peadb-maxmemory-')
    try:
        if REDIS:
            check_engine('redis', REDIS, work)
        check_engine('peadb', PEADB, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print('ok')


if __name__ == '__main__':
    main()

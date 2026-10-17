#!/usr/bin/env python3
"""CONFIG GET and CONFIG SET behave like Redis 7.2.5 (compat/delta.md items 23 and 25).

The same commands go to peadb-server and to the pinned redis-server, and the replies must be the
same: every setting name and default, Redis's error text for unknown, immutable, protected and
repeated names and for bad values, the subcommand errors and CONFIG HELP. PeaDB's own answers are
checked where they differ on purpose: the Redis settings whose behaviour PeaDB fixes, and the
values it refuses because it cannot apply them. Then the settings that used to say OK and do
nothing are checked to take effect: appendonly at runtime (with a restart), maxclients,
proto-max-bulk-len, client-query-buffer-limit and the protected dir and dbfilename. Without a
pinned redis-server, only the PeaDB side runs.
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
REDIS = redis_server_path()
PEADB_PORT = 6564
REDIS_PORT = 6565

# Settings where PeaDB reports its own fixed behaviour instead of Redis's default.
PEADB_FIXED = {
    'aof-use-rdb-preamble': 'no',
    'auto-aof-rewrite-percentage': '0',
    'enable-debug-command': 'yes',
    'lfu-decay-time': '0',
    'rdbcompression': 'no',
}


class Conn:
    def __init__(self, port):
        # DEBUG LOADAOF replays 300,000 keys; sanitizer instrumentation needs a larger I/O budget.
        timeout = 30 if os.environ.get('PEADB_TEST_SANITIZED') == '1' else 5
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=timeout)
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
            return [self.read() for _ in range(int(rest))]
        if kind == b'%':
            return {'%map': dict((self.read(), self.read()) for _ in range(int(rest)))}
        raise ValueError(f'unexpected reply {line!r}')

    def __call__(self, *args):
        out = f'*{len(args)}\r\n'.encode()
        for a in args:
            b = a.encode() if isinstance(a, str) else a
            out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
        self.sock.sendall(out)
        return self.read()


def start(binary, port, workdir, *extra):
    log = open(os.path.join(workdir, 'server.log'), 'ab')
    # Started from / with --dir: CONFIG GET dir must name the data directory, not the cwd.
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--save', '',
                             '--dir', workdir, '--loglevel', 'warning', *extra],
                            cwd='/', stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'{binary} exited: {open(os.path.join(workdir, "server.log")).read()}')
        try:
            socket.create_connection(('127.0.0.1', port), timeout=0.2).close()
            return proc
        except OSError:
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError(f'{binary} did not start on port {port}')


def stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def config_map(conn):
    reply = conn('CONFIG', 'GET', '*')
    assert isinstance(reply, list) and len(reply) % 2 == 0, reply
    return dict(zip(reply[0::2], reply[1::2]))


# Commands whose replies must be the same on both engines (the server state is the same after
# each one on both, since every case that changes something puts it back).
SAME_REPLY = [
    ('CONFIG', 'GET_XX'),
    ('CONFIG', 'SET', 'a'),
    ('CONFIG', 'GET'),
    ('CONFIG', 'HELP', 'x'),
    ('CONFIG', 'RESETSTAT', 'x'),
    ('CONFIG', 'HELP'),
    ('CONFIG', 'REWRITE'),
    ('CONFIG', 'SET', 'maxmemory', '1', 'oops'),
    ('CONFIG', 'SET', 'no-such-setting', '1'),
    ('CONFIG', 'SET', 'maxmemory', '10', 'no-such-setting', '1'),
    ('CONFIG', 'SET', 'daemonize', 'yes', 'no-such-setting', '1'),
    ('CONFIG', 'SET', 'dir', '/tmp'),
    ('CONFIG', 'SET', 'dbfilename', 'x.rdb'),
    ('CONFIG', 'SET', 'unixsocketperm', '700'),
    ('CONFIG', 'SET', 'cluster-enabled', 'yes'),
    ('CONFIG', 'SET', 'enable-protected-configs', 'yes'),
    ('CONFIG', 'SET', 'maxmemory', '1', 'MAXMEMORY', '2'),
    ('CONFIG', 'SET', 'port', 'abc'),
    ('CONFIG', 'SET', 'maxclients', '0'),
    ('CONFIG', 'SET', 'proto-max-bulk-len', '1'),
    ('CONFIG', 'SET', 'slave-serve-stale-data', 'maybe'),
    ('CONFIG', 'SET', 'save', '100 10 200'),
    ('CONFIG', 'SET', 'save', '100  10'),
    ('CONFIG', 'SET', 'notify-keyspace-events', 'Q'),
    ('CONFIG', 'SET', 'maxmemory', '1kb', 'maxmemory-policy', 'nope'),
    ('CONFIG', 'GET', 'maxmemory'),
    ('CONFIG', 'SET', 'activedefrag', 'yes'),
    ('CONFIG', 'SET', 'shutdown-on-sigterm', 'save nosave'),
    ('CONFIG', 'SET', 'lfu-log-factor', '-1'),
    # Changes that work on both, read back, then put back.
    ('CONFIG', 'SET', 'maxmemory', '10mb', 'maxmemory-policy', 'ALLKEYS-LRU', 'hz', '20'),
    ('CONFIG', 'GET', 'maxmemory'),
    ('CONFIG', 'GET', 'maxmemory-policy'),
    ('CONFIG', 'GET', 'hz'),
    ('CONFIG', 'SET', 'maxmemory', '0', 'maxmemory-policy', 'noeviction', 'hz', '10'),
    ('CONFIG', 'SET', 'lua-time-limit', '6000', 'busy-reply-threshold', '7000'),
    ('CONFIG', 'GET', 'lua-time-limit'),
    ('CONFIG', 'SET', 'busy-reply-threshold', '5000'),
    ('CONFIG', 'GET', 'Slave-Serve-Stale-DATA'),
    ('CONFIG', 'GET', 'rdb-key-save-delay'),
    ('CONFIG', 'GET', 'rdb-key*'),
    ('CONFIG', 'SET', 'notify-keyspace-events', 'Exgm'),
    ('CONFIG', 'GET', 'notify-keyspace-events'),
    ('CONFIG', 'SET', 'notify-keyspace-events', 'KEA'),
    ('CONFIG', 'GET', 'notify-keyspace-events'),
    ('CONFIG', 'SET', 'notify-keyspace-events', ''),
    ('CONFIG', 'SET', 'save', '100 10 200 20'),
    ('CONFIG', 'GET', 'save'),
    ('CONFIG', 'SET', 'save', ''),
    ('CONFIG', 'SET', 'client-output-buffer-limit', 'normal 1mb 2mb 60'),
    ('CONFIG', 'GET', 'client-output-buffer-limit'),
    ('CONFIG', 'SET', 'client-output-buffer-limit', 'normal 0 0 0'),
    ('CONFIG', 'SET', 'list-max-ziplist-size', '5'),
    ('CONFIG', 'GET', 'list-max-listpack-size'),
    ('CONFIG', 'SET', 'list-max-listpack-size', '-2'),
    ('CONFIG', 'SET', 'repl-backlog-size', '1'),
    ('CONFIG', 'GET', 'repl-backlog-size'),
    ('CONFIG', 'SET', 'repl-backlog-size', '1mb'),
    ('CONFIG', 'SET', 'shutdown-on-sigterm', 'now save'),
    ('CONFIG', 'GET', 'shutdown-on-sigterm'),
    ('CONFIG', 'SET', 'shutdown-on-sigterm', 'default'),
    ('CONFIG', 'SET', 'latency-tracking-info-percentiles', '50.0 99.90'),
    ('CONFIG', 'GET', 'latency-tracking-info-percentiles'),
    ('CONFIG', 'SET', 'latency-tracking-info-percentiles', '50 99 99.9'),
    ('CONFIG', 'SET', 'loglevel', 'Warning'),
    ('CONFIG', 'GET', 'loglevel'),
    ('CONFIG', 'SET', 'proto-max-bulk-len', '2mb', 'client-query-buffer-limit', '4mb'),
    ('CONFIG', 'GET', 'proto-max-bulk-len'),
    ('CONFIG', 'GET', 'client-query-buffer-limit'),
    ('CONFIG', 'SET', 'proto-max-bulk-len', '512mb', 'client-query-buffer-limit', '1gb'),
    ('CONFIG', 'SET', 'tcp-keepalive', '60', 'lfu-log-factor', '5', 'timeout', '30'),
    ('CONFIG', 'GET', 'tcp-keepalive'),
    ('CONFIG', 'GET', 'lfu-log-factor'),
    ('CONFIG', 'SET', 'tcp-keepalive', '300', 'lfu-log-factor', '10', 'timeout', '0'),
    ('CONFIG', 'SET', 'port', 'PORT_SELF'),
    ('CONFIG', 'SET', 'masteruser', 'repl', 'masterauth', 'password'),
    ('CONFIG', 'GET', 'masteruser'),
    ('CONFIG', 'GET', 'masterauth'),
    ('CONFIG', 'SET', 'masteruser', '', 'masterauth', ''),
    # Every CONFIG subcommand except HELP is NOSCRIPT.
    ('EVAL', "return redis.pcall('config','get','maxmemory')", '0'),
    ('EVAL', "return redis.pcall('config','set','maxmemory','1')", '0'),
    ('EVAL', "return redis.pcall('config','help')[1]", '0'),
]

# What PeaDB answers where it differs from Redis on purpose.
PEADB_ONLY = [
    (('CONFIG', 'SET', 'rdbcompression', 'yes'),
     "-ERR CONFIG SET failed (possibly related to argument 'rdbcompression') - PeaDB always uses "
     "'no' for this setting and cannot change it (compat/delta.md item 24)"),
    (('CONFIG', 'SET', 'rdbcompression', 'no'), '+OK'),
    (('CONFIG', 'SET', 'replica-read-only', 'no'),
     "-ERR CONFIG SET failed (possibly related to argument 'replica-read-only') - PeaDB always "
     "uses 'yes' for this setting and cannot change it (compat/delta.md item 24)"),
    (('CONFIG', 'SET', 'port', '1'),
     "-ERR CONFIG SET failed (possibly related to argument 'port') - PeaDB cannot change this "
     "setting while it runs yet (compat/delta.md item 25)"),
    # A value that cannot be applied is found before anything changes: appendonly stays off.
    (('CONFIG', 'SET', 'appendonly', 'yes', 'maxclients', '4294967295'),
     "-ERR CONFIG SET failed (possibly related to argument 'maxclients') - The operating system "
     "is not able to handle the specified number of clients, try with MAXCLIENTS_ALLOWED"),
    (('CONFIG', 'GET', 'appendonly'), ['appendonly', 'no']),
    # A failed setting leaves the earlier ones of the same command unchanged.
    (('CONFIG', 'SET', 'maxmemory', '10mb', 'rdbcompression', 'yes'),
     "-ERR CONFIG SET failed (possibly related to argument 'rdbcompression') - PeaDB always uses "
     "'no' for this setting and cannot change it (compat/delta.md item 24)"),
    (('CONFIG', 'GET', 'maxmemory'), ['maxmemory', '0']),
    (('CONFIG', 'SET', 'maxmemory', '10mb', 'cluster-announce-ip', '0.0.0.0'),
     "-ERR CONFIG SET failed (possibly related to argument 'cluster-announce-ip') - "
     "cluster-announce-ip must not resolve to a wildcard address ('0.0.0.0'); no peer could dial "
     "it back"),
    (('CONFIG', 'GET', 'maxmemory'), ['maxmemory', '0']),
    # Replication heartbeat settings are live (delta 47); their config replies match Redis.
    (('CONFIG', 'SET', 'repl-ping-replica-period', '3600'), '+OK'),
    (('CONFIG', 'GET', 'repl-ping-replica-period'), ['repl-ping-replica-period', '3600']),
    (('CONFIG', 'SET', 'repl-ping-replica-period', '10'), '+OK'),
]


def check_same_replies(peadb, redis):
    names_p, names_r = config_map(peadb), config_map(redis)
    assert set(names_p) == set(names_r), (
        f'only PeaDB: {sorted(set(names_p) - set(names_r))}, '
        f'only Redis: {sorted(set(names_r) - set(names_p))}')
    for name, value in names_r.items():
        if name in ('dir', 'port'):
            continue
        want = PEADB_FIXED.get(name, value)
        assert names_p[name] == want, f'CONFIG GET {name}: PeaDB {names_p[name]!r}, want {want!r}'
    for cmd in SAME_REPLY:
        rp = peadb(*[str(PEADB_PORT) if a == 'PORT_SELF' else a for a in cmd])
        rr = redis(*[str(REDIS_PORT) if a == 'PORT_SELF' else a for a in cmd])
        assert rp == rr, f'{cmd}: PeaDB {rp!r}, Redis {rr!r}'
    peadb('HELLO', '3')
    redis('HELLO', '3')
    rp, rr = peadb('CONFIG', 'GET', 'maxmemory'), redis('CONFIG', 'GET', 'maxmemory')
    assert rp == rr == {'%map': {'maxmemory': '0'}}, (rp, rr)
    peadb('HELLO', '2')
    redis('HELLO', '2')
    print(f'CONFIG GET/SET: {len(names_p)} names and {len(SAME_REPLY)} replies match Redis passed')


def check_peadb_only(peadb, workdir):
    assert config_map(peadb)['dir'] == os.path.realpath(workdir), config_map(peadb)['dir']
    names = config_map(peadb)
    assert len(names) == 195, len(names)
    for name, value in PEADB_FIXED.items():
        assert names[name] == value, (name, names[name])
    for cmd, want in PEADB_ONLY:
        got = peadb(*cmd)
        if isinstance(want, str) and 'MAXCLIENTS_ALLOWED' in want:
            # The number depends on this machine's open-files limit.
            want = want.replace('MAXCLIENTS_ALLOWED', got.rsplit(' ', 1)[-1])
        assert got == want, f'{cmd}: {got!r}, want {want!r}'
    print('CONFIG GET/SET: PeaDB refusals and fixed values passed')


def wait_rewrite(conn):
    deadline = time.time() + 30
    while time.time() < deadline:
        info = conn('INFO', 'persistence')
        if 'aof_rewrite_in_progress:0' in info and 'aof_rewrite_scheduled:0' in info:
            assert 'aof_last_bgrewrite_status:ok' in info, info
            return info
        time.sleep(0.05)
    raise AssertionError('AOF rewrite did not finish')


def check_runtime_effects(workdir):
    # CONFIG SET appendonly yes writes the dataset to a new AOF and keeps every write made while
    # the rewrite runs, each in its own database (the rewritten file ends in the last database
    # that holds keys, so a buffered write needs its own SELECT).
    proc = start(PEADB, PEADB_PORT, workdir)
    try:
        c = Conn(PEADB_PORT)
        assert c('CONFIG', 'SET', 'appendonly', 'yes', 'appendonly', 'no') == (
            "-ERR CONFIG SET failed (possibly related to argument 'appendonly') - duplicate "
            "parameter")
        c('SELECT', '3')
        c('SET', 'in3', 'x')
        c('SELECT', '0')
        assert c('DEBUG', 'POPULATE', '300000') == '+OK'  # makes the rewrite take a while
        c('SET', 'k1', '1')
        c('RPUSH', 'list', 'a', 'b')
        assert c('CONFIG', 'SET', 'appendonly', 'yes') == '+OK'
        info = c('INFO', 'persistence')
        assert 'aof_enabled:1' in info, info
        buffered = 'aof_rewrite_in_progress:1' in info
        c('INCR', 'k1')
        c('SELECT', '3')
        c('INCR', 'n3')
        c('SELECT', '0')
        wait_rewrite(c)
        c('INCR', 'k1')
        assert os.path.exists(os.path.join(workdir, 'appendonly.aof'))
        assert c('DEBUG', 'LOADAOF') == '+OK'
        assert c('DBSIZE') == 300002 and c('GET', 'k1') == '3', (c('DBSIZE'), c('GET', 'k1'))
        assert c('EXISTS', 'n3') == 0, 'a buffered db-3 write was replayed into db 0'
        c('SELECT', '3')
        assert c('GET', 'n3') == '1' and c('GET', 'in3') == 'x'
        c('SELECT', '0')
        # The same for BGREWRITEAOF while the AOF is on and its last write was in db 0.
        c('SET', 'k2', '1')
        assert c('BGREWRITEAOF') == '+Background append only file rewriting started'
        c('INCR', 'k2')
        wait_rewrite(c)
        assert c('DEBUG', 'LOADAOF') == '+OK'
        assert c('GET', 'k2') == '2', c('GET', 'k2')
        c('SELECT', '3')
        assert c('EXISTS', 'k2') == 0, 'a buffered db-0 write was replayed into db 3'
        c('SELECT', '0')
        # appendonly no while a rewrite runs stops it and turns the AOF off.
        assert c('CONFIG', 'SET', 'appendonly', 'no') == '+OK'
        assert c('BGREWRITEAOF') == '+Background append only file rewriting started'
        assert c('CONFIG', 'SET', 'appendonly', 'yes') == '+OK'
        assert c('CONFIG', 'SET', 'appendonly', 'no') == '+OK'
        info = c('INFO', 'persistence')
        assert 'aof_enabled:0' in info and 'aof_rewrite_in_progress:0' in info, info
        assert c('CONFIG', 'SET', 'appendonly', 'yes') == '+OK'
        wait_rewrite(c)
        # A command replayed from the AOF is not held to proto-max-bulk-len (Redis's
        # mustObeyClient()), so lowering it does not drop data written under a higher one.
        assert c('SETRANGE', 'big', str(2 * 1024 * 1024), 'x') == 2 * 1024 * 1024 + 1
        assert c('CONFIG', 'SET', 'proto-max-bulk-len', '1mb') == '+OK'
        assert c('DEBUG', 'LOADAOF') == '+OK'
        assert c('STRLEN', 'big') == 2 * 1024 * 1024 + 1
        assert c('CONFIG', 'SET', 'proto-max-bulk-len', '512mb') == '+OK'
        c.close()
    finally:
        stop(proc)
    print(f'CONFIG SET appendonly: rewrite buffer {"used" if buffered else "not reached"} '
          'during the test')
    proc = start(PEADB, PEADB_PORT, workdir, '--appendonly', 'yes')
    try:
        c = Conn(PEADB_PORT)
        assert c('DBSIZE') == 300004 and c('GET', 'k1') == '3', (c('DBSIZE'), c('GET', 'k1'))
        assert c('LRANGE', 'list', '0', '-1') == ['a', 'b']
        assert c('CONFIG', 'SET', 'appendonly', 'no') == '+OK'
        assert 'aof_enabled:0' in c('INFO', 'persistence')
        assert c('FLUSHALL') == '+OK'
        # maxclients is enforced at once.
        assert c('CONFIG', 'SET', 'maxclients', '2') == '+OK'
        second = Conn(PEADB_PORT)
        assert second('PING') == '+PONG'
        third = Conn(PEADB_PORT)
        assert third.read() == '-ERR max number of clients reached'
        third.close()
        second.close()
        assert c('CONFIG', 'SET', 'maxclients', '10000') == '+OK'
        # proto-max-bulk-len is enforced at once, for the protocol and for string growth.
        assert c('CONFIG', 'SET', 'proto-max-bulk-len', '1mb') == '+OK'
        assert c('SETRANGE', 'sr', str(2 * 1024 * 1024), 'x') == (
            '-ERR string exceeds maximum allowed size (proto-max-bulk-len)')
        big = Conn(PEADB_PORT)
        try:
            reply = big('SET', 'big', 'x' * (2 * 1024 * 1024))
        except (ConnectionError, OSError):
            reply = 'closed'
        assert reply in ('-ERR Protocol error: invalid bulk length', 'closed'), reply
        big.close()
        assert c('CONFIG', 'SET', 'proto-max-bulk-len', '512mb') == '+OK'
        # client-query-buffer-limit closes a client whose unparsed input grows past it. The bulk
        # length (100 MB) is under proto-max-bulk-len, so only the query-buffer limit can stop it.
        assert c('CONFIG', 'SET', 'client-query-buffer-limit', '1mb') == '+OK'
        flood = socket.create_connection(('127.0.0.1', PEADB_PORT), timeout=5)
        got = b''
        try:
            flood.sendall(b'*1\r\n$100000000\r\n' + b'x' * (3 * 1024 * 1024))
            got = flood.recv(64)
        except OSError:
            pass
        flood.close()
        assert got == b'', f'expected the connection to close with no reply, got {got!r}'
        log = open(os.path.join(workdir, 'server.log'), errors='replace').read()
        assert 'exceeded query buffer limit' in log, log[-2000:]
        assert c('CONFIG', 'SET', 'client-query-buffer-limit', '1gb') == '+OK'
        assert c('PING') == '+PONG'
        c.close()
    finally:
        stop(proc)
    # dir and dbfilename are protected configs; enable-protected-configs yes allows them.
    sub = os.path.join(workdir, 'sub')
    os.makedirs(sub)
    proc = start(PEADB, PEADB_PORT, workdir, '--enable-protected-configs', 'yes')
    try:
        c = Conn(PEADB_PORT)
        assert c('CONFIG', 'SET', 'dir', sub, 'dbfilename', 'moved.rdb') == '+OK'
        assert c('CONFIG', 'GET', 'dir') == ['dir', os.path.realpath(sub)]
        assert c('SAVE') == '+OK'
        assert os.listdir(sub) == ['moved.rdb'], os.listdir(sub)
        assert c('CONFIG', 'SET', 'dir', '/no/such/dir') == (
            "-ERR CONFIG SET failed (possibly related to argument 'dir') - No such file or directory")
        c.close()
    finally:
        stop(proc)
    print('CONFIG SET: appendonly, maxclients, proto-max-bulk-len, client-query-buffer-limit, '
          'dir and dbfilename take effect passed')


def main():
    work = tempfile.mkdtemp(prefix='peadb-config-set-')
    try:
        os.makedirs(os.path.join(work, 'peadb'))
        os.makedirs(os.path.join(work, 'redis'))
        peadb_proc = start(PEADB, PEADB_PORT, os.path.join(work, 'peadb'))
        redis_proc = start(REDIS, REDIS_PORT, os.path.join(work, 'redis')) if REDIS else None
        try:
            peadb = Conn(PEADB_PORT)
            if redis_proc is not None:
                check_same_replies(peadb, Conn(REDIS_PORT))
            else:
                print('no pinned redis-server: skipping the comparison with Redis')
            check_peadb_only(peadb, os.path.join(work, 'peadb'))
        finally:
            stop(peadb_proc)
            if redis_proc is not None:
                stop(redis_proc)
        os.makedirs(os.path.join(work, 'runtime'))
        check_runtime_effects(os.path.join(work, 'runtime'))
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()

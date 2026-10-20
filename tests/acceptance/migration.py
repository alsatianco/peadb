#!/usr/bin/env python3
"""Bounded Redis 7.2.5 ↔ PeaDB migration release gate (see docs/compatibility.md)."""
from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import json
import os
import pathlib
import resource
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / 'scripts/redis'), str(ROOT / 'tests/integration')]
from _rdb_bridge import BridgeError, Connection  # noqa: E402
from _redis_path import redis_server_path  # noqa: E402
from test_dump_native_interop import populate, state  # noqa: E402
from test_rdb_redis_interop import LIB  # noqa: E402

DATABASES = (0, 1, 15)
DEADLINE = 4102444800000  # 2100: fixed absolute deadlines, never extended on transfer.
KINDS = ('string', 'list', 'set', 'hash', 'zset', 'stream')
SECOND_LIB = ("#!lua name=migrationlib\nredis.register_function{function_name='migration_get', "
              "callback=function(keys,args) return redis.call('GET',keys[1]) end, "
              "flags={'no-writes'}}")


def require(ok, message):
    if not ok:
        raise AssertionError(message)


def canonical(value):
    """Typed, length-framed logical data, independent of container encoding/order."""
    if isinstance(value, dict):
        return b'D' + canonical([[k, value[k]] for k in sorted(value)])
    if isinstance(value, (list, tuple)):
        return b'L' + str(len(value)).encode() + b':' + b''.join(canonical(v) for v in value)
    if isinstance(value, bytes):
        return b'B' + str(len(value)).encode() + b':' + value
    if isinstance(value, str):
        return b'S' + canonical(value.encode())
    if value is None:
        return b'N'
    if isinstance(value, int):
        return b'I' + str(value).encode() + b';'
    if isinstance(value, float):
        return b'F' + canonical(value.hex())
    raise TypeError(f'unsupported logical data: {type(value).__name__}')


def replay_state(value, expected=False, earliest=None):
    """Redis command-AOF semantics; native transfer always compares unmodified state."""
    value = copy.deepcopy(value)
    if value[0] != '+stream':
        return value
    stream = value[1]
    ids = {entry[0] for entry in stream[b'entries']}
    for group in stream[b'groups']:
        if expected:
            group[b'pending'] = [p for p in group[b'pending'] if p[0] in ids]
            group[b'pel-count'] = len(group[b'pending'])
        for consumer in group[b'consumers']:
            if expected:
                consumer[b'pending'] = [p for p in consumer[b'pending'] if p[0] in ids]
                consumer[b'pel-count'] = len(consumer[b'pending'])
            for field in (b'seen-time', b'active-time'):
                if consumer[field] >= 0:
                    if earliest is not None:
                        require(earliest <= consumer[field] <= time.time_ns() // 1000000,
                                'AOF consumer activity time was not recreated during replay')
                    consumer[field] = b'recreated-at-replay'
    return value


def function_state(c):
    libraries = []
    for raw in c.command('FUNCTION', 'LIST', 'WITHCODE'):
        library = dict(zip(raw[::2], raw[1::2]))
        functions = []
        for raw_function in library[b'functions']:
            function = dict(zip(raw_function[::2], raw_function[1::2]))
            function[b'flags'] = sorted(function[b'flags'])
            functions.append(function)
        library[b'functions'] = sorted(functions, key=lambda f: f[b'name'])
        libraries.append(library)
    return sorted(libraries, key=lambda lib: lib[b'library_name'])


def pipeline(c, commands):
    c.sock.sendall(b''.join(c.encode(args) for args in commands))
    return [c.reply(args[0]) for args in commands]


def fixture_client(c):
    # Reuse the existing Redis-first native-format fixtures and metadata oracle.
    def call(*args):
        result = c.command(*args)
        if args[0] in ('TYPE', 'FLUSHALL'):
            return '+' + result.decode()
        return result
    return call


def indices(keys, db):
    slot = DATABASES.index(db)
    return range(keys * slot // len(DATABASES), keys * (slot + 1) // len(DATABASES))


def bulk(i):
    kind = i % len(KINDS)
    key, value = f'bulk:{i}'.encode(), f'value:{i}:'.encode() + b'\x00\xff'
    commands = [
        ('SET', key, value), ('RPUSH', key, value), ('SADD', key, value),
        ('HSET', key, b'f\x00', value), ('ZADD', key, 1, value),
        ('XADD', key, '1-0', b'f\x00', value),
    ]
    reads = [('GET', key), ('LRANGE', key, 0, -1), ('SMEMBERS', key),
             ('HGETALL', key), ('ZRANGE', key, 0, -1, 'WITHSCORES'),
             ('XRANGE', key, '-', '+')]
    values = [value, [value], [value], [b'f\x00', value], [value, b'1'],
              [[b'1-0', [b'f\x00', value]]]]
    expires = DEADLINE + i if i % 17 == 0 else -1
    return key, KINDS[kind], commands[kind], reads[kind], values[kind], expires


def chunks(items, size=1000):
    for start in range(items.start, items.stop, size):
        yield range(start, min(start + size, items.stop))


def wait_for(predicate, seconds, description):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(.05)
    raise TimeoutError(description)


def info(c, section):
    return dict(line.split(b':', 1) for line in c.command('INFO', section).splitlines()
                if b':' in line and not line.startswith(b'#'))


class Gate:
    def __init__(self, args):
        self.args = args
        self.work = pathlib.Path(args.work_dir).resolve()
        self.work.mkdir(parents=True, exist_ok=False, mode=0o700)
        self.password = secrets.token_hex(32)
        self.cap_memory = sys.platform == 'linux' and not (
            args.smoke and os.environ.get('PEADB_TEST_SANITIZED') == '1')
        self.children = []
        self.configs = set()
        self.started = time.monotonic()
        self.report = {'status': 'running', 'bulk_keys': args.keys,
                       'databases': list(DATABASES), 'paths': [], 'phases': [],
                       'server_address_space_mib': args.memory_mib if self.cap_memory else None,
                       'budget_seconds': args.budget_seconds, 'peak_sampled_rss_mib': 0,
                       'platform': sys.platform,
                       'binaries_sha256': {name: hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()
                                           for name, path in (('redis', args.redis), ('peadb', args.peadb))}}

    def log(self, message):
        print(f'[{time.monotonic() - self.started:.1f}s] {message}', flush=True)

    def sample_memory(self):
        live = [str(p.pid) for p in self.children if p.poll() is None]
        if live:
            output = subprocess.run(['ps', '-o', 'rss=', '-p', ','.join(live)],
                                    capture_output=True, text=True).stdout
            rss = sum(int(line) for line in output.split()) / 1024
            self.report['peak_sampled_rss_mib'] = max(self.report['peak_sampled_rss_mib'], rss)

    @contextlib.contextmanager
    def phase(self, name):
        start = time.monotonic()
        self.log(name)
        try:
            yield
        finally:
            self.sample_memory()
            self.report['phases'].append({'name': name, 'seconds': round(time.monotonic() - start, 3)})
            self.write_report()

    def write_report(self):
        self.report['elapsed_seconds'] = round(time.monotonic() - self.started, 3)
        (self.work / 'report.json').write_text(json.dumps(self.report, indent=2) + '\n')

    def child_limit(self):
        # Redis/PeaDB are native executables. ASAN is deliberately outside this Release gate.
        resource.setrlimit(resource.RLIMIT_AS, (self.args.memory_mib * 1024**2,) * 2)

    def spawn(self, argv, log, env=None):
        proc = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, env=env,
                                start_new_session=True,
                                preexec_fn=self.child_limit if self.cap_memory else None)
        self.children.append(proc)
        return proc

    def stop(self, proc):
        if proc not in self.children:
            return
        # Every spawned command has its own process group, including bridge-owned Redis.
        if proc.poll() is None:
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()
        # A bridge killed by a deadline can leave its temporary Redis in this group.
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.children.remove(proc)

    @contextlib.contextmanager
    def server(self, binary, name, snapshot=None, aof=False, restart=False):
        directory = self.work / name
        directory.mkdir(exist_ok=restart)
        if snapshot:
            shutil.copyfile(snapshot, directory / 'dump.rdb')
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        # An unpredictable password plus owned-child liveness rejects port reservation races.
        config = directory / 'server.conf'
        self.configs.add(config)
        config.write_text(f'requirepass {self.password}\nmasterauth {self.password}\n')
        config.chmod(0o600)
        argv = [binary, str(config), '--port', str(port), '--bind', '127.0.0.1',
                '--dir', str(directory), '--save', '', '--appendonly', 'yes' if aof else 'no',
                '--appendfsync', 'everysec', '--aof-use-rdb-preamble', 'no',
                '--auto-aof-rewrite-percentage', '0', '--shutdown-timeout', '0',
                '--repl-diskless-sync', 'no', '--repl-diskless-sync-delay', '0',
                '--repl-timeout', str(self.args.timeout), '--loglevel', 'notice']
        c = None
        with (directory / ('restart.log' if restart else 'server.log')).open('wb') as log:
            proc = self.spawn(argv, log)
            start = time.monotonic()
            completed = False
            try:
                def ready():
                    nonlocal c
                    require(proc.poll() is None, f'{name}: child exited; see server log')
                    try:
                        c = Connection('127.0.0.1', port, .5, self.password)
                        c.sock.settimeout(self.args.timeout)
                        require(proc.poll() is None, f'{name}: child exited during readiness')
                        return True
                    except (OSError, BridgeError):
                        if c:
                            c.close()
                            c = None
                        return False
                wait_for(ready, self.args.timeout, f'{name}: authenticated readiness timeout')
                elapsed = time.monotonic() - start
                self.report.setdefault('server_start_seconds', {})[name + (':restart' if restart else '')] = round(elapsed, 3)
                self.log(f'{name}: authenticated load/start {elapsed:.3f}s')
                yield c, port, directory
                completed = True
            finally:
                if c:
                    c.close()
                self.stop(proc)
                config.unlink(missing_ok=True)
                self.configs.discard(config)
                if completed:
                    require(proc.returncode == 0, f'{name}: server did not stop cleanly')

    def seed(self, c):
        fixtures = {}
        rich = populate(fixture_client(c))
        c.expect_ok('SET', b'\x00binary\xff', b'\x00value\xff')
        c.expect_ok('SET', b'', b'empty key')
        c.expect_ok('SET', 'wide-expiry', 'wide', 'PXAT', 2**53 + 17)
        rich += [b'\x00binary\xff', b'', 'wide-expiry']
        dumps = [(k, c.command('DUMP', k), c.command('PEXPIRETIME', k)) for k in rich]
        for db in DATABASES:
            c.expect_ok('SELECT', db)
            if db:
                for key, dump, expires in dumps:
                    c.expect_ok('RESTORE', key, max(expires, 0), dump, 'ABSTTL')
            fixtures[db] = {k: state(fixture_client(c), k) for k in rich}
            for batch in chunks(indices(self.args.keys, db)):
                commands, expected = [], []
                for i in batch:
                    key, _, command, _, _, expires = bulk(i)
                    commands.append(command)
                    expected.append(b'1-0' if i % 6 == 5 else b'OK' if i % 6 == 0 else 1)
                    if expires != -1:
                        commands.append(('PEXPIREAT', key, expires))
                        expected.append(1)
                require(pipeline(c, commands) == expected, f'seed DB {db}: write refusal')
                if batch.stop % 100000 < 1000:
                    self.sample_memory()
                    self.log(f'seed DB {db}: through key {batch.stop}')
        require(c.command('FUNCTION', 'LOAD', LIB) == b'interoplib', 'first library load')
        require(c.command('FUNCTION', 'LOAD', SECOND_LIB) == b'migrationlib', 'second library load')
        self.report['key_counts'] = {str(db): len(indices(self.args.keys, db)) + len(fixtures[db])
                                     for db in DATABASES}
        return fixtures, function_state(c)

    def verify(self, c, fixtures, functions, label, replay_since=None):
        require(function_state(c) == functions, f'{label}: function libraries differ')
        require(c.command('FCALL_RO', 'interop_f', 0) == b'from-lib', f'{label}: FCALL_RO')
        digests = {}
        for db in range(c.databases()):
            c.expect_ok('SELECT', db)
            if db not in DATABASES:
                require(c.command('DBSIZE') == 0, f'{label}: unexpected data in DB {db}')
                continue
            count = len(indices(self.args.keys, db)) + len(fixtures[db])
            require(c.command('DBSIZE') == count, f'{label}: DB {db} key count differs')
            digest = hashlib.sha256()
            for batch in chunks(indices(self.args.keys, db)):
                commands, rows = [], []
                for i in batch:
                    key, kind, _, read, value, expires = bulk(i)
                    commands.extend((read, ('PEXPIRETIME', key)))
                    rows.append((key, kind, value, expires))
                replies = pipeline(c, commands)
                for j, (key, kind, value, expires) in enumerate(rows):
                    require(replies[2*j:2*j+2] == [value, expires],
                            f'{label}: DB {db} {key!r} value/absolute expiry differs')
                    digest.update(canonical([key, kind, replies[2*j], replies[2*j+1]]))
                if batch.stop % 100000 < 1000:
                    self.sample_memory()
                    self.log(f'{label} DB {db}: verified through key {batch.stop}')
            for key, expected in fixtures[db].items():
                actual = state(fixture_client(c), key)
                if replay_since is not None:
                    actual = replay_state(actual, earliest=replay_since)
                    expected = replay_state(expected, expected=True)
                require(actual == expected, f'{label}: DB {db} {key!r} metadata differs')
                digest.update(canonical([key, actual]))
            require(c.command('FCALL_RO', 'migration_get', 1, 'str') == fixtures[db]['str'][1],
                    f'{label}: DB {db} function execution differs')
            digests[str(db)] = digest.hexdigest()
        self.log(f'{label}: all keys, absolute expiries, stream metadata and functions verified')
        return digests

    def bridge(self, kind, port, path):
        argv = [sys.executable, str(ROOT / f'scripts/redis/{kind}_rdb_via_redis.py'),
                '--peadb-port', str(port), '--timeout', str(self.args.timeout),
                '--rdb' if kind == 'import' else '--out', str(path)]
        if kind == 'import':
            argv.append('--flush-target')
        scratch = self.work / 'bridge-scratch'
        scratch.mkdir(exist_ok=True, mode=0o700)
        env = dict(os.environ, PEADB_PASSWORD=self.password, REDIS_SERVER=self.args.redis,
                   TMPDIR=str(scratch))
        with (self.work / f'bridge-{kind}.log').open('wb') as log:
            proc = self.spawn(argv, log, env)
            try:
                code = proc.wait(timeout=self.args.budget_seconds)
                require(code == 0, f'bridge {kind}: exit {code}; see bridge log')
            finally:
                self.stop(proc)

    def direction(self, source, target, name):
        with self.server(source, name + '-source') as (src, sport, sdir):
            with self.phase(name + ': seed and verify source'):
                fixtures, functions = self.seed(src)
                baseline = self.verify(src, fixtures, functions, name + ': source')
                self.report.setdefault('snapshot_digests', {})[name] = baseline
                src.expect_ok('SAVE')
            with self.phase(name + ': native snapshot load and verification'):
                with self.server(target, name + '-snapshot', sdir / 'dump.rdb') as (dst, _, _):
                    require(self.verify(dst, fixtures, functions, name + ': snapshot') == baseline,
                            'snapshot logical digest differs')
                self.report['paths'].append(name + ':snapshot')
            with self.phase(name + ': full sync, live writes, promotion and AOF restart'):
                with self.server(target, name + '-replica', aof=True) as (dst, _, _):
                    dst.expect_ok('SET', 'must-be-replaced', 'stale')
                    require(dst.command('FUNCTION', 'LOAD', "#!lua name=stale\n"
                            "redis.register_function('stale',function() return 0 end)") == b'stale',
                            'stale library load')
                    sync_start = time.monotonic()
                    dst.expect_ok('REPLICAOF', '127.0.0.1', sport)
                    wait_for(lambda: info(dst, 'replication').get(b'master_link_status') == b'up',
                             self.args.timeout, 'full sync timeout')
                    self.report.setdefault('full_sync_seconds', {})[name] = round(time.monotonic() - sync_start, 3)
                    self.sample_memory()
                    for db in DATABASES:
                        dst.expect_ok('SELECT', db)
                        for key, expected in fixtures[db].items():
                            require(state(fixture_client(dst), key) == expected,
                                    f'{name}: full-sync DB {db} {key!r} metadata differs')
                    # Non-idempotent live effects after full sync, in a nonzero DB.
                    src.expect_ok('SELECT', 15)
                    require(src.command('RPUSH', 'list', b'live\x00') == 5, 'live RPUSH')
                    require(src.command('XADD', 'stream', '105-0', 'f', b'live\xff') == b'105-0',
                            'live XADD')
                    for k in ('list', 'stream'):
                        fixtures[15][k] = state(fixture_client(src), k)
                    offset = int(info(src, 'replication')[b'master_repl_offset'])
                    wait_for(lambda: int(info(dst, 'replication').get(b'slave_repl_offset', b'-1'))
                             >= offset, self.args.timeout, 'live replication catch-up timeout')
                    # Writers are now quiesced. Do not promote on link-up alone.
                    dst.expect_ok('REPLICAOF', 'NO', 'ONE')
                    wait_for(lambda: info(dst, 'persistence').get(b'aof_rewrite_in_progress') == b'0'
                             and info(dst, 'persistence').get(b'aof_last_bgrewrite_status') == b'ok'
                             and info(dst, 'persistence').get(b'aof_rewrite_scheduled') == b'0',
                             self.args.timeout, 'replacement AOF rewrite timeout')
                    promoted = self.verify(dst, fixtures, functions, name + ': promoted')
                replay_since = time.time_ns() // 1000000
                with self.server(target, name + '-replica', aof=True, restart=True) as (dst, _, _):
                    require(info(dst, 'replication').get(b'role') == b'master', 'restart role')
                    self.report.setdefault('aof_restart_digests', {})[name] = self.verify(
                        dst, fixtures, functions, name + ': AOF restart', replay_since=replay_since)
                self.report['paths'].append(name + ':replication-promotion-aof')
            # The bridge must copy the live changes too; take a fresh, quiescent backup.
            src.expect_ok('SAVE')
            with self.phase(name + ': offline bridge and verification'):
                if name == 'redis-to-peadb':
                    with self.server(target, name + '-bridge') as (dst, dport, _):
                        self.bridge('import', dport, sdir / 'dump.rdb')
                        bridged = self.verify(dst, fixtures, functions, name + ': bridge import')
                else:
                    output = self.work / 'bridge-export.rdb'
                    self.bridge('export', sport, output)
                    with self.server(target, name + '-bridge', output) as (dst, _, _):
                        bridged = self.verify(dst, fixtures, functions, name + ': bridge export')
                require(bridged == promoted, 'bridge logical digest differs')
                self.report['paths'].append(name + ':bridge')
                self.report.setdefault('logical_digests', {})[name] = bridged

    def check_aof_reference(self):
        with self.phase('Redis command-AOF metadata reference'):
            with self.server(self.args.redis, 'aof-reference') as (c, _, _):
                keys = populate(fixture_client(c))
                before = {k: state(fixture_client(c), k) for k in keys}
                c.expect_ok('CONFIG', 'SET', 'appendonly', 'yes')
                wait_for(lambda: info(c, 'persistence').get(b'aof_rewrite_in_progress') == b'0'
                         and info(c, 'persistence').get(b'aof_last_bgrewrite_status') == b'ok',
                         self.args.timeout, 'reference AOF rewrite timeout')
            replay_since = time.time_ns() // 1000000
            with self.server(self.args.redis, 'aof-reference', aof=True, restart=True) as (c, _, _):
                after = {k: state(fixture_client(c), k) for k in keys}
                require(before['stream'] != after['stream'], 'reference AOF limitation not exercised')
                for key in keys:
                    require(replay_state(after[key], earliest=replay_since) == replay_state(before[key], expected=True),
                            f'Redis command-AOF reference semantics changed: {key}')
            self.report['redis_command_aof_reference'] = 'passed'

    def run(self):
        def expired(signum, frame):
            if signum == signal.SIGALRM:
                raise TimeoutError('migration acceptance exceeded total execution budget')
            raise InterruptedError('migration acceptance interrupted')
        previous = {sig: signal.signal(sig, expired)
                    for sig in (signal.SIGALRM, signal.SIGTERM, signal.SIGINT)}
        signal.alarm(self.args.budget_seconds)
        try:
            self.log('SMOKE ONLY (not release-scale acceptance)' if self.args.smoke else
                     f'RELEASE SCALE: {self.args.keys} bulk keys plus rich fixtures per source')
            self.check_aof_reference()
            self.direction(self.args.redis, self.args.peadb, 'redis-to-peadb')
            self.direction(self.args.peadb, self.args.redis, 'peadb-to-redis')
            require(len(self.report['paths']) == 6, 'incomplete migration acceptance')
            self.report['status'] = 'smoke_passed' if self.args.smoke else 'accepted'
            self.log(self.report['status'])
        except BaseException as exc:
            self.report['status'] = 'failed'
            self.report['failure'] = type(exc).__name__
            raise
        finally:
            signal.alarm(0)
            for sig, handler in previous.items():
                signal.signal(sig, handler)
            try:
                for proc in reversed(self.children[:]):
                    self.stop(proc)
            finally:
                for config in self.configs:
                    config.unlink(missing_ok=True)
                self.write_report()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--peadb', default=str(ROOT / 'peadb-server'))
    parser.add_argument('--redis', default=redis_server_path())
    parser.add_argument('--work-dir', required=True, help='new directory; retains logs/data/report')
    parser.add_argument('--keys', type=int, default=2000100)
    parser.add_argument('--smoke', action='store_true', help='permit small run; never reports accepted')
    parser.add_argument('--budget-seconds', type=int, default=5400)
    parser.add_argument('--timeout', type=int, default=300, help='readiness/I/O/sync budget')
    parser.add_argument('--memory-mib', type=int, default=3072, help='Linux per-child address space cap')
    args = parser.parse_args()
    if args.keys < 6 or (not args.smoke and args.keys <= 2000000):
        parser.error('release acceptance requires >2M bulk keys; use --smoke for a small run')
    if min(args.budget_seconds, args.timeout, args.memory_mib) <= 0:
        parser.error('execution and memory budgets must be positive')
    if not args.redis or ' v=7.2.5 ' not in subprocess.check_output([args.redis, '--version'], text=True, timeout=10):
        parser.error('pinned Redis 7.2.5 is required (ensure_redis_reference.sh)')
    args.redis, args.peadb = str(pathlib.Path(args.redis).resolve()), str(pathlib.Path(args.peadb).resolve())
    Gate(args).run()


if __name__ == '__main__':
    main()

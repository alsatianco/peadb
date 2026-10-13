#!/usr/bin/env python3
"""MIGRATE works like Redis 7.2.5's migrateCommand() (compat/delta.md item 19).

The same checks run on peadb-server and on the pinned redis-server (Redis side first, only when it is Redis 7.2.x); each source migrates into a target of its own engine:

- propagation: a moved key reaches the AOF and the replicas as `DEL` of the keys the target took (also when the target refused some of them, and inside MULTI/EXEC), a `COPY` or a refused key sends nothing; a real replica and an AOF reload lose the moved keys and keep the copied ones (PeaDB sent MIGRATE itself, so its replica ran it against the target, failed, and kept the key).
- each moved key sends a `del` event and counts as one change (`rdb_changes_since_last_save`).
- replies: `Target instance replied with error: <error>` for a target's error (RESTORE, AUTH, SELECT), Redis's argument errors, `NOKEY`, a timeout of 0 or less is 1000 ms, the `-IOERR` texts for a port that cannot be reached and for a target that answers too late.
- the multi-key `KEYS` form, `AUTH` and `AUTH2`, `REPLACE`, the TTL, a value bigger than 64 KB.
- `COMMAND GETKEYS` and `GETKEYSANDFLAGS` of MIGRATE, MONITOR shows its passwords as `(redacted)`, and a key it finds counts as an access (`OBJECT IDLETIME`).
- the connection to a target is kept (INFO `migrate_cached_sockets`, the target sees one connection and one SELECT per database), and a kept connection that the target closed is opened again without an error.
- `RESTORE-ASKING` works like `RESTORE` and is propagated as it came.

Stream keys migrate with other keys and their deletion reaches replicas and AOF replay.

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
# source, its replica, the target, and a port nothing listens on
PORTS = {'peadb': (6720, 6721, 6722, 6723), 'redis': (6725, 6726, 6727, 6728)}
HOST = '127.0.0.1'
BUSY = '-ERR Target instance replied with error: BUSYKEY Target key name already exists.'
SYNTAX = '-ERR syntax error'
NOT_INT = '-ERR value is not an integer or out of range'


def encode(*args):
    out = f'*{len(args)}\r\n'.encode()
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode('latin-1')
        out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
    return out


class Conn:
    """A RESP2 client; bulk strings come back as latin-1 text, so binary DUMP payloads survive."""

    def __init__(self, port):
        self.sock = socket.create_connection((HOST, port), timeout=10)
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

    def send(self, *args):
        self.sock.sendall(encode(*args))

    def __call__(self, *args):
        self.send(*args)
        return self.read()


class FakeReplica:
    """A replica that sends PSYNC ? -1, skips the RDB, then reads the commands its master sends."""

    def __init__(self, port):
        self.sock = socket.create_connection((HOST, port), timeout=10)
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
        """The commands before `last` (a marker write), without PING, GETACK and SELECT."""
        out = []
        while True:
            cmd = self.command()
            if cmd == ['PING'] or cmd[:2] == ['REPLCONF', 'GETACK'] or cmd[0] == 'SELECT':
                continue
            if cmd == last:
                return out
            out.append(cmd)

    def close(self):
        self.f.close()
        self.sock.close()


class Events:
    """Keyevent notifications of database 0, read up to a marker SET."""

    def __init__(self, port):
        self.c = Conn(port)
        assert self.c('PSUBSCRIBE', '__keyevent@0__:*')[0] == 'psubscribe'

    def until_marker(self, marker):
        out = []
        while True:
            msg = self.c.read()
            assert msg[0] == 'pmessage', msg
            event = msg[2].split(':', 1)[1]
            if (event, msg[3]) == ('set', marker):
                return out
            out.append((event, msg[3]))

    def close(self):
        self.c.close()


def start(binary, port, workdir, *extra):
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    log = open(log_path, 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', HOST, '--dir', workdir,
                             '--loglevel', 'notice', '--save', '', '--shutdown-timeout', '0', *extra],
                            stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'{binary} exited: {open(log_path).read()}')
        try:
            socket.create_connection((HOST, port), timeout=0.2).close()
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


def info(c, field):
    for line in c('INFO', 'all').splitlines():
        if line.startswith(field + ':'):
            return line.split(':', 1)[1]
    raise KeyError(field)


def changes(c):
    return int(info(c, 'rdb_changes_since_last_save'))


def select_calls(c):
    """How many SELECTs the target ran (its commandstats line, 0 before the first)."""
    for line in c('INFO', 'commandstats').splitlines():
        if line.startswith('cmdstat_select:'):
            return int(line.split('calls=', 1)[1].split(',', 1)[0])
    return 0


def wait_for(cond, what, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.02)
    raise AssertionError(f'timed out waiting for {what}')


class Marker:
    """A write after each step, so the fake replica and the events can be read up to it."""

    def __init__(self, src, fake, events):
        self.src, self.fake, self.events, self.n = src, fake, events, 0

    def step(self):
        """(commands propagated, events sent) since the last step."""
        self.n += 1
        key = f'marker{self.n}'
        assert self.src('SET', key, 'x') == '+OK'
        return self.fake.commands_until(['SET', key, 'x']), self.events.until_marker(key)


def migrate(src, tport, key, db=0, timeout=5000, *opts):
    return src('MIGRATE', HOST, tport, key, db, timeout, *opts)


def check_propagation(name, src, tgt, tport, m):
    """What a MIGRATE sends to the replicas and the AOF, its events and its change count."""
    assert src('SET', 'k1', 'v1') == '+OK' and src('EXPIRE', 'k1', 10) == 1
    m.step()
    before = changes(src)
    assert migrate(src, tport, 'k1') == '+OK', name
    assert changes(src) - before == 1, name
    assert m.step() == ([['DEL', 'k1']], [('del', 'k1')]), name
    assert 7 <= tgt('TTL', 'k1') <= 10, name
    # COPY moves nothing here, so nothing is propagated or counted.
    assert src('RPUSH', 'copied', 'a', 'b') == 2
    m.step()
    before = changes(src)
    assert migrate(src, tport, 'copied', 0, 5000, 'COPY') == '+OK', name
    assert changes(src) == before, name
    assert m.step() == ([], []), name
    assert tgt('LRANGE', 'copied', 0, -1) == ['a', 'b'], name
    # A key the target has already: the target's error, nothing moved or sent.
    assert migrate(src, tport, 'copied') == BUSY, name
    assert m.step() == ([], []), name
    assert src('EXISTS', 'copied') == 1, name
    # KEYS with keys the target refuses: the error, and the keys it took are deleted and sent.
    assert src('MSET', 'a', 1, 'b', 2, 'c', 3, 'd', 4, 'e', 5) == '+OK'
    assert tgt('MSET', 'b', '_', 'd', '_') == '+OK'
    m.step()
    before = changes(src)
    assert migrate(src, tport, '', 0, 5000, 'KEYS', 'a', 'nokey', 'b', 'c', 'd', 'e') == BUSY, name
    assert changes(src) - before == 3, name
    assert m.step() == ([['DEL', 'a', 'c', 'e']], [('del', 'a'), ('del', 'c'), ('del', 'e')]), name
    assert sorted(src('KEYS', '[a-e]')) == ['b', 'd'], name
    assert (tgt('GET', 'a'), tgt('GET', 'b'), tgt('GET', 'e')) == ('1', '_', '5'), name
    # REPLACE overwrites them; a transaction sends its DELs inside MULTI/EXEC.
    assert src('SET', 'f', 6) == '+OK'
    m.step()
    assert src('MULTI') == '+OK'
    assert src('MIGRATE', HOST, tport, '', 0, 5000, 'REPLACE', 'KEYS', 'b', 'd') == '+QUEUED'
    assert src('MIGRATE', HOST, tport, 'f', 0, 5000) == '+QUEUED'
    assert src('EXEC') == ['+OK', '+OK'], name
    assert m.step() == ([['MULTI'], ['DEL', 'b', 'd'], ['DEL', 'f'], ['EXEC']],
                        [('del', 'b'), ('del', 'd'), ('del', 'f')]), name
    assert (tgt('GET', 'b'), tgt('GET', 'd'), tgt('GET', 'f')) == ('2', '4', '6'), name
    # Every key missing: NOKEY, nothing sent.
    assert migrate(src, tport, '', 0, 5000, 'KEYS', 'nokey1', 'nokey2', 'nokey1') == '+NOKEY', name
    assert migrate(src, tport, 'nokey3') == '+NOKEY', name
    assert m.step() == ([], []), name
    # A value bigger than the 64 KB write chunks.
    big = 'x' * 200000
    assert src('SET', 'big', big) == '+OK' and src('RPUSH', 'biglist', *range(30000)) == 30000
    m.step()
    assert migrate(src, tport, '', 0, 10000, 'KEYS', 'big', 'biglist') == '+OK', name
    assert m.step()[0] == [['DEL', 'big', 'biglist']], name
    assert tgt('STRLEN', 'big') == 200000 and tgt('LLEN', 'biglist') == 30000, name
    # RESTORE-ASKING is RESTORE (no cluster mode here), propagated as it came.
    assert src('SET', 'ra', 'v') == '+OK'
    payload = src('DUMP', 'ra')
    m.step()
    assert src('RESTORE-ASKING', 'ra2', 0, payload) == '+OK', name
    assert src('RESTORE-ASKING', 'ra2', 0, payload) == '-BUSYKEY Target key name already exists.', name
    assert m.step() == ([['RESTORE-ASKING', 'ra2', '0', payload]], [('restore', 'ra2')]), name
    assert src('GET', 'ra2') == 'v', name


def check_replies(name, src, sport, tgt, tport, closed_port, seen):
    """Argument errors, the target's errors, AUTH, the I/O errors, and the connection cache."""
    assert src('SET', 'r', 'v') == '+OK'
    for args, reply in [
        (('MIGRATE', HOST, tport, 'r', 0), "-ERR wrong number of arguments for 'migrate' command"),
        (('MIGRATE', HOST, tport, 'r', 0, 5000, 'AUTH'), SYNTAX),
        (('MIGRATE', HOST, tport, 'r', 0, 5000, 'AUTH2', 'user'), SYNTAX),
        (('MIGRATE', HOST, tport, 'r', 0, 5000, 'NOPE'), SYNTAX),
        (('MIGRATE', HOST, tport, 'r', 0, 5000, 'KEYS', 'r'),
         '-ERR When using MIGRATE KEYS option, the key argument must be set to the empty string'),
        (('MIGRATE', HOST, tport, 'r', 0, 'abc'), NOT_INT),
        (('MIGRATE', HOST, tport, 'r', 'x', 5000), NOT_INT),
        (('MIGRATE', HOST, tport, 'r', '01', 5000), NOT_INT),
        (('MIGRATE', HOST, 'abc', 'r', 0, 5000), '-IOERR error or timeout connecting to the client'),
    ]:
        assert src(*args) == reply, (name, args, src(*args))
    # A database the target does not have: its SELECT error. Redis sends the RESTOREs in the same
    # write, so its target restores the key into the connection's database anyway (here 0); PeaDB
    # sends them only after a good SELECT (and AUTH), with the same reply.
    assert src('MIGRATE', HOST, tport, 'r', 99, 5000) == (
        '-ERR Target instance replied with error: ERR DB index is out of range'), name
    assert src('EXISTS', 'r') == 1, name
    assert tgt('EXISTS', 'r') == (1 if name == 'redis' else 0), name
    # A port nothing listens on: the text depends on whether connect() fails at once, so it is
    # compared with what Redis answered on this machine.
    reply = migrate(src, closed_port, 'r', 0, 500)
    assert reply.startswith('-IOERR error or timeout '), (name, reply)
    if 'closed' in seen:
        assert reply == seen['closed'], (name, reply, seen['closed'])
    seen['closed'] = reply
    # A timeout of 0 or less means 1000 ms; a negative database goes to the target as it is.
    assert src('SET', 'r2', 'v') == '+OK' and src('SET', 'neg', 'v') == '+OK'
    assert migrate(src, tport, 'r2', 0, -5) == '+OK', name
    assert migrate(src, tport, 'neg', -1, 0) == (
        '-ERR Target instance replied with error: ERR DB index is out of range'), name
    assert src('EXISTS', 'neg') == 1 and tgt('EXISTS', 'neg') == (1 if name == 'redis' else 0), name
    # The connection to the target is kept: one connection, one SELECT per change of database, a
    # new SELECT after the target answered an error.
    tgt('CONFIG', 'RESETSTAT')
    assert src('MSET', 's1', 1, 's2', 2, 's3', 3, 's4', 4) == '+OK'
    assert migrate(src, tport, 's1', 5) == '+OK' and migrate(src, tport, 's2', 5) == '+OK', name
    assert select_calls(tgt) == 1, name
    assert migrate(src, tport, 's3', 6) == '+OK', name
    assert select_calls(tgt) == 2, name
    assert tgt('SELECT', 6) == '+OK' and tgt('SET', 's4', 'busy') == '+OK' and tgt('SELECT', 0) == '+OK'
    assert migrate(src, tport, 's4', 6) == BUSY, name
    assert src('DEL', 's4') == 1 and src('SET', 's4', 4) == '+OK'
    assert migrate(src, tport, 's4', 6, 5000, 'REPLACE') == '+OK', name
    assert select_calls(tgt) == 5, name  # 2, the test's own 2, and 1 after the error
    assert info(src, 'migrate_cached_sockets') == '1', name
    clients = [ln for ln in tgt('CLIENT', 'LIST').splitlines() if 'cmd=restore' in ln]
    assert len(clients) == 1, (name, clients)
    # The target closes the kept connection: the next MIGRATE opens a new one, no error.
    client_id = clients[0].split('id=', 1)[1].split(' ', 1)[0]
    assert tgt('CLIENT', 'KILL', 'ID', client_id) == 1
    assert src('SET', 's5', 5) == '+OK'
    assert migrate(src, tport, 's5', 6) == '+OK', name
    assert info(src, 'migrate_cached_sockets') == '1', name
    # AUTH and AUTH2: each MIGRATE sends it again on the kept connection; MONITOR shows the
    # passwords as (redacted).
    mon = Conn(sport)
    assert mon('MONITOR') == '+OK'
    assert tgt('CONFIG', 'SET', 'requirepass', 'foobar') == '+OK'
    assert src('RPUSH', 'al', 'a', 'b') == 2
    assert migrate(src, tport, 'al', 0, 5000, 'AUTH', 'foobar') == '+OK', name
    assert src('RPUSH', 'al', 'c') == 1
    assert migrate(src, tport, 'al', 0, 5000, 'AUTH', 'wrong') == (
        '-ERR Target instance replied with error: WRONGPASS invalid username-password pair or user is disabled.'), name
    assert migrate(src, tport, 'al', 0, 5000, 'AUTH2', 'default', 'foobar', 'REPLACE') == '+OK', name
    assert src('EXISTS', 'al') == 0, name
    assert tgt('AUTH', 'foobar') == '+OK' and tgt('LRANGE', 'al', 0, -1) == ['c'], name
    # A MIGRATE without AUTH right after: Redis sends it on the connection the AUTH logged in, so it
    # writes to the target without its password; PeaDB connects again and the target refuses it.
    assert src('SET', 'noauth', 'v') == '+OK'
    reply = migrate(src, tport, 'noauth')
    if name == 'redis':
        assert reply == '+OK', (name, reply)
    else:
        assert reply == '-ERR Target instance replied with error: NOAUTH Authentication required.', reply
        assert src('EXISTS', 'noauth') == 1 and tgt('EXISTS', 'noauth') == 0
    assert tgt('CONFIG', 'SET', 'requirepass', '') == '+OK'
    assert src('SET', 'monmarker', 1) == '+OK'
    shown = []
    while True:
        line = mon.f.readline().decode()
        if 'monmarker' in line:
            break
        low = line.lower()
        if '"migrate"' in low and ('"auth"' in low or '"auth2"' in low):
            shown.append(line)
    assert len(shown) == 3, (name, shown)
    for line in shown:
        assert '"(redacted)"' in line and 'foobar' not in line and '"wrong"' not in line, (name, line)
    mon.close()
    # A target that answers too late.
    sleeper = Conn(tport)
    sleeper.send('DEBUG', 'SLEEP', '1')
    time.sleep(0.1)
    assert src('SET', 't', 'v') == '+OK'
    late = migrate(src, tport, 't', 0, 300)
    assert late == '-IOERR error or timeout reading to target instance', (name, late)
    assert sleeper.read() == '+OK', name
    sleeper.close()
    assert src('EXISTS', 't') == 1, name


def check_introspection(name, src, tport):
    """COMMAND GETKEYS like Redis's key specs and migrateGetKeys(); a key found counts as an access."""
    for args, reply in [
        (('MIGRATE', 'h', 1, 'k', 0, 1000), ['k']),
        (('MIGRATE', 'h', 1, 'k', 0, 1000, 'COPY', 'REPLACE', 'AUTH', 'pw'), ['k']),
        (('MIGRATE', 'h', 1, 'k', 0, 1000, 'AUTH'), ['k']),
        (('MIGRATE', 'h', 1, '', 0, 1000, 'AUTH2', 'u', 'p', 'KEYS', 'a', 'b', 'c'), ['a', 'b', 'c']),
        (('MIGRATE', 'h', 1, '', 0, 1000, 'AUTH', 'keys', 'KEYS', 'a'), ['a']),
        (('MIGRATE', 'h', 1, '', 0, 1000, 'KEYS'), ['']),
        (('MIGRATE', 'h', 1, 'nonempty', 0, 1000, 'KEYS', 'a', 'b'),
         '-ERR Invalid arguments specified for command'),
    ]:
        assert src('COMMAND', 'GETKEYS', *args) == reply, (name, args, src('COMMAND', 'GETKEYS', *args))
    flags = ['+RW', '+access', '+delete']
    assert src('COMMAND', 'GETKEYSANDFLAGS', 'MIGRATE', 'h', 1, '', 0, 1000, 'KEYS', 'a', 'b') == [
        ['a', flags], ['b', flags]], name
    # A key MIGRATE finds counts as an access, also with COPY.
    assert src('SET', 'idle', 'v') == '+OK'
    assert src('RESTORE', 'idle2', 0, src('DUMP', 'idle'), 'IDLETIME', 1000) == '+OK'
    assert src('OBJECT', 'IDLETIME', 'idle2') >= 1000, name
    assert migrate(src, tport, 'idle2', 0, 5000, 'COPY') == '+OK', name
    assert src('OBJECT', 'IDLETIME', 'idle2') < 10, name


def check_engine(name, binary, work, seen):
    sport, rport, tport, closed_port = PORTS[name]
    sdir, rdir, tdir = (os.path.join(work, name, d) for d in ('src', 'replica', 'target'))
    src_proc = start(binary, sport, sdir, '--appendonly', 'yes', '--notify-keyspace-events', 'EA')
    procs = [src_proc, start(binary, rport, rdir),
             start(binary, tport, tdir, '--enable-debug-command', 'yes')]
    try:
        src, replica, tgt = Conn(sport), Conn(rport), Conn(tport)
        assert replica('REPLICAOF', HOST, sport) == '+OK'
        wait_for(lambda: info(replica, 'master_link_status') == 'up', 'the replica link')
        fake = FakeReplica(sport)
        events = Events(sport)
        check_propagation(name, src, tgt, tport, Marker(src, fake, events))
        check_replies(name, src, sport, tgt, tport, closed_port, seen)
        check_introspection(name, src, tport)
        assert src('XADD', 'st', '1-1', 'f', 'v') == '1-1' and src('SET', 'sk', 'v') == '+OK'
        assert migrate(src, tport, '', 0, 5000, 'KEYS', 'sk', 'st') == '+OK'
        assert src('EXISTS', 'sk', 'st') == 0 and tgt('EXISTS', 'sk', 'st') == 2
        assert tgt('XRANGE', 'st', '-', '+') == [['1-1', ['f', 'v']]]
        # The real replica and an AOF reload: the moved keys are gone, the copied ones stay.
        assert src('SET', 'last', 'x') == '+OK'
        wait_for(lambda: replica('GET', 'last') == 'x', 'the replica to catch up')
        moved = ['k1', 'a', 'c', 'e', 'b', 'd', 'f', 'big', 'biglist', 'r2', 's1', 's2', 's3', 's4',
                 's5', 'al', 'sk', 'st']
        assert replica('EXISTS', *moved) == 0, name
        assert replica('LRANGE', 'copied', 0, -1) == ['a', 'b'] and replica('GET', 't') == 'v', name
        assert replica('GET', 'r') == 'v' and replica('GET', 'neg') == 'v', name
        fake.close()
        events.close()
        src.close()
        stop(src_proc)
        procs[0] = start(binary, sport, sdir, '--appendonly', 'yes')
        src = Conn(sport)
        assert src('EXISTS', *moved) == 0, name
        assert src('LRANGE', 'copied', 0, -1) == ['a', 'b'] and src('GET', 't') == 'v', name
        for c in (src, replica, tgt):
            c.close()
    finally:
        for p in procs:
            stop(p)
    print(f'{name}: ok')


def main():
    work = tempfile.mkdtemp(prefix='peadb-migrate-')
    seen = {}
    try:
        if REDIS:
            check_engine('redis', REDIS, work, seen)
        else:
            print('no pinned Redis 7.2.x: skipping the Redis side')
        check_engine('peadb', PEADB, work, seen)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()

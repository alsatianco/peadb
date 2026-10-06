#!/usr/bin/env python3
"""CLIENT PAUSE and CLIENT UNPAUSE work like Redis 7.2.5 (compat/delta.md item 26).

The same checks run on peadb-server and on the pinned redis-server, and the replies and the effects must be the same. CLIENT PAUSE checks its arguments with Redis's errors; it holds the commands it pauses (writes, scripts that may write, PUBLISH, PFCOUNT, a transaction with a write; everything for ALL) and runs them in arrival order when the pause ends, keeps pipelined commands behind them, keeps the longer end time, does not expire keys while it lasts (an expired key stays counted and expires afterwards), starts only after the transaction that sent it, and also holds the writes a replica gets from its master. Two script fixes that pausing depends on are checked too: read-only scripts may not call a command that writes or may replicate, and a script with a shebang loads with SCRIPT LOAD and keeps its flags under EVALSHA. PeaDB's own answer is checked where it differs on purpose: CLIENT UNBLOCK does not release a paused client (Redis 7.2.5 crashes there). Without a pinned redis-server, only the PeaDB side runs.
"""
import hashlib
import os
import pathlib
import select
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
# Each engine gets a server and a second one for the replica test.
PORTS = {'peadb': (6576, 6578), 'redis': (6577, 6579)}

# Redis goes on with ' script: <sha>, on @user_script:<line>.'; PeaDB's script errors lack that
# suffix (compat/delta.md item 14), so only the text before it is compared.
RO_SCRIPT_WRITE = '-ERR Write commands are not allowed from read-only scripts.'
SHEBANG_SCRIPT = '#!lua flags=no-writes\nreturn 2'
SHEBANG_SHA = hashlib.sha1(SHEBANG_SCRIPT.encode()).hexdigest()


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=5)
        self.buf = b''

    def close(self):
        self.sock.close()

    def _fill(self):
        data = self.sock.recv(65536)
        if not data:
            raise ConnectionError('connection closed')
        self.buf += data

    def _line(self):
        while b'\r\n' not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b'\r\n', 1)
        return line

    def _exact(self, n):
        while len(self.buf) < n + 2:
            self._fill()
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def read(self):
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind in (b'+', b'-'):
            return (kind + rest).decode()
        if kind == b':':
            return int(rest)
        if kind == b'$':
            n = int(rest)
            return None if n < 0 else self._exact(n).decode()
        if kind == b'*':
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise ValueError(f'unexpected reply {line!r}')

    def send(self, *args):
        out = f'*{len(args)}\r\n'.encode()
        for a in args:
            b = a.encode() if isinstance(a, str) else a
            out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
        self.sock.sendall(out)

    def send_many(self, *commands):
        """Pipeline several commands in one write."""
        out = b''
        for args in commands:
            out += f'*{len(args)}\r\n'.encode()
            for a in args:
                b = a.encode()
                out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
        self.sock.sendall(out)

    def __call__(self, *args):
        self.send(*args)
        return self.read()

    def has_reply(self, wait):
        if self.buf:
            return True
        ready, _, _ = select.select([self.sock], [], [], wait)
        return bool(ready)


def start(binary, port, workdir, *extra):
    os.makedirs(workdir, exist_ok=True)
    log = open(os.path.join(workdir, 'server.log'), 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--save', '',
                             '--dir', workdir, '--loglevel', 'warning', *extra],
                            stdout=log, stderr=subprocess.STDOUT)
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


def info_field(conn, field):
    for line in conn('INFO', 'everything').splitlines():
        if line.startswith(field + ':'):
            return line.split(':', 1)[1]
    raise AssertionError(f'INFO has no {field}')


def wait_for(cond, what, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.02)
    raise AssertionError(f'timed out waiting for {what}')


def wait_blocked(conn, n):
    wait_for(lambda: info_field(conn, 'blocked_clients') == str(n), f'blocked_clients:{n}')


def assert_held(conn, what):
    assert not conn.has_reply(0.2), f'{what} was not held by the pause'


SAME_REPLY = [
    (('CLIENT', 'PAUSE'), "-ERR wrong number of arguments for 'client|pause' command"),
    (('CLIENT', 'PAUSE', '10', 'WRITE', 'x'),
     "-ERR unknown subcommand or wrong number of arguments for 'PAUSE'. Try CLIENT HELP."),
    (('CLIENT', 'PAUSE', 'abc'), '-ERR timeout is not an integer or out of range'),
    (('CLIENT', 'PAUSE', '1.5'), '-ERR timeout is not an integer or out of range'),
    (('CLIENT', 'PAUSE', '-1'), '-ERR timeout is negative'),
    (('CLIENT', 'PAUSE', '-0'), '-ERR timeout is not an integer or out of range'),
    (('CLIENT', 'PAUSE', '+5'), '-ERR timeout is not an integer or out of range'),
    (('CLIENT', 'PAUSE', '9223372036854775807'), '-ERR timeout is out of range'),
    (('CLIENT', 'PAUSE', '10', 'later'), '-ERR CLIENT PAUSE mode must be WRITE or ALL'),
    (('CLIENT', 'PAUSE', 'abc', 'later'), '-ERR CLIENT PAUSE mode must be WRITE or ALL'),
    (('CLIENT', 'PAUSE', '0', 'write'), '+OK'),
    (('CLIENT', 'UNPAUSE', 'x'), "-ERR wrong number of arguments for 'client|unpause' command"),
    (('CLIENT', 'UNPAUSE'), '+OK'),
    (('SCRIPT', 'LOAD', SHEBANG_SCRIPT), SHEBANG_SHA),
    (('EVALSHA', SHEBANG_SHA, '0'), 2),
    (('SCRIPT', 'LOAD', '#!pyth\nreturn 1'), '-ERR Unexpected engine in script shebang: #!pyth'),
    (('SCRIPT', 'LOAD', '#!lua'), '-ERR Invalid script shebang'),
    (('SCRIPT', 'LOAD', '#!lua flags=no-writes,bad\nreturn 1'),
     '-ERR Unexpected flag in script shebang: bad'),
    (('SCRIPT', 'LOAD', '#!lua name=x\nreturn 1'), '-ERR Unknown lua shebang option: name=x'),
    (('EVAL', '#!lua flags=no-writes,allow-stale\nreturn 5', '0'), 5),
    (('EVAL_RO', '#!lua\nreturn 1', '0'),
     '-ERR Can not execute a script with write flag using *_ro command.'),
]

# Read-only scripts may not call a command that writes or may replicate (Redis's
# scriptVerifyWriteCommandAllow).
RO_REFUSED = [
    ('EVAL_RO', "return redis.call('publish','ch','msg')", '0'),
    ('EVAL_RO', "return redis.call('hincrby','h','f',1)", '0'),
    ('EVAL_RO', "return redis.call('pfcount','hll')", '0'),
    ('EVAL', "#!lua flags=no-writes\nreturn redis.call('lmove','a','b','left','left')", '0'),
    ('EVAL', "#!lua flags=no-writes\nreturn redis.call('xgroup','create','s','g','$','MKSTREAM')", '0'),
]


def check_replies(c):
    for args, expected in SAME_REPLY:
        got = c(*args)
        assert got == expected, (args, got, expected)
    for args in RO_REFUSED:
        got = c(*args)
        assert isinstance(got, str) and got.startswith(RO_SCRIPT_WRITE), (args, got)
    assert c('EXISTS', 'h', 'b', 's') == 0
    # EVALSHA_RO is read-only too.
    sha = c('SCRIPT', 'LOAD', "return redis.call('set','ro-sha','1')")
    got = c('EVALSHA_RO', sha, '0')
    assert isinstance(got, str) and got.startswith(RO_SCRIPT_WRITE), got
    assert c('EXISTS', 'ro-sha') == 0
    # "-0" is not an integer for HINCRBY either (Redis's string2ll, also used by CLIENT PAUSE).
    assert c('HSET', 'neg0h', 'f', '-0') == 1
    assert c('HINCRBY', 'neg0h', 'f', '1') == '-ERR hash value is not an integer'


def check_write_pause(port):
    a, b = Conn(port), Conn(port)
    assert a('CLIENT', 'PAUSE', '100000', 'WRITE') == '+OK'
    b.send('SET', 'k', 'v')
    assert_held(b, 'SET')
    assert a('GET', 'k') is None
    assert a('PING') == '+PONG'
    wait_blocked(a, 1)
    flags = [ln for ln in a('CLIENT', 'LIST').splitlines() if ' cmd=client|list ' not in ln]
    assert any('flags=b' in ln for ln in flags), flags
    assert a('CLIENT', 'UNPAUSE') == '+OK'
    assert b.read() == '+OK'
    assert a('GET', 'k') == 'v'
    wait_blocked(a, 0)

    # Pipelined commands wait behind the held one and keep their order.
    assert a('CLIENT', 'PAUSE', '100000', 'WRITE') == '+OK'
    b.send_many(('SET', 'p', '1'), ('GET', 'p'), ('PING',))
    assert_held(b, 'pipelined SET')
    assert a('CLIENT', 'UNPAUSE') == '+OK'
    assert [b.read(), b.read(), b.read()] == ['+OK', '1', '+PONG']

    # Held clients run in the order they arrived.
    clients = [Conn(port) for _ in range(3)]
    assert a('CLIENT', 'PAUSE', '100000', 'WRITE') == '+OK'
    for i, c in enumerate(clients):
        c.send('INCR', 'n')
        wait_blocked(a, i + 1)
    assert a('CLIENT', 'UNPAUSE') == '+OK'
    assert [c.read() for c in clients] == [1, 2, 3]

    # A held client that disconnects is dropped and its command never runs.
    assert a('CLIENT', 'PAUSE', '100000', 'WRITE') == '+OK'
    b.send('SET', 'gone', '1')
    wait_blocked(a, 1)
    b.close()
    wait_blocked(a, 0)
    assert a('CLIENT', 'UNPAUSE') == '+OK'
    assert a('GET', 'gone') is None

    # A later, shorter pause keeps the longer end time.
    assert a('CLIENT', 'PAUSE', '100000', 'WRITE') == '+OK'
    assert a('CLIENT', 'PAUSE', '10', 'WRITE') == '+OK'
    time.sleep(0.1)
    d = Conn(port)
    d.send('SET', 'longer', '1')
    assert_held(d, 'SET after a shorter pause')
    assert a('CLIENT', 'UNPAUSE') == '+OK'
    assert d.read() == '+OK'
    for c in clients + [a, d]:
        c.close()


def check_blocking_order(port):
    # Blocking commands held by the pause block in arrival order when it ends, so the first one
    # sent is served first. c2 connects first, so serving by connection order would pick it.
    a, c2, c1 = Conn(port), Conn(port), Conn(port)
    a('DEL', 'bq')
    assert a('CLIENT', 'PAUSE', '100000', 'WRITE') == '+OK'
    c1.send('BLPOP', 'bq', '0')
    wait_blocked(a, 1)
    c2.send('BLPOP', 'bq', '0')
    wait_blocked(a, 2)
    assert a('CLIENT', 'UNPAUSE') == '+OK'
    wait_blocked(a, 2)  # both now block on the empty list
    assert a('RPUSH', 'bq', 'first') == 1
    assert c1.read() == ['bq', 'first']
    assert a('RPUSH', 'bq', 'second') == 1
    assert c2.read() == ['bq', 'second']
    for c in (a, c1, c2):
        c.close()


def check_all_pause(port):
    a, b = Conn(port), Conn(port)
    start_t = time.time()
    assert a('CLIENT', 'PAUSE', '400', 'ALL') == '+OK'
    assert b('PING') == '+PONG'
    assert time.time() - start_t >= 0.35, 'PING ran during CLIENT PAUSE ALL'
    # The pause has ended; nothing is held now.
    assert b('SET', 'after-all', '1') == '+OK'
    a.close()
    b.close()


def check_transactions(port):
    a, b, c = Conn(port), Conn(port), Conn(port)
    # A transaction with a queued write is held at EXEC.
    assert b('MULTI') == '+OK'
    assert b('SET', 'mk', '1') == '+QUEUED'
    assert a('CLIENT', 'PAUSE', '100000', 'WRITE') == '+OK'
    b.send('EXEC')
    assert_held(b, 'EXEC with a write')
    # A read-only transaction runs.
    assert c('MULTI') == '+OK'
    assert c('GET', 'mk') == '+QUEUED'
    assert c('EXEC') == [None]
    # A write sent inside MULTI is held before it is queued.
    assert c('MULTI') == '+OK'
    c.send('SET', 'q', '1')
    assert_held(c, 'SET inside MULTI')
    assert a('CLIENT', 'UNPAUSE') == '+OK'
    assert b.read() == ['+OK']
    assert c.read() == '+QUEUED'
    assert c('DISCARD') == '+OK'

    # CLIENT PAUSE inside a transaction starts after it: the writes queued after it still run.
    assert a('MULTI') == '+OK'
    assert a('SET', 't1', '1') == '+QUEUED'
    assert a('CLIENT', 'PAUSE', '100000', 'WRITE') == '+QUEUED'
    assert a('SET', 't2', '1') == '+QUEUED'
    assert a('EXEC') == ['+OK', '+OK', '+OK']
    b.send('SET', 't3', '1')
    assert_held(b, 'SET after a pause sent in a transaction')
    assert a('MGET', 't1', 't2', 't3') == ['1', '1', None]
    assert a('CLIENT', 'UNPAUSE') == '+OK'
    assert b.read() == '+OK'
    for x in (a, b, c):
        x.close()


def check_scripts(port):
    a = Conn(port)
    assert a('FUNCTION', 'LOAD', 'REPLACE',
             "#!lua name=pauselib\nredis.register_function{function_name='ro_f', "
             "callback=function() return 'hello' end, flags={'no-writes'}}\n"
             "redis.register_function('rw_f', function() return 'rw' end)") == 'pauselib'
    assert a('PFADD', 'hll', 'x') == 1
    assert a('CLIENT', 'PAUSE', '100000', 'WRITE') == '+OK'
    held = {}
    for name, args in (('eval', ('EVAL', 'return 1', '0')),
                       ('eval-shebang', ('EVAL', '#!lua\nreturn 11', '0')),
                       ('fcall', ('FCALL', 'rw_f', '0')),
                       ('publish', ('PUBLISH', 'ch', 'm')),
                       ('pfcount', ('PFCOUNT', 'hll'))):
        c = Conn(port)
        c.send(*args)
        held[name] = c
    for name, c in held.items():
        assert_held(c, name)
    # Scripts that declare no-writes, EVAL_RO and FCALL_RO run.
    assert a('EVAL', '#!lua flags=no-writes\nreturn 2', '0') == 2
    sha = a('SCRIPT', 'LOAD', '#!lua flags=no-writes\nreturn 4')
    assert a('EVALSHA', sha, '0') == 4
    assert a('EVAL_RO', "return redis.call('get','missing')", '0') is None
    assert a('FCALL', 'ro_f', '0') == 'hello'
    assert a('FCALL_RO', 'ro_f', '0') == 'hello'
    assert a('CLIENT', 'UNPAUSE') == '+OK'
    assert held['eval'].read() == 1
    assert held['eval-shebang'].read() == 11
    assert held['fcall'].read() == 'rw'
    assert held['publish'].read() == 0
    assert held['pfcount'].read() == 1
    for c in list(held.values()) + [a]:
        c.close()


def check_expiry_paused(port):
    a = Conn(port)
    a('FLUSHALL')
    before = int(info_field(a, 'expired_keys'))
    assert a('SET', 'e1', 'v', 'PX', '50') == '+OK'
    assert a('CLIENT', 'PAUSE', '100000', 'WRITE') == '+OK'
    time.sleep(0.3)
    assert a('GET', 'e1') is None
    assert a('EXISTS', 'e1') == 0
    assert a('DBSIZE') == 1, 'an expired key must stay until the pause ends'
    assert int(info_field(a, 'expired_keys')) == before
    assert a('CLIENT', 'UNPAUSE') == '+OK'
    assert a('GET', 'e1') is None
    assert int(info_field(a, 'expired_keys')) == before + 1
    assert a('DBSIZE') == 0
    a.close()


def check_replica_pause(binary, master_port, replica_port, workdir):
    proc = start(binary, replica_port, os.path.join(workdir, 'replica'))
    try:
        m, r = Conn(master_port), Conn(replica_port)
        assert r('REPLICAOF', '127.0.0.1', str(master_port)) == '+OK'
        wait_for(lambda: info_field(r, 'master_link_status') == 'up', 'replication link', 15)
        assert m('SET', 'r0', '1') == '+OK'
        wait_for(lambda: r('GET', 'r0') == '1', 'first write on the replica')
        assert r('CLIENT', 'PAUSE', '100000', 'WRITE') == '+OK'
        assert m('SET', 'r1', '1') == '+OK'
        time.sleep(0.5)
        assert r('GET', 'r1') is None, 'a paused replica applied a write from its master'
        assert r('CLIENT', 'UNPAUSE') == '+OK'
        wait_for(lambda: r('GET', 'r1') == '1', 'held write on the replica after the pause')
        # A write the pause holds is dropped when the replica is promoted meanwhile: nothing
        # from the old master may run on the new one.
        assert r('CLIENT', 'PAUSE', '100000', 'WRITE') == '+OK'
        assert m('SET', 'r2', '1') == '+OK'
        time.sleep(0.5)
        assert r('REPLICAOF', 'NO', 'ONE') == '+OK'
        assert r('CLIENT', 'UNPAUSE') == '+OK'
        time.sleep(0.3)
        assert r('GET', 'r2') is None, 'a write from the old master ran after the promotion'
        m.close()
        r.close()
    finally:
        stop(proc)


def check_peadb_only(port):
    a, b = Conn(port), Conn(port)
    # CLIENT UNBLOCK does not release a paused client (Redis 7.2.5 crashes on this).
    held_id = b('CLIENT', 'ID')
    assert a('CLIENT', 'PAUSE', '100000', 'WRITE') == '+OK'
    b.send('SET', 'u', '1')
    wait_blocked(a, 1)
    assert a('CLIENT', 'UNBLOCK', str(held_id)) == 0
    assert a('CLIENT', 'UNBLOCK', str(held_id), 'ERROR') == 0
    assert_held(b, 'SET after CLIENT UNBLOCK')
    assert a('CLIENT', 'UNPAUSE') == '+OK'
    assert b.read() == '+OK'
    a.close()
    b.close()


def run_engine(name, binary, work):
    port, second = PORTS[name]
    proc = start(binary, port, os.path.join(work, name))
    try:
        check_replies(Conn(port))
        check_write_pause(port)
        check_blocking_order(port)
        check_all_pause(port)
        check_transactions(port)
        check_scripts(port)
        check_expiry_paused(port)
        check_replica_pause(binary, port, second, os.path.join(work, name))
        if name == 'peadb':
            check_peadb_only(port)
    finally:
        stop(proc)
    print(f'{name}: ok')


def main():
    work = tempfile.mkdtemp(prefix='peadb-pause-')
    try:
        if REDIS:
            run_engine('redis', REDIS, work)
        else:
            print('no pinned redis-server: skipping the Redis side')
        run_engine('peadb', PEADB, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()

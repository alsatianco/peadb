#!/usr/bin/env python3
"""A replica keeps, acknowledges and passes on the replication offset it has applied, and no replica misses writes bigger than the backlog, as in Redis 7.2.5 (compat/delta.md items 40, 44 and 47).

The same checks run on peadb-server and on the pinned redis-server (Redis side first, only when it is Redis 7.2.x), with a master M, a replica R and a third server S:

- offsets: once R has applied what M sent, R's INFO master_repl_offset, slave_repl_offset and slave_read_repl_offset equal M's master_repl_offset (PeaDB's replica kept the offset its full sync started from), R's own backlog holds the stream since its full sync, M's slave0 line and ROLE show R's acknowledged offset, R's ROLE shows its link and offset, and the INFO replication fields come in Redis's order on both roles.
- a chained replica S of R gets M's writes, with the same offset (PeaDB's replica passed nothing on), in the database M's stream had selected when S synced (the RDB's repl-stream-db).
- WAIT counts only what replicas have applied: a replica whose CLIENT PAUSE WRITE holds M's write does not acknowledge it, so WAIT 1 400 answers 0 (PeaDB's replica acknowledged the bytes it had received); a client that has written nothing is answered at once (WAIT waits for the client's own last write, Redis's c->woff); a blocked WAIT counts in blocked_clients, shows flag b and is answered by CLIENT UNBLOCK (with its count, or -UNBLOCKED); what a client sent after a command CLIENT UNBLOCK released runs as its own commands (a pipelined WAIT waits for the client's own write); REPLICAOF answers blocked WAIT and BLPOP clients with -UNBLOCKED; WAIT on a replica and bad arguments get Redis's errors; REPLCONF GETACK from a normal client gets no reply.
- promotion: after REPLICAOF NO ONE, R's replid2 is M's replid with second_repl_offset = its offset + 1, and S and the old master M, attached to R, get every write R takes afterwards (PeaDB's promoted replica sent its new replicas nothing) and take R's new replid; on Redis both resume with a partial resync.
- a full sync that REPLICAOF NO ONE or a REPLICAOF to another master replaced is never loaded (a fake master stalls halfway through its RDB): PeaDB loaded it late and wiped the writes made after the promotion.
- big writes: with a 16 KB backlog, a 2 MB SET, a pipeline of 300 KB of SETs, a script that writes 200 KB and a backlog shrunk right after a 200 KB write in the same pipeline all reach both replicas (PeaDB's ring dropped the bytes no replica had taken yet, and the replicas silently lost them).

A last run makes PeaDB a replica of the pinned Redis, with a second Redis server as a replica of PeaDB: PeaDB's offsets equal Redis's, Redis's WAIT sees PeaDB's acknowledgement at once, the chained Redis replica gets the stream through PeaDB in the right databases, and after PeaDB is promoted both Redis servers resume from it with a partial resync.

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
    """The redis-server to compare with, only if it is Redis 7.2.x (replies differ in other versions)."""
    path = redis_server_path()
    if not path:
        return None
    version = subprocess.run([path, '--version'], capture_output=True, text=True).stdout
    return path if ' v=7.2.' in version else None


REDIS = redis_7_2()
PORTS = {
    'redis': (6698, 6699, 6700),
    'peadb': (6701, 6702, 6703),
    'cross': (6704, 6705, 6706),  # Redis master, PeaDB replica, Redis replica of the PeaDB replica
    'redis-stale': (6707, 6708, 6709),  # fake master, replica, second master
    'peadb-stale': (6710, 6711, 6712),
}
FAKE_REPLID = 'a1' * 20
WAIT_ERROR = ('-ERR WAIT cannot be used with replica instances. Please also note that since Redis 4.0 '
              'if a replica is configured to be writable (which is not the default) writes to '
              'replicas are just local and are not propagated.')


def encode(*args):
    out = f'*{len(args)}\r\n'.encode()
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode()
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

    def pipeline(self, commands):
        self.sock.sendall(b''.join(encode(*c) for c in commands))
        return [self.read() for _ in commands]


def start(binary, port, workdir):
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    log = open(log_path, 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                             '--loglevel', 'notice', '--save', '', '--shutdown-timeout', '0',
                             '--repl-diskless-sync-delay', '0'],
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


def info_section(c, section):
    return dict(line.split(':', 1) for line in c('INFO', section).splitlines()
                if ':' in line and not line.startswith('#'))


def info(c, field, section='all'):
    return info_section(c, section).get(field)


def field_names(c):
    """The INFO replication field names, in order."""
    return [line.split(':', 1)[0] for line in c('INFO', 'replication').splitlines()
            if ':' in line and not line.startswith('#')]


def offset(c):
    return int(info(c, 'master_repl_offset', 'replication'))


def wait_for(cond, what, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.02)
    raise AssertionError(f'timed out waiting for {what}')


def check(cond, what):
    if not cond:
        raise AssertionError(what)


def wait_link(c):
    wait_for(lambda: info(c, 'master_link_status', 'replication') == 'up', 'the replica link')


def wait_same_offset(name, *conns):
    def same():
        offsets = [offset(c) for c in conns]
        return len(set(offsets)) == 1
    try:
        wait_for(same, f'{name}: the same replication offset')
    except AssertionError:
        raise AssertionError(f'{name}: offsets differ: {[offset(c) for c in conns]}')


def check_applied_offsets(name, m, r):
    """R shows M's offset as applied, read and its own, and its backlog holds what it applied."""
    def applied():
        rep = info_section(r, 'replication')
        return int(rep['slave_repl_offset']) == offset(m)
    try:
        wait_for(applied, f'{name}: R applies the stream')
    except AssertionError:
        raise AssertionError(f'{name}: R shows {info_section(r, "replication")}, M is at {offset(m)}')
    rep = info_section(r, 'replication')
    want = offset(m)
    check(int(rep['master_repl_offset']) == want, f'{name}: R master_repl_offset {rep}')
    check(int(rep['slave_read_repl_offset']) == want, f'{name}: R slave_read_repl_offset {rep}')
    check(rep['repl_backlog_active'] == '1', f'{name}: R has a backlog {rep}')
    first, hist = int(rep['repl_backlog_first_byte_offset']), int(rep['repl_backlog_histlen'])
    check(first >= 1 and first + hist - 1 == want, f'{name}: R backlog window {rep}')


def check_engine(name, binary, work, layouts):
    mport, rport, sport = PORTS[name]
    procs = [start(binary, port, os.path.join(work, name, role))
             for port, role in ((mport, 'm'), (rport, 'r'), (sport, 's'))]
    try:
        m, r, s = Conn(mport), Conn(rport), Conn(sport)

        # ── offsets, the slave0 line and the INFO layout ──
        check(r('REPLICAOF', '127.0.0.1', mport) == '+OK', f'{name}: REPLICAOF')
        wait_link(r)
        check(m('SET', 'k1', 'v1') == '+OK', f'{name}: SET')
        check(m.pipeline([('MULTI',), ('SET', 'k2', 'v2'), ('INCR', 'n'), ('EXEC',)])[-1] == ['+OK', 1],
              f'{name}: EXEC')
        check(m.pipeline([('SELECT', 3), ('SET', 'k3', 'v3'), ('SELECT', 0)]) == ['+OK'] * 3,
              f'{name}: SELECT')
        check_applied_offsets(name, m, r)
        # The replica answers the GETACK that WAIT sends with the offset before the GETACK.
        before = offset(m)
        check(m('WAIT', 1, 2000) == 1, f'{name}: WAIT before the slave0 check')
        slave0 = dict(kv.split('=') for kv in info(m, 'slave0', 'replication').split(','))
        check(slave0['ip'] == '127.0.0.1' and slave0['port'] == str(rport) and
              slave0['state'] == 'online' and before <= int(slave0['offset']) < offset(m) and
              slave0['lag'] in ('0', '1'), f'{name}: slave0 {slave0}, offset before WAIT {before}')
        role = m('ROLE')
        check(role[0] == 'master' and role[1] == offset(m) and len(role[2]) == 1 and
              role[2][0][:2] == ['127.0.0.1', str(rport)] and int(role[2][0][2]) == int(slave0['offset']),
              f'{name}: ROLE of the master {role}')
        role = r('ROLE')
        check(role == ['slave', '127.0.0.1', mport, 'connected', offset(r)], f'{name}: ROLE of R {role}')
        layouts[name] = {'master': field_names(m), 'replica': field_names(r)}
        memory = info_section(m, 'memory')
        check(int(memory['mem_total_replication_buffers']) > 0 and
              int(memory['mem_replication_backlog']) > 0, f'{name}: replication buffers {memory}')

        # ── a chained replica gets M's stream through R ──
        # M's stream is in database 5 when S syncs from R, so S must apply what follows there
        # (the full sync's RDB names it: repl-stream-db).
        d = Conn(mport)
        check(d.pipeline([('SELECT', 5), ('SET', 'd5', 'x')]) == ['+OK', '+OK'], f'{name}: SET in db 5')
        wait_same_offset(f'{name} db 5', m, r)
        check(s('REPLICAOF', '127.0.0.1', rport) == '+OK', f'{name}: chained REPLICAOF')
        wait_link(s)
        check(d('SET', 'd5b', 'y') == '+OK', f'{name}: SET in db 5 after the chained sync')
        check(m('SET', 'chain', '1') == '+OK', f'{name}: SET chain')
        wait_for(lambda: s('GET', 'chain') == '1', f'{name}: the chained replica gets the write')
        wait_same_offset(f'{name} chain', m, r, s)
        check(s.pipeline([('SELECT', 5), ('GET', 'd5b'), ('SELECT', 0)]) == ['+OK', 'y', '+OK'] and
              s('GET', 'd5b') is None, f'{name}: the chained replica applies db 5 writes in db 5')
        d.close()

        # ── WAIT counts only applied writes, from the client's own last write ──
        a, b = Conn(mport), Conn(mport)
        check(a('SET', 'w1', '1') == '+OK', f'{name}: SET w1')
        t0 = time.time()
        check(a('WAIT', 1, 2000) == 1 and time.time() - t0 < 1.0, f'{name}: WAIT is answered at once')
        check(r('CLIENT', 'PAUSE', 5000, 'WRITE') == '+OK', f'{name}: CLIENT PAUSE on R')
        check(a('SET', 'w2', '2') == '+OK', f'{name}: SET w2')
        t0 = time.time()
        got = a('WAIT', 1, 400)
        took = time.time() - t0
        check(got == 0 and took >= 0.35, f'{name}: WAIT for a held write gave {got} after {took:.2f}s')
        t0 = time.time()
        got = b('WAIT', 1, 400)
        took = time.time() - t0
        check(got == 1 and took < 0.3, f'{name}: WAIT without a write gave {got} after {took:.2f}s')
        rep = info_section(r, 'replication')
        check(int(rep['slave_read_repl_offset']) > int(rep['slave_repl_offset']),
              f'{name}: R has read more than it applied while paused {rep}')
        # A blocked WAIT counts as a blocked client, and CLIENT UNBLOCK answers it.
        a_id = a('CLIENT', 'ID')
        for how, want in ((('TIMEOUT',), 0), (('ERROR',), '-UNBLOCKED client unblocked via CLIENT UNBLOCK')):
            a.sock.sendall(encode('WAIT', 1, 0))
            wait_for(lambda: info(m, 'blocked_clients', 'clients') == '1', f'{name}: WAIT is blocked')
            flags = [line for line in m('CLIENT', 'LIST').splitlines() if f'id={a_id} ' in line]
            check(len(flags) == 1 and ' flags=b ' in flags[0], f'{name}: CLIENT LIST of a WAIT {flags}')
            check(m('CLIENT', 'UNBLOCK', a_id, *how) == 1, f'{name}: CLIENT UNBLOCK {how}')
            got = a.read()
            check(got == want, f'{name}: WAIT unblocked with {how} answered {got!r}')
            check(info(m, 'blocked_clients', 'clients') == '0', f'{name}: no blocked client left')
        # What a client sent after a command CLIENT UNBLOCK released runs as its own commands, so
        # a WAIT there waits for the client's own write (it ran inside CLIENT UNBLOCK before).
        e = Conn(mport)
        e_id = e('CLIENT', 'ID')
        e.sock.sendall(encode('BLPOP', 'nolist', 0) + encode('SET', 'wk', 'v') + encode('WAIT', 1, 400))
        wait_for(lambda: info(m, 'blocked_clients', 'clients') == '1', f'{name}: BLPOP is blocked')
        t0 = time.time()
        check(m('CLIENT', 'UNBLOCK', e_id) == 1, f'{name}: CLIENT UNBLOCK of the BLPOP')
        got = [e.read(), e.read(), e.read()]
        took = time.time() - t0
        check(got == [None, '+OK', 0] and took >= 0.35,
              f'{name}: the pipeline after an unblocked BLPOP gave {got} after {took:.2f}s')
        e.close()
        check(r('CLIENT', 'UNPAUSE') == '+OK', f'{name}: CLIENT UNPAUSE')
        check(a('WAIT', 1, 2000) == 1, f'{name}: WAIT after the pause')
        check(r('GET', 'w2') == '2', f'{name}: R applied w2')
        check(r('WAIT', 0, 0) == WAIT_ERROR, f'{name}: WAIT on a replica')
        check(a('WAIT', 'x', 0) == '-ERR value is not an integer or out of range', f'{name}: WAIT x')
        check(a('WAIT', 1, 'abc') == '-ERR timeout is not an integer or out of range', f'{name}: WAIT abc')
        check(a('WAIT', 1, -1) == '-ERR timeout is negative', f'{name}: WAIT -1')
        check(a('WAIT', -5, 0) == 1, f'{name}: WAIT -5')
        a.sock.sendall(encode('REPLCONF', 'GETACK', '*') + encode('PING'))
        check(a.read() == '+PONG', f'{name}: REPLCONF GETACK from a client has no reply')
        a.close()
        b.close()

        # ── promotion: R continues the stream for S and for the old master ──
        old_replid = info(m, 'master_replid', 'replication')
        partial_before = int(info(r, 'sync_partial_ok', 'stats'))
        check(r('REPLICAOF', 'NO', 'ONE') == '+OK', f'{name}: REPLICAOF NO ONE')
        rep = info_section(r, 'replication')
        check(rep['role'] == 'master' and rep['master_replid2'] == old_replid and
              int(rep['second_repl_offset']) == int(rep['master_repl_offset']) + 1,
              f'{name}: R after the promotion {rep}')
        check(r('SET', 'promo', '1') == '+OK', f'{name}: SET promo')
        check(r('RPUSH', 'l', 'a', 'b', 'c') == 3, f'{name}: RPUSH')
        wait_link(s)
        wait_for(lambda: s('GET', 'promo') == '1', f'{name}: S gets the write after the promotion')
        check(m('REPLICAOF', '127.0.0.1', rport) == '+OK', f'{name}: the old master follows R')
        wait_link(m)
        check(r('SET', 'promo2', '2') == '+OK', f'{name}: SET promo2')
        wait_for(lambda: m('GET', 'promo2') == '2' and s('GET', 'promo2') == '2',
                 f'{name}: both replicas get the write')
        check(m('LRANGE', 'l', 0, -1) == ['a', 'b', 'c'], f'{name}: the old master has the list')
        wait_same_offset(f'{name} promotion', r, s, m)
        replids = [info(c, 'master_replid', 'replication') for c in (r, s, m)]
        check(len(set(replids)) == 1, f'{name}: the replicas take the promoted replid {replids}')
        if name == 'redis':
            partial = int(info(r, 'sync_partial_ok', 'stats')) - partial_before
            check(partial == 2, f'{name}: S and M resume with a partial resync ({partial})')

        # ── writes bigger than the backlog reach every replica ──
        check(r('CONFIG', 'SET', 'repl-backlog-size', 16384) == '+OK', f'{name}: a 16 KB backlog')
        check(r('SET', 'big', 'x' * 2_000_000) == '+OK', f'{name}: a 2 MB SET')
        replies = r.pipeline([('SET', f'p{i}', 'y' * 1000) for i in range(300)])
        check(replies == ['+OK'] * 300, f'{name}: the pipeline')
        script = "for i = 1, 200 do redis.call('set', 'e' .. i, string.rep('z', 1000)) end return 1"
        check(r('EVAL', script, 0) == 1, f'{name}: a script that writes 200 KB')
        check(r('CONFIG', 'SET', 'repl-backlog-size', 1048576) == '+OK', f'{name}: a 1 MB backlog')
        replies = r.pipeline([('SET', 'big2', 'w' * 200_000),
                              ('CONFIG', 'SET', 'repl-backlog-size', 16384), ('SET', 'after', '1')])
        check(replies == ['+OK'] * 3, f'{name}: shrink the backlog right after a write')
        wait_same_offset(f'{name} big writes', r, s, m)
        for c in (s, m):
            check(c('STRLEN', 'big') == 2_000_000 and c('STRLEN', 'big2') == 200_000 and
                  c('GET', 'after') == '1' and c('STRLEN', 'p299') == 1000 and
                  c('STRLEN', 'e200') == 1000 and c('DBSIZE') == r('DBSIZE'),
                  f'{name}: a replica lost a big write')
        memory = info_section(r, 'memory')
        check(int(memory['mem_total_replication_buffers']) > 0 and
              int(memory['mem_replication_backlog']) > 0, f'{name}: replication buffers {memory}')

        for c in (m, r, s):
            c.close()
    finally:
        for p in procs:
            stop(p)


def read_command(f):
    line = f.readline()
    if not line:
        return None
    if not line.startswith(b'*'):
        return line.strip().decode().split()
    args = []
    for _ in range(int(line[1:-2])):
        n = int(f.readline()[1:-2])
        args.append(f.read(n + 2)[:-2].decode())
    return args


class FakeMaster:
    """Answers a replica's handshake with +FULLRESYNC and sends the first half of a real RDB, the rest only once go is set."""

    def __init__(self, port, rdb):
        self.rdb = rdb
        self.go = threading.Event()
        self.half_sent = threading.Event()
        self.lsock = socket.socket()
        self.lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.lsock.bind(('127.0.0.1', port))
        self.lsock.listen(1)
        self.conn = None
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def serve(self):
        try:
            self.lsock.settimeout(10)
            self.conn, _ = self.lsock.accept()
            f = self.conn.makefile('rb')
            while True:
                args = read_command(f)
                if not args:
                    return
                if args[0].upper() in ('PSYNC', 'SYNC'):
                    half = len(self.rdb) // 2
                    self.conn.sendall(f'+FULLRESYNC {FAKE_REPLID} 0\r\n${len(self.rdb)}\r\n'.encode() +
                                      self.rdb[:half])
                    self.half_sent.set()
                    self.go.wait(30)
                    self.conn.sendall(self.rdb[half:])
                    while f.read(1):  # keep the link open until the replica drops it
                        pass
                    return
                self.conn.sendall(b'+PONG\r\n' if args[0].upper() == 'PING' else b'+OK\r\n')
        except OSError:
            pass

    def close(self):
        self.go.set()
        for sock in (self.conn, self.lsock):
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
        self.thread.join(5)


def read_rdb(port):
    """A full-sync RDB of the server's dataset."""
    sock = socket.create_connection(('127.0.0.1', port), timeout=10)
    f = sock.makefile('rb')
    sock.sendall(encode('PSYNC', '?', '-1'))
    check(f.readline().startswith(b'+FULLRESYNC'), 'read_rdb: +FULLRESYNC')
    while True:  # Redis sends newlines while it prepares the RDB
        b = f.read(1)
        if b == b'$':
            break
        check(b == b'\n', f'read_rdb: unexpected {b!r}')
    data = f.read(int(f.readline()[:-2]))
    f.close()
    sock.close()
    return data


def check_stale_sync(name, binary, work):
    """A full sync that a later REPLICAOF NO ONE, or a REPLICAOF to another master, replaced is never loaded."""
    fport, bport, cport = PORTS[name + '-stale']
    bproc = start(binary, bport, os.path.join(work, name + '-stale', 'b'))
    cproc = start(binary, cport, os.path.join(work, name + '-stale', 'c'))
    fakes = []
    try:
        b, c = Conn(bport), Conn(cport)
        check(c.pipeline([('SET', f'fromA:{i}', 'a' * 1000) for i in range(50)]) == ['+OK'] * 50,
              f"{name}: the old master's data")
        rdb = read_rdb(cport)
        check(c('FLUSHALL') == '+OK', f'{name}: FLUSHALL')
        check(c.pipeline([('SET', f'fromC:{i}', 'c') for i in range(10)]) == ['+OK'] * 10,
              f"{name}: the new master's data")

        # The transfer stalls halfway, then the replica is promoted and takes a write.
        fake = FakeMaster(fport, rdb)
        fakes.append(fake)
        check(b('REPLICAOF', '127.0.0.1', fport) == '+OK', f'{name}: REPLICAOF the fake master')
        check(fake.half_sent.wait(10), f'{name}: the fake master sent half of the RDB')
        time.sleep(0.3)
        check(b('REPLICAOF', 'NO', 'ONE') == '+OK', f'{name}: REPLICAOF NO ONE during the transfer')
        check(b('SET', 'written-after-promotion', 'important') == '+OK', f'{name}: SET after promotion')
        fake.go.set()
        time.sleep(1.0)
        rep = info_section(b, 'replication')
        check(b('GET', 'written-after-promotion') == 'important' and b('DBSIZE') == 1 and
              rep['role'] == 'master' and FAKE_REPLID not in (rep['master_replid'], rep['master_replid2']),
              f"{name}: a promoted replica loaded the old master's RDB: dbsize {b('DBSIZE')}, {rep}")
        fake.close()

        # Becoming a replica answers every blocked client (Redis's disconnectAllBlockedClients()):
        # a WAIT would report a write the full sync replaces, a pop would be served by the stream.
        f, g = Conn(bport), Conn(bport)
        f.sock.sendall(encode('WAIT', 1, 0))
        g.sock.sendall(encode('BLPOP', 'nolist', 0))
        wait_for(lambda: info(b, 'blocked_clients', 'clients') == '2', f'{name}: WAIT and BLPOP block')

        # The transfer stalls halfway, then the replica follows another master.
        fake = FakeMaster(fport, rdb)
        fakes.append(fake)
        check(b('REPLICAOF', '127.0.0.1', fport) == '+OK', f'{name}: REPLICAOF the fake master again')
        unblocked = ('-UNBLOCKED force unblock from blocking operation, instance state changed '
                     '(master -> replica?)')
        check(f.read() == unblocked and g.read() == unblocked, f'{name}: REPLICAOF unblocks WAIT and BLPOP')
        check(info(b, 'blocked_clients', 'clients') == '0', f'{name}: no blocked client after REPLICAOF')
        f.close()
        g.close()
        check(fake.half_sent.wait(10), f'{name}: the fake master sent half of the RDB again')
        time.sleep(0.3)
        check(b('REPLICAOF', '127.0.0.1', cport) == '+OK', f'{name}: REPLICAOF the second master')
        wait_link(b)
        wait_for(lambda: b('GET', 'fromC:9') == 'c', f"{name}: the replica has the second master's data")
        fake.go.set()
        time.sleep(1.0)
        check(c('SET', 'after-switch', 'x') == '+OK', f'{name}: SET on the second master')
        wait_for(lambda: b('GET', 'after-switch') == 'x', f'{name}: the replica follows the second master')
        rep = info_section(b, 'replication')
        check(b('GET', 'fromA:0') is None and b('DBSIZE') == 11 and rep['master_link_status'] == 'up' and
              rep['master_replid'] == info(c, 'master_replid', 'replication'),
              f"{name}: the old master's RDB replaced the new one: dbsize {b('DBSIZE')}, {rep}")
        b.close()
        c.close()
    finally:
        for fake in fakes:
            fake.close()
        stop(bproc)
        stop(cproc)


def check_cross(work):
    """PeaDB as a replica of the pinned Redis with a Redis replica of its own, then promoted, with both Redis servers resuming from it."""
    mport, rport, sport = PORTS['cross']
    mproc = start(REDIS, mport, os.path.join(work, 'cross', 'm'))
    rproc = start(PEADB, rport, os.path.join(work, 'cross', 'r'))
    sproc = start(REDIS, sport, os.path.join(work, 'cross', 's'))
    try:
        m, r, s = Conn(mport), Conn(rport), Conn(sport)
        check(r('REPLICAOF', '127.0.0.1', mport) == '+OK', 'cross: REPLICAOF')
        wait_link(r)
        check(m('SET', 'k', 'v') == '+OK', 'cross: SET')
        check(m.pipeline([('MULTI',), ('SET', 'k2', 'v2'), ('INCR', 'n'), ('EXEC',)])[-1] == ['+OK', 1],
              'cross: EXEC')
        d = Conn(mport)
        check(d.pipeline([('SELECT', 2), ('SET', 'k3', 'v3')]) == ['+OK', '+OK'], 'cross: SELECT')
        check_applied_offsets('cross', m, r)
        t0 = time.time()
        check(m('WAIT', 1, 2000) == 1 and time.time() - t0 < 1.0, 'cross: Redis WAIT sees PeaDB at once')
        # A Redis replica of the PeaDB replica: PeaDB's RDB says the stream is in database 2.
        check(s('REPLICAOF', '127.0.0.1', rport) == '+OK', 'cross: Redis follows the PeaDB replica')
        wait_link(s)
        check(d('SET', 'k4', 'v4') == '+OK', 'cross: SET in db 2')
        wait_same_offset('cross chain', m, r, s)
        check(s.pipeline([('SELECT', 2), ('GET', 'k4'), ('SELECT', 0)]) == ['+OK', 'v4', '+OK'] and
              s('GET', 'k2') == 'v2', 'cross: the chained Redis replica has the data in its databases')
        d.close()
        check(r('REPLICAOF', 'NO', 'ONE') == '+OK', 'cross: promote PeaDB')
        check(r('SET', 'promo', '1') == '+OK', 'cross: SET promo')
        check(m('REPLICAOF', '127.0.0.1', rport) == '+OK', 'cross: Redis follows PeaDB')
        wait_link(m)
        wait_link(s)
        wait_for(lambda: m('GET', 'promo') == '1' and s('GET', 'promo') == '1',
                 'cross: both Redis servers get the write after the promotion')
        wait_same_offset('cross', m, r, s)
        # +CONTINUE names the new replid, which a PSYNC2 replica takes (its old one becomes replid2).
        replids = [info(c, 'master_replid', 'replication') for c in (r, s, m)]
        check(len(set(replids)) == 1, f'cross: the Redis servers take the promoted replid {replids}')
        check(info(r, 'sync_partial_ok', 'stats') == '2',
              f'cross: both Redis servers resume with a partial resync ({info(r, "sync_partial_ok", "stats")})')
        check(m('GET', 'k2') == 'v2' and m('GET', 'n') == '1', 'cross: Redis keeps its data')
        for c in (m, r, s):
            c.close()
    finally:
        for p in (mproc, rproc, sproc):
            stop(p)


def main():
    work = tempfile.mkdtemp(prefix='peadb-replica-offset-')
    try:
        layouts = {}
        if REDIS:
            check_engine('redis', REDIS, work, layouts)
        check_engine('peadb', PEADB, work, layouts)
        if REDIS:
            check_stale_sync('redis', REDIS, work)
        check_stale_sync('peadb', PEADB, work)
        if REDIS:
            for role in ('master', 'replica'):
                check(layouts['peadb'][role] == layouts['redis'][role],
                      f'INFO replication fields of a {role}: PeaDB {layouts["peadb"][role]}, '
                      f'Redis {layouts["redis"][role]}')
            check_cross(work)
        print('replica offset parity: ok' + ('' if REDIS else ' (PeaDB side only)'))
        return 0
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    sys.exit(main())

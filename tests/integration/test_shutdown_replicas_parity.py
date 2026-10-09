#!/usr/bin/env python3
"""SHUTDOWN and SIGTERM wait for lagging replicas like Redis 7.2.5 (compat/delta.md item 33).

The same checks run on peadb-server and on the pinned redis-server, with a fake replica (PSYNC, then it reads the stream and sends REPLCONF ACK only when the test says so): with a replica that has not acknowledged every byte, SHUTDOWN and SIGTERM ask it for an ACK (REPLCONF GETACK *), pause writes and keep serving reads, the SHUTDOWN client stays blocked (blocked_clients, INFO shutdown_in_milliseconds, "Waiting for replicas before shutting down."), SHUTDOWN ABORT answers +OK, releases the held writes and answers every blocked SHUTDOWN client "Errors trying to SHUTDOWN. Check logs.", CLIENT UNBLOCK ... ERROR releases one blocked SHUTDOWN client, the server exits once the replica acknowledges the master's offset ("1 of 1 replicas are in sync when shutting down.") or after shutdown-timeout seconds ("Lagging replica <ip>:<port> reported offset <n> behind master, lag=<s>, state=online." and "0 of 1 replicas are in sync"), SHUTDOWN NOW and shutdown-timeout 0 do not wait, and a final save that fails after the wait answers the blocked clients with the error and keeps the server running. Without a pinned redis-server, only the PeaDB side runs.
"""
import os
import pathlib
import re
import select
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _redis_path import redis_server_path  # noqa: E402

PEADB = str(ROOT / 'peadb-server')
REDIS = redis_server_path()
PORTS = {'peadb': 6680, 'redis': 6681}
# Disk-based sync at once, and no master PING during a check, so the fake replica's offset is
# exactly the bytes it has read.
SYNC = ('--repl-diskless-sync', 'no', '--repl-diskless-sync-delay', '0',
        '--repl-ping-replica-period', '60')
FAILED = '-ERR Errors trying to SHUTDOWN. Check logs.'
REPLICA_PORT = 7777  # what the fake replica says it listens on (REPLCONF listening-port)


def encode(*args):
    out = f'*{len(args)}\r\n'.encode()
    for a in args:
        b = str(a).encode()
        out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
    return out


class Conn:
    def __init__(self, port, timeout=5):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=timeout)
        self.f = self.sock.makefile('rb')

    def close(self):
        self.f.close()
        self.sock.close()

    def read(self):
        try:
            line = self.f.readline()
        except ConnectionResetError:
            return None
        if not line:
            return None  # connection closed
        kind, rest = line[:1], line[1:-2]
        if kind in (b'+', b'-'):
            return (kind + rest).decode()
        if kind == b':':
            return int(rest)
        if kind in (b'$', b'='):
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2].decode()
        if kind == b'*':
            return [self.read() for _ in range(int(rest))]
        raise ValueError(f'unexpected reply {line!r}')

    def send(self, *args):
        self.sock.sendall(encode(*args))

    def has_reply(self, wait=0.3):
        # Every earlier reply was read whole, so nothing waits in self.f's buffer.
        return bool(select.select([self.sock], [], [], wait)[0])

    def __call__(self, *args):
        self.send(*args)
        return self.read()


class FakeReplica:
    """PSYNC ? -1, skip the RDB, then read the stream and keep its offset like a replica."""

    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=10)
        self.f = self.sock.makefile('rb')
        self.sock.sendall(encode('REPLCONF', 'listening-port', REPLICA_PORT))
        assert self.f.readline() == b'+OK\r\n'
        self.sock.sendall(encode('PSYNC', '?', '-1'))
        line = self.f.readline()
        assert line.startswith(b'+FULLRESYNC'), line
        self.offset = int(line.split()[2])
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
        size = len(line)
        args = []
        for _ in range(int(line[1:-2])):
            head = self.f.readline()
            n = int(head[1:-2])
            args.append(self.f.read(n + 2)[:-2].decode())
            size += len(head) + n + 2
        self.offset += size
        return args

    def until_getack(self):
        """The commands up to the next REPLCONF GETACK *, without it and without pings."""
        out = []
        while True:
            cmd = self.command()
            if [a.upper() for a in cmd] == ['REPLCONF', 'GETACK', '*']:
                return out
            if cmd != ['PING']:
                out.append(cmd)

    def ack(self):
        self.sock.sendall(encode('REPLCONF', 'ACK', self.offset))

    def close(self):
        self.f.close()
        self.sock.close()


def start(binary, port, workdir, *extra):
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    log = open(log_path, 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                             '--loglevel', 'notice', '--save', '', *SYNC, *extra],
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
    proc.kill()
    proc.wait()


def log_of(workdir):
    return open(os.path.join(workdir, 'server.log'), errors='replace').read()


def wait_for(cond, what, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.01)
    raise AssertionError(f'timed out waiting for {what}')


def wait_exit(proc, timeout=10):
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        raise AssertionError('the server did not exit')


def info(c, section):
    fields = {}
    for line in c('INFO', section).splitlines():
        if ':' in line and not line.startswith('#'):
            k, v = line.split(':', 1)
            fields[k] = v
    return fields


def check_wait_abort_timeout(name, binary, work):
    """A replica that never ACKs: the shutdown waits, ABORT cancels it, then it times out."""
    port = PORTS[name]
    workdir = os.path.join(work, f'{name}-wait')
    proc = start(binary, port, workdir)
    try:
        c = Conn(port)
        assert c('CONFIG', 'GET', 'shutdown-timeout') == ['shutdown-timeout', '10'], name
        # REPLCONF ip-address takes names of up to 255 bytes (NET_HOST_STR_LEN).
        assert c('REPLCONF', 'ip-address', 'h' * 255) == '+OK', name
        assert c('REPLCONF', 'ip-address', 'h' * 256) == \
            '-ERR REPLCONF ip-address provided by replica instance is too long: 256 bytes', name
        replica = FakeReplica(port)
        assert c('SET', 'k', '1') == '+OK'
        a, a2, b = Conn(port), Conn(port), Conn(port)
        a_id = a('CLIENT', 'ID')
        assert info(c, 'clients')['connected_clients'] == '4', name  # replicas are not counted
        a.send('SHUTDOWN')
        wait_for(lambda: 'Waiting for replicas before shutting down.' in log_of(workdir),
                 f'{name}: the wait log line')
        ms = int(info(c, 'server')['shutdown_in_milliseconds'])
        assert 0 < ms <= 10000, (name, ms)
        assert not a.has_reply(), name
        # The master asked the replica for an ACK.
        assert replica.until_getack() == [['SELECT', '0'], ['SET', 'k', '1']], name
        # Writes wait for the end of the shutdown; reads are served.
        b.send('SET', 'x', '1')
        assert c('GET', 'k') == '1', name
        assert not b.has_reply(), f'{name}: a write ran during the shutdown wait'
        a2.send('SHUTDOWN')
        wait_for(lambda: info(c, 'clients')['blocked_clients'] == '3', f'{name}: blocked clients')
        a_line = next(line for line in c('CLIENT', 'LIST').splitlines()
                      if line.startswith(f'id={a_id} '))
        assert ' flags=b ' in a_line, (name, a_line)
        # CLIENT UNBLOCK does not release a SHUTDOWN client on PeaDB. On Redis 7.2.5 it would
        # crash the server: TIMEOUT reaches serverPanic() in replyToBlockedClientTimedOut(), and
        # after ERROR the client's next command fails the assertion 'c->argc == 0'
        # (networking.c:2260), so the Redis side does not run this.
        if name == 'peadb':
            assert c('CLIENT', 'UNBLOCK', a_id, 'ERROR') == 0, name
            assert c('CLIENT', 'UNBLOCK', a_id, 'TIMEOUT') == 0, name
            assert not a.has_reply(0.1), name
        # A script cannot block on SHUTDOWN (EVAL_RO: the pause holds EVAL, which may write).
        reply = c('EVAL_RO', "return redis.pcall('shutdown')", '0')
        assert isinstance(reply, str) and reply.startswith('-'), (name, reply)
        assert 'shutdown_in_milliseconds' in info(c, 'server'), name
        errors = int(info(c, 'stats')['total_error_replies'])
        # ABORT: every blocked SHUTDOWN client gets the error, the held write runs.
        assert c('SHUTDOWN', 'ABORT') == '+OK', name
        assert a.read() == FAILED, name
        assert a2.read() == FAILED, name
        assert b.read() == '+OK', name
        # Both errors are counted, as Redis's addReplyError() counts them.
        assert int(info(c, 'stats')['total_error_replies']) == errors + 2, name
        assert 'Shutdown manually aborted.' in log_of(workdir), name
        assert 'shutdown_in_milliseconds' not in info(c, 'server'), name
        assert c('SHUTDOWN', 'ABORT') == '-ERR No shutdown in progress.', name
        # A released SHUTDOWN client works on. Redis 7.2.5 does not reset it (unblockClient()
        # skips resetClient() for BLOCKED_SHUTDOWN), so its next command fails the assertion
        # 'c->argc == 0' (networking.c:2260) and crashes the server: the Redis side skips this.
        if name == 'peadb':
            assert a('PING') == '+PONG' and a2('PING') == '+PONG', name
        assert replica.command() == ['SET', 'x', '1'], name

        # shutdown-timeout passes: the server exits and logs the lagging replica.
        assert c('CONFIG', 'SET', 'shutdown-timeout', '1') == '+OK'
        d = Conn(port)
        t0 = time.time()
        d.send('SHUTDOWN')
        replica.until_getack()
        assert wait_exit(proc) == 0, name
        elapsed = time.time() - t0
        assert 0.8 <= elapsed < 5, (name, elapsed)
        assert d.read() is None, name  # closed with no reply
        log = log_of(workdir)
        m = re.search(r'Lagging replica 127\.0\.0\.1:(\d+) reported offset (\d+) behind master, '
                      r'lag=(\d+), state=online\.', log)
        assert m, (name, log[-2000:])
        assert int(m.group(1)) == REPLICA_PORT, (name, m.group(0))
        assert int(m.group(2)) == replica.offset, (name, m.group(0), replica.offset)
        assert '0 of 1 replicas are in sync when shutting down.' in log, (name, log[-2000:])
        assert 'ready to exit, bye bye...' in log, name
    finally:
        stop(proc)


def check_ack(name, binary, work):
    """A replica that ACKs late: the server exits as soon as it has the whole stream, after it has answered the WAIT that ACK completes."""
    port = PORTS[name]
    for how in ('shutdown', 'sigterm'):
        workdir = os.path.join(work, f'{name}-ack-{how}')
        proc = start(binary, port, workdir)
        try:
            c = Conn(port)
            replica = FakeReplica(port)
            assert c('INCR', 'k') == 1
            a = Conn(port)
            t0 = time.time()
            if how == 'shutdown':
                a.send('SHUTDOWN')
            else:
                proc.send_signal(signal.SIGTERM)
            assert replica.until_getack() == [['SELECT', '0'], ['INCR', 'k']], (name, how)
            time.sleep(0.3)
            assert proc.poll() is None, (name, how)
            assert int(info(c, 'server')['shutdown_in_milliseconds']) > 0, (name, how)
            replica.ack()
            assert wait_exit(proc) == 0, (name, how)
            assert time.time() - t0 < 5, (name, how)
            log = log_of(workdir)
            assert '1 of 1 replicas are in sync when shutting down.' in log, (name, how)
            assert 'Lagging replica' not in log, (name, how, log[-2000:])
            if how == 'shutdown':
                assert a.read() is None, (name, how)
            else:
                assert 'Received SIGTERM scheduling shutdown...' in log, (name, how)
        finally:
            stop(proc)

    # A WAIT that the same ACK completes gets its reply before the server exits.
    workdir = os.path.join(work, f'{name}-ack-wait')
    proc = start(binary, port, workdir)
    try:
        c = Conn(port)
        replica = FakeReplica(port)
        w = Conn(port)
        assert w('SET', 'k', '1') == '+OK'  # Redis's WAIT waits for this client's own writes
        c.send('SHUTDOWN')
        replica.until_getack()
        w.send('WAIT', '1', '0')
        assert not w.has_reply(0.2), name
        replica.ack()
        assert w.read() == 1, name
        assert wait_exit(proc) == 0, name
        assert c.read() is None, name
    finally:
        stop(proc)


def check_no_wait(name, binary, work):
    """SHUTDOWN NOW and shutdown-timeout 0 exit at once, with the lagging replica logged."""
    port = PORTS[name]
    for how in ('now', 'timeout0'):
        workdir = os.path.join(work, f'{name}-{how}')
        proc = start(binary, port, workdir)
        try:
            c = Conn(port)
            replica = FakeReplica(port)
            assert c('SET', 'k', '1') == '+OK'
            replica.command()
            if how == 'now':
                assert c('SHUTDOWN', 'NOW') is None, name
            else:
                assert c('CONFIG', 'SET', 'shutdown-timeout', '0') == '+OK'
                proc.send_signal(signal.SIGTERM)
            assert wait_exit(proc, timeout=2) == 0, (name, how)
            log = log_of(workdir)
            assert 'Waiting for replicas' not in log, (name, how)
            assert 'Lagging replica' in log, (name, how, log[-2000:])
            assert '0 of 1 replicas are in sync when shutting down.' in log, (name, how)
        finally:
            stop(proc)


def check_fail_after_wait(name, binary, work):
    """The final save fails once the replica is in sync: every blocked SHUTDOWN client gets the
    error, the writes held by the wait run, and the server keeps running."""
    port = PORTS[name]
    workdir = os.path.join(work, f'{name}-fail')
    proc = start(binary, port, workdir, '--save', '3600 1')
    try:
        c = Conn(port)
        replica = FakeReplica(port)
        assert c('INCR', 'k') == 1
        a, a2, b = Conn(port), Conn(port), Conn(port)
        a.send('SHUTDOWN')
        a2.send('SHUTDOWN')
        replica.until_getack()
        b.send('SET', 'x', '1')
        wait_for(lambda: info(c, 'clients')['blocked_clients'] == '3', f'{name}: blocked clients')
        os.chmod(workdir, stat.S_IRUSR | stat.S_IXUSR)
        try:
            replica.ack()
            assert a.read() == FAILED, name
            assert a2.read() == FAILED, name
            assert b.read() == '+OK', name
        finally:
            os.chmod(workdir, stat.S_IRWXU)
        assert proc.poll() is None, name
        log = log_of(workdir)
        for line in ('1 of 1 replicas are in sync when shutting down.',
                     "Error trying to save the DB, can't exit.",
                     'Errors trying to shut down the server. Check the logs for more information.'):
            assert line in log, (name, line, log[-2000:])
        assert 'shutdown_in_milliseconds' not in info(c, 'server'), name
        assert c('SET', 'y', '1') == '+OK', name
        assert c('SHUTDOWN', 'ABORT') == '-ERR No shutdown in progress.', name
    finally:
        stop(proc)


def check_engine(name, binary, work):
    check_wait_abort_timeout(name, binary, work)
    check_ack(name, binary, work)
    check_no_wait(name, binary, work)
    check_fail_after_wait(name, binary, work)
    print(f'{name}: ok')


def main():
    work = tempfile.mkdtemp(prefix='peadb-shutdown-replicas-')
    try:
        if REDIS:
            check_engine('redis', REDIS, work)
        else:
            print('no pinned redis-server: skipping the Redis side')
        check_engine('peadb', PEADB, work)
    finally:
        for root, dirs, _ in os.walk(work):
            for d in dirs:
                os.chmod(os.path.join(root, d), stat.S_IRWXU)
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()

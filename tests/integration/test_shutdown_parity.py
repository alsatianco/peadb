#!/usr/bin/env python3
"""SHUTDOWN, SIGTERM, SIGINT and SIGHUP behave like Redis 7.2.5 (compat/delta.md items 33 and 34).

The same checks run on peadb-server and on the pinned redis-server: SHUTDOWN checks its arguments (an unknown word, SAVE with NOSAVE, and ABORT with any other word are syntax errors), SHUTDOWN ABORT answers "No shutdown in progress." and the server keeps running, SIGHUP does not stop the server, a successful SHUTDOWN closes the connection with no reply, and when the final save fails SHUTDOWN answers "Errors trying to SHUTDOWN. Check logs.", logs why and keeps running, unless FORCE is given, which exits anyway.

check_signals(): SIGTERM and SIGINT do what SHUTDOWN does (with save points the data is saved and the exit code is 0), shutdown-on-sigterm and shutdown-on-sigint change that (nosave, save, force), SHUTDOWN ABORT cancels a signal the server has not acted on yet (DEBUG PAUSE-CRON holds it), a final save that fails on a signal keeps the server running, SAVE is refused while a BGSAVE runs, a shutdown stops a BGSAVE child and deletes its temp-<child pid>.rdb, and a second SIGINT during the final save exits at once with code 1 and deletes temp-<pid>.rdb. Without a pinned redis-server, only the PeaDB side runs.
"""
import os
import pathlib
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
PORTS = {'peadb': 6566, 'redis': 6568}
SYNTAX = '-ERR syntax error'


class Conn:
    def __init__(self, port, timeout=5):
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
            return [self.read() for _ in range(int(rest))]
        raise ValueError(f'unexpected reply {line!r}')

    def __call__(self, *args):
        out = f'*{len(args)}\r\n'.encode()
        for a in args:
            b = a.encode()
            out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
        self.sock.sendall(out)
        return self.read()


def start(binary, port, workdir, *extra):
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    log = open(log_path, 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                             '--loglevel', 'notice', *extra],
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


def check_engine(name, binary, work):
    port = PORTS[name]
    workdir = os.path.join(work, name)
    # One save point, so a plain SHUTDOWN saves (as Redis does by default).
    proc = start(binary, port, workdir, '--save', '3600 1')
    try:
        c = Conn(port)
        for args in (('SHUTDOWN', 'later'), ('SHUTDOWN', 'SAVE', 'NOSAVE'),
                     ('SHUTDOWN', 'nosave', 'save'), ('SHUTDOWN', 'ABORT', 'NOW'),
                     ('SHUTDOWN', 'FORCE', 'ABORT')):
            assert c(*args) == SYNTAX, (name, args)
        assert c('SHUTDOWN', 'ABORT') == '-ERR No shutdown in progress.', name
        assert c('SHUTDOWN', 'abort') == '-ERR No shutdown in progress.', name
        assert c('SHUTDOWN', 'ABORT', 'ABORT') == '-ERR No shutdown in progress.', name
        assert c('PING') == '+PONG', name

        # SIGHUP is ignored.
        proc.send_signal(signal.SIGHUP)
        time.sleep(0.3)
        assert proc.poll() is None, f'{name}: SIGHUP stopped the server'
        assert c('PING') == '+PONG', name

        # A failing final save keeps the server running and is logged; FORCE exits anyway.
        assert c('SET', 'k', 'v') == '+OK'
        os.chmod(workdir, stat.S_IRUSR | stat.S_IXUSR)
        try:
            assert c('SHUTDOWN') == '-ERR Errors trying to SHUTDOWN. Check logs.', name
            assert c('SHUTDOWN', 'SAVE') == '-ERR Errors trying to SHUTDOWN. Check logs.', name
            assert proc.poll() is None, f'{name}: exited after a failed save'
            assert c('PING') == '+PONG', name
            log = open(os.path.join(workdir, 'server.log'), errors='replace').read()
            assert "Error trying to save the DB, can't exit." in log, (name, log[-2000:])
            # A successful SHUTDOWN closes the connection without a reply.
            assert c('SHUTDOWN', 'SAVE', 'FORCE') is None, name
            proc.wait(timeout=10)
            log = open(os.path.join(workdir, 'server.log'), errors='replace').read()
            assert 'Error trying to save the DB. Exit anyway.' in log, (name, log[-2000:])
        finally:
            os.chmod(workdir, stat.S_IRWXU)
        c.close()
    finally:
        stop(proc)

    # SHUTDOWN NOSAVE: no reply, the process exits, nothing is saved. The start counts as the
    # last save, so `save 3600 1` does not save after the first write either.
    proc = start(binary, port, workdir, '--save', '3600 1', '--dbfilename', 'nosave.rdb')
    try:
        c = Conn(port)
        info = c('INFO', 'persistence')
        last_save = int(info.split('rdb_last_save_time:')[1].split()[0])
        assert abs(last_save - time.time()) < 60, (name, last_save)
        assert c('SET', 'k', 'v') == '+OK'
        assert c('SHUTDOWN', 'NOSAVE') is None, name
        proc.wait(timeout=10)
        assert not os.path.exists(os.path.join(workdir, 'nosave.rdb')), name
        c.close()
    finally:
        stop(proc)

    # A plain SHUTDOWN with save points saves before it exits.
    proc = start(binary, port, workdir, '--save', '3600 1', '--dbfilename', 'final.rdb')
    try:
        c = Conn(port)
        assert c('SET', 'k', 'v') == '+OK'
        assert c('SHUTDOWN') is None, name
        proc.wait(timeout=10)
        assert os.path.exists(os.path.join(workdir, 'final.rdb')), name
        c.close()
    finally:
        stop(proc)
    print(f'{name}: ok')


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


def info_field(c, section, field):
    info = c('INFO', section)
    return info.split(field + ':')[1].split()[0]


def temp_rdbs(workdir):
    return sorted(f for f in os.listdir(workdir) if f.startswith('temp-') and f.endswith('.rdb'))


def check_signals(name, binary, work):
    """SIGTERM and SIGINT shut the server down like the SHUTDOWN command (Redis's
    sigShutdownHandler, then prepareForShutdown from serverCron)."""
    port = PORTS[name]

    # With save points, SIGTERM and SIGINT save the data before exiting, with exit code 0.
    for sig, word in ((signal.SIGTERM, 'SIGTERM'), (signal.SIGINT, 'SIGINT')):
        workdir = os.path.join(work, f'{name}-{word}')
        proc = start(binary, port, workdir, '--save', '3600 1')
        try:
            c = Conn(port)
            assert int(info_field(c, 'server', 'process_id')) == proc.pid, name
            for i in range(1001):
                assert c('SET', f'k{i}', f'v{i}') == '+OK'
            c.close()
            proc.send_signal(sig)
            assert wait_exit(proc) == 0, (name, word)
            log = log_of(workdir)
            for line in (f'Received {word} scheduling shutdown...', 'User requested shutdown...',
                         'Saving the final RDB snapshot before exiting.',
                         'ready to exit, bye bye...'):
                assert line in log, (name, word, line, log[-2000:])
        finally:
            stop(proc)
        proc = start(binary, port, workdir)
        try:
            c = Conn(port)
            assert c('DBSIZE') == 1001, (name, word)
            c.close()
        finally:
            stop(proc)

    # shutdown-on-sigterm and shutdown-on-sigint change what the signal does: nosave skips the
    # save that save points ask for, save saves without save points.
    workdir = os.path.join(work, f'{name}-sigflags')
    proc = start(binary, port, workdir, '--save', '3600 1', '--shutdown-on-sigterm', 'nosave')
    try:
        c = Conn(port)
        assert c('CONFIG', 'GET', 'shutdown-on-sigterm') == ['shutdown-on-sigterm', 'nosave'], name
        assert c('SET', 'k', 'v') == '+OK'
        c.close()
        proc.send_signal(signal.SIGTERM)
        assert wait_exit(proc) == 0, name
        assert not os.path.exists(os.path.join(workdir, 'dump.rdb')), name
    finally:
        stop(proc)
    proc = start(binary, port, workdir, '--save', '')
    try:
        c = Conn(port)
        assert c('CONFIG', 'SET', 'shutdown-on-sigint', 'save') == '+OK'
        assert c('SET', 'k', 'v') == '+OK'
        c.close()
        proc.send_signal(signal.SIGINT)
        assert wait_exit(proc) == 0, name
        assert os.path.exists(os.path.join(workdir, 'dump.rdb')), name
        os.unlink(os.path.join(workdir, 'dump.rdb'))
    finally:
        stop(proc)
    # Without save points and with the default flags, a signal saves nothing.
    proc = start(binary, port, workdir, '--save', '')
    try:
        c = Conn(port)
        assert c('SET', 'k', 'v') == '+OK'
        c.close()
        proc.send_signal(signal.SIGTERM)
        assert wait_exit(proc) == 0, name
        assert not os.path.exists(os.path.join(workdir, 'dump.rdb')), name
    finally:
        stop(proc)

    # SHUTDOWN ABORT cancels a signal the cron has not handled yet.
    workdir = os.path.join(work, f'{name}-abort')
    proc = start(binary, port, workdir, '--save', '3600 1', '--enable-debug-command', 'yes')
    try:
        c = Conn(port)
        assert c('DEBUG', 'PAUSE-CRON', '1') == '+OK'
        proc.send_signal(signal.SIGTERM)
        time.sleep(0.2)
        assert c('SHUTDOWN', 'ABORT') == '+OK', name
        assert c('SHUTDOWN', 'ABORT') == '-ERR No shutdown in progress.', name
        assert 'Shutdown manually aborted.' in log_of(workdir), name
        assert c('DEBUG', 'PAUSE-CRON', '0') == '+OK'
        time.sleep(0.5)
        assert proc.poll() is None, f'{name}: exited after SHUTDOWN ABORT'
        assert c('PING') == '+PONG', name
        c.close()
    finally:
        stop(proc)

    # A final save that fails on a signal is logged and the server keeps running (the signal is
    # forgotten); shutdown-on-sigterm force exits anyway.
    workdir = os.path.join(work, f'{name}-failsave')
    proc = start(binary, port, workdir, '--save', '3600 1')
    try:
        c = Conn(port)
        assert c('SET', 'k', 'v') == '+OK'
        os.makedirs(os.path.join(workdir, 'dump.rdb'))  # the rename onto it fails
        proc.send_signal(signal.SIGTERM)
        wait_for(lambda: "Error trying to save the DB, can't exit." in log_of(workdir),
                 f'{name}: the failed-save log line')
        time.sleep(0.3)
        assert proc.poll() is None, f'{name}: exited after a failed save on SIGTERM'
        assert c('PING') == '+PONG', name
        assert not temp_rdbs(workdir), (name, temp_rdbs(workdir))
        assert c('CONFIG', 'SET', 'shutdown-on-sigterm', 'force') == '+OK'
        c.close()
        proc.send_signal(signal.SIGTERM)
        assert wait_exit(proc) == 0, name
        assert 'Error trying to save the DB. Exit anyway.' in log_of(workdir), name
    finally:
        stop(proc)

    # SAVE is refused while a BGSAVE runs, and a shutdown stops the BGSAVE child and deletes its
    # temp-<child pid>.rdb before the server exits.
    workdir = os.path.join(work, f'{name}-bgsave')
    proc = start(binary, port, workdir, '--save', '')
    try:
        c = Conn(port)
        for i in range(20):
            assert c('SET', f'k{i}', 'v') == '+OK'
        assert c('CONFIG', 'SET', 'rdb-key-save-delay', '1000000') == '+OK'
        assert c('BGSAVE') == '+Background saving started', name
        wait_for(lambda: info_field(c, 'persistence', 'rdb_bgsave_in_progress') == '1', 'bgsave')
        wait_for(lambda: temp_rdbs(workdir), f'{name}: the BGSAVE temp file')
        temps = temp_rdbs(workdir)
        child = int(temps[0][len('temp-'):-len('.rdb')])
        assert child != proc.pid, (name, temps)
        assert c('SAVE') == '-ERR Background save already in progress', name
        assert c('SHUTDOWN', 'NOSAVE') is None, name
        assert wait_exit(proc) == 0, name
        assert not temp_rdbs(workdir), (name, temp_rdbs(workdir))
        assert 'There is a child saving an .rdb. Killing it!' in log_of(workdir), name

        def child_gone():
            try:
                os.kill(child, 0)
                return False
            except ProcessLookupError:
                return True
        wait_for(child_gone, f'{name}: the BGSAVE child to stop')
        assert not os.path.exists(os.path.join(workdir, 'dump.rdb')), name
    finally:
        stop(proc)

    # A shutdown during a BGSAVE: the child is stopped before the final save, so it cannot rename
    # its older snapshot over it later, and the writes made during the BGSAVE survive.
    workdir = os.path.join(work, f'{name}-bgsave-final')
    proc = start(binary, port, workdir, '--save', '')
    try:
        c = Conn(port)
        for i in range(5):
            assert c('SET', f'early{i}', 'v') == '+OK'
        assert c('CONFIG', 'SET', 'rdb-key-save-delay', '1000000') == '+OK'
        assert c('BGSAVE') == '+Background saving started', name
        wait_for(lambda: temp_rdbs(workdir), f'{name}: the BGSAVE temp file')
        child = int(temp_rdbs(workdir)[0][len('temp-'):-len('.rdb')])
        assert c('CONFIG', 'SET', 'rdb-key-save-delay', '0') == '+OK'  # the child keeps its own
        for i in range(5):
            assert c('SET', f'late{i}', 'v') == '+OK'
        assert c('SHUTDOWN', 'SAVE') is None, name
        assert wait_exit(proc) == 0, name

        def child_gone():
            try:
                os.kill(child, 0)
                return False
            except ProcessLookupError:
                return True
        wait_for(child_gone, f'{name}: the BGSAVE child to stop', timeout=15)
        assert not temp_rdbs(workdir), (name, temp_rdbs(workdir))
    finally:
        stop(proc)
    proc = start(binary, port, workdir)
    try:
        c = Conn(port)
        assert c('DBSIZE') == 10, name
        assert c('GET', 'late4') == 'v', name
        c.close()
    finally:
        stop(proc)

    # No command runs after a successful SHUTDOWN, even one pipelined behind it.
    workdir = os.path.join(work, f'{name}-pipeline')
    proc = start(binary, port, workdir, '--save', '', '--appendonly', 'yes',
                 '--appendfsync', 'always')
    try:
        c = Conn(port)
        c.sock.sendall(b'*3\r\n$3\r\nSET\r\n$5\r\nfirst\r\n$1\r\n1\r\n*1\r\n$8\r\nSHUTDOWN\r\n'
                       b'*3\r\n$3\r\nSET\r\n$4\r\nlate\r\n$1\r\n1\r\n')
        # Redis exits inside SHUTDOWN before it sends the SET's +OK; PeaDB sends it first. Either
        # way the connection then closes with no reply to SHUTDOWN or to the late SET.
        first = c.read()
        assert first in ('+OK', None), (name, first)
        if first is not None:
            assert c.read() is None, name
        assert wait_exit(proc) == 0, name
        c.close()
    finally:
        stop(proc)
    proc = start(binary, port, workdir, '--save', '', '--appendonly', 'yes')
    try:
        c = Conn(port)
        assert c('GET', 'first') == '1', name
        assert c('GET', 'late') is None, name
        c.close()
    finally:
        stop(proc)

    # A signal during the startup load exits without saving, even with save points. The check
    # needs the load to be still running when the signal arrives; on a machine fast enough to
    # finish first it reports that and checks nothing.
    workdir = os.path.join(work, f'{name}-loading')
    proc = start(binary, port, workdir, '--save', '', '--enable-debug-command', 'yes')
    try:
        c = Conn(port, timeout=120)
        assert c('DEBUG', 'POPULATE', '2000000') == '+OK', name
        assert c('SAVE') == '+OK', name
        assert c('SHUTDOWN', 'NOSAVE') is None, name
        assert wait_exit(proc, timeout=60) == 0, name
    finally:
        stop(proc)
    dump = os.path.join(workdir, 'dump.rdb')
    before = os.stat(dump).st_mtime_ns
    log_path = os.path.join(workdir, 'server.log')
    log_start = os.path.getsize(log_path)
    with open(log_path, 'ab') as log:
        proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir',
                                 workdir, '--loglevel', 'notice', '--save', '3600 1'],
                                stdout=log, stderr=subprocess.STDOUT)
    try:
        def new_log():
            with open(log_path, errors='replace') as f:
                f.seek(log_start)
                return f.read()
        wait_for(lambda: 'Server initialized' in new_log(), f'{name}: startup', timeout=30)
        proc.send_signal(signal.SIGTERM)
        assert wait_exit(proc, timeout=120) == 0, name
        text = new_log()
        if 'Received shutdown signal during loading' not in text:
            print(f'{name}: the load ended before the signal; startup-load check skipped')
        else:
            assert 'Ready to accept' not in text, (name, text[-2000:])
            assert 'Saving the final RDB snapshot' not in text, (name, text[-2000:])
            assert os.stat(dump).st_mtime_ns == before, name
    finally:
        stop(proc)

    # A second SIGINT during the final save exits at once with code 1 and deletes the server's
    # temp-<pid>.rdb.
    workdir = os.path.join(work, f'{name}-insist')
    proc = start(binary, port, workdir, '--save', '3600 1')
    try:
        c = Conn(port)
        for i in range(20):
            assert c('SET', f'k{i}', 'v') == '+OK'
        assert c('CONFIG', 'SET', 'rdb-key-save-delay', '100000') == '+OK'
        c.close()
        temp = os.path.join(workdir, f'temp-{proc.pid}.rdb')
        proc.send_signal(signal.SIGINT)
        wait_for(lambda: os.path.exists(temp), f'{name}: the final-save temp file', timeout=1.5)
        proc.send_signal(signal.SIGINT)
        assert wait_exit(proc) == 1, name
        assert not os.path.exists(temp), name
        assert 'You insist... exiting now.' in log_of(workdir), name
        assert not os.path.exists(os.path.join(workdir, 'dump.rdb')), name
    finally:
        stop(proc)
    print(f'{name}: signals ok')


def main():
    work = tempfile.mkdtemp(prefix='peadb-shutdown-')
    try:
        if REDIS:
            check_engine('redis', REDIS, work)
            check_signals('redis', REDIS, work)
        else:
            print('no pinned redis-server: skipping the Redis side')
        check_engine('peadb', PEADB, work)
        check_signals('peadb', PEADB, work)
    finally:
        for root, dirs, _ in os.walk(work):
            for d in dirs:
                os.chmod(os.path.join(root, d), stat.S_IRWXU)
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()

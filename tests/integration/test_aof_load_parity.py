#!/usr/bin/env python3
"""Startup loads the data the way Redis 7.2.5 does when the AOF is on (compat/delta.md item 28).

The same checks run on peadb-server and on the pinned redis-server. With `appendonly yes` the server loads only the AOF and never the RDB as well (Redis's loadDataFromDisk()), so a restart after SAVE or SHUTDOWN SAVE does not apply INCR, HINCRBY, RPUSH, LPOP or APPEND a second time, in any database. A dump.rdb alone is not loaded when the AOF is on, even when no AOF exists yet: the server starts with no data, as Redis does (PeaDB also logs a warning naming the file it did not load). A server whose AOF cannot be opened refuses to start instead of starting with no data. Without a pinned redis-server, only the PeaDB side runs.
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
PORTS = {'peadb': 6584, 'redis': 6585}


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=5)
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
            b = str(a).encode()
            out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
        self.sock.sendall(out)
        return self.read()


def start(binary, port, workdir, *extra):
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    log = open(log_path, 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                             '--loglevel', 'notice', '--save', '', *extra],
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


def shutdown(proc, port, *args):
    c = Conn(port)
    assert c('SHUTDOWN', *args) is None
    c.close()
    proc.wait(timeout=10)


def read_log(workdir):
    return open(os.path.join(workdir, 'server.log'), errors='replace').read()


def snapshot(c):
    state = {
        'counter': c('GET', 'counter'),
        'balance': c('HGET', 'acct', 'balance'),
        'queue': c('LRANGE', 'queue', 0, -1),
        'log': c('GET', 'log'),
        'dbsize0': c('DBSIZE'),
    }
    assert c('SELECT', 1) == '+OK'
    state['other'] = c('GET', 'other')
    state['dbsize1'] = c('DBSIZE')
    assert c('SELECT', 0) == '+OK'
    return state


def check_aof_and_dump(name, binary, work):
    """AOF on and a dump.rdb present: a restart loads the AOF only, so nothing is applied twice."""
    port = PORTS[name]
    workdir = os.path.join(work, name, 'both')
    aof_args = ('--appendonly', 'yes', '--appendfsync', 'always')
    proc = start(binary, port, workdir, *aof_args)
    try:
        c = Conn(port)
        for _ in range(3):
            c('INCR', 'counter')
        assert c('HINCRBY', 'acct', 'balance', 100) == 100
        assert c('RPUSH', 'queue', 'job1', 'job2', 'job3') == 3
        assert c('LPOP', 'queue') == 'job1'
        assert c('APPEND', 'log', 'ab') == 2
        assert c('SELECT', 1) == '+OK'
        assert c('INCR', 'other') == 1
        assert c('SELECT', 0) == '+OK'
        assert c('SAVE') == '+OK'  # dump.rdb now holds everything above
        assert c('INCR', 'counter') == 4  # a write after the SAVE: only the AOF has it
        expected = snapshot(c)
        assert expected == {'counter': '4', 'balance': '100', 'queue': ['job2', 'job3'],
                            'log': 'ab', 'dbsize0': 4, 'other': '1', 'dbsize1': 1}, (name, expected)
        c.close()
        shutdown(proc, port, 'SAVE')  # the dump now holds every write, and so does the AOF
    finally:
        stop(proc)
    assert os.path.exists(os.path.join(workdir, 'dump.rdb')), name

    # Two restarts in a row: the data must be the same each time.
    for restart in (1, 2):
        proc = start(binary, port, workdir, *aof_args)
        try:
            c = Conn(port)
            got = snapshot(c)
            assert got == expected, (name, restart, got, expected)
            c.close()
            shutdown(proc, port, 'SAVE')
        finally:
            stop(proc)


def check_dump_only(name, binary, work):
    """A dump.rdb alone is not loaded once the AOF is on: the server starts with no data."""
    port = PORTS[name]
    workdir = os.path.join(work, name, 'dump-only')
    proc = start(binary, port, workdir, '--appendonly', 'no')
    try:
        c = Conn(port)
        assert c('SET', 'k', 'v') == '+OK'
        assert c('SAVE') == '+OK'
        c.close()
        shutdown(proc, port, 'NOSAVE')
    finally:
        stop(proc)

    proc = start(binary, port, workdir, '--appendonly', 'yes')
    try:
        c = Conn(port)
        assert c('DBSIZE') == 0, f'{name}: the RDB was loaded although the AOF is on'
        assert c('SET', 'k2', 'v2') == '+OK'
        c.close()
        shutdown(proc, port, 'NOSAVE')
    finally:
        stop(proc)
    if name == 'peadb':
        log = read_log(workdir)
        assert 'dump.rdb is not loaded' in log, log[-2000:]

    # The next restart loads what the AOF holds, and still not the RDB.
    proc = start(binary, port, workdir, '--appendonly', 'yes')
    try:
        c = Conn(port)
        assert c('GET', 'k2') == 'v2', name
        assert c('GET', 'k') is None, name
        assert c('DBSIZE') == 1, name
        c.close()
    finally:
        stop(proc)

    # With the AOF off again, the untouched dump.rdb loads.
    proc = start(binary, port, workdir, '--appendonly', 'no')
    try:
        c = Conn(port)
        assert c('GET', 'k') == 'v', name
        assert c('DBSIZE') == 1, name
        c.close()
    finally:
        stop(proc)


def expect_start_fails(name, binary, port, workdir, *extra):
    """Starts the server and expects it to exit with an error; returns the new log text."""
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    before = os.path.getsize(log_path) if os.path.exists(log_path) else 0
    with open(log_path, 'ab') as log:
        proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                                 '--loglevel', 'notice', '--save', '', *extra],
                                stdout=log, stderr=subprocess.STDOUT)
    try:
        rc = proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        stop(proc)
        raise AssertionError(f'{name}: started although it should not ({extra})')
    assert rc != 0, (name, rc)
    return read_log(workdir)[before:]


def check_aof_cannot_open(name, binary, work):
    """An AOF that cannot be opened stops startup instead of starting with no data."""
    workdir = os.path.join(work, name, 'cannot-open')
    os.makedirs(workdir, exist_ok=True)
    # Redis keeps its AOF in the directory `appendonlydir`, PeaDB in the file `appendonly.aof`;
    # a regular file in place of the directory, or a directory in place of the file, blocks it.
    if name == 'redis':
        open(os.path.join(workdir, 'appendonlydir'), 'w').close()
    else:
        os.makedirs(os.path.join(workdir, 'appendonly.aof'))
    log = expect_start_fails(name, binary, PORTS[name], workdir, '--appendonly', 'yes')
    if name == 'peadb':
        assert "Can't open the append-only file appendonly.aof" in log, log[-2000:]


def aof_file(name, workdir):
    """The file the server appends to: PeaDB's single file, or Redis's last INCR file."""
    if name == 'peadb':
        return os.path.join(workdir, 'appendonly.aof')
    d = os.path.join(workdir, 'appendonlydir')
    with open(os.path.join(d, 'appendonly.aof.manifest')) as f:
        incr = [ln.split()[1] for ln in f if ln.split() and ln.split()[-1] == 'i']
    return os.path.join(d, incr[-1])


def set_keys(port, prefix, count):
    c = Conn(port)
    for i in range(count):
        assert c('SET', f'{prefix}{i}', f'v{i}') == '+OK'
    c.close()


def check_torn_tail(name, binary, work):
    """A torn last command is cut off the file, so the next restart works too (delta item 30)."""
    port = PORTS[name]
    workdir = os.path.join(work, name, 'torn')
    aof_args = ('--appendonly', 'yes', '--appendfsync', 'always')
    proc = start(binary, port, workdir, *aof_args)
    try:
        set_keys(port, 'k', 100)
        shutdown(proc, port, 'NOSAVE')
    finally:
        stop(proc)
    path = aof_file(name, workdir)
    data = open(path, 'rb').read()
    last = data.rfind(b'*3\r\n$3\r\nSET\r\n$3\r\nk99\r\n')
    assert last > 0, name
    with open(path, 'wb') as f:
        f.write(data[:-5])  # a crash in the middle of the last write

    # Restart 1 drops the torn command and cuts the file at the last whole one.
    proc = start(binary, port, workdir, *aof_args)
    try:
        c = Conn(port)
        assert c('DBSIZE') == 99, name
        assert c('GET', 'k98') == 'v98' and c('GET', 'k99') is None, name
        c.close()
        assert os.path.getsize(path) == last, (name, os.path.getsize(path), last)
        log = read_log(workdir)
        assert f'Truncating the AOF {os.path.basename(path)} at offset {last}' in log, (name, log[-2000:])
        assert 'loaded anyway because aof-load-truncated is enabled' in log, (name, log[-2000:])
        set_keys(port, 'n', 50)
        shutdown(proc, port, 'NOSAVE')
    finally:
        stop(proc)

    # Restart 2 loads every write made after restart 1.
    proc = start(binary, port, workdir, *aof_args)
    try:
        c = Conn(port)
        assert c('DBSIZE') == 149, name
        assert c('GET', 'n49') == 'v49', name
        c.close()
        shutdown(proc, port, 'NOSAVE')
    finally:
        stop(proc)

    # With aof-load-truncated no, a torn tail stops startup and the file is left as it is.
    data = open(path, 'rb').read()
    with open(path, 'wb') as f:
        f.write(data[:-5])
    log = expect_start_fails(name, binary, port, workdir, *aof_args, '--aof-load-truncated', 'no')
    assert 'Unexpected end of file reading the append only file' in log, (name, log[-2000:])
    assert os.path.getsize(path) == len(data) - 5, name
    proc = start(binary, port, workdir, *aof_args)
    try:
        c = Conn(port)
        assert c('DBSIZE') == 148, name
        c.close()
        shutdown(proc, port, 'NOSAVE')
    finally:
        stop(proc)

    # A MULTI with no EXEC at the end is cut off, so it cannot hold the next writes in its queue.
    size = os.path.getsize(path)
    with open(path, 'ab') as f:
        f.write(b'*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$1\r\nm\r\n$1\r\n1\r\n')
    proc = start(binary, port, workdir, *aof_args)
    try:
        c = Conn(port)
        assert c('GET', 'm') is None, name
        assert c('DBSIZE') == 148, name
        assert c('SET', 'after', '1') == '+OK'
        c.close()
        assert 'Revert incomplete MULTI/EXEC transaction' in read_log(workdir), name
        shutdown(proc, port, 'NOSAVE')
    finally:
        stop(proc)
    assert os.path.getsize(path) > size, name  # cut back to `size`, then `after` appended
    proc = start(binary, port, workdir, *aof_args)
    try:
        c = Conn(port)
        assert c('GET', 'after') == '1', name
        assert c('DBSIZE') == 149, name
        c.close()
        shutdown(proc, port, 'NOSAVE')
    finally:
        stop(proc)

    # A `#` annotation line is skipped.
    data = open(path, 'rb').read()
    with open(path, 'wb') as f:
        f.write(data + b'#TS:1700000000\r\n*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n2\r\n')
    proc = start(binary, port, workdir, *aof_args)
    try:
        c = Conn(port)
        assert c('GET', 'a') == '2', name
        assert c('DBSIZE') == 150, name
        c.close()
        shutdown(proc, port, 'NOSAVE')
    finally:
        stop(proc)

    # Bytes that are not a command stop startup, wherever they are, and are not cut off.
    data = open(path, 'rb').read()
    with open(path, 'wb') as f:
        f.write(data + b'\x00\x01garbage\r\n')
    log = expect_start_fails(name, binary, port, workdir, *aof_args)
    assert 'Bad file format reading the append only file' in log, (name, log[-2000:])
    assert open(path, 'rb').read() == data + b'\x00\x01garbage\r\n', name


def check_torn_inside_multi(work):
    """PeaDB only: a short read inside a MULTI cuts the whole MULTI off.

    Redis 7.2.5 cuts after the last whole command there, which leaves an open MULTI at the end of
    the file; every command appended after the restart is then queued in it on the next load and
    dropped with it. PeaDB cuts back to before the MULTI instead.
    """
    port = PORTS['peadb']
    workdir = os.path.join(work, 'peadb', 'torn-multi')
    aof_args = ('--appendonly', 'yes', '--appendfsync', 'always')
    proc = start(PEADB, port, workdir, *aof_args)
    try:
        set_keys(port, 'k', 3)
        shutdown(proc, port, 'NOSAVE')
    finally:
        stop(proc)
    path = aof_file('peadb', workdir)
    size = os.path.getsize(path)
    with open(path, 'ab') as f:
        f.write(b'*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$1\r\nm\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$1\r\nn')
    proc = start(PEADB, port, workdir, *aof_args)
    try:
        c = Conn(port)
        assert c('DBSIZE') == 3 and c('GET', 'm') is None
        c.close()
        assert os.path.getsize(path) == size
        set_keys(port, 'after', 2)
        shutdown(proc, port, 'NOSAVE')
    finally:
        stop(proc)
    proc = start(PEADB, port, workdir, *aof_args)
    try:
        c = Conn(port)
        assert c('DBSIZE') == 5 and c('GET', 'after1') == 'v1'
        c.close()
    finally:
        stop(proc)


def check_damaged_by_old_peadb(work):
    """PeaDB only: torn bytes in the middle of the file stop startup at the torn command.

    Older PeaDB versions kept a torn last command and appended after it. The reader checks the
    lines the way redis-check-aof does, so the torn command is a format error where it starts,
    instead of being read together with the next command's bytes as a different command.
    """
    port = PORTS['peadb']
    workdir = os.path.join(work, 'peadb', 'damaged')
    os.makedirs(workdir, exist_ok=True)
    good = b'*3\r\n$3\r\nSET\r\n$2\r\nk8\r\n$2\r\nv8\r\n'
    torn = b'*3\r\n$3\r\nSET\r\n$2\r\nk9\r\n$2\r'
    later = b'*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n*3\r\n$3\r\nSET\r\n$2\r\nn0\r\n$2\r\nv0\r\n'
    with open(os.path.join(workdir, 'appendonly.aof'), 'wb') as f:
        f.write(good + torn + later)
    log = expect_start_fails('peadb', PEADB, port, workdir, '--appendonly', 'yes')
    assert f'Bad file format reading the append only file appendonly.aof at offset {len(good)}' in log, \
        log[-2000:]


def check_engine(name, binary, work):
    check_aof_and_dump(name, binary, work)
    check_dump_only(name, binary, work)
    check_aof_cannot_open(name, binary, work)
    check_torn_tail(name, binary, work)
    if name == 'peadb':
        check_torn_inside_multi(work)
        check_damaged_by_old_peadb(work)
    print(f'{name}: ok')


def main():
    work = tempfile.mkdtemp(prefix='peadb-aofload-')
    try:
        if REDIS:
            check_engine('redis', REDIS, work)
        else:
            print('no pinned redis-server: skipping the Redis side')
        check_engine('peadb', PEADB, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()

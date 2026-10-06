#!/usr/bin/env python3
"""BGSAVE, SAVE, FLUSHALL and LASTSAVE report and keep saves like Redis 7.2.5 (compat/delta.md item 34).

The same checks run on peadb-server and on the pinned redis-server (only a Redis 7.2.x; without one, only the PeaDB side runs).

check_fork_failure(): a fork() that fails (forced by a small library put in front of the C library, which makes fork() fail while a control file exists) gives BGSAVE a bare -ERR, logs "Can't save in background: fork: ...", sets rdb_last_bgsave_status:err and leaves the save time and the change count alone; then writes, PING and an EXEC whose transaction writes are refused with Redis's -MISCONF text, reads are not, scripts that declare flags are refused unless no-writes, stop-writes-on-bgsave-error no lets writes through, and the save points try again only 5 s after the last try. Skipped when the library cannot be built or does not take effect.

check_child_error(): a BGSAVE child that cannot write logs Redis's reason with its pid, the parent logs "Background saving error" and refuses writes; SAVE and DEBUG RELOAD answer a bare -ERR and log why; the next good BGSAVE clears it all. Skipped as root (a read-only directory does not stop root).

check_flushall(): FLUSHALL stops a running BGSAVE (not counted as a failed one); with save points it also saves the empty dataset at once, so the keys of an older dump.rdb do not come back on restart, and the change count is 0 after it.

check_schedule(): BGSAVE [SCHEDULE] arguments, BGSAVE inside MULTI/EXEC waits until EXEC ends (the snapshot holds the whole transaction), BGSAVE during an AOF rewrite is refused or scheduled, the INFO fields rdb_last_bgsave_time_sec, rdb_current_bgsave_time_sec and rdb_saves, CONFIG RESETSTAT, and the log lines of a good BGSAVE.

check_child_sockets(): CLIENT KILL during a BGSAVE closes the connection at once, and when the server dies while a BGSAVE or AOF rewrite child runs, the port is free at once (the child has no listener). PeaDB only: a client connected before the fork also sees its connection close then (Redis keeps the client sockets in the child until it ends).

check_replica_keeps_applying(), PeaDB only: a replica whose own BGSAVE failed refuses its clients' PING with -MISCONF but still applies what its master sends, MULTI/EXEC blocks included.
"""
import errno
import os
import pathlib
import re
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
PORTS = {'peadb': 6620, 'redis': 6621}
REPLICA_PORT = 6622
SYNTAX = '-ERR syntax error'
MISCONF = ("-MISCONF Redis is configured to save RDB snapshots, but it's currently unable to "
           "persist to disk. Commands that may modify the data set are disabled, because this "
           "instance is configured to report errors during writes if RDB snapshotting fails "
           "(stop-writes-on-bgsave-error option). Please check the Redis logs for details about "
           "the RDB error.")
AOF_CHILD_ACTIVE = ("-ERR Another child process is active (AOF?): can't BGSAVE right now. Use "
                    "BGSAVE SCHEDULE in order to schedule a BGSAVE whenever possible.")

# fork() fails with ENOMEM while the file named by FAILFORK_FILE exists. macOS interposes it
# (DYLD_INSERT_LIBRARIES), Linux puts it first in the symbol search (LD_PRELOAD).
SHIM_SOURCE = r'''
#define _GNU_SOURCE
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

static int should_fail(void) {
  const char* path = getenv("FAILFORK_FILE");
  return path != NULL && access(path, F_OK) == 0;
}

#ifdef __APPLE__
static pid_t failfork_fork(void) {
  if (should_fail()) {
    errno = ENOMEM;
    return -1;
  }
  return fork();
}
__attribute__((used)) static const struct {
  const void* replacement;
  const void* replacee;
} interposers[] __attribute__((section("__DATA,__interpose"))) = {
    {(const void*)&failfork_fork, (const void*)&fork},
};
#else
#include <dlfcn.h>
pid_t fork(void) {
  static pid_t (*real_fork)(void) = NULL;
  if (should_fail()) {
    errno = ENOMEM;
    return -1;
  }
  if (real_fork == NULL) real_fork = (pid_t(*)(void))dlsym(RTLD_NEXT, "fork");
  return real_fork();
}
#endif
'''


def redis_7_2():
    """The redis-server to compare with, only if it is Redis 7.2.x."""
    path = redis_server_path()
    if not path:
        return None
    version = subprocess.run([path, '--version'], capture_output=True, text=True).stdout
    return path if ' v=7.2.' in version else None


REDIS = redis_7_2()


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

    def send(self, *args):
        out = f'*{len(args)}\r\n'.encode()
        for a in args:
            b = a.encode()
            out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
        self.sock.sendall(out)

    def __call__(self, *args):
        self.send(*args)
        return self.read()

    def closed_within(self, seconds):
        """True when the server closes this connection within `seconds`."""
        self.sock.settimeout(seconds)
        try:
            return self.sock.recv(16) == b''
        except ConnectionResetError:
            return True
        except socket.timeout:
            return False


def start(binary, port, workdir, *extra, env=None):
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    log = open(log_path, 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                             '--loglevel', 'notice', *extra],
                            stdout=log, stderr=subprocess.STDOUT, env=env)
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


def wait_for(cond, what, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.05)
    raise AssertionError(f'timed out waiting for {what}')


def info(c, field):
    for line in c('INFO', 'persistence').split('\r\n'):
        if line.startswith(field + ':'):
            return line.split(':', 1)[1]
    raise AssertionError(f'INFO has no {field}')


def log_of(workdir):
    return open(os.path.join(workdir, 'server.log'), errors='replace').read()


def child_pids(workdir):
    return [int(p) for p in re.findall(r'Background saving started by pid (\d+)', log_of(workdir))]


def kill_quietly(pid):
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def fill(c, count, prefix='k'):
    """SET <prefix>0 .. <prefix><count-1>, pipelined."""
    for i in range(count):
        c.send('SET', f'{prefix}{i}', 'v')
    for _ in range(count):
        assert c.read() == '+OK'


def build_shim(work):
    src = os.path.join(work, 'failfork.c')
    lib = os.path.join(work, 'failfork.so')
    with open(src, 'w') as f:
        f.write(SHIM_SOURCE)
    cmd = ['cc', '-shared', '-fPIC', '-o', lib, src]
    if sys.platform.startswith('linux'):
        cmd.append('-ldl')
    try:
        built = subprocess.run(cmd, capture_output=True, text=True)
    except OSError:
        return None
    return lib if built.returncode == 0 else None


def shim_env(shim, flag):
    env = dict(os.environ, FAILFORK_FILE=flag)
    env['DYLD_INSERT_LIBRARIES' if sys.platform == 'darwin' else 'LD_PRELOAD'] = shim
    # An ASan build (the nightly job) refuses to start when another library loads before its
    # runtime.
    asan = env.get('ASAN_OPTIONS', '')
    env['ASAN_OPTIONS'] = (asan + ':' if asan else '') + 'verify_asan_link_order=0'
    return env


def start_with_shim(name, binary, port, workdir, shim, flag, *extra):
    """start() with the fork-failure library, or None (and a note) when the server will not run
    with it."""
    try:
        return start(binary, port, workdir, *extra, env=shim_env(shim, flag))
    except RuntimeError as e:
        print(f'{name}: the server does not start with the fork-failure library ({e}): skipped')
        return None


def check_fork_failure(name, binary, work, shim):
    port = PORTS[name]
    workdir = os.path.join(work, f'{name}-fork')
    flag = os.path.join(workdir, 'fail-fork')
    proc = start_with_shim(name, binary, port, workdir, shim, flag, '--save', '3600 1')
    if proc is None:
        return
    try:
        c, reader, pinger = Conn(port), Conn(port), Conn(port)
        assert c('SET', 'a', '1') == '+OK'
        t0 = c('LASTSAVE')
        assert info(c, 'rdb_last_save_time') == str(t0), name
        # Transactions queued before the failure, checked at EXEC below.
        assert c('MULTI') == '+OK' and c('SET', 'queued', '1') == '+QUEUED'
        assert pinger('MULTI') == '+OK' and pinger('PING') == '+QUEUED'

        open(flag, 'w').close()
        tried_at = time.time()
        reply = reader('BGSAVE')
        if reply == '+Background saving started':
            print(f'{name}: fork() could not be made to fail here: fork-failure checks skipped')
            return
        assert reply == '-ERR', (name, reply)
        assert info(reader, 'rdb_last_bgsave_status') == 'err', name
        assert info(reader, 'rdb_bgsave_in_progress') == '0', name
        assert info(reader, 'rdb_saves') == '1', name
        assert info(reader, 'rdb_last_bgsave_time_sec') == '-1', name
        assert info(reader, 'rdb_current_bgsave_time_sec') == '-1', name
        # Nothing was saved: the save time and the change count stay.
        assert info(reader, 'rdb_changes_since_last_save') == '1', name
        assert info(reader, 'rdb_last_save_time') == str(t0), name
        assert reader('LASTSAVE') == t0, name
        assert f"Can't save in background: fork: {os.strerror(errno.ENOMEM)}" in log_of(workdir), name

        # Writes and PING are refused, reads and CONFIG are not.
        assert reader('SET', 'b', '2') == MISCONF, name
        assert reader('DEL', 'a') == MISCONF, name
        assert reader('FLUSHALL') == MISCONF, name
        assert reader('PING') == MISCONF, name
        assert reader('GET', 'a') == '1', name
        assert reader('CONFIG', 'SET', 'maxmemory-samples', '5') == '+OK', name
        # An EXEC whose transaction writes is discarded; one that does not write runs.
        assert c('EXEC') == '-EXECABORT Transaction discarded because of: ' + MISCONF[1:], name
        assert c('GET', 'queued') is None, name
        assert pinger('EXEC') == ['+PONG'], name
        # A write sent inside MULTI now flags the transaction.
        assert c('MULTI') == '+OK' and c('SET', 'b', '2') == MISCONF, name
        assert c('EXEC') == '-EXECABORT Transaction discarded because of previous errors.', name
        # A script without flags fails at its write (Redis adds " script: ...", delta item 14); a
        # script that declares flags is refused before it runs unless it declares no-writes.
        assert reader('EVAL', "return redis.call('set','x',1)", '1', 'x').startswith(MISCONF), name
        assert reader('EVAL', "return redis.call('get','a')", '1', 'a') == '1', name
        assert reader('EVAL', "return redis.call('ping')", '0') == '+PONG', name
        assert reader('EVAL', "#!lua\nreturn redis.call('get','a')", '1', 'a') == MISCONF, name
        assert reader('EVAL', "#!lua flags=no-writes\nreturn redis.call('get','a')", '1', 'a') == '1'
        lib = "#!lua name=lib\nredis.register_function('f', function(k, a) return 1 end)"
        assert reader('FUNCTION', 'LOAD', lib) == MISCONF, name

        # stop-writes-on-bgsave-error no lets writes through.
        assert reader('CONFIG', 'SET', 'stop-writes-on-bgsave-error', 'no') == '+OK'
        assert reader('SET', 'b', '2') == '+OK', name
        assert reader('CONFIG', 'SET', 'stop-writes-on-bgsave-error', 'yes') == '+OK'
        assert reader('SET', 'b', '3') == MISCONF, name

        # The save points try again only CONFIG_BGSAVE_RETRY_DELAY (5 s) after the last try.
        assert reader('CONFIG', 'SET', 'save', '1 1') == '+OK'
        fork_errors = lambda: log_of(workdir).count("Can't save in background: fork:")  # noqa: E731
        wait_for(lambda: fork_errors() >= 2, f'{name}: the save points to try again', timeout=9)
        assert time.time() - tried_at > 4.5, (name, time.time() - tried_at)
        assert fork_errors() == 2, name
        assert '1 changes in 1 seconds. Saving...' in log_of(workdir), name
        assert info(reader, 'rdb_saves') == '2', name
        # The next good BGSAVE clears it all (the command does not wait for the retry delay).
        assert reader('CONFIG', 'SET', 'save', '3600 1') == '+OK'
        os.unlink(flag)
        assert reader('BGSAVE') == '+Background saving started', name
        wait_for(lambda: info(reader, 'rdb_bgsave_in_progress') == '0', f'{name}: BGSAVE end')
        assert info(reader, 'rdb_last_bgsave_status') == 'ok', name
        assert info(reader, 'rdb_changes_since_last_save') == '0', name
        assert reader('LASTSAVE') > t0, name
        assert reader('SET', 'b', '4') == '+OK', name
        assert reader('PING') == '+PONG', name
        assert 'Background saving terminated with success' in log_of(workdir), name
    finally:
        stop(proc)
    print(f'{name}: fork failure ok')


def check_replica_keeps_applying(work, shim):
    """PeaDB only: a replica whose own BGSAVE failed still applies everything its master sends,
    MULTI/EXEC blocks included (the master stream is never refused, compat/delta.md item 43).
    Redis 7.2.5 stops such a replica (serverPanic) unless replica-ignore-disk-write-errors is yes,
    and then applies them like this."""
    master_dir = os.path.join(work, 'peadb-repl-master')
    replica_dir = os.path.join(work, 'peadb-repl-replica')
    flag = os.path.join(replica_dir, 'fail-fork')
    master = start(PEADB, PORTS['peadb'], master_dir, '--save', '')
    replica = None
    try:
        replica = start_with_shim('peadb', PEADB, REPLICA_PORT, replica_dir, shim, flag,
                                  '--save', '3600 1')
        if replica is None:
            return
        m, r = Conn(PORTS['peadb']), Conn(REPLICA_PORT)
        assert r('REPLICAOF', '127.0.0.1', str(PORTS['peadb'])) == '+OK'
        wait_for(lambda: 'master_link_status:up' in r('INFO', 'replication'), 'replica link up')
        open(flag, 'w').close()
        if r('BGSAVE') != '-ERR':
            print('peadb: fork() could not be made to fail here: replica check skipped')
            return
        assert info(r, 'rdb_last_bgsave_status') == 'err'
        assert r('PING') == MISCONF
        assert m('MULTI') == '+OK' and m('SET', 'x', '1') == '+QUEUED'
        assert m('SET', 'y', '2') == '+QUEUED' and m('EXEC') == ['+OK', '+OK']
        assert m('SET', 'z', '3') == '+OK'
        wait_for(lambda: r('MGET', 'x', 'y', 'z') == ['1', '2', '3'],
                 'the replica to apply the transaction and the write after it')
    finally:
        if replica is not None:
            stop(replica)
        stop(master)
    print('peadb: replica with a failed BGSAVE ok')


def check_child_error(name, binary, work):
    if os.geteuid() == 0:
        print(f'{name}: running as root, a read-only directory cannot make a save fail: skipped')
        return
    port = PORTS[name]
    workdir = os.path.join(work, f'{name}-childerr')
    proc = start(binary, port, workdir, '--save', '3600 1', '--enable-debug-command', 'yes')
    try:
        c = Conn(port)
        assert c('SET', 'x', '1') == '+OK'
        t0 = c('LASTSAVE')
        realdir = os.path.realpath(workdir)
        os.chmod(workdir, stat.S_IRUSR | stat.S_IXUSR)
        try:
            assert c('BGSAVE') == '+Background saving started', name
            wait_for(lambda: info(c, 'rdb_bgsave_in_progress') == '0', f'{name}: BGSAVE end')
            child = child_pids(workdir)[-1]
            log = log_of(workdir)
            assert (f'Failed opening the temp RDB file temp-{child}.rdb (in server root dir '
                    f'{realdir}) for saving: Permission denied') in log, name
            assert 'Background saving error' in log, name
            assert info(c, 'rdb_last_bgsave_status') == 'err', name
            assert info(c, 'rdb_changes_since_last_save') == '1', name
            assert info(c, 'rdb_last_save_time') == str(t0), name
            assert info(c, 'rdb_last_bgsave_time_sec') != '-1', name
            assert c('LASTSAVE') == t0, name
            assert c('SET', 'y', '1') == MISCONF, name
            # SAVE and DEBUG RELOAD answer a bare -ERR and log why.
            assert c('SAVE') == '-ERR', name
            assert (f'Failed opening the temp RDB file temp-{proc.pid}.rdb (in server root dir '
                    f'{realdir}) for saving: Permission denied') in log_of(workdir), name
            assert c('DEBUG', 'RELOAD') == '-ERR', name
            assert c('GET', 'x') == '1', name
            assert info(c, 'rdb_saves') == '2', name  # BGSAVE and SAVE; not DEBUG RELOAD
        finally:
            os.chmod(workdir, stat.S_IRWXU)
        assert c('BGSAVE') == '+Background saving started', name
        wait_for(lambda: info(c, 'rdb_bgsave_in_progress') == '0', f'{name}: BGSAVE end')
        assert info(c, 'rdb_last_bgsave_status') == 'ok', name
        assert info(c, 'rdb_changes_since_last_save') == '0', name
        assert c('LASTSAVE') >= t0, name
        assert c('SET', 'y', '1') == '+OK', name
    finally:
        stop(proc)
    print(f'{name}: child error ok')


def check_flushall(name, binary, work):
    port = PORTS[name]
    workdir = os.path.join(work, f'{name}-flushall')
    proc = start(binary, port, workdir, '--save', '')
    try:
        c = Conn(port)
        # Without save points FLUSHALL stops a running BGSAVE, which is not a failed one, and
        # saves nothing (integration/rdb "Test FLUSHALL aborts bgsave").
        fill(c, 2000)
        assert int(info(c, 'rdb_changes_since_last_save')) >= 2000, name
        assert c('CONFIG', 'SET', 'rdb-key-save-delay', '1000000') == '+OK'
        assert c('BGSAVE') == '+Background saving started', name
        assert info(c, 'rdb_bgsave_in_progress') == '1', name
        assert c('FLUSHALL') == '+OK', name
        wait_for(lambda: info(c, 'rdb_bgsave_in_progress') == '0', f'{name}: BGSAVE stopped',
                 timeout=2)
        assert info(c, 'rdb_last_bgsave_status') == 'ok', name
        assert int(info(c, 'rdb_changes_since_last_save')) > 999, name
        assert 'Background saving terminated by signal' in log_of(workdir), name
        wait_for(lambda: not [f for f in os.listdir(workdir) if f.endswith('.rdb')],
                 f'{name}: no dump.rdb and no temp file')
        assert c('SET', 'x', 'xx') == '+OK', name
        # unit/other: FLUSHALL keeps the change count when no save point is set.
        assert c('SET', 'key', 'value') == '+OK' and c('FLUSHALL') == '+OK'
        assert int(info(c, 'rdb_changes_since_last_save')) > 0, name

        # With save points FLUSHALL stops the BGSAVE and saves the empty dataset at once.
        assert c('CONFIG', 'SET', 'rdb-key-save-delay', '0') == '+OK'
        assert c('CONFIG', 'SET', 'save', '3600 1') == '+OK'
        fill(c, 50)
        assert c('SAVE') == '+OK', name  # dump.rdb now holds 50 keys
        assert c('CONFIG', 'SET', 'rdb-key-save-delay', '1000000') == '+OK'
        assert c('BGSAVE') == '+Background saving started', name
        saves = log_of(workdir).count('DB saved on disk')
        before = c('LASTSAVE')
        assert c('FLUSHALL') == '+OK', name
        assert info(c, 'rdb_changes_since_last_save') == '0', name
        assert c('LASTSAVE') >= before, name
        assert log_of(workdir).count('DB saved on disk') == saves + 1, name
        # unit/other: FLUSHALL resets the change count to 0 with save points.
        assert c('SET', 'key', 'value') == '+OK' and c('FLUSHALL') == '+OK'
        assert info(c, 'rdb_changes_since_last_save') == '0', name
        assert c('SHUTDOWN', 'NOSAVE') is None, name
        proc.wait(timeout=10)
        # The flushed keys do not come back from the older dump.rdb.
        proc = start(binary, port, workdir, '--save', '')
        assert Conn(port)('DBSIZE') == 0, name
    finally:
        stop(proc)
    print(f'{name}: flushall ok')


def check_schedule(name, binary, work):
    port = PORTS[name]
    workdir = os.path.join(work, f'{name}-schedule')
    proc = start(binary, port, workdir, '--save', '')
    try:
        c = Conn(port)
        assert c('BGSAVE', 'X') == SYNTAX, name
        assert c('BGSAVE', 'SCHEDULE', 'X') == SYNTAX, name
        assert info(c, 'rdb_last_bgsave_time_sec') == '-1', name
        assert info(c, 'rdb_current_bgsave_time_sec') == '-1', name
        assert info(c, 'rdb_saves') == '0', name
        assert c('BGSAVE', 'SCHEDULE') == '+Background saving started', name
        wait_for(lambda: info(c, 'rdb_bgsave_in_progress') == '0', f'{name}: BGSAVE end')
        assert info(c, 'rdb_saves') == '1', name
        assert info(c, 'rdb_last_bgsave_time_sec') != '-1', name
        log = log_of(workdir)
        for line in ('Background saving started by pid', 'DB saved on disk',
                     'Background saving terminated with success'):
            assert line in log, (name, line)

        # Inside MULTI/EXEC a BGSAVE waits until EXEC ends, so the snapshot holds the whole
        # transaction.
        assert c('MULTI') == '+OK'
        for args in (('SET', 'a', '1'), ('BGSAVE',), ('SET', 'b', '2')):
            assert c(*args) == '+QUEUED'
        assert c('EXEC') == ['+OK', '+Background saving scheduled', '+OK'], name
        wait_for(lambda: info(c, 'rdb_saves') == '2' and info(c, 'rdb_bgsave_in_progress') == '0',
                 f'{name}: the scheduled BGSAVE')
        assert c('SHUTDOWN', 'NOSAVE') is None, name
        proc.wait(timeout=10)
        proc = start(binary, port, workdir, '--save', '')
        c = Conn(port)
        assert c('MGET', 'a', 'b') == ['1', '2'], name

        # During an AOF rewrite a BGSAVE is refused, or with SCHEDULE started once the rewrite ends.
        assert c('CONFIG', 'SET', 'rdb-key-save-delay', '500000') == '+OK'
        assert c('BGREWRITEAOF') == '+Background append only file rewriting started', name
        assert c('BGSAVE') == AOF_CHILD_ACTIVE, name
        assert c('BGSAVE', 'SCHEDULE') == '+Background saving scheduled', name
        assert c('CONFIG', 'SET', 'rdb-key-save-delay', '0') == '+OK'
        assert info(c, 'rdb_bgsave_in_progress') == '0', name
        wait_for(lambda: info(c, 'rdb_saves') == '1' and info(c, 'rdb_bgsave_in_progress') == '0'
                 and info(c, 'aof_rewrite_in_progress') == '0', f'{name}: rewrite, then BGSAVE',
                 timeout=10)
        assert info(c, 'rdb_last_bgsave_status') == 'ok', name
        # A BGSAVE that runs shows its running time.
        assert c('CONFIG', 'SET', 'rdb-key-save-delay', '1000000') == '+OK'
        assert c('BGSAVE') == '+Background saving started', name
        assert int(info(c, 'rdb_current_bgsave_time_sec')) >= 0, name
        assert c('CONFIG', 'RESETSTAT') == '+OK'
        assert info(c, 'rdb_saves') == '0', name
    finally:
        stop(proc)
    print(f'{name}: schedule ok')


def check_child_sockets(name, binary, work):
    port = PORTS[name]
    for kind in ('bgsave', 'aofrw'):
        workdir = os.path.join(work, f'{name}-sockets-{kind}')
        proc = start(binary, port, workdir, '--save', '')
        children = []
        try:
            c, victim, witness = Conn(port), Conn(port), Conn(port)
            victim_id = victim('CLIENT', 'ID')
            assert witness('PING') == '+PONG'
            assert c('SET', 'k', 'v') == '+OK'
            assert c('CONFIG', 'SET', 'rdb-key-save-delay', '100000000') == '+OK'
            if kind == 'bgsave':
                assert c('BGSAVE') == '+Background saving started', name
                wait_for(lambda: child_pids(workdir), f'{name}: the BGSAVE child pid')
                children = child_pids(workdir)
            else:
                assert c('BGREWRITEAOF') == '+Background append only file rewriting started'
                found = subprocess.run(['pgrep', '-P', str(proc.pid)], capture_output=True,
                                       text=True).stdout.split()
                children = [int(p) for p in found]
                assert children, name
            # CLIENT KILL closes the connection at once, though the child holds a copy of it
            # (unit/introspection "CLIENT KILL close the client connection during bgsave").
            assert c('CLIENT', 'KILL', 'ID', str(victim_id)) == 1, name
            assert victim.closed_within(2.0), (name, kind)
            # The server dies while the child runs: the port is free at once.
            proc.kill()
            proc.wait()
            try:
                socket.create_connection(('127.0.0.1', port), timeout=1).close()
                raise AssertionError(f'{name} {kind}: the child still listens on the port')
            except ConnectionRefusedError:
                pass
            if name == 'peadb':
                assert witness.closed_within(2.0), (name, kind)
        finally:
            for pid in children:
                kill_quietly(pid)
            stop(proc)
    print(f'{name}: child sockets ok')


def main():
    work = tempfile.mkdtemp(prefix='peadb-bgsave-')
    try:
        shim = build_shim(work)
        if not shim:
            print('cc could not build the fork-failure library: fork-failure checks skipped')
        engines = ([('redis', REDIS)] if REDIS else []) + [('peadb', PEADB)]
        if not REDIS:
            print('no pinned Redis 7.2.x: skipping the Redis side')
        for name, binary in engines:
            if shim:
                check_fork_failure(name, binary, work, shim)
            check_child_error(name, binary, work)
            check_flushall(name, binary, work)
            check_schedule(name, binary, work)
            check_child_sockets(name, binary, work)
        if shim:
            check_replica_keeps_applying(work, shim)
    finally:
        for root, dirs, _ in os.walk(work):
            for d in dirs:
                os.chmod(os.path.join(root, d), stat.S_IRWXU)
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()

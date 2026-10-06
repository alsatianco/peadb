#!/usr/bin/env python3
"""AOF write and fsync errors are reported, refused and cleared like Redis 7.2.5 (compat/delta.md item 35).

The same checks run on peadb-server and on the pinned redis-server (only a Redis 7.2.x; without one, only the PeaDB side runs). A small library put in front of the C library makes write() or the fsync of the AOF file fail while a control file exists (macOS interposes it with DYLD_INSERT_LIBRARIES, Linux puts it first with LD_PRELOAD); the file's first word picks the fault. Only the AOF file is affected (PeaDB's appendonly.aof, Redis's appendonly.aof.<n>.incr.aof), not the sockets, the RDB or the rewrite files. Skipped when the library cannot be built, or when it does not take effect on Redis; it must take effect on PeaDB.

check_write_error(): with appendfsync everysec a failed AOF write keeps the command (its reply is sent; a script whose first write fails still runs to its end, as Redis never stops a script that has written), sets aof_last_write_status:err and logs "Error writing to the AOF file: <reason>" once (at most one line per 30 s while it keeps failing); then writes, PING, scripts that write and an EXEC whose transaction writes get "-MISCONF Errors writing to the AOF file: <reason>", reads do not; once the disk works again the kept bytes are written ("AOF write error looks solved, Redis can write again.", PeaDB names itself), writes work again, and after a kill -9 the restart has every acknowledged write. CONFIG SET appendonly no lifts the refusal (the status stays err) and CONFIG SET appendonly yes clears it ("AOF reopen, just ignore the last error.").

check_short_write(): a short write is cut off the file again (its size is back to what it was), logged as "Short write while writing to the AOF file: (nwritten=…, expected=…)" and refused with the ENOSPC text; after recovery the file holds the whole command and loads with no torn tail.

check_fsync_error(): a failed everysec fsync logs "Fail to fsync the AOF file: <reason>" (once) and refuses writes and PING with its reason. Redis clears the status only when the fsync of a later write works (here the DEL that RANDOMKEY writes for a key that expired); PeaDB also tries the fsync again every second while it fails, because no write may come (writes are refused, and PeaDB writes no expiry DELs to the AOF, compat/delta.md item 37).

check_always(): with appendfsync always a failed write or fsync stops the server with exit code 1 before the client gets a reply ("Can't recover from AOF write error when the AOF fsync policy is 'always'. Exiting..." / "Can't persist AOF for fsync error when the AOF fsync policy is 'always': <reason>. Exiting..."), so no client is told that data is safe when it is not on disk.

check_rewrite_drops_kept_bytes() and check_shutdown_keeps_data(), PeaDB only: a good BGREWRITEAOF drops the kept bytes (the new file already holds them), and SHUTDOWN and SIGTERM refuse to exit while kept bytes are not in the file (SHUTDOWN FORCE exits anyway); Redis 7.2.5 differs in both (see compat/delta.md item 35).
"""
import glob
import os
import pathlib
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _redis_path import redis_server_path  # noqa: E402

PEADB = str(ROOT / 'peadb-server')
PORTS = {'peadb': 6630, 'redis': 6631}
SERVER_NAME = {'peadb': 'PeaDB', 'redis': 'Redis'}  # log lines name the server
ENOSPC_TEXT = '-MISCONF Errors writing to the AOF file: No space left on device'
EIO_TEXT = '-MISCONF Errors writing to the AOF file: Input/output error'

# While the file named by FAILAOF_FILE exists, its first word picks a fault for the AOF file:
# "write <errno>" fails every write(), "short" writes half of the bytes and fails the rest with
# ENOSPC, "fsync <errno>" fails fsync(), fdatasync() (Redis on Linux) and fcntl(F_FULLFSYNC)
# (Redis on macOS).
SHIM_SOURCE = r'''
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#ifndef __APPLE__
#include <dlfcn.h>
#endif

/* 0 when no fault is set, else 'w', 's' or 'f'; *err gets the errno to fail with. */
static int fault(int* err) {
  const char* path = getenv("FAILAOF_FILE");
  if (path == NULL || access(path, F_OK) != 0) return 0;
  char buf[64] = {0};
  int fd = open(path, O_RDONLY);
  if (fd < 0) return 0;
  ssize_t n = read(fd, buf, sizeof(buf) - 1);
  close(fd);
  if (n <= 0) return 0;
  *err = strstr(buf, "EIO") != NULL ? EIO : ENOSPC;
  if (strncmp(buf, "write", 5) == 0) return 'w';
  if (strncmp(buf, "short", 5) == 0) return 's';
  if (strncmp(buf, "fsync", 5) == 0) return 'f';
  return 0;
}

static int is_aof(int fd) {
  char p[PATH_MAX + 1] = {0};
#ifdef __APPLE__
  if (fcntl(fd, F_GETPATH, p) == -1) return 0;
#else
  char link[64];
  snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
  ssize_t n = readlink(link, p, PATH_MAX);
  if (n <= 0) return 0;
  p[n] = '\0';
#endif
  const char* base = strrchr(p, '/');
  base = base != NULL ? base + 1 : p;
  return strcmp(base, "appendonly.aof") == 0 || strstr(base, ".incr.aof") != NULL;
}

static int short_pending = 0;

/* -1 (with errno set) when the call must fail, -2 to write only *count bytes, 0 to go on. */
static int write_fault(int fd, size_t* count) {
  int err = 0;
  int mode = fault(&err);
  if (mode != 'w' && mode != 's') return 0;
  if (!is_aof(fd)) return 0;
  if (mode == 'w' || short_pending || *count < 2) {
    short_pending = 0;
    errno = err;
    return -1;
  }
  short_pending = 1;
  *count /= 2;
  return -2;
}

static int fsync_fault(int fd) {
  int err = 0;
  if (fault(&err) != 'f' || !is_aof(fd)) return 0;
  errno = err;
  return -1;
}

#ifdef __APPLE__
static ssize_t failaof_write(int fd, const void* buf, size_t count) {
  if (write_fault(fd, &count) == -1) return -1;
  return write(fd, buf, count);
}
static int failaof_fsync(int fd) {
  if (fsync_fault(fd) == -1) return -1;
  return fsync(fd);
}
static int failaof_fcntl(int fd, int cmd, ...) {
  va_list ap;
  va_start(ap, cmd);
  void* arg = va_arg(ap, void*);
  va_end(ap);
  if (cmd == F_FULLFSYNC && fsync_fault(fd) == -1) return -1;
  return fcntl(fd, cmd, arg);
}
__attribute__((used)) static const struct {
  const void* replacement;
  const void* replacee;
} interposers[] __attribute__((section("__DATA,__interpose"))) = {
    {(const void*)&failaof_write, (const void*)&write},
    {(const void*)&failaof_fsync, (const void*)&fsync},
    {(const void*)&failaof_fcntl, (const void*)&fcntl},
};
#else
ssize_t write(int fd, const void* buf, size_t count) {
  static ssize_t (*real_write)(int, const void*, size_t) = NULL;
  if (real_write == NULL) real_write = (ssize_t(*)(int, const void*, size_t))dlsym(RTLD_NEXT, "write");
  if (write_fault(fd, &count) == -1) return -1;
  return real_write(fd, buf, count);
}
int fsync(int fd) {
  static int (*real_fsync)(int) = NULL;
  if (real_fsync == NULL) real_fsync = (int(*)(int))dlsym(RTLD_NEXT, "fsync");
  if (fsync_fault(fd) == -1) return -1;
  return real_fsync(fd);
}
int fdatasync(int fd) {
  static int (*real_fdatasync)(int) = NULL;
  if (real_fdatasync == NULL) real_fdatasync = (int(*)(int))dlsym(RTLD_NEXT, "fdatasync");
  if (fsync_fault(fd) == -1) return -1;
  return real_fdatasync(fd);
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


def start(binary, port, workdir, *extra, env=None):
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    log = open(log_path, 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                             '--loglevel', 'notice', '--save', '', '--appendonly', 'yes',
                             '--enable-debug-command', 'yes', *extra],
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


def kill9(proc):
    if proc.poll() is None:
        proc.send_signal(signal.SIGKILL)
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


def aof_file(workdir):
    """The file the AOF appends to: PeaDB's appendonly.aof, or Redis's newest incr file."""
    single = os.path.join(workdir, 'appendonly.aof')
    if os.path.exists(single):
        return single
    incr = sorted(glob.glob(os.path.join(workdir, 'appendonlydir', '*.incr.aof')),
                  key=lambda p: int(p.rsplit('.', 3)[-3]))
    assert incr, f'no AOF file in {workdir}'
    return incr[-1]


def wait_in_aof(workdir, *args):
    """Waits until the AOF file holds the command `args`: with appendfsync everysec Redis may hold a write back for up
    to 2 s while a background fsync runs (flushAppendOnlyFile()), so a kill -9 right after the reply can lose it."""
    needle = b''.join(b'$%d\r\n%s\r\n' % (len(str(a)), str(a).encode()) for a in args)
    wait_for(lambda: needle in open(aof_file(workdir), 'rb').read(), f'{args} in the AOF')


def build_shim(work):
    src = os.path.join(work, 'failaof.c')
    lib = os.path.join(work, 'failaof.so')
    with open(src, 'w') as f:
        f.write(SHIM_SOURCE)
    cmd = ['cc', '-shared', '-fPIC', '-o', lib, src]
    if sys.platform.startswith('linux'):
        cmd.append('-ldl')
    try:
        built = subprocess.run(cmd, capture_output=True, text=True)
    except OSError:
        return None
    if built.returncode != 0:
        print(f'cannot build the AOF fault library: {built.stderr}')
        return None
    return lib


def shim_env(shim, flag):
    env = dict(os.environ, FAILAOF_FILE=flag)
    env['DYLD_INSERT_LIBRARIES' if sys.platform == 'darwin' else 'LD_PRELOAD'] = shim
    # An ASan build refuses to start when another library loads before its runtime.
    asan = env.get('ASAN_OPTIONS', '')
    env['ASAN_OPTIONS'] = (asan + ':' if asan else '') + 'verify_asan_link_order=0'
    return env


def set_fault(flag, text):
    with open(flag, 'w') as f:
        f.write(text)


def clear_fault(flag):
    if os.path.exists(flag):
        os.unlink(flag)


def start_faulty(name, binary, workdir, shim, *extra):
    """start() with the AOF fault library, or None (and a note) when the server will not run with
    it."""
    flag = os.path.join(workdir, 'fail-aof')
    try:
        return start(binary, PORTS[name], workdir, *extra, env=shim_env(shim, flag)), flag
    except RuntimeError as e:
        print(f'{name}: the server does not start with the AOF fault library ({e}): skipped')
        return None, flag


def check_write_error(name, binary, work, shim):
    workdir = os.path.join(work, f'{name}-write')
    proc, flag = start_faulty(name, binary, workdir, shim)
    if proc is None:
        return False
    try:
        c = Conn(PORTS[name])
        assert c('SET', 'a', '1') == '+OK'
        assert c('INCR', 'n') == 1
        assert info(c, 'aof_last_write_status') == 'ok'
        set_fault(flag, 'write ENOSPC')
        # The script has run, all of it: its first write fails in the AOF, but a script that has
        # written is not stopped half applied. Only the AOF write failed, and its bytes are kept.
        assert c('EVAL', "redis.call('incr','n'); redis.call('set','s','1'); return 'done'",
                 '0') == 'done'
        wait_for(lambda: info(c, 'aof_last_write_status') == 'err', 'aof_last_write_status:err')
        if 'Error writing to the AOF file: No space left on device' not in log_of(workdir):
            # The library did not reach this server's write() (for example a static build).
            if name == 'peadb':
                raise AssertionError('the AOF fault library does not take effect on PeaDB')
            print(f'{name}: the AOF fault library does not take effect: skipped')
            return False
        assert c('SET', 'b', '2') == ENOSPC_TEXT
        assert c('PING') == ENOSPC_TEXT
        assert c('GET', 'a') == '1'
        assert c('GET', 'n') == '2'
        assert c('GET', 's') == '1'
        assert c('EVAL', "#!lua\nreturn redis.call('set','x','1')", '0') == ENOSPC_TEXT
        assert c('EVAL', "#!lua flags=no-writes\nreturn redis.call('get','a')", '0') == '1'
        reply = c('EVAL', "return redis.call('set','x','1')", '0')
        assert reply.startswith(ENOSPC_TEXT), reply
        assert c('MULTI') == '+OK'
        assert c('GET', 'a') == '+QUEUED'
        assert c('SET', 'x', '1') == ENOSPC_TEXT
        assert c('EXEC') == '-EXECABORT Transaction discarded because of previous errors.'
        time.sleep(0.5)  # the server tries the write again many times, but logs it once per 30 s
        assert log_of(workdir).count('Error writing to the AOF file') == 1, log_of(workdir)
        clear_fault(flag)
        wait_for(lambda: info(c, 'aof_last_write_status') == 'ok', 'aof_last_write_status:ok')
        assert f'AOF write error looks solved, {SERVER_NAME[name]} can write again.' in log_of(workdir)
        assert c('PING') == '+PONG'
        assert c('SET', 'b', '2') == '+OK'
        assert c('INCR', 'n') == 3

        # CONFIG SET appendonly no lifts the refusal (the status stays err); appendonly yes starts
        # a new AOF from the dataset and forgets the error.
        set_fault(flag, 'write ENOSPC')
        assert c('SET', 'd', '4') == '+OK'
        wait_for(lambda: info(c, 'aof_last_write_status') == 'err', 'a second write error')
        assert c('SET', 'e', '5') == ENOSPC_TEXT
        assert c('CONFIG', 'SET', 'appendonly', 'no') == '+OK'
        assert c('SET', 'e', '5') == '+OK'
        assert info(c, 'aof_last_write_status') == 'err'
        clear_fault(flag)
        assert c('CONFIG', 'SET', 'appendonly', 'yes') == '+OK'
        assert 'AOF reopen, just ignore the last error.' in log_of(workdir)
        assert info(c, 'aof_last_write_status') == 'ok'
        wait_for(lambda: info(c, 'aof_rewrite_in_progress') == '0'
                 and info(c, 'aof_last_bgrewrite_status') == 'ok', 'the AOF rewrite')
        assert c('SET', 'f', '6') == '+OK'
        wait_in_aof(workdir, 'SET', 'f', '6')
        c.close()
    finally:
        kill9(proc)
    # kill -9: no final flush, so the restart shows what the AOF had.
    proc = start(binary, PORTS[name], workdir)
    try:
        c = Conn(PORTS[name])
        got = [c('GET', k) for k in ('a', 'n', 's', 'b', 'd', 'e', 'f', 'x')]
        assert got == ['1', '3', '1', '2', '4', '5', '6', None], got
        c.close()
    finally:
        kill9(proc)
    return True


def check_short_write(name, binary, work, shim):
    workdir = os.path.join(work, f'{name}-short')
    proc, flag = start_faulty(name, binary, workdir, shim)
    if proc is None:
        return
    big = 'v' * 5000
    try:
        c = Conn(PORTS[name])
        assert c('SET', 'a', '1') == '+OK'
        assert c('FUNCTION', 'LOAD', "#!lua name=lib\nredis.register_function('two', "
                 "function(keys, args) redis.call('set', keys[1], args[1]); "
                 "redis.call('incr', keys[2]); return 'done' end)") == 'lib'
        size_before = os.path.getsize(aof_file(workdir))
        set_fault(flag, 'short')
        # The function's first write is cut short in the AOF; its second write still runs.
        assert c('FCALL', 'two', '2', 'big', 'calls', big) == 'done'
        wait_for(lambda: info(c, 'aof_last_write_status') == 'err', 'aof_last_write_status:err')
        m = re.search(r'Short write while writing to the AOF file: \(nwritten=(\d+), expected=(\d+)\)',
                      log_of(workdir))
        assert m and 0 < int(m.group(1)) < int(m.group(2)), log_of(workdir)
        # The part that was written is cut off again, so the file never ends inside a command.
        assert os.path.getsize(aof_file(workdir)) == size_before
        assert c('SET', 'b', '2') == ENOSPC_TEXT
        clear_fault(flag)
        wait_for(lambda: info(c, 'aof_last_write_status') == 'ok', 'aof_last_write_status:ok')
        assert c('SET', 'b', '2') == '+OK'
        wait_in_aof(workdir, 'SET', 'b', '2')
        c.close()
    finally:
        kill9(proc)
    proc = start(binary, PORTS[name], workdir)
    try:
        c = Conn(PORTS[name])
        got = [c('GET', 'a'), c('GET', 'big') == big, c('GET', 'calls'), c('GET', 'b')]
        assert got == ['1', True, '1', '2'], got
        c.close()
    finally:
        kill9(proc)
    assert 'short read' not in log_of(workdir).lower(), log_of(workdir)


def check_fsync_error(name, binary, work, shim):
    workdir = os.path.join(work, f'{name}-fsync')
    proc, flag = start_faulty(name, binary, workdir, shim, '--appendfsync', 'everysec')
    if proc is None:
        return
    try:
        c = Conn(PORTS[name])
        assert c('DEBUG', 'SET-ACTIVE-EXPIRE', '0') == '+OK'
        assert c('SET', 'a', '1') == '+OK'
        assert c('SELECT', '9') == '+OK'  # a database that holds only the key that will expire
        assert c('SET', 'tmp', 'v', 'PX', '300') == '+OK'
        assert c('SELECT', '0') == '+OK'
        set_fault(flag, 'fsync EIO')
        assert c('SET', 'b', '2') == '+OK'
        wait_for(lambda: info(c, 'aof_last_write_status') == 'err', 'the everysec fsync error',
                 timeout=4)
        assert 'Fail to fsync the AOF file: Input/output error' in log_of(workdir), log_of(workdir)
        assert c('SET', 'c', '3') == EIO_TEXT
        assert c('PING') == EIO_TEXT
        clear_fault(flag)
        if name == 'redis':
            # Redis syncs only new bytes, so with nothing new the error stays.
            time.sleep(1.3)
            assert info(c, 'aof_last_write_status') == 'err'
            # RANDOMKEY meets the expired key and deletes it, which writes a DEL; its fsync works.
            assert c('SELECT', '9') == '+OK'
            assert c('RANDOMKEY') is None
            assert c('SELECT', '0') == '+OK'
        # PeaDB tries the fsync again every second while it fails, because no write may come.
        wait_for(lambda: info(c, 'aof_last_write_status') == 'ok', 'the next good fsync',
                 timeout=4)
        assert c('SET', 'c', '3') == '+OK'
        assert log_of(workdir).count('Fail to fsync the AOF file') == 1, log_of(workdir)
        c.close()
    finally:
        kill9(proc)


def check_always(name, binary, work, shim, fault, line):
    workdir = os.path.join(work, f'{name}-always-{fault.split()[0]}')
    proc, flag = start_faulty(name, binary, workdir, shim, '--appendfsync', 'always')
    if proc is None:
        return
    try:
        c = Conn(PORTS[name])
        assert c('SET', 'a', '1') == '+OK'
        set_fault(flag, fault)
        # The server exits before the client gets any reply.
        assert c('SET', 'b', '2') is None
        assert proc.wait(timeout=10) == 1
        assert line in log_of(workdir), log_of(workdir)
        c.close()
    finally:
        kill9(proc)
    proc = start(binary, PORTS[name], workdir)
    try:
        c = Conn(PORTS[name])
        assert c('GET', 'a') == '1'
        # The failed write left nothing in the file; the failed fsync left the command there.
        assert c('GET', 'b') == (None if fault.startswith('write') else '2')
        c.close()
    finally:
        kill9(proc)


def check_rewrite_drops_kept_bytes(work, shim):
    """PeaDB only: a good BGREWRITEAOF makes a new file from the dataset, which already holds what a
    failed write kept, so those bytes are dropped and not written a second time. (Redis 7.2.5
    writes them to its new incr file, after a base that already holds them.)"""
    workdir = os.path.join(work, 'peadb-rewrite')
    proc, flag = start_faulty('peadb', PEADB, workdir, shim)
    if proc is None:
        return
    try:
        c = Conn(PORTS['peadb'])
        assert c('INCR', 'n') == 1
        set_fault(flag, 'write ENOSPC')
        assert c('INCR', 'n') == 2  # kept, not in the file
        wait_for(lambda: info(c, 'aof_last_write_status') == 'err', 'aof_last_write_status:err')
        # The rewrite files are not the AOF file, so the fault does not touch them.
        assert c('BGREWRITEAOF') == '+Background append only file rewriting started'
        wait_for(lambda: info(c, 'aof_rewrite_in_progress') == '0', 'the AOF rewrite')
        assert info(c, 'aof_last_bgrewrite_status') == 'ok'
        assert info(c, 'aof_last_write_status') == 'ok'
        clear_fault(flag)
        time.sleep(0.3)  # time for a retry of kept bytes, if any were left
        assert c('INCR', 'n') == 3
        c.close()
    finally:
        kill9(proc)
    proc = start(PEADB, PORTS['peadb'], workdir)
    try:
        c = Conn(PORTS['peadb'])
        assert c('GET', 'n') == '3'
        c.close()
    finally:
        kill9(proc)


def check_shutdown_keeps_data(work, shim):
    """PeaDB only: SHUTDOWN and SIGTERM refuse to exit while acknowledged writes are still only in
    memory because the AOF file cannot take them (Redis 7.2.5 exits and loses them), as for a
    final RDB save that fails; SHUTDOWN FORCE exits anyway."""
    workdir = os.path.join(work, 'peadb-shutdown')
    proc, flag = start_faulty('peadb', PEADB, workdir, shim)
    if proc is None:
        return
    try:
        c = Conn(PORTS['peadb'])
        assert c('SET', 'a', '1') == '+OK'
        set_fault(flag, 'write ENOSPC')
        assert c('SET', 'b', '2') == '+OK'
        wait_for(lambda: info(c, 'aof_last_write_status') == 'err', 'aof_last_write_status:err')
        assert c('SHUTDOWN') == '-ERR Errors trying to SHUTDOWN. Check logs.'
        assert "Error trying to write the AOF, can't exit." in log_of(workdir)
        proc.send_signal(signal.SIGTERM)
        wait_for(lambda: log_of(workdir).count("Error trying to write the AOF, can't exit.") == 2,
                 'the refused SIGTERM shutdown')
        assert proc.poll() is None and c('GET', 'b') == '2'
        clear_fault(flag)
        wait_for(lambda: info(c, 'aof_last_write_status') == 'ok', 'aof_last_write_status:ok')
        set_fault(flag, 'write ENOSPC')
        assert c('SET', 'c', '3') == '+OK'
        wait_for(lambda: info(c, 'aof_last_write_status') == 'err', 'a second write error')
        c.send('SHUTDOWN', 'FORCE')
        assert c.read() is None
        assert proc.wait(timeout=10) == 0
        assert 'Error trying to write the AOF. Exit anyway.' in log_of(workdir)
        c.close()
    finally:
        kill9(proc)
    proc = start(PEADB, PORTS['peadb'], workdir)
    try:
        c = Conn(PORTS['peadb'])
        # b was written once the disk worked again; FORCE gave up c.
        assert [c('GET', 'a'), c('GET', 'b'), c('GET', 'c')] == ['1', '2', None]
        c.close()
    finally:
        kill9(proc)


def run(name, binary, work, shim):
    if not check_write_error(name, binary, work, shim):
        return
    check_short_write(name, binary, work, shim)
    check_fsync_error(name, binary, work, shim)
    check_always(name, binary, work, shim, 'write ENOSPC',
                 "Can't recover from AOF write error when the AOF fsync policy is 'always'. "
                 "Exiting...")
    check_always(name, binary, work, shim, 'fsync EIO',
                 "Can't persist AOF for fsync error when the AOF fsync policy is 'always': "
                 "Input/output error. Exiting...")
    if name == 'peadb':
        check_rewrite_drops_kept_bytes(work, shim)
        check_shutdown_keeps_data(work, shim)
    print(f'{name}: ok')


def main():
    work = tempfile.mkdtemp(prefix='peadb-aof-error-')
    try:
        shim = build_shim(work)
        if shim is None:
            print('SKIP: cannot build the AOF fault library (no C compiler?)')
            return
        if REDIS:
            run('redis', REDIS, work, shim)
        else:
            print('no Redis 7.2.x found: PeaDB side only')
        run('peadb', PEADB, work, shim)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print('test_aof_write_error_parity: PASS')


if __name__ == '__main__':
    main()

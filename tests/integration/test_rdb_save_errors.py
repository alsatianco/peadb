#!/usr/bin/env python3
"""Owned RDB disk fault injection: fsync/rename parity and streamed write failures.

No fault may silently replace an existing backup. Directory fsync fails after rename;
that error must be reported even though the new file is already visible. Redis uses
stdio internal writes, so write/short/EINTR injection specifically exercises PeaDB's
write(2) sink; fsync/rename are checked against pinned Redis first.
"""
import os
import pathlib
import socket
import subprocess
import sys
import tempfile

from _readiness import wait_until_ready
from test_replica_offset_parity import Conn, PEADB, REDIS

SHIM = r'''
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#ifndef __APPLE__
#include <dlfcn.h>
#endif
static int mode(const char* expected) {
    const char* flag = getenv("RDB_FAULT_FILE");
    if (!flag) return 0;
    int fd = open(flag, O_RDONLY);
    if (fd < 0) return 0;
    char text[32] = {0};
    ssize_t n = read(fd, text, sizeof(text)-1);
    close(fd);
    return n > 0 && strcmp(text, expected) == 0;
}
/* 1: owned temp RDB; 2: owned parent directory; 0: unrelated fd. */
static int target(int fd) {
    char path[PATH_MAX+1] = {0};
#ifdef __APPLE__
    if (fcntl(fd, F_GETPATH, path) < 0) return 0;
#else
    char link[64];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, path, PATH_MAX);
    if (n <= 0) return 0;
    path[n] = 0;
#endif
    const char* dir = getenv("RDB_FAULT_DIR");
    if (!dir) return 0;
    size_t dirlen = strlen(dir);
    if (strcmp(path, dir) == 0) return 2;
    if (strncmp(path, dir, dirlen) != 0 || path[dirlen] != '/') return 0;
    return strncmp(path+dirlen+1, "temp-", 5) == 0 && strstr(path+dirlen+1, ".rdb") ? 1 : 0;
}
static int writes = 0;
static int write_fault(int fd, size_t* size) {
    if (target(fd) != 1) return 0;
    if (mode("write")) { errno = ENOSPC; return -1; }
    if (mode("partial")) {
        if (++writes % 2 == 0) { errno = ENOSPC; return -1; }
        *size = *size > 1 ? *size / 2 : 1;
    }
    if (mode("short") && *size > 97) *size = 97;
    if (mode("eintr") && ++writes % 3 == 1) { errno = EINTR; return -1; }
    return 0;
}
static int sync_fault(int fd) {
    int kind = target(fd);
    if ((kind == 1 && mode("fsync")) || (kind == 2 && mode("dirsync"))) {
        errno = EIO; return -1;
    }
    return 0;
}
static int rename_fault(const char* old) {
    const char* base = strrchr(old, '/');
    base = base ? base+1 : old;
    if (strncmp(base, "temp-", 5) == 0 && mode("rename")) { errno = EACCES; return -1; }
    return 0;
}
#ifdef __APPLE__
static ssize_t hooked_write(int fd, const void* p, size_t n) {
    if (write_fault(fd, &n)) return -1;
    return write(fd, p, n);
}
static int hooked_fsync(int fd) {
    if (sync_fault(fd)) return -1;
    return fsync(fd);
}
static int hooked_fcntl(int fd, int command, ...) {
    if (command == F_FULLFSYNC) {
        if (sync_fault(fd)) return -1;
        return fcntl(fd, command);
    }
    if (command == F_GETFD || command == F_GETFL || command == F_GETOWN)
        return fcntl(fd, command);
    va_list args;
    va_start(args, command);
    int result;
    if (command == F_SETFD || command == F_SETFL || command == F_SETOWN ||
        command == F_DUPFD || command == F_DUPFD_CLOEXEC)
        result = fcntl(fd, command, va_arg(args, int));
    else
        result = fcntl(fd, command, va_arg(args, void*));
    va_end(args);
    return result;
}
static int hooked_rename(const char* a, const char* b) {
    if (rename_fault(a)) return -1;
    return rename(a, b);
}
__attribute__((used)) static const struct {const void* replacement; const void* original;}
interposers[] __attribute__((section("__DATA,__interpose"))) = {
    {(const void*)&hooked_write, (const void*)&write},
    {(const void*)&hooked_fsync, (const void*)&fsync},
    {(const void*)&hooked_rename, (const void*)&rename},
    {(const void*)&hooked_fcntl, (const void*)&fcntl},
};
#else
ssize_t write(int fd, const void* p, size_t n) {
    static ssize_t (*real)(int,const void*,size_t);
    if (!real) real = dlsym(RTLD_NEXT, "write");
    if (write_fault(fd, &n)) return -1;
    return real(fd, p, n);
}
int fsync(int fd) {
    static int (*real)(int);
    if (!real) real = dlsym(RTLD_NEXT, "fsync");
    if (sync_fault(fd)) return -1;
    return real(fd);
}
int fdatasync(int fd) {
    static int (*real)(int);
    if (!real) real = dlsym(RTLD_NEXT, "fdatasync");
    if (sync_fault(fd)) return -1;
    return real(fd);
}
int rename(const char* a, const char* b) {
    static int (*real)(const char*,const char*);
    if (!real) real = dlsym(RTLD_NEXT, "rename");
    if (rename_fault(a)) return -1;
    return real(a,b);
}
#endif
'''


def run(binary, shim, work, writes=False):
    work = pathlib.Path(work)
    work.mkdir()
    flag = work / 'fault'
    # Resolve macOS /var aliases so F_GETPATH and the ownership check agree.
    env = dict(os.environ, RDB_FAULT_FILE=str(flag), RDB_FAULT_DIR=str(work.resolve()))
    env['DYLD_INSERT_LIBRARIES' if sys.platform == 'darwin' else 'LD_PRELOAD'] = str(shim)
    env['ASAN_OPTIONS'] = env.get('ASAN_OPTIONS', '') + ':verify_asan_link_order=0'
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    with (work / 'server.log').open('wb') as log:
        proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1',
                                 '--dir', str(work), '--save', '', '--shutdown-timeout', '0'],
                                stdout=log, stderr=log, env=env)
        conn = None
        try:
            wait_until_ready(port, process=proc)
            conn = Conn(port)
            snapshot = work / 'dump.rdb'
            for fault in ('fsync', 'rename', 'dirsync') + (('write', 'partial') if writes else ()):
                assert conn('SET', 'version', 'old') == '+OK'
                assert conn('SAVE') == '+OK'
                old = snapshot.read_bytes()
                assert conn('SET', 'version', fault) == '+OK'
                assert conn('SET', 'bulk', 'x' * 200000) == '+OK'
                flag.write_text(fault)
                reply = conn('SAVE')
                assert reply and reply.startswith('-ERR'), (binary, fault, reply)
                if fault == 'dirsync':
                    assert snapshot.read_bytes() != old, 'rename occurs before directory fsync'
                else:
                    assert snapshot.read_bytes() == old, (fault, 'old backup overwritten')
                assert not list(work.glob('temp-*.rdb')), (fault, 'temp leaked')
                flag.unlink()
                assert conn('SAVE') == '+OK', (fault, 'retry failed')
                assert conn('GET', 'version') == fault
            if writes:
                for fault in ('short', 'eintr'):
                    flag.write_text(fault)
                    assert conn('SAVE') == '+OK', fault
                    flag.unlink()
                    # Redis's checker is independent of PeaDB's writer and validates the CRC.
                    checker = pathlib.Path(REDIS).with_name('redis-check-rdb')
                    result = subprocess.run([str(checker), str(snapshot)], capture_output=True, text=True)
                    assert result.returncode == 0, result.stdout + result.stderr
            print(pathlib.Path(binary).name, 'disk errors/retry ok', flush=True)
        finally:
            flag.unlink(missing_ok=True)
            if conn:
                conn.close()
            if proc.poll() is None:
                proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


if __name__ == '__main__':
    assert REDIS, 'pinned Redis 7.2.5 required'
    with tempfile.TemporaryDirectory(prefix='peadb-rdb-faults-') as scratch:
        work = pathlib.Path(scratch)
        source, shim = work / 'fault.c', work / 'fault.so'
        source.write_text(SHIM)
        subprocess.run(['cc', '-shared', '-fPIC', '-o', str(shim), str(source)] +
                       (['-ldl'] if sys.platform.startswith('linux') else []), check=True)
        run(REDIS, shim, work / 'redis')
        run(os.environ.get('PEADB_BIN', PEADB), shim, work / 'peadb', writes=True)

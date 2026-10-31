#!/usr/bin/env python3
"""Parent loss during final fsync/close must be observed before RDB rename.

The Redis-first serialization/lifecycle comparison is test_rdb_child_parent_loss.
This PeaDB fault fixture stalls real file operations at the publication boundary,
then resumes after the owning server is killed. The preload touches only the
fixture's temp RDB, and must publish its effect PID before the test proceeds.
"""
import os
import pathlib
import signal
import socket
import subprocess
import sys
import tempfile
import time

from _readiness import ObservedProcess, wait_until_ready
from test_rdb_child_parent_loss import stop, wait_for
from test_replica_offset_parity import Conn, PEADB

SHIM = r'''
#define _GNU_SOURCE
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#ifndef __APPLE__
#include <dlfcn.h>
#endif
static void checkpoint(int fd, const char* operation) {
    const char* selected = getenv("RDB_PARENT_OPERATION");
    const char* arm = getenv("RDB_PARENT_ARM");
    if (!selected || strcmp(selected, operation) || !arm || access(arm, F_OK)) return;
    char path[PATH_MAX+1] = {0};
#ifdef __APPLE__
    if (fcntl(fd, F_GETPATH, path) < 0) return;
#else
    char link[64];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, path, PATH_MAX);
    if (n <= 0) return;
    path[n] = 0;
#endif
    const char* dir = getenv("RDB_PARENT_DIR");
    size_t len = strlen(dir);
    if (strncmp(path, dir, len) || strncmp(path+len, "/temp-", 6)) return;
    int marker = open(getenv("RDB_PARENT_REACHED"), O_WRONLY|O_CREAT|O_TRUNC, 0600);
    if (marker < 0) _exit(91);
    char pid[32];
    int count = snprintf(pid, sizeof(pid), "%d\n", (int)getpid());
    if (write(marker, pid, count) != count) _exit(92);
    close(marker);
    /* Bounded even if the Python fixture fails. */
    for (int i = 0; i < 10000; ++i) {
        if (!access(getenv("RDB_PARENT_RELEASE"), F_OK)) return;
        usleep(1000);
    }
    _exit(93);
}
#ifdef __APPLE__
static int hooked_fsync(int fd) { checkpoint(fd, "fsync"); return fsync(fd); }
static int hooked_close(int fd) { checkpoint(fd, "close"); return close(fd); }
__attribute__((used)) static const struct {const void* replacement; const void* original;}
interposers[] __attribute__((section("__DATA,__interpose"))) = {
    {(const void*)&hooked_fsync, (const void*)&fsync},
    {(const void*)&hooked_close, (const void*)&close},
};
#else
int fsync(int fd) {
    static int (*real)(int);
    if (!real) real = dlsym(RTLD_NEXT, "fsync");
    checkpoint(fd, "fsync"); return real(fd);
}
int close(int fd) {
    static int (*real)(int);
    if (!real) real = dlsym(RTLD_NEXT, "close");
    checkpoint(fd, "close"); return real(fd);
}
#endif
'''


def run(binary, shim, work, operation):
    work.mkdir()
    arm, reached, release = (work / name for name in ('arm', 'reached', 'release'))
    env = dict(os.environ, RDB_PARENT_OPERATION=operation, RDB_PARENT_ARM=str(arm),
               RDB_PARENT_DIR=str(work.resolve()), RDB_PARENT_REACHED=str(reached),
               RDB_PARENT_RELEASE=str(release))
    env['DYLD_INSERT_LIBRARIES' if sys.platform == 'darwin' else 'LD_PRELOAD'] = str(shim)
    env['ASAN_OPTIONS'] = env.get('ASAN_OPTIONS', '') + ':verify_asan_link_order=0'
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    with (work / 'server.log').open('wb') as log:
        proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1',
                                 '--dir', str(work), '--save', '', '--shutdown-timeout', '0'],
                                stdout=log, stderr=log, env=env)
        conn, child = None, None
        try:
            wait_until_ready(port, process=proc)
            conn = Conn(port)
            assert conn('SET', 'version', 'old') == '+OK'
            assert conn('SAVE') == '+OK'
            backup = work / 'dump.rdb'
            old = backup.read_bytes()
            assert conn('SET', 'version', 'new') == '+OK'
            arm.touch()
            assert conn('BGSAVE') == '+Background saving started'
            wait_for(lambda: reached.exists() and reached.read_text().endswith('\n'),
                     f'{operation} hook did not execute')
            child = int(reached.read_text())
            # Hook identity is checked against the server's actual direct child.
            from test_rdb_child_parent_loss import children
            assert children(proc.pid) == [child]
            assert conn('PING') == '+PONG'
            proc.kill()
            proc.wait(timeout=5)
            release.touch()
            wait_for(lambda: ObservedProcess(child).poll() is not None,
                     f'child did not exit after {operation} resumed')
            assert backup.read_bytes() == old, f'{operation}: orphan published a snapshot'
            assert not list(work.glob('temp-*.rdb')), f'{operation}: temp output leaked'
            print('parent loss during', operation, 'preserves backup and cleans temp', flush=True)
        finally:
            release.touch()
            if conn:
                conn.close()
            stop(proc)
            if child and ObservedProcess(child).poll() is None:
                os.kill(child, signal.SIGKILL)
                wait_for(lambda: ObservedProcess(child).poll() is not None, 'owned child survived')


if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='peadb-rdb-publication-') as tmp:
        work = pathlib.Path(tmp).resolve()
        source, shim = work / 'pause.c', work / 'pause.so'
        source.write_text(SHIM)
        subprocess.run(['cc', '-shared', '-fPIC', str(source), '-o', str(shim)] +
                       ([] if sys.platform == 'darwin' else ['-ldl']), check=True)
        for operation in ('fsync', 'close'):
            run(os.environ.get('PEADB_BIN', PEADB), shim, work / operation, operation)

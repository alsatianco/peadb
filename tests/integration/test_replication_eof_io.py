#!/usr/bin/env python3
"""Real replica socket/spool EINTR, short reads/writes and failed writes, with effect proof."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from _replication_snapshot import MARK, Master, PEADB, check, server, snapshot
from test_replication_eof_framing import run

SHIM = r'''
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#ifdef __APPLE__
#define REAL(name) name
#else
#define REAL(name) ((__typeof__(&name))dlsym(RTLD_NEXT, #name))
#endif
static void note(char c) {
  const char* path = getenv("REPL_FAULT_TRACE");
  int fd = open(path, O_WRONLY|O_CREAT|O_APPEND, 0600);
  if (fd >= 0) { ssize_t written = REAL(write)(fd, &c, 1); (void)written; close(fd); }
}
static int spool(int fd) {
  char path[PATH_MAX+1] = {0};
#ifdef __APPLE__
  if (fcntl(fd, F_GETPATH, path) < 0) return 0;
#else
  char link[80]; snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
  ssize_t n = readlink(link, path, PATH_MAX);
  if (n <= 0) return 0;
  path[n] = 0;
#endif
  const char* base = strrchr(path, '/');
  return base && !strncmp(base+1, "temp-", 5) && strstr(base, ".rdb");
}
static int recv_fault(size_t* size) {
  static int interrupted = 0;
  if (*size > 1) {
    if (!interrupted++) { note('r'); errno = EINTR; return -1; }
    if (*size > 257) { *size = 257; note('s'); }
  }
  return 0;
}
static ssize_t hooked_recv(int fd, void* buf, size_t size, int flags) {
  if (recv_fault(&size) < 0) return -1;
  return REAL(recv)(fd, buf, size, flags);
}
static __attribute__((noinline)) int poll_fault(struct pollfd* fds, nfds_t count) {
  static int interrupted = 0;
  if (count == 1 && fds[0].events == POLLIN && !interrupted++) {
    note('p'); errno = EINTR; return -1;
  }
  return 0;
}
static int hooked_poll(struct pollfd* fds, nfds_t count, int ms) {
  if (poll_fault(fds, count) < 0) return -1;
  return REAL(poll)(fds, count, ms);
}
static ssize_t hooked_write(int fd, const void* buf, size_t size) {
  static int calls = 0;
  if (spool(fd)) {
    const char* mode = getenv("REPL_FAULT_MODE");
    if (mode && !strcmp(mode, "zero")) { note('z'); return 0; }
    if (mode && !strcmp(mode, "error")) { note('e'); errno = EIO; return -1; }
    if (!calls++) { note('w'); errno = EINTR; return -1; }
    if (size > 1) { size = (size + 1) / 2; note('h'); }
  }
  return REAL(write)(fd, buf, size);
}
#ifdef __APPLE__
__attribute__((used)) static const struct {const void* replacement; const void* original;}
interposers[] __attribute__((section("__DATA,__interpose"))) = {
  {(const void*)hooked_recv, (const void*)recv},
  {(const void*)hooked_poll, (const void*)poll},
  {(const void*)hooked_write, (const void*)write}
};
#else
ssize_t recv(int fd, void* buf, size_t n, int flags) {return hooked_recv(fd, buf, n, flags);}
int poll(struct pollfd* fds, nfds_t n, int ms) {return hooked_poll(fds, n, ms);}
ssize_t write(int fd, const void* buf, size_t n) {return hooked_write(fd, buf, n);}
/* GCC's fortified sanitizer builds call these symbols instead of recv/poll.
 * Preserve libc's bounds checks before injecting faults or shortening reads. */
ssize_t __recv_chk(int fd, void* buf, size_t size, size_t bufsize, int flags) {
  if (size <= bufsize && recv_fault(&size) < 0) return -1;
  return REAL(__recv_chk)(fd, buf, size, bufsize, flags);
}
int __poll_chk(struct pollfd* fds, nfds_t count, int ms, size_t bufsize) {
  if (count <= bufsize / sizeof(*fds) && poll_fault(fds, count) < 0) return -1;
  return REAL(__poll_chk)(fds, count, ms, bufsize);
}
#endif
'''


def main():
    rdb, deadline = snapshot()
    with tempfile.TemporaryDirectory(prefix='peadb-repl-io-') as td:
        td = Path(td)
        source, lib = td / 'shim.c', td / 'shim.so'
        source.write_text(SHIM)
        cc = shutil.which('cc')
        check(cc is not None, 'C compiler required for actual I/O fault regression')
        command = [cc, '-O2', '-Wall', '-Wextra', '-shared', '-fPIC', str(source), '-o', str(lib)]
        if sys.platform != 'darwin':
            command.append('-ldl')
        subprocess.run(command, check=True)
        env = dict(os.environ)
        # A preload shim must precede libasan for syscall interception on Linux.
        # Keep all normal leak/error checks while permitting that established order.
        if sys.platform != 'darwin':
            options = env.get('ASAN_OPTIONS', '')
            env['ASAN_OPTIONS'] = options + (':' if options else '') + 'verify_asan_link_order=0'
        variable = 'DYLD_INSERT_LIBRARIES' if sys.platform == 'darwin' else 'LD_PRELOAD'
        env[variable] = str(lib) + (':' + env[variable] if env.get(variable) else '')
        env['REPL_FAULT_TRACE'] = str(td / 'trace')
        run(PEADB, rdb, deadline, coalesced=True, env=env)
        trace = (td / 'trace').read_text()
        effects = {c: trace.count(c) for c in 'rspwh'}
        print('I/O fault effects:', effects, flush=True)
        check(all(effects.values()), f'every EINTR/short I/O fault took effect: {effects}')
        for mode, mark in (('zero', 'z'), ('error', 'e')):
            env['REPL_FAULT_MODE'] = mode
            with server(PEADB, env=env) as (r, _, work, _), Master() as master:
                r('SET', 'preserved', 'old')
                r('REPLICAOF', '127.0.0.1', master.port)
                sock, _, _ = master.accept()
                master.header(sock, b'$EOF:' + MARK)
                sock.sendall(rdb[:1000])
                master.accept()  # reconnect proves the previous attempt terminated
                check(r('GET', 'preserved') == b'old' and r('EXISTS', 'binary') == 0,
                      'spool write failure does not publish snapshot')
                check(not list(work.glob('temp-*.rdb')), 'failed spool removed')
                check(mark in (td / 'trace').read_text(), 'write fault took effect')
        print('PASS real poll/recv/write EINTR, short I/O, zero/error write cleanup', flush=True)


if __name__ == '__main__':
    main()

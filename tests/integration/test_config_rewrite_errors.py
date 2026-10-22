#!/usr/bin/env python3
"""Redis-first CONFIG REWRITE I/O failures preserve errno, old files and temp ownership."""
import argparse
import errno
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

from test_config_rewrite_parity import PEADB, Server, reference

# Only the owned config/temporary file and its parent directory are intercepted.
SHIM_SOURCE = r'''
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#ifndef __APPLE__
#include <dlfcn.h>
#endif
static int mode(const char *wanted) {
  const char *flag = getenv("FAILCONFIG_FILE");
  if (!flag) return 0;
  FILE *f = fopen(flag, "r");
  if (!f) return 0;
  char buf[32] = {0};
  /* read rather than fread: the latter is an injected operation. */
  ssize_t n = read(fileno(f), buf, sizeof(buf)-1);
  fclose(f);
  if (n <= 0) return 0;
  return strcmp(buf, wanted) == 0;
}
static int path_is_config(const char *p, int temporary) {
  const char *config = getenv("FAILCONFIG_PATH");
  if (!config || !p) return 0;
  size_t n = strlen(config);
  return temporary ? strncmp(p, config, n) == 0 && p[n] == '.' : strcmp(p, config) == 0;
}
static int fd_path(int fd, char *p) {
#ifdef __APPLE__
  return fcntl(fd, F_GETPATH, p) != -1;
#else
  char link[64];
  snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
  ssize_t n = readlink(link, p, PATH_MAX);
  if (n < 0) return 0;
  p[n] = 0;
  return 1;
#endif
}
static int is_config(int fd, int temporary) {
  char p[PATH_MAX+1] = {0};
  return fd_path(fd, p) && path_is_config(p, temporary);
}
static int is_parent(int fd) {
  char p[PATH_MAX+1] = {0}, parent[PATH_MAX+1] = {0};
  const char *config = getenv("FAILCONFIG_PATH");
  if (!config || !fd_path(fd, p)) return 0;
  snprintf(parent, sizeof(parent), "%s", config);
  char *slash = strrchr(parent, '/');
  if (!slash) return 0;
  *slash = 0;
  return strcmp(p, parent) == 0;
}
#ifdef __APPLE__
#define REAL(name) name
#else
#define REAL(name) real_##name
static ssize_t (*real_write)(int, const void *, size_t);
static int (*real_fsync)(int);
static int (*real_fchmod)(int, mode_t);
static int (*real_rename)(const char *, const char *);
static size_t (*real_fread)(void *, size_t, size_t, FILE *);
__attribute__((constructor)) static void init(void) {
  real_write = dlsym(RTLD_NEXT, "write");
  real_fsync = dlsym(RTLD_NEXT, "fsync");
  real_fchmod = dlsym(RTLD_NEXT, "fchmod");
  real_rename = dlsym(RTLD_NEXT, "rename");
  real_fread = dlsym(RTLD_NEXT, "fread");
}
#endif
static int short_pending, interrupted;
static ssize_t config_write(int fd, const void *buf, size_t n) {
  if (is_config(fd, 1)) {
    if (mode("write") || (mode("short") && short_pending)) {
      short_pending = 0;
      errno = ENOSPC;
      return -1;
    }
    if (mode("short") && n > 1) {
      short_pending = 1;
      n /= 2;
    }
    if (mode("eintr") && !interrupted++) {
      errno = EINTR;
      return -1;
    }
  }
  return REAL(write)(fd, buf, n);
}
static int config_fsync(int fd) {
  if ((is_config(fd, 1) && mode("fsync")) || (is_parent(fd) && mode("dirsync"))) {
    errno = EIO;
    return -1;
  }
  return REAL(fsync)(fd);
}
static int config_fchmod(int fd, mode_t m) {
  if (is_config(fd, 1) && mode("chmod")) { errno = EPERM; return -1; }
  return REAL(fchmod)(fd, m);
}
static int config_rename(const char *src, const char *dst) {
  if (path_is_config(src, 1) && path_is_config(dst, 0) && mode("rename")) {
    errno = EACCES;
    return -1;
  }
  return REAL(rename)(src, dst);
}
static size_t config_fread(void *p, size_t size, size_t n, FILE *stream) {
  if (is_config(fileno(stream), 0) && mode("read")) { errno = EIO; return 0; }
  return REAL(fread)(p, size, n, stream);
}
#ifdef __APPLE__
#define INTERPOSE(replacement, replacee) \
  __attribute__((used)) static struct { const void *r; const void *o; } interpose_##replacee \
  __attribute__((section("__DATA,__interpose"))) = { (const void *)replacement, (const void *)replacee };
static int config_fcntl(int fd, int cmd, ...) {
  if (cmd == F_FULLFSYNC) {
    if (is_parent(fd) && (mode("dirsync") || mode("native-dirsync"))) { errno = EIO; return -1; }
    return fcntl(fd, cmd);
  }
  if (cmd == F_GETFD || cmd == F_GETFL || cmd == F_GETOWN) return fcntl(fd, cmd);
  va_list ap;
  va_start(ap, cmd);
  int result;
  if (cmd == F_SETFD || cmd == F_SETFL || cmd == F_DUPFD || cmd == F_NOCACHE ||
      cmd == F_SETOWN || cmd == F_RDAHEAD || cmd == F_SETNOSIGPIPE) {
    int arg = va_arg(ap, int);
    result = fcntl(fd, cmd, arg);
  } else {
    void *arg = va_arg(ap, void *);
    result = fcntl(fd, cmd, arg);
  }
  va_end(ap);
  return result;
}
INTERPOSE(config_fcntl, fcntl)
INTERPOSE(config_write, write)
INTERPOSE(config_fsync, fsync)
INTERPOSE(config_fchmod, fchmod)
INTERPOSE(config_rename, rename)
INTERPOSE(config_fread, fread)
#else
ssize_t write(int f, const void *p, size_t n) { return config_write(f,p,n); }
int fsync(int f) { return config_fsync(f); }
int fdatasync(int f) {
  static int (*real_fdatasync)(int);
  if (!real_fdatasync) real_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
  if (is_parent(f) && (mode("dirsync") || mode("native-dirsync"))) { errno = EIO; return -1; }
  return real_fdatasync(f);
}
int fchmod(int f, mode_t m) { return config_fchmod(f,m); }
int rename(const char *s, const char *d) { return config_rename(s,d); }
size_t fread(void *p, size_t s, size_t n, FILE *f) { return config_fread(p,s,n,f); }
#endif
'''


def build_shim(work):
    source = pathlib.Path(work) / 'rewrite_fault.c'
    source.write_text(SHIM_SOURCE)
    lib = pathlib.Path(work) / ('rewrite_fault.dylib' if sys.platform == 'darwin' else 'rewrite_fault.so')
    cc = shutil.which('cc')
    assert cc, 'C compiler required for CONFIG REWRITE fault injection'
    flags = ['-dynamiclib'] if sys.platform == 'darwin' else ['-shared', '-fPIC', '-ldl']
    subprocess.run([cc, '-Wall', '-Wextra', '-O2', str(source), '-o', str(lib), *flags], check=True)
    return lib


def io_failures(binary, lib):
    with tempfile.TemporaryDirectory(prefix='peadb-rewrite-errors-') as work:
        flag = pathlib.Path(work) / 'fault'
        env = os.environ.copy()
        env['DYLD_INSERT_LIBRARIES' if sys.platform == 'darwin' else 'LD_PRELOAD'] = str(lib)
        # Match the other fault fixtures: a preload shim precedes the Linux ASAN
        # runtime, while all actual address/undefined-behavior checks stay enabled.
        asan = env.get('ASAN_OPTIONS', '')
        env['ASAN_OPTIONS'] = (asan + ':' if asan else '') + 'verify_asan_link_order=0'
        env['FAILCONFIG_FILE'] = str(flag)
        env['FAILCONFIG_PATH'] = str((pathlib.Path(work) / 'server.conf').resolve())
        server = Server(binary, work, 'timeout 3\n', env=env)
        with server.running():
            with server.conn() as c:
                for fault, code in [('read', errno.EIO), ('write', errno.ENOSPC),
                                    ('short', errno.ENOSPC), ('fsync', errno.EIO),
                                    ('chmod', errno.EPERM), ('rename', errno.EACCES),
                                    ('dirsync', errno.EIO), ('native-dirsync', errno.EIO)]:
                    before = server.path.read_bytes()
                    assert c('CONFIG', 'SET', 'timeout', '11') == b'+OK'
                    flag.write_text(fault)
                    result = c('CONFIG', 'REWRITE')
                    expected = b'-ERR Rewriting config file: ' + os.strerror(code).encode()
                    assert result == expected, (fault, result, expected)
                    after = server.path.read_bytes()
                    if fault in ('dirsync', 'native-dirsync'):
                        assert b'timeout 11\n' in after, 'rename precedes directory sync'
                    else:
                        assert after == before, (fault, 'old config changed before publication')
                    assert not list(pathlib.Path(work).glob('server.conf.*')), fault
                    flag.unlink()
                    assert c('CONFIG', 'REWRITE') == b'+OK', (fault, 'recovery')
                    assert c('CONFIG', 'SET', 'timeout', '3') == b'+OK'
                    assert c('CONFIG', 'REWRITE') == b'+OK'
                flag.write_text('eintr')
                assert c('CONFIG', 'REWRITE') == b'+OK', 'interrupted writes must retry'
                flag.unlink()
        with server.running():
            with server.conn() as c:
                assert c.get('timeout') == b'3'


def permissions(binary):
    if os.geteuid() == 0:
        print('SKIP permission-only cases: root bypasses chmod; run as a nonroot user', flush=True)
        return
    with tempfile.TemporaryDirectory(prefix='peadb-rewrite-permissions-') as work:
        server = Server(binary, work, 'timeout 3\n')
        with server.running():
            with server.conn() as c:
                before = server.path.read_bytes()
                server.path.chmod(0)
                try:
                    assert c('CONFIG', 'REWRITE') == (b'-ERR Rewriting config file: ' +
                                                     os.strerror(errno.EACCES).encode())
                finally:
                    server.path.chmod(0o600)
                assert server.path.read_bytes() == before
                pathlib.Path(work).chmod(0o500)
                try:
                    assert c('CONFIG', 'REWRITE') == (b'-ERR Rewriting config file: ' +
                                                     os.strerror(errno.EACCES).encode())
                finally:
                    pathlib.Path(work).chmod(0o700)
                assert server.path.read_bytes() == before
                assert not list(pathlib.Path(work).glob('server.conf.*'))
                assert c('CONFIG', 'REWRITE') == b'+OK'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--engine', choices=('redis', 'peadb', 'both'), default='both')
    args = parser.parse_args()
    engines = [('redis', reference()), ('peadb', PEADB)]
    with tempfile.TemporaryDirectory(prefix='peadb-rewrite-shim-') as work:
        lib = build_shim(work)
        for name, binary in engines:
            if args.engine not in ('both', name):
                continue
            io_failures(binary, lib)
            permissions(binary)
            print(f'PASS {name}: I/O fault injection, recovery and permissions', flush=True)


if __name__ == '__main__':
    main()

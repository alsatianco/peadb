#!/usr/bin/env python3
"""Actual pipe/fork/read/write failures and interrupted/short relay I/O, with effect proof."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from _replication_snapshot import PEADB, check, encode, info, server, wait_for
from test_rdb_child_parent_loss import children
from test_replication_pipe_relay import Peer

SHIM = r'''
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#ifdef __APPLE__
#define REAL(name) name
#else
#define REAL(name) ((__typeof__(&name))dlsym(RTLD_NEXT, #name))
#endif
static int data_read = -1, data_write = -1, pipe_count, reader_once, writer_once;
static pid_t owner;
static char selected;
static char mode(void) {
  const char* path = getenv("PIPE_FAULT_ARM");
  if (!path) return 0;
  int fd = open(path, O_RDONLY);
  char current = 0;
  if (fd >= 0) { ssize_t n = REAL(read)(fd, &current, 1); (void)n; close(fd); }
  if (current != selected) {
    pipe_count = reader_once = writer_once = 0;
    data_read = data_write = -1;
    selected = current;
  }
  return current;
}
static void note(char effect) {
  int fd = open(getenv("PIPE_FAULT_TRACE"), O_WRONLY|O_CREAT|O_APPEND, 0600);
  if (fd >= 0) { ssize_t n = REAL(write)(fd, &effect, 1); (void)n; close(fd); }
}
static int hooked_pipe(int fds[2]) {
  char m = mode();
  if (m && m != 'o') {
    ++pipe_count;
    if ((m == 'p' && pipe_count == 1) || (m == 'c' && pipe_count == 2)) {
      note(m); errno = EMFILE; return -1;
    }
  }
  int rc = REAL(pipe)(fds);
  if (rc == 0 && pipe_count == 1) { data_read=fds[0]; data_write=fds[1]; owner=getpid(); }
  return rc;
}
static pid_t hooked_fork(void) {
  if (mode() == 'f') { note('f'); errno = EAGAIN; return -1; }
  return REAL(fork)();
}
static int read_fault(int fd, size_t* size) {
  char m = mode();
  if (fd == data_read && owner == getpid()) {
    if (m == 'r') { note('r'); errno = EIO; return -1; }
    if (m == 'i') {
      if (!reader_once++) { note('R'); errno = EINTR; return -1; }
      if (*size > 257) { *size = 257; note('s'); }
    }
  }
  return 0;
}
static ssize_t hooked_read(int fd, void* buf, size_t size) {
  if (read_fault(fd, &size) < 0) return -1;
  return REAL(read)(fd, buf, size);
}
static ssize_t hooked_write(int fd, const void* buf, size_t size) {
  char m = mode();
  if (fd == data_write && owner != getpid()) {
    if (m == 'w') { note('w'); errno = EIO; return -1; }
    if (m == 'z') { note('z'); return 0; }
    if (m == 'i') {
      if (!writer_once++) { note('W'); errno = EINTR; return -1; }
      if (size > 257) { size = 257; note('h'); }
    }
  }
  return REAL(write)(fd, buf, size);
}
#ifdef __APPLE__
__attribute__((used)) static const struct {const void* replacement; const void* original;}
interposers[] __attribute__((section("__DATA,__interpose"))) = {
  {(const void*)hooked_pipe, (const void*)pipe},
  {(const void*)hooked_fork, (const void*)fork},
  {(const void*)hooked_read, (const void*)read},
  {(const void*)hooked_write, (const void*)write}
};
#else
int pipe(int fds[2]) {return hooked_pipe(fds);}
pid_t fork(void) {return hooked_fork();}
ssize_t read(int fd, void* buf, size_t size) {return hooked_read(fd, buf, size);}
ssize_t write(int fd, const void* buf, size_t size) {return hooked_write(fd, buf, size);}
ssize_t __read_chk(int fd, void* buf, size_t size, size_t capacity) {
  if (size <= capacity && read_fault(fd, &size) < 0) return -1;
  return REAL(__read_chk)(fd, buf, size, capacity);
}
#endif
'''


def main():
    with tempfile.TemporaryDirectory(prefix='peadb-pipe-io-') as tmp:
        td = Path(tmp)
        source, shim = td / 'shim.c', td / 'shim.so'
        source.write_text(SHIM)
        subprocess.run(['cc', '-O2', '-shared', '-fPIC', str(source), '-o', str(shim)] +
                       ([] if sys.platform == 'darwin' else ['-ldl']), check=True)
        arm, trace = td / 'arm', td / 'trace'
        arm.write_text('o')
        env = dict(os.environ, PIPE_FAULT_ARM=str(arm), PIPE_FAULT_TRACE=str(trace))
        env['DYLD_INSERT_LIBRARIES' if sys.platform == 'darwin' else 'LD_PRELOAD'] = str(shim)
        env['ASAN_OPTIONS'] = env.get('ASAN_OPTIONS', '') + ':verify_asan_link_order=0'
        for mode in 'pcfrwzi':
            with server(PEADB, env=env) as (c, port, work, proc):
                c('SET', 'payload', 'binary\x00' * 10000)
                c('CONFIG', 'SET', 'repl-ping-replica-period', 3600)
                peer = Peer(port, request=False)
                try:
                    arm.write_text(mode)
                    peer.request()
                    if mode == 'i':
                        peer.read_header()
                        check(peer.header.startswith(b'$EOF:'), 'interrupted pipe keeps EOF framing')
                        check(peer.snapshot().startswith(b'REDIS'), 'interrupted/short pipe completes')
                    else:
                        # Drain any partial header/RDB until the owned attempt closes.
                        while peer.stream.read1(65536):
                            pass
                    wait_for(lambda: not children(proc.pid), f'{mode}: child reaped')
                    check(mode in trace.read_text() if mode != 'i' else
                          set('RsWh') <= set(trace.read_text()), f'{mode}: actual syscall effects')
                    check(not (work / 'dump.rdb').exists() and not list(work.glob('temp-*.rdb')),
                          'faulted socket target cannot publish disk snapshot')
                    check(c('PING') == '+PONG', 'server survives actual I/O fault')
                finally:
                    peer.close()
                    arm.write_text('o')
                wait_for(lambda: info(c, 'connected_slaves') == '0', 'failed peer cleanup')
                # After every injected failure the same process must accept a fresh pipe.
                peer = Peer(port)
                try:
                    check(peer.snapshot().startswith(b'REDIS'), 'healthy retry after actual fault')
                    wait_for(lambda: not children(proc.pid), 'retry child reaped')
                finally:
                    peer.close()
        print('actual pipe/second-pipe/fork/read/write/zero/EINTR/short effects:',
              {mark: trace.read_text().count(mark) for mark in 'pcfrwzRsWh'}, flush=True)


if __name__ == '__main__':
    main()

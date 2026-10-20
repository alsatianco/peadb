#!/usr/bin/env python3
"""Redis-first startup refusal and RDB load counters across databases/reloads."""
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import time

from _readiness import wait_until_ready
from test_rdb_redis_interop import Conn, REDIS, with_checksum

PEADB = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).resolve().parents[2] / 'peadb-server')


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    proc.wait()


def spawn(binary, work, p, *extra):
    with (work / 'server.log').open('wb') as log:
        return subprocess.Popen([
            binary, '--port', str(p), '--bind', '127.0.0.1', '--dir', str(work),
            '--save', '', '--loglevel', 'notice', '--enable-debug-command', 'yes',
            '--shutdown-timeout', '0', *extra], stdout=log, stderr=subprocess.STDOUT)


def rejects(binary, work, data, mode=0o600):
    dump = work / 'dump.rdb'
    dump.write_bytes(data)
    dump.chmod(mode)
    proc = spawn(binary, work, port())
    try:
        try:
            code = proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            raise AssertionError(f'{binary} served an unreadable/invalid snapshot') from None
        log = (work / 'server.log').read_text(errors='replace')
        assert code != 0, (binary, code, log)
        # Redis's startup diagnostic, also used by its native RDB suite.
        if mode == 0o200:
            assert 'Fatal error loading the DB' in log, log
    finally:
        stop(proc)
        dump.chmod(0o600)


def fixture():
    body = b'REDIS0011'
    future = int(time.time() * 1000) + 3600000
    for db in (0, 1, 15):
        body += b'\xfe' + bytes([db])
        for key, deadline in ((b'live', None), (b'future', future), (b'old', 1)):
            if deadline is not None:
                body += b'\xfc' + struct.pack('<Q', deadline)
            body += b'\x00' + bytes([len(key)]) + key + b'\x01v'
    return with_checksum(body + b'\xff')


def counters(c, loaded, expired):
    info = dict(line.split(b':', 1) for line in c('INFO', 'persistence').splitlines()
                if b':' in line)
    assert info.get(b'rdb_last_load_keys_loaded') == str(loaded).encode(), info
    assert info.get(b'rdb_last_load_keys_expired') == str(expired).encode(), info


def loads(binary, work, data, replica=False):
    (work / 'dump.rdb').write_bytes(data)
    p = port()
    extra = ('--replicaof', '127.0.0.1', str(port())) if replica else ()
    proc = spawn(binary, work, p, *extra)
    c = None
    try:
        try:
            wait_until_ready(p, process=proc, timeout=10)
        except Exception:
            print((work / 'server.log').read_text(errors='replace'), file=sys.stderr)
            raise
        c = Conn(p)
        counters(c, 9 if replica else 6, 0 if replica else 3)
        assert c('DEBUG', 'RELOAD', 'NOSAVE') == '+OK'
        counters(c, 9 if replica else 6, 0 if replica else 3)
        if not replica:
            for db in (0, 1, 15):
                assert c('SELECT', db) == '+OK'
                assert c('GET', 'live') == b'v'
                assert c('GET', 'old') is None
                assert c('PTTL', 'future') > 0
            assert c('SAVE') == '+OK'
            counters(c, 6, 3)  # A save/read does not change the last load's counters.
            assert c('DEBUG', 'RELOAD', 'NOSAVE') == '+OK'
            counters(c, 6, 0)
            (work / 'dump.rdb').write_bytes(with_checksum(b'REDIS0011\xff'))
            assert c('DEBUG', 'RELOAD', 'NOSAVE') == '+OK'
            counters(c, 0, 0)
    finally:
        if c is not None:
            c.close()
        stop(proc)


def check(binary, work):
    data = fixture()
    if os.geteuid() != 0:
        rejects(binary, work, data, 0o200)
    else:
        print('Unreadable-file permission check unavailable as root', flush=True)
    for invalid in (b'', b'not a snapshot\n', b'REDIS0011', data[:-1] + bytes([data[-1] ^ 1])):
        rejects(binary, work, invalid)
    loads(binary, work, data)
    loads(binary, work, data, replica=True)
    print(f'{binary}: startup refusal and RDB load counters passed', flush=True)


def main():
    assert REDIS and ' v=7.2.5 ' in subprocess.check_output(
        [REDIS, '--version'], text=True), 'Pinned Redis 7.2.5 is required'
    with tempfile.TemporaryDirectory(prefix='peadb-rdb-status-') as scratch:
        work = Path(scratch)
        check(REDIS, work)
        check(PEADB, work)
        # The documented legacy recovery format remains readable, but a damaged one is fatal.
        rejects(PEADB, work, b'PEADB-SNAPSHOT-V1\nS malformed\n')
        (work / 'dump.rdb').write_bytes(b'PEADB-SNAPSHOT-V1\nS 0 -1 "key" "value"\n')
        p = port()
        proc = spawn(PEADB, work, p)
        c = None
        try:
            wait_until_ready(p, process=proc, timeout=10)
            c = Conn(p)
            assert c('GET', 'key') == b'value'
        finally:
            if c is not None:
                c.close()
            stop(proc)
    print('RDB load status parity passed')


if __name__ == '__main__':
    main()

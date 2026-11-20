#!/usr/bin/env python3
"""Disk full sync replaces large function owners; shutdown safely drains deferred callbacks."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

from _readiness import wait_until_ready
from _redis_path import redis_server_path
from test_replica_offset_parity import Conn, stop, wait_for
from test_replica_resync_parity import ports

ROOT = Path(__file__).resolve().parents[2]
PEADB = os.environ.get('PEADB_BIN', str(ROOT / 'peadb-server'))
COUNT = 2048


def run(binary, work):
    processes, clients = [], []
    pm, pr = ports(2)
    try:
        for role, port in [('master', pm), ('replica', pr)]:
            directory = work / role
            directory.mkdir()
            with (directory / 'server.log').open('wb') as log:
                proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1',
                    '--dir', str(directory), '--save', '', '--shutdown-timeout', '0',
                    '--repl-diskless-sync', 'no', '--enable-debug-command', 'yes',
                    '--loading-process-events-interval-bytes', '1024'],
                    stdout=log, stderr=subprocess.STDOUT)
            processes.append(proc)
            wait_until_ready(port, process=proc)
            clients.append(Conn(port))
        master, replica = clients
        code = '#!lua name=old\nlocal t={};for i=1,65536 do t[i]=i end;\n' + ''.join(
            "redis.register_function{function_name='old%d',flags={'no-writes'},"
            "callback=function() return #t end}\n" % i
            for i in range(COUNT))
        assert replica('FUNCTION', 'LOAD', code) == 'old'
        assert replica('FCALL', 'old0', 0) == 65536
        assert replica('FCALL', 'old2047', 0) == 65536
        incoming = ("#!lua name=incoming\nlocal n=0;redis.register_function{"
                    "function_name='fresh',flags={'no-writes'},"
                    "callback=function() n=n+1;return n end}")
        assert master('FUNCTION', 'LOAD', incoming) == 'incoming'
        assert master.pipeline([['SET', 'key:' + str(i), 'v' * 1024] for i in range(64)]) == ['+OK'] * 64
        assert replica('CONFIG', 'SET', 'key-load-delay', 5000) == '+OK'
        assert replica('REPLICAOF', '127.0.0.1', pm) == '+OK'
        def loading_admission():
            # One pump opportunity: separate requests can cross a load phase/publication.
            # Declare these read-only callbacks so Redis's earlier READONLY gate does
            # not mask loading admission while PeaDB still validates the old registry.
            state, reply = replica.pipeline([['INFO', 'persistence'], ['FCALL', 'old0', 0]])
            if 'loading:1\r\n' not in state:
                return False
            assert reply.startswith('-LOADING'), reply
            return True

        wait_for(loading_admission, 'real disk RDB loading and function admission')
        # Stream writes and function calls cannot see the detached registry while loading.
        assert master('SET', 'suffix', 'after') == '+OK'
        wait_until_ready(pr, process=processes[1], timeout=10)
        wait_for(lambda: replica('GET', 'suffix') == 'after', 'applied post-snapshot suffix')
        assert replica('GET', 'key:63') == 'v' * 1024
        assert replica('FCALL', 'fresh', 0) == 1
        assert replica('FCALL', 'fresh', 0) == 2
        assert replica('FCALL', 'old0', 0) == '-ERR Function not found'
        assert master('WAIT', 1, 3000) == 1
        # Repeat with no delayed keys, then request shutdown immediately: the server owns
        # any unfinished callback retirement until before Lua is closed.
        assert replica('REPLICAOF', 'NO', 'ONE') == '+OK'
        assert replica('FUNCTION', 'FLUSH') == '+OK'
        assert replica('FUNCTION', 'LOAD', code) == 'old'
        assert replica('CONFIG', 'SET', 'key-load-delay', 0) == '+OK'
        assert replica('REPLICAOF', '127.0.0.1', pm) == '+OK'
        wait_for(lambda: replica('FCALL', 'fresh', 0) == 1, 'second function replacement', timeout=10)
        started = time.monotonic()
        assert replica('PING') == '+PONG'
        assert time.monotonic() - started < 1, 'retirement stalled the protocol loop'
        assert replica('SHUTDOWN', 'NOSAVE') is None
        assert processes[1].wait(timeout=5) == 0
        print(binary, 'disk function replacement, guarded/normal service and shutdown passed', flush=True)
    finally:
        for client in clients:
            client.close()
        for proc in reversed(processes):
            stop(proc)
        if sys.exc_info()[0] is not None:
            for log in work.glob('*/server.log'):
                print(log.read_text(errors='replace'), flush=True)


def main():
    redis = redis_server_path()
    assert redis and 'v=7.2.5 ' in subprocess.check_output([redis, '--version'], text=True)
    with tempfile.TemporaryDirectory(prefix='peadb-function-retirement-') as directory:
        for index, binary in enumerate([redis, PEADB]):
            work = Path(directory) / str(index)
            work.mkdir()
            run(binary, work)


if __name__ == '__main__':
    main()

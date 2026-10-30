#!/usr/bin/env python3
"""Redis-first failed restores preserve existing callback closures, not just sources."""
from pathlib import Path
import socket
import subprocess
import sys
import tempfile

from _readiness import wait_until_ready
from test_rdb_redis_interop import Conn, REDIS, crc64

PEADB = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).resolve().parents[2] / 'peadb-server')
COUNTER = ("#!lua name=counter\nlocal n=0; redis.register_function{function_name='count', "
           "callback=function() n=n+1; return n end, flags={'no-writes'}}")


def payload(*sources):
    body = b''
    for source in sources:
        code = source.encode()
        assert len(code) < 16384
        length = bytes([len(code)]) if len(code) < 64 else bytes([0x40 | (len(code) >> 8), len(code) & 255])
        body += b'\xf5' + length + code
    body += b'\x0b\x00'
    return body + crc64(body).to_bytes(8, 'little')


def run(binary):
    with tempfile.TemporaryDirectory(prefix='peadb-functions-isolation-') as directory:
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        with (Path(directory) / 'server.log').open('wb') as log:
            proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1',
                                     '--dir', directory, '--save', '', '--shutdown-timeout', '0'],
                                    stdout=log, stderr=subprocess.STDOUT)
        try:
            wait_until_ready(port, process=proc)
            c = Conn(port)
            try:
                assert c('FUNCTION', 'LOAD', COUNTER) == b'counter'
                expected = 0
                # Every mode used to flush and recompile the old callback after an error.
                for policy in ('APPEND', 'REPLACE', 'FLUSH'):
                    expected += 1
                    assert c('FCALL_RO', 'count', 0) == expected
                    before = c('FUNCTION', 'DUMP')
                    good = "#!lua name=staged\nredis.register_function('staged',function() return 9 end)"
                    for bad in ("#!lua name=bad\nthis is not Lua",
                                "#!lua name=bad\nredis.register_function('x',function() end); error('abort')",
                                "#!lua name=bad\nredis.register_function('staged',function() end)"):
                        reply = c('FUNCTION', 'RESTORE', payload(good, bad), policy)
                        assert isinstance(reply, str) and reply.startswith('-ERR'), reply
                        assert c('FUNCTION', 'DUMP') == before
                        assert c('FCALL', 'staged', 0).startswith('-ERR Function not found')
                        expected += 1
                        actual = c('FCALL_RO', 'count', 0)
                        assert actual == expected, (policy, 'closure reset on failed restore', actual, expected)
                # Replacing the same library is also isolated if the body registers then fails.
                reply = c('FUNCTION', 'LOAD', 'REPLACE',
                          "#!lua name=counter\nredis.register_function('count',function() return 99 end); error('abort')")
                assert reply.startswith('-ERR'), reply
                expected += 1
                assert c('FCALL_RO', 'count', 0) == expected
                assert c('FUNCTION', 'RESTORE', payload(good), 'APPEND') == '+OK'
                assert c('FCALL', 'staged', 0) == 9
                expected += 1
                assert c('FCALL_RO', 'count', 0) == expected
            finally:
                c.close()
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
            proc.wait()
        print(f'{binary}: callback isolation passed', flush=True)


if __name__ == '__main__':
    assert REDIS, 'pinned Redis 7.2.5 required'
    run(REDIS)
    run(PEADB)

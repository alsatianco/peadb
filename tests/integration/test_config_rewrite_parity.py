#!/usr/bin/env python3
"""Redis-first CONFIG REWRITE round trips, role changes and file semantics (delta 25)."""
import argparse
import contextlib
import os
import pathlib
import socket
import subprocess
import tempfile
import time

from _readiness import wait_until_ready
from _redis_path import redis_server_path

ROOT = pathlib.Path(__file__).resolve().parents[2]
PEADB = os.environ.get('PEADB_CONFIG_TEST_BIN', str(ROOT / 'peadb-server'))


def reference():
    binary = redis_server_path()
    assert binary, 'Pinned Redis 7.2.5 is required'
    version = subprocess.check_output([binary, '--version'], text=True)
    assert 'v=7.2.5 ' in version, version
    return binary


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=5)
        self.stream = self.sock.makefile('rb')

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stream.close()
        self.sock.close()

    def read(self):
        line = self.stream.readline()
        assert line.endswith(b'\r\n'), line
        kind, value = line[:1], line[1:-2]
        if kind in (b'+', b'-'):
            return line[:-2]
        if kind == b':':
            return int(value)
        if kind == b'$':
            size = int(value)
            return None if size < 0 else self.stream.read(size + 2)[:-2]
        if kind == b'*':
            return [self.read() for _ in range(int(value))]
        raise AssertionError(line)

    def __call__(self, *args):
        parts = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        self.sock.sendall(f'*{len(parts)}\r\n'.encode() + b''.join(
            f'${len(a)}\r\n'.encode() + a + b'\r\n' for a in parts))
        return self.read()

    def get_all(self):
        values = self('CONFIG', 'GET', '*')
        return dict(zip(values[0::2], values[1::2]))

    def get(self, key):
        result = self('CONFIG', 'GET', key)
        assert isinstance(result, list) and len(result) == 2, result
        return result[1]


class Server:
    def __init__(self, binary, root, text=None, env=None):
        self.binary, self.root, self.env = binary, pathlib.Path(root).resolve(), env
        self.port = free_port()
        self.path = self.root / 'server.conf' if text is not None else None
        self.proc = None
        self.password = None
        self.data = self.root / 'data'
        self.data.mkdir()
        if self.path:
            self.path.write_text(
                f'port {self.port}\nbind 127.0.0.1\ndir "{self.data}"\n'
                'save ""\nshutdown-timeout 0\nenable-protected-configs yes\n' + text)

    def start(self):
        args = [self.binary, self.path.name] if self.path else [self.binary,
                '--port', str(self.port), '--bind', '127.0.0.1', '--dir', str(self.data),
                '--save', '', '--shutdown-timeout', '0']
        with open(self.root / 'server.log', 'ab') as log:
            self.proc = subprocess.Popen(args, cwd=self.root, stdout=log,
                                         stderr=subprocess.STDOUT, env=self.env, umask=0o027)
        try:
            wait_until_ready(self.port, process=self.proc, password=self.password, timeout=10)
        except BaseException:
            self.stop(check=False)
            print((self.root / 'server.log').read_text(errors='replace'))
            raise

    def stop(self, check=True):
        if self.proc is not None:
            if self.proc.poll() is None:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
                    self.proc.wait()
            else:
                self.proc.wait()
            code = self.proc.returncode
            self.proc = None
            if check:
                log = (self.root / 'server.log').read_text(errors='replace')
                assert code == 0, (code, log)
                assert 'ERROR: AddressSanitizer' not in log and 'runtime error:' not in log, log

    @contextlib.contextmanager
    def running(self):
        self.start()
        try:
            yield self
        finally:
            self.stop()

    def conn(self):
        c = Conn(self.port)
        if self.password:
            assert c('AUTH', self.password) == b'+OK'
        return c


def roundtrip(binary):
    with tempfile.TemporaryDirectory(prefix='peadb-config-rewrite-') as work:
        server = Server(binary, work, '# keep this comment\nTIMEOUT 3\ntimeout 4\n'
                        'slave-read-only yes\nreplica-read-only yes\n'
                        'save 10000 10\nsave 20000 20\n'
                        'client-output-buffer-limit normal 0 0 0\n'
                        'client-output-buffer-limit normal 0 0 0\n'
                        'rename-command ECHO SAY\n')
        newdir = pathlib.Path(work) / 'new data'
        newdir.mkdir()
        with server.running():
            with server.conn() as c:
                assert c('CONFIG', 'REWRITE', 'extra') == (
                    b"-ERR wrong number of arguments for 'config|rewrite' command")
                assert c('CONFIG', 'REWRITE') == b'+OK'  # Original failure on PeaDB.
                binary_value = b'quote" slash\\ newline\n tab\t nul\x00 byte\xff'
                values = {
                    'timeout': b'11', 'tcp-keepalive': b'45', 'maxmemory': b'67108864',
                    'maxmemory-policy': b'allkeys-lru', 'lfu-log-factor': b'7',
                    'save': b'10000 30 20000 40', 'masterauth': binary_value,
                    'requirepass': binary_value, 'masteruser': b'user\x00ignored',
                    'notify-keyspace-events': b'KEA', 'list-max-ziplist-size': b'-3',
                    'slowlog-max-len': b'42', 'slowlog-log-slower-than': b'999',
                    'repl-backlog-ttl': b'120', 'repl-timeout': b'90',
                    'lazyfree-lazy-server-del': b'yes', 'shutdown-on-sigint': b'now nosave',
                    'latency-tracking-info-percentiles': b'50 99.5',
                    'client-output-buffer-limit': b'normal 0 0 0 replica 64mb 32mb 9 pubsub 16mb 8mb 7',
                    'dir': str(newdir).encode(), 'dbfilename': b'new dump.rdb',
                }
                flat = [a for pair in values.items() for a in pair]
                assert c('CONFIG', 'SET', *flat) == b'+OK'
                server.password = binary_value
                expected = {k: c.get(k) for k in values}
                all_expected = c.get_all()
                assert c('CONFIG', 'REWRITE') == b'+OK'
                first = server.path.read_bytes()
                assert b'# keep this comment\n' in first
                assert b'slave-read-only' not in first
                assert first.count(b'timeout 11\n') == 1
                assert b'rename-command ECHO SAY\n' in first
                assert b'maxmemory 64mb\n' in first
                assert b'save 10000 30\n' in first and b'save 20000 40\n' in first
                assert server.path.stat().st_mode & 0o777 == 0o640
                assert c('CONFIG', 'REWRITE') == b'+OK'
                second = server.path.read_bytes()
                # Redis adds one trailing blank line on the second rewrite, then stabilizes.
                assert second.rstrip(b'\n') == first.rstrip(b'\n')
                assert c('CONFIG', 'REWRITE') == b'+OK'
                assert server.path.read_bytes() == second, 'rewrite must stabilize'
                assert not list(pathlib.Path(work).glob('server.conf.*'))
        with server.running():
            with server.conn() as c:
                assert {k: c.get(k) for k in values} == expected
                assert c.get_all() == all_expected, 'all reported settings must survive restart'
                assert c('SAY', 'renamed') == b'renamed'
                assert c('ACL', 'WHOAMI') == b'default'
                # Defaults already present stay explicit; absent defaults are not appended.
                assert c('CONFIG', 'SET', 'timeout', '0', 'masterauth', '', 'requirepass', '',
                         'save', '', 'notify-keyspace-events', '') == b'+OK'
                server.password = None
                assert c('CONFIG', 'REWRITE') == b'+OK'
                content = server.path.read_bytes()
                assert b'timeout 0\n' in content and b'save ""\n' in content
                assert not any(line.startswith(b'masterauth ') for line in content.splitlines())
                assert not any(line.startswith(b'requirepass ') for line in content.splitlines())
        with server.running():
            with server.conn() as c:
                assert c('PING') == b'+PONG' and c.get('save') == b''


def role_and_includes(binary):
    with tempfile.TemporaryDirectory(prefix='peadb-rewrite-role-') as work:
        included = pathlib.Path(work) / 'included.conf'
        included.write_text('timeout 3\n')
        server = Server(binary, work, f'include "{included}"\ntimeout 4\n')
        port = free_port()
        with server.running():
            with server.conn() as c:
                # External edits must be preserved/commented by read-old-file handling.
                with server.path.open('a') as f:
                    f.write('unknown-retired-setting value\ninvalid "unbalanced\n')
                assert c('CONFIG', 'SET', 'timeout', '8') == b'+OK'
                assert c('REPLICAOF', '127.0.0.1', port) == b'+OK'
                assert c('CONFIG', 'REWRITE') == b'+OK'
                content = server.path.read_text()
                assert f'include "{included}"' in content
                assert '# ??? unknown-retired-setting value\n' in content
                assert '# ??? invalid "unbalanced\n' in content
                assert included.read_text() == 'timeout 3\n', 'includes are not rewritten'
        with server.running():
            with server.conn() as c:
                assert c.get('timeout') == b'8'
                assert c.get('replicaof') == f'127.0.0.1 {port}'.encode()
                assert c('ROLE')[0] == b'slave'
                assert c('REPLICAOF', 'NO', 'ONE') == b'+OK'
                assert c('CONFIG', 'REWRITE') == b'+OK'
                assert not any(line.startswith((b'replicaof ', b'slaveof '))
                               for line in server.path.read_bytes().splitlines())
        with server.running():
            with server.conn() as c:
                assert c('ROLE')[0] == b'master'
                assert c('CONFIG', 'REWRITE') == b'+OK'


def no_and_missing_config(binary):
    with tempfile.TemporaryDirectory(prefix='peadb-rewrite-missing-') as work:
        server = Server(binary, work)
        with server.running():
            with server.conn() as c:
                assert c('CONFIG', 'REWRITE') == b'-ERR The server is running without a config file'
        # Reuse the directory while giving this instance a real startup config.
        server.path = pathlib.Path(work) / 'server.conf'
        server.path.write_text(f'port {server.port}\nbind 127.0.0.1\ndir "{server.data}"\n'
                               'save ""\nshutdown-timeout 0\n')
        with server.running():
            with server.conn() as c:
                server.path.unlink()
                assert c('CONFIG', 'SET', 'timeout', '6') == b'+OK'
                assert c('CONFIG', 'REWRITE') == b'+OK'
                assert b'timeout 6\n' in server.path.read_bytes()
                server.path.write_bytes(b'')
                assert c('CONFIG', 'REWRITE') == b'+OK'
                assert b'timeout 6\n' in server.path.read_bytes()
        with server.running():
            with server.conn() as c:
                assert c.get('timeout') == b'6'


def appendonly_and_cli(binary):
    with tempfile.TemporaryDirectory(prefix='peadb-rewrite-aof-') as work:
        server = Server(binary, work, '')
        with server.running():
            with server.conn() as c:
                assert c('SET', 'config-key', 'persisted') == b'+OK'
                assert c('CONFIG', 'SET', 'appendonly', 'yes', 'appendfsync', 'always') == b'+OK'
                deadline = time.monotonic() + 10
                while True:
                    info = c('INFO', 'persistence')
                    if (b'aof_rewrite_in_progress:0' in info and
                            b'aof_rewrite_scheduled:0' in info and
                            b'aof_last_bgrewrite_status:ok' in info):
                        break
                    assert time.monotonic() < deadline, info
                    time.sleep(0.02)
                assert c('CONFIG', 'REWRITE') == b'+OK'
                assert b'appendonly yes\n' in server.path.read_bytes()
        with server.running():
            with server.conn() as c:
                assert c.get('appendonly') == b'yes' and c.get('appendfsync') == b'always'
                assert c('GET', 'config-key') == b'persisted'
        # CLI overrides enter the rewritten file, including immutable settings.
        server.path.write_text(server.path.read_text() + '\nport 1\n')
        # Server.start intentionally has no arbitrary extra-args interface; this owned child
        # exercises the actual positional-config plus CLI path directly.
        with open(server.root / 'server.log', 'ab') as log:
            proc = subprocess.Popen([binary, server.path.name, '--port', str(server.port),
                                     '--tcp-backlog', '499'], cwd=server.root, stdout=log,
                                    stderr=subprocess.STDOUT, umask=0o027)
        server.proc = proc
        try:
            wait_until_ready(server.port, process=proc, timeout=10)
            with server.conn() as c:
                assert c('CONFIG', 'REWRITE') == b'+OK'
        finally:
            server.stop()
        with server.running():
            with server.conn() as c:
                assert c.get('tcp-backlog') == b'499'


def include_precedence(binary):
    with tempfile.TemporaryDirectory(prefix='peadb-rewrite-relative-include-') as work:
        server = Server(binary, work, 'timeout 4\ninclude included.conf\n')
        old_include = server.data / 'included.conf'
        old_include.write_text('timeout 3\n')
        newdir = server.root / 'new-dir'
        newdir.mkdir()
        (newdir / 'included.conf').write_text('timeout 9\n')
        with server.running():
            with server.conn() as c:
                assert c.get('timeout') == b'3'
                assert c('CONFIG', 'SET', 'timeout', '8', 'dir', str(newdir)) == b'+OK'
                assert c('CONFIG', 'REWRITE') == b'+OK'
                content = server.path.read_bytes()
                assert content.index(b'timeout 8') < content.index(b'include included.conf')
                assert old_include.read_text() == 'timeout 3\n'
        with server.running():
            with server.conn() as c:
                # Redis retains includes in place; the relative include now resolves
                # under the rewritten dir, and it overrides the earlier timeout line.
                assert c.get('timeout') == b'9'


def empty_replica_host(binary):
    # PeaDB quotes the hostname so even this accepted role reloads losslessly.
    # Redis 7.2.5 writes this edge case without quotes and cannot reload its own output.
    with tempfile.TemporaryDirectory(prefix='peadb-rewrite-empty-host-') as work:
        server = Server(binary, work, '')
        port = free_port()
        with server.running():
            with server.conn() as c:
                assert c('REPLICAOF', '', port) == b'+OK'
                assert c('CONFIG', 'REWRITE') == b'+OK'
                assert f'replicaof "" {port}'.encode() in server.path.read_bytes()
        with server.running():
            with server.conn() as c:
                assert c('ROLE')[0] == b'slave'
                assert c.get('replicaof') == f' {port}'.encode()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--engine', choices=('redis', 'peadb', 'both'), default='both')
    args = parser.parse_args()
    engines = [('redis', reference()), ('peadb', PEADB)]
    for name, binary in engines:
        if args.engine not in ('both', name):
            continue
        for check in (roundtrip, role_and_includes, no_and_missing_config, appendonly_and_cli, include_precedence):
            check(binary)
            print(f'PASS {name}: {check.__name__}', flush=True)
        if name == 'peadb':
            empty_replica_host(binary)
            print('PASS peadb: empty_replica_host', flush=True)


if __name__ == '__main__':
    main()

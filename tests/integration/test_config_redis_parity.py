#!/usr/bin/env python3
"""Config file and command-line loading behave like Redis 7.2.5 (compat/delta.md item 24).

Each case gives the same config file and command-line options to peadb-server and to the pinned
redis-server: either both must refuse to start with the same reason, or both must start and
behave the same. A last group covers the Redis settings that PeaDB refuses because it cannot
honour them safely (Redis accepts them). Without a pinned redis-server, only the PeaDB side runs.
"""
import os
import pathlib
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _redis_path import redis_server_path  # noqa: E402

PEADB = str(ROOT / 'peadb-server')
REDIS = redis_server_path()
PEADB_PORT = 6562
REDIS_PORT = 6563


def resp(*args):
    out = f'*{len(args)}\r\n'.encode()
    for a in args:
        b = a.encode()
        out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
    return out


def call(port, *args):
    """One command on a new connection; returns the first reply line (decoded)."""
    with socket.create_connection(('127.0.0.1', port), timeout=3) as s:
        s.sendall(resp(*args))
        data = b''
        while b'\r\n' not in data:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
        line = data.split(b'\r\n', 1)[0].decode()
        if line.startswith('*') or line.startswith('$'):
            # Small arrays/bulks (CONFIG GET): read the rest and return the last bulk value.
            deadline = time.time() + 2
            while time.time() < deadline:
                try:
                    s.settimeout(0.2)
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                except socket.timeout:
                    break
            parts = data.split(b'\r\n')
            vals = [parts[i + 1].decode() for i, p in enumerate(parts[:-1]) if p.startswith(b'$') and p != b'$-1']
            return vals[-1] if vals else ''
        return line


def run(binary, port, conf_text, args, workdir):
    """Start `binary`; return ('exit', output) when it refuses to start, or ('up', proc, path)."""
    argv = [binary]
    conf = workdir / 'test.conf'
    if conf_text is not None:
        conf.write_text(conf_text + '\n', encoding='utf-8')
        argv.append(str(conf))
    argv += list(args) + ['--port', str(port)]
    log = workdir / 'out.log'
    with open(log, 'wb') as fh:
        p = subprocess.Popen(argv, cwd=workdir, stdout=fh, stderr=subprocess.STDOUT)
    deadline = time.time() + 5
    while time.time() < deadline:
        if p.poll() is not None:
            return ('exit', log.read_text(errors='replace'))
        try:
            if call(port, 'PING') in ('+PONG', '-NOAUTH Authentication required.'):
                return ('up', p, log)
        except OSError:
            pass
        time.sleep(0.05)
    p.kill()
    p.wait()
    raise AssertionError(f'{binary} neither started nor exited: {argv}')


def stop(proc, port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=2) as s:
            s.sendall(resp('SHUTDOWN', 'NOSAVE'))
            s.recv(64)
    except OSError:
        pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def engines():
    yield ('peadb', PEADB, PEADB_PORT)
    if REDIS is not None:
        yield ('redis', str(REDIS), REDIS_PORT)


# (config text, extra command-line options, text both engines must print when they refuse)
REFUSED_BY_BOTH = [
    ("save ''\nport 6379 # main port", [], "Reading the configuration file, at line 2"),
    ("port 6379 # main port", [], "wrong number of arguments"),
    ("no-such-setting 1", [], "Bad directive or wrong number of arguments"),
    ('requirepass "abc', [], "Unbalanced quotes in configuration line"),
    ("rename-command DEBUG", [], "Bad directive or wrong number of arguments"),
    ("rename-command NOSUCHCMD x", [], "No such command in rename-command"),
    ("rename-command GET SET", [], "Target command name already exists"),
    ('rename-command FLUSHALL ""\nrename-command FLUSHALL ""', [], "No such command in rename-command"),
    ("rename-command SENTINEL x", [], "No such command in rename-command"),
    ("rename-command GET LOLWUT", [], "Target command name already exists"),
    ("save 60 1 30", [], "Invalid save parameters"),
    ("logfile /no/such/dir/peadb.log", [], "Can't open the log file"),
    (None, ["--sentinel"], None),
    ("appendonly maybe", [], "argument must be 'yes' or 'no'"),
    ("port 70000", [], "argument must be between 0 and 65535 inclusive"),
    ("maxclients 0", [], "argument must be between 1 and 4294967295 inclusive"),
    ("maxmemory 1x", [], "argument must be a memory value"),
    ("loglevel info", [], "argument(s) must be one of the following: debug, verbose, notice, warning, nothing"),
    ("dbfilename a/b", [], "dbfilename can't be a path, just a filename"),
    ("save 900", [], "Invalid save parameters"),
    ("client-output-buffer-limit master 0 0 0", [], "Invalid client class specified in buffer limit configuration."),
    ("notify-keyspace-events KEX", [], "Invalid event class character. Use 'Ag$lshzxeKEtmdn'."),
    ("dir /no/such/dir/for/peadb", [], "No such file or directory"),
    ("hz abc", [], "argument couldn't be parsed into an integer"),
    ("unixsocketperm 800", [], "argument couldn't be parsed as an octal number"),
    ("shutdown-on-sigterm save nosave", [], "shutdown options SAVE and NOSAVE can't be used simultaneously"),
    ("sentinel monitor m 127.0.0.1 6379 2", [], "sentinel directive while not in sentinel mode"),
    ("include /no/such/peadb-include.conf", [], "Fatal error, can't open config file '/no/such/peadb-include.conf'"),
    ("tls-port 6390", [], "Failed finding TLS support"),
    (None, ["--no-such-option", "1"], "Bad directive or wrong number of arguments"),
    (None, ["--loglevel", "error"], "argument(s) must be one of the following"),
    (None, ["--maxmemory", "lots"], "argument must be a memory value"),
]

# Redis settings PeaDB refuses because ignoring them would be unsafe; Redis accepts them.
REFUSED_BY_PEADB_ONLY = [
    ("user default on >secret ~* +@all", [], "PeaDB has no ACL users"),
    ("aclfile /etc/redis/users.acl", [], "cannot load an ACL file"),
    ("replicaof 127.0.0.1 7399", [], "cannot start as a replica"),
    ("masteruser repl", [], "named ACL user"),
    ("enable-debug-command no", [], "cannot restrict the DEBUG command"),
    ("loadmodule /no/such/module.so", [], "PeaDB cannot load Redis modules"),
    ("bind 127.0.0.1 -::1", [], "exactly one bind address"),
]


def check_refusals(tmp):
    for conf, args, needle in REFUSED_BY_BOTH:
        for name, binary, port in engines():
            work = pathlib.Path(tempfile.mkdtemp(dir=tmp))
            r = run(binary, port, conf, args, work)
            if r[0] == 'up':
                stop(r[1], port)
                raise AssertionError(f'{name} started with {conf!r} {args}, expected a refusal')
            # needle None: both refuse, for different reasons (--sentinel: Redis needs a config
            # file for Sentinel mode; PeaDB has no Sentinel mode).
            assert needle is None or needle in r[1], f'{name} refusal for {conf!r} {args} lacks {needle!r}:\n{r[1]}'
    for conf, args, needle in REFUSED_BY_PEADB_ONLY:
        work = pathlib.Path(tempfile.mkdtemp(dir=tmp))
        r = run(PEADB, PEADB_PORT, conf, args, work)
        if r[0] == 'up':
            stop(r[1], PEADB_PORT)
            raise AssertionError(f'peadb started with {conf!r}, expected a refusal')
        assert needle in r[1], f'peadb refusal for {conf!r} lacks {needle!r}:\n{r[1]}'
    # A config file that does not exist (Redis: "Fatal error, can't open config file").
    for name, binary, port in engines():
        out = subprocess.run([binary, str(tmp / 'missing.conf'), '--port', str(port)],
                             capture_output=True, text=True, timeout=10)
        assert out.returncode != 0, f'{name} started without its config file'
        assert "Fatal error, can't open config file" in out.stdout + out.stderr, out.stdout + out.stderr
    print('config parity: refusals passed')


def check_nul_bytes(tmp):
    # Redis reads the file with fgets()+sdscat(): NUL padding at the end is dropped and the
    # command-line options after it still apply; a NUL inside a line joins the next line to it.
    for name, binary, port in engines():
        work = pathlib.Path(tempfile.mkdtemp(dir=tmp))
        (work / 'pad.conf').write_bytes(b'timeout 5\n' + b'\0' * 4000)
        log = work / 'out.log'
        with open(log, 'wb') as fh:
            p = subprocess.Popen([binary, str(work / 'pad.conf'), '--requirepass', 'pad-s3cret',
                                  '--port', str(port)], cwd=work, stdout=fh, stderr=subprocess.STDOUT)
        try:
            deadline = time.time() + 5
            reply = ''
            while time.time() < deadline:
                try:
                    reply = call(port, 'PING')
                    break
                except OSError:
                    time.sleep(0.05)
            assert reply.startswith('-NOAUTH'), f'{name}: --requirepass after NUL padding was lost: {reply!r}'
        finally:
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=2) as s:
                    s.sendall(resp('AUTH', 'pad-s3cret') + resp('SHUTDOWN', 'NOSAVE'))
                    s.recv(64)
            except OSError:
                pass
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        (work / 'mid.conf').write_bytes(b'timeout 5\0garbage\nmaxclients 66\n')
        out = subprocess.run([binary, str(work / 'mid.conf'), '--port', str(port)],
                             capture_output=True, timeout=10)
        text = (out.stdout + out.stderr).decode(errors='replace')
        assert out.returncode != 0 and ">>> 'timeout 5maxclients 66'" in text, f'{name}: {text}'
    print('config parity: NUL bytes passed')


def check_port_zero(tmp):
    # `port 0` opens no TCP listener (initListeners(), server.c): with a unix socket the server
    # listens there only, and with nothing to listen on it exits.
    for name, binary, port in engines():
        work = pathlib.Path(tempfile.mkdtemp(dir=tmp))
        out = subprocess.run([binary, '--port', '0'], cwd=work, capture_output=True, timeout=10)
        text = (out.stdout + out.stderr).decode(errors='replace')
        assert out.returncode != 0 and 'Configured to not listen anywhere, exiting.' in text, f'{name}: {text}'
        sock = f'/tmp/peadb-cfgtest-{os.getpid()}-{name}-p0.sock'
        log = work / 'out.log'
        with open(log, 'wb') as fh:
            p = subprocess.Popen([binary, '--port', '0', '--unixsocket', sock], cwd=work,
                                 stdout=fh, stderr=subprocess.STDOUT)
        try:
            deadline = time.time() + 5
            reply = b''
            while time.time() < deadline and not reply:
                try:
                    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as u:
                        u.settimeout(2)
                        u.connect(sock)
                        u.sendall(resp('PING'))
                        reply = u.recv(64)
                except OSError:
                    time.sleep(0.05)
            assert reply.startswith(b'+PONG'), f'{name}: no PONG on the unix socket: {reply!r}'
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as u:
                u.settimeout(2)
                u.connect(sock)
                u.sendall(resp('CONFIG', 'GET', 'port'))
                assert b'port\r\n$1\r\n0' in u.recv(256), f'{name}: CONFIG GET port'
                u.sendall(resp('SHUTDOWN', 'NOSAVE'))
                u.recv(64)
        finally:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
            if os.path.exists(sock):
                os.unlink(sock)
    print('config parity: port 0 passed')


def check_started(tmp):
    empty_glob = tmp / 'conf.d' / '*.conf'
    for name, binary, port in engines():
        work = pathlib.Path(tempfile.mkdtemp(dir=tmp))
        conf = '\n'.join([
            'requirepass "Str0ng#Pass 2026"',
            'rename-command DEBUG ""',
            'rename-command FLUSHDB MYFLUSH',
            'MAXCLIENTS 77',
            f'include {empty_glob}',
            'save ""',
        ])
        r = run(binary, port, conf, ['--timeout', '123'], work)
        assert r[0] == 'up', f'{name} refused a valid config:\n{r[1] if r[0] == "exit" else ""}'
        proc = r[1]
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=3) as s:
                def c(*a):
                    s.sendall(resp(*a))
                    return s.recv(4096).decode(errors='replace')
                assert c('AUTH', 'Str0ng#Pass 2026').startswith('+OK'), f'{name}: password with # and space'
                assert c('DEBUG', 'SLEEP', '0').startswith('-ERR unknown command'), f'{name}: DEBUG removed'
                assert c('', 'SLEEP', '0').startswith('-ERR unknown command'), f'{name}: no "" command'
                assert c('FLUSHDB').startswith('-ERR unknown command'), f'{name}: FLUSHDB renamed'
                assert c('MYFLUSH').startswith('+OK'), f'{name}: MYFLUSH'
                assert 'maxclients\r\n$2\r\n77' in c('CONFIG', 'GET', 'maxclients'), f'{name}: MAXCLIENTS 77'
                assert 'timeout\r\n$3\r\n123' in c('CONFIG', 'GET', 'timeout'), f'{name}: --timeout'
        finally:
            s2 = None
            try:
                s2 = socket.create_connection(('127.0.0.1', port), timeout=2)
                s2.sendall(resp('AUTH', 'Str0ng#Pass 2026') + resp('SHUTDOWN', 'NOSAVE'))
                s2.recv(64)
            except OSError:
                pass
            finally:
                if s2:
                    s2.close()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    # The last value wins: an option undoes a file line that PeaDB alone would refuse.
    for name, binary, port in engines():
        for conf, args in [('bind 127.0.0.1 -::1', ['--bind', '127.0.0.1']),
                           ('replicaof 127.0.0.1 7399', ['--replicaof', 'no one']),
                           ('tls-port 6390', ['--tls-port', '0'])]:
            work = pathlib.Path(tempfile.mkdtemp(dir=tmp))
            r = run(binary, port, conf, args, work)
            assert r[0] == 'up', f'{name} refused {conf!r} + {args}:\n{r[1] if r[0] == "exit" else ""}'
            stop(r[1], port)

    # --maxclients on the command line (it used to be dropped) and a settings line PeaDB
    # applies now that it used to ignore.
    for name, binary, port in engines():
        work = pathlib.Path(tempfile.mkdtemp(dir=tmp))
        r = run(binary, port, 'slowlog-log-slower-than 1234\nlatency-monitor-threshold 7', ['--maxclients', '5'], work)
        assert r[0] == 'up', f'{name} refused --maxclients:\n{r[1] if r[0] == "exit" else ""}'
        try:
            assert call(port, 'CONFIG', 'GET', 'maxclients') == '5', name
            assert call(port, 'CONFIG', 'GET', 'slowlog-log-slower-than') == '1234', name
            assert call(port, 'CONFIG', 'GET', 'latency-monitor-threshold') == '7', name
        finally:
            stop(r[1], port)

    # unixsocketperm is applied to the socket file, as Redis does.
    for name, binary, port in engines():
        sock = f'/tmp/peadb-cfgtest-{os.getpid()}-{name}.sock'
        work = pathlib.Path(tempfile.mkdtemp(dir=tmp))
        r = run(binary, port, f'unixsocket {sock}\nunixsocketperm 700', [], work)
        assert r[0] == 'up', f'{name} refused unixsocketperm:\n{r[1] if r[0] == "exit" else ""}'
        try:
            mode = stat.S_IMODE(os.stat(sock).st_mode)
            assert mode == 0o700, f'{name}: socket mode {oct(mode)}'
        finally:
            stop(r[1], port)
            if os.path.exists(sock):
                os.unlink(sock)
    print('config parity: accepted configs passed')


def check_warnings(tmp):
    # A Redis setting PeaDB does not apply starts with a warning naming the line; one whose value
    # is what PeaDB always does starts silently.
    work = pathlib.Path(tempfile.mkdtemp(dir=tmp))
    r = run(PEADB, PEADB_PORT, 'daemonize no\nhz 10\ntls-key-file-pass s3cret-value', ['--loglevel', 'warning'], work)
    assert r[0] == 'up', r[1] if r[0] == 'exit' else ''
    stop(r[1], PEADB_PORT)
    out = r[2].read_text(errors='replace')
    assert "config line 2: 'hz' is a Redis setting that PeaDB does not support yet" in out, out
    assert "config line 3: 'tls-key-file-pass'" in out, out
    assert "'daemonize'" not in out, out
    assert 's3cret-value' not in out, 'a warning printed a setting value'
    print('config parity: warnings passed')


def main():
    tmp = pathlib.Path(tempfile.mkdtemp(prefix='pcfg', dir='/tmp'))
    try:
        if REDIS is None:
            print('SKIP redis side: no pinned redis-server found; checking PeaDB only')
        check_refusals(tmp)
        check_nul_bytes(tmp)
        check_port_zero(tmp)
        check_started(tmp)
        check_warnings(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print('test_config_redis_parity passed')


if __name__ == '__main__':
    main()

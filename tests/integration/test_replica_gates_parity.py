#!/usr/bin/env python3
"""A replica refuses writes inside MULTI, EXEC and scripts, and a replica whose link is down refuses what Redis 7.2.5 refuses, with Redis's errors (compat/delta.md item 78).

Redis's processCommand() runs the same checks for a command it queues in MULTI as for one it runs at once, and checks EXEC by the commands it queued; scripts check each command they call (scriptCall()) and their declared flags before they start (scriptPrepareForRun()). The same checks run on peadb-server and on the pinned redis-server (Redis side first, only when it is Redis 7.2.x), with a master M, a replica R of M and a third server S:

- read-only replica R: a write, a MIGRATE, a script or function that may write and FUNCTION FLUSH sent inside MULTI get -READONLY and EXEC answers -EXECABORT (PeaDB queued them and EXEC ran them, so R held a key its master did not have); a script without a shebang that writes inside EXEC gets -READONLY (PeaDB ran its write); reads, no-writes scripts and functions and PUBLISH run; the rejected command counts in errorstats and commandstats.
- a command renamed with rename-command keeps Redis's flags: a renamed write is refused on R, also inside MULTI and from a read-only script, and a renamed EVAL_RO reads its script's flags.
- S queues a write, a script that may write, a script without a shebang that writes and a script loaded inside the transaction while it is a master, then becomes a replica before EXEC: EXEC answers -EXECABORT Transaction discarded because of: READONLY (Redis's execCommandAbort() text), the script without a shebang gets -READONLY for its write, and the script loaded inside the transaction gets "-READONLY Can not run script with write flag on readonly replica".
- SYNC, PSYNC, SAVE and SHUTDOWN (Redis's NO_MULTI commands) sent inside MULTI are refused (PeaDB queued SYNC and PSYNC and EXEC answered with a full sync inside its reply), as is a MULTI under -OOM, which flags the transaction; a transaction that already has an error stores nothing more, so EXEC is not refused for what came after the error; MONITOR shows an EXEC the dispatch aborts; a script refused before it runs counts its error and is cached first, like Redis's evalGenericCommand().
- min-replicas-to-write on M: a write queued in MULTI gets -NOREPLICAS and EXEC -EXECABORT; a transaction queued before the setting changed answers -EXECABORT Transaction discarded because of: NOREPLICAS (PeaDB said "because of previous errors: NOREPLICAS"); scripts without a shebang are refused at their first write, whatever command it is.
- S as a replica whose link is down, with replica-serve-stale-data no: only the commands Redis flags STALE run (AUTH, ECHO, SELECT, TIME, CLIENT, CONFIG, ROLE, LASTSAVE, SCRIPT LOAD; PeaDB refused all but a short list), others get -MASTERDOWN, writes -READONLY; scripts and functions run only with the allow-stale flag, and their commands only when Redis flags them STALE ("Can not execute the command on a stale replica"); MULTI refuses such commands at queue time, and EXEC refuses a transaction queued before the link went down with -EXECABORT Transaction discarded because of: MASTERDOWN; a script whose flags were not known when it was queued gets scriptPrepareForRun()'s errors.

Without a pinned Redis 7.2.x, only the PeaDB side runs.
"""
import hashlib
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _redis_path import redis_server_path  # noqa: E402

PEADB = str(ROOT / 'peadb-server')


def redis_7_2():
    """The redis-server to compare with, only if it is Redis 7.2.x (replies differ in other versions)."""
    path = redis_server_path()
    if not path:
        return None
    version = subprocess.run([path, '--version'], capture_output=True, text=True).stdout
    return path if ' v=7.2.' in version else None


REDIS = redis_7_2()
PORTS = {'redis': (6729, 6730, 6731), 'peadb': (6732, 6733, 6734)}

READONLY = "-READONLY You can't write against a read only replica."
NOREPLICAS = '-NOREPLICAS Not enough good replicas to write.'
MASTERDOWN = "-MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."
EXECABORT = '-EXECABORT Transaction discarded because of previous errors.'
STALE_CMD = '-ERR Can not execute the command on a stale replica'
LIBRARY = """#!lua name=gates
redis.register_function('wfn', function(keys) return redis.call('set', keys[1], 'w') end)
redis.register_function{function_name='rofn', callback=function(keys) return redis.call('get', keys[1]) end, flags={'no-writes'}}
redis.register_function{function_name='stalefn', callback=function() return 'hello' end, flags={'allow-stale', 'no-writes'}}
redis.register_function{function_name='stalegetfn', callback=function(keys) return redis.call('get', keys[1]) end, flags={'allow-stale', 'no-writes'}}
"""
SCRIPT_WRITES = "#!lua\nreturn 1"
SCRIPT_NO_WRITES_GET = "#!lua flags=no-writes\nreturn redis.call('get', KEYS[1])"
SCRIPT_PLAIN_GET = "redis.call('get', KEYS[1]) return 1"
SCRIPT_PLAIN_SET = "return redis.call('set', KEYS[1], '2')"


def sha1(text):
    return hashlib.sha1(text.encode()).hexdigest()


def encode(*args):
    out = f'*{len(args)}\r\n'.encode()
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode()
        out += f'${len(b)}\r\n'.encode() + b + b'\r\n'
    return out


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=10)
        self.f = self.sock.makefile('rb')

    def close(self):
        self.f.close()
        self.sock.close()

    def read(self):
        line = self.f.readline()
        if not line:
            return None  # connection closed
        kind, rest = line[:1], line[1:-2]
        if kind in (b'+', b'-'):
            return (kind + rest).decode()
        if kind == b':':
            return int(rest)
        if kind == b'$':
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2].decode()
        if kind == b'*':
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise ValueError(f'unexpected reply {line!r}')

    def __call__(self, *args):
        self.sock.sendall(encode(*args))
        return self.read()


def start(binary, port, workdir, extra=()):
    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, 'server.log')
    log = open(log_path, 'ab')
    proc = subprocess.Popen([binary, '--port', str(port), '--bind', '127.0.0.1', '--dir', workdir,
                             '--loglevel', 'notice', '--save', '', '--shutdown-timeout', '0',
                             '--repl-diskless-sync-delay', '0', *extra],
                            stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'{binary} exited: {open(log_path).read()}')
        try:
            socket.create_connection(('127.0.0.1', port), timeout=0.2).close()
            return proc
        except OSError:
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError(f'{binary} did not start on port {port}')


def stop(proc):
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def info_section(c, section):
    return dict(line.split(':', 1) for line in c('INFO', section).splitlines()
                if ':' in line and not line.startswith('#'))


def wait_for(cond, what, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.02)
    raise AssertionError(f'timed out waiting for {what}')


def starts(prefix):
    """A reply that begins with prefix: PeaDB's script errors lack Redis's " script: …" suffix (compat/delta.md item 14)."""
    return lambda got: isinstance(got, str) and got.startswith(prefix)


def matches(want, got):
    if callable(want):
        return want(got)
    if isinstance(want, list):
        return isinstance(got, list) and len(got) == len(want) and all(
            matches(w, g) for w, g in zip(want, got))
    return got == want


def expect(name, c, steps):
    """Send each command of steps and compare its reply with the wanted one."""
    for args, want in steps:
        got = c(*args)
        if not matches(want, got):
            raise AssertionError(f'{name}: {list(args)!r} answered {got!r}')


def call_counts(c, cmd):
    """calls, rejected_calls and failed_calls of a command in INFO commandstats."""
    line = info_section(c, 'commandstats').get(f'cmdstat_{cmd}', '')
    fields = dict(kv.split('=', 1) for kv in line.split(',') if '=' in kv)
    return tuple(int(fields.get(k, 0)) for k in ('calls', 'rejected_calls', 'failed_calls'))


def error_counts(c):
    return {k[len('errorstat_'):]: int(v.split('=', 1)[1])
            for k, v in info_section(c, 'errorstats').items()}


def wait_link(c, what):
    wait_for(lambda: info_section(c, 'replication').get('master_link_status') == 'up', what)


def check_read_only_replica(name, m, r, sport):
    """R is a replica of M with its link up."""
    check_name = f'{name} read-only replica'
    expect(check_name, r, [
        (('SET', 'x', '1'), READONLY),
        (('EVAL', SCRIPT_WRITES, 0), READONLY),
        (('EVAL', SCRIPT_NO_WRITES_GET, 1, 'x'), 'mv'),
        (('EVAL', SCRIPT_PLAIN_SET, 1, 'x'), starts(READONLY)),
        (('FCALL', 'wfn', 1, 'x'), READONLY),
        (('FCALL', 'rofn', 1, 'x'), 'mv'),
        (('FCALL_RO', 'rofn', 1, 'x'), 'mv'),
        (('FUNCTION', 'DELETE', 'gates'), READONLY),
        (('PUBLISH', 'ch', 'm'), 0),
    ])
    check_name = f'{name} MULTI on a read-only replica'
    expect(check_name, r, [(('CONFIG', 'RESETSTAT'), '+OK')])
    expect(check_name, r, [
        (('MULTI',), '+OK'), (('SET', 'x', '1'), READONLY), (('GET', 'x'), '+QUEUED'),
        (('EXEC',), EXECABORT), (('GET', 'x'), 'mv'),
    ])
    if call_counts(r, 'set') != (0, 1, 0) or call_counts(r, 'exec') != (1, 0, 1):
        raise AssertionError(f'{check_name}: commandstats set {call_counts(r, "set")}, '
                             f'exec {call_counts(r, "exec")}')
    errors = error_counts(r)
    if errors.get('READONLY') != 1 or errors.get('EXECABORT') != 1:
        raise AssertionError(f'{check_name}: errorstats {errors}')
    expect(check_name, r, [
        # The key R would move to S: on PeaDB the replica moved its own copy away.
        (('MULTI',), '+OK'), (('MIGRATE', '127.0.0.1', sport, 'x', 0, 1000), READONLY),
        (('EXEC',), EXECABORT), (('GET', 'x'), 'mv'),
        (('MULTI',), '+OK'), (('EVAL', SCRIPT_WRITES, 0), READONLY), (('EXEC',), EXECABORT),
        (('MULTI',), '+OK'), (('FCALL', 'wfn', 1, 'x'), READONLY), (('EXEC',), EXECABORT),
        (('MULTI',), '+OK'), (('FUNCTION', 'FLUSH'), READONLY), (('EXEC',), EXECABORT),
        (('FCALL', 'rofn', 1, 'x'), 'mv'),
        (('MULTI',), '+OK'), (('GET', 'x'), '+QUEUED'),
        (('EVAL', SCRIPT_NO_WRITES_GET, 1, 'x'), '+QUEUED'),
        (('EVAL', SCRIPT_PLAIN_GET, 1, 'x'), '+QUEUED'), (('FCALL', 'rofn', 1, 'x'), '+QUEUED'),
        (('FCALL_RO', 'rofn', 1, 'x'), '+QUEUED'), (('PUBLISH', 'ch', 'm'), '+QUEUED'),
        (('EXEC',), ['mv', 'mv', 1, 'mv', 'mv', 0]),
        # A script without a shebang is queued (it declares nothing); its write is refused when
        # EXEC runs it.
        (('MULTI',), '+OK'), (('EVAL', SCRIPT_PLAIN_SET, 1, 'x'), '+QUEUED'),
        (('EXEC',), [starts(READONLY)]), (('GET', 'x'), 'mv'),
    ])
    if m('GET', 'x') != 'mv':
        raise AssertionError(f'{check_name}: the master changed')


def check_renamed(name, r):
    """R renamed HSET to MYHSET and EVAL_RO to MYEVALRO."""
    check_name = f'{name} renamed commands on a replica'
    expect(check_name, r, [
        (('MYHSET', 'h', 'f', 'v'), READONLY),
        (('MULTI',), '+OK'), (('MYHSET', 'h', 'f', 'v'), READONLY), (('EXEC',), EXECABORT),
        (('MYEVALRO', SCRIPT_WRITES, 0), READONLY),
        (('MYEVALRO', "#!lua flags=no-writes\nreturn 1", 0), 1),
        (('EVAL', "#!lua flags=no-writes\nreturn redis.call('myhset', 'h', 'f', 'v')", 0),
         starts('-ERR Write commands are not allowed from read-only scripts.')),
        (('EXISTS', 'h'), 0),
    ])


def check_demoted_in_transaction(name, s, mport):
    """S, a master, queues transactions and becomes a replica of M before EXEC."""
    check_name = f'{name} transactions queued before REPLICAOF'
    loaded = "#!lua\nreturn 2"
    clients = [Conn(s.sock.getpeername()[1]) for _ in range(6)]
    a1, a2, a3, a4, a5, a6 = clients
    expect(check_name, a1, [(('MULTI',), '+OK'), (('SET', 'y', '1'), '+QUEUED')])
    expect(check_name, a2, [(('MULTI',), '+OK'), (('EVAL', SCRIPT_WRITES, 0), '+QUEUED')])
    expect(check_name, a3, [(('MULTI',), '+OK'), (('EVAL', SCRIPT_PLAIN_SET, 1, 'y'), '+QUEUED')])
    expect(check_name, a4, [(('MULTI',), '+OK'), (('GET', 'y'), '+QUEUED'),
                            (('ECHO', 'a'), '+QUEUED')])
    expect(check_name, a5, [(('MULTI',), '+OK'), (('SCRIPT', 'LOAD', loaded), '+QUEUED'),
                            (('EVALSHA', sha1(loaded), 0), '+QUEUED')])
    # A transaction that already has an error stores nothing more, so its EXEC is not refused for
    # a write queued after the error.
    expect(check_name, a6, [(('MULTI',), '+OK'), (('NOSUCHCMD',), starts('-ERR unknown command')),
                            (('SET', 'y', '1'), '+QUEUED')])
    expect(check_name, s, [(('REPLICAOF', '127.0.0.1', mport), '+OK')])
    expect(check_name, a6, [(('EXEC',), EXECABORT)])
    abort_ro = '-EXECABORT Transaction discarded because of: ' + READONLY[1:]
    expect(check_name, a1, [(('EXEC',), abort_ro)])
    expect(check_name, a2, [(('EXEC',), abort_ro)])
    expect(check_name, a3, [(('EXEC',), [starts(READONLY)])])
    expect(check_name, a4, [(('EXEC',), [None, 'a'])])
    expect(check_name, a5, [(('EXEC',), [sha1(loaded), '-READONLY Can not run script with write flag on '
                                               'readonly replica'])])
    wait_link(s, f'{check_name}: the link')
    expect(check_name, s, [(('GET', 'y'), None), (('GET', 'x'), 'mv')])
    for c in clients:
        c.close()


def check_no_multi(name, m):
    """The commands Redis flags NO_MULTI are refused inside MULTI, after the arity check."""
    check_name = f'{name} NO_MULTI commands'
    not_allowed = '-ERR Command not allowed inside a transaction'
    expect(check_name, m, [
        (('CONFIG', 'RESETSTAT'), '+OK'),
        (('MULTI',), '+OK'), (('SYNC',), not_allowed), (('PSYNC', '?', '-1'), not_allowed),
        (('SAVE',), not_allowed), (('SHUTDOWN',), not_allowed),
        (('SAVE', 'x'), "-ERR wrong number of arguments for 'save' command"),
        (('EXEC',), EXECABORT),
    ])
    if call_counts(m, 'sync') != (0, 1, 0) or call_counts(m, 'psync') != (0, 1, 0):
        raise AssertionError(f'{check_name}: commandstats sync {call_counts(m, "sync")}, '
                             f'psync {call_counts(m, "psync")}')
    if int(info_section(m, 'replication')['connected_slaves']) != 1:
        raise AssertionError(f'{check_name}: {info_section(m, "replication")}')
    # A command refused with -OOM inside MULTI (here MULTI itself) flags the transaction.
    expect(check_name, m, [
        (('CONFIG', 'SET', 'maxmemory', 1), '+OK'),
        (('MULTI',), '+OK'), (('MULTI',), "-OOM command not allowed when used memory > 'maxmemory'."),
        (('EXEC',), EXECABORT),
        (('CONFIG', 'SET', 'maxmemory', 0), '+OK'),
    ])
    # A script refused before it runs counts its error, also after a script whose error came from
    # redis.pcall(); EVAL caches a script before it is refused.
    cached = "#!lua\nreturn 'cached'"
    expect(check_name, m, [
        (('CONFIG', 'RESETSTAT'), '+OK'),
        (('EVAL', "return redis.pcall('set', 'x', '1', 'xx', 'nx')", 0), '-ERR syntax error'),
        (('EVAL_RO', cached, 0),
         '-ERR Can not execute a script with write flag using *_ro command.'),
        (('SCRIPT', 'EXISTS', sha1(cached)), [1]),
    ])
    if error_counts(m) != {'ERR': 2}:
        raise AssertionError(f'{check_name}: errorstats {error_counts(m)}')


def check_noreplicas(name, m):
    """M has one replica; min-replicas-to-write asks for two."""
    check_name = f'{name} NOREPLICAS'
    other = Conn(m.sock.getpeername()[1])
    monitor = Conn(m.sock.getpeername()[1])
    expect(check_name, monitor, [(('MONITOR',), '+OK')])
    abort_nr = '-EXECABORT Transaction discarded because of: ' + NOREPLICAS[1:]
    expect(check_name, m, [
        (('CONFIG', 'SET', 'min-replicas-to-write', 2), '+OK'),
        (('MULTI',), '+OK'), (('SET', 'z', '1'), NOREPLICAS), (('EXEC',), EXECABORT),
        (('MULTI',), '+OK'), (('GET', 'z'), '+QUEUED'), (('EXEC',), [None]),
        (('MULTI',), '+OK'), (('EVAL', SCRIPT_WRITES, 0), NOREPLICAS), (('DISCARD',), '+OK'),
        (('MULTI',), '+OK'), (('EVAL', "#!lua flags=no-writes\nreturn 1", 0), '+QUEUED'),
        (('EXEC',), [1]),
        (('EVAL', SCRIPT_WRITES, 0), NOREPLICAS),
        # Scripts without a shebang: refused at their first write, whatever command it is.
        (('EVAL', "return redis.call('hset', KEYS[1], 'f', 'v')", 1, 'h'), starts(NOREPLICAS)),
        (('EVAL', "return redis.call('sadd', KEYS[1], 'm')", 1, 's'), starts(NOREPLICAS)),
        (('EVAL', SCRIPT_PLAIN_GET, 1, 'x'), 1),
        (('EXISTS', 'h', 's'), 0),
        (('CONFIG', 'SET', 'min-replicas-to-write', 0), '+OK'),
        (('MULTI',), '+OK'), (('INCR', 'z'), '+QUEUED'),
    ])
    expect(check_name, other, [(('CONFIG', 'SET', 'min-replicas-to-write', 2), '+OK')])
    expect(check_name, m, [(('EXEC',), abort_nr), (('GET', 'z'), None)])
    expect(check_name, other, [(('CONFIG', 'SET', 'min-replicas-to-write', 0), '+OK')])
    # MONITOR shows the aborted EXEC after its MULTI, and none of what it had queued. (CONFIG is
    # left out: Redis never shows admin commands there, PeaDB does, a known difference.)
    expect(check_name, m, [(('ECHO', 'monitor-end'), 'monitor-end')])
    seen = []
    while not seen or 'monitor-end' not in seen[-1]:
        seen.append(monitor.read())
    names = [line.split('] ', 1)[1].split(' ', 1)[0] for line in seen]
    names = [n for n in names if n != '"CONFIG"']
    if '"MULTI"' not in names:
        raise AssertionError(f'{check_name}: MONITOR saw {seen}')
    last_multi = len(names) - 1 - names[::-1].index('"MULTI"')
    if names[last_multi:] != ['"MULTI"', '"EXEC"', '"GET"', '"ECHO"']:
        raise AssertionError(f'{check_name}: MONITOR saw {seen}')
    monitor.close()
    other.close()


def check_stale(name, s):
    """S is a replica of a port nothing listens on, with replica-serve-stale-data no."""
    check_name = f'{name} stale replica'
    other = Conn(s.sock.getpeername()[1])
    stale_sha = sha1("#!lua flags=allow-stale,no-writes\nreturn redis.call('echo', 'foobar')")
    expect(check_name, s, [
        (('REPLICAOF', 'NO', 'ONE'), '+OK'),
        (('CONFIG', 'SET', 'replica-serve-stale-data', 'no'), '+OK'),
        (('REPLICAOF', '127.0.0.1', 1), '+OK'),
    ])
    wait_for(lambda: info_section(s, 'replication').get('master_link_status') == 'down',
             f'{check_name}: the link down')
    expect(check_name, s, [
        (('GET', 'x'), MASTERDOWN),
        (('PING',), MASTERDOWN),
        (('DBSIZE',), MASTERDOWN),
        (('SET', 'x', '1'), READONLY),
        (('ECHO', 'a'), 'a'),
        (('SELECT', 0), '+OK'),
        (('TIME',), lambda got: isinstance(got, list) and len(got) == 2),
        (('AUTH', 'foo'), '-ERR AUTH <password> called without any password configured for the '
                          'default user. Are you sure your configuration is correct?'),
        (('CLIENT', 'SETNAME', 'n'), '+OK'),
        (('CONFIG', 'GET', 'maxmemory'), ['maxmemory', '0']),
        (('CONFIG', 'FOO'), "-ERR unknown subcommand 'FOO'. Try CONFIG HELP."),
        (('ROLE',), lambda got: isinstance(got, list) and got[0] == 'slave'),
        (('LASTSAVE',), lambda got: isinstance(got, int)),
        (('EVAL', "return redis.call('get', KEYS[1])", 1, 'x'), MASTERDOWN),
        (('EVAL', "#!lua flags=no-writes\nreturn 1", 0), MASTERDOWN),
        (('EVAL', "#!lua flags=allow-stale,no-writes\nreturn 1", 0), 1),
        (('EVAL', "#!lua flags=allow-stale,no-writes\nreturn redis.call('get', KEYS[1])", 1, 'x'),
         starts(STALE_CMD)),
        # The stale check comes before the no-writes one.
        (('EVAL', "#!lua flags=allow-stale,no-writes\nreturn redis.call('set', KEYS[1], 1)", 1,
          'x'), starts(STALE_CMD)),
        (('EVAL', "#!lua flags=allow-stale,no-writes\nreturn redis.call('echo', 'foobar')", 0),
         'foobar'),
        (('EVAL', "#!lua flags=allow-stale\nreturn 1", 0), READONLY),
        (('SCRIPT', 'LOAD', "#!lua flags=allow-stale,no-writes\nreturn redis.call('echo', 'foobar')"),
         stale_sha),
        (('EVALSHA', stale_sha, 0), 'foobar'),
        (('FCALL', 'rofn', 1, 'x'), MASTERDOWN),
        (('FCALL', 'stalefn', 0), 'hello'),
        (('FCALL', 'stalegetfn', 1, 'x'), starts(STALE_CMD)),
        (('FCALL', 'wfn', 1, 'x'), READONLY),
        (('MULTI',), '+OK'), (('GET', 'x'), MASTERDOWN), (('ECHO', 'a'), '+QUEUED'),
        (('EXEC',), EXECABORT),
        (('MULTI',), '+OK'), (('ECHO', 'a'), '+QUEUED'),
        (('EVAL', "#!lua flags=allow-stale,no-writes\nreturn 1", 0), '+QUEUED'),
        (('EXEC',), ['a', 1]),
        (('MULTI',), '+OK'), (('EVAL', "#!lua flags=no-writes\nreturn 1", 0), MASTERDOWN),
        (('DISCARD',), '+OK'),
    ])
    abort_md = '-EXECABORT Transaction discarded because of: ' + MASTERDOWN[1:]
    loaded = "#!lua flags=no-writes\nreturn 3"
    loaded_sha = sha1(loaded)
    for queued, after in ((('GET', 'x'), abort_md), (('PING',), abort_md),
                          (('EVAL', 'return 1', 0), [MASTERDOWN])):
        expect(check_name, other, [(('REPLICAOF', 'NO', 'ONE'), '+OK')])
        expect(check_name, s, [(('MULTI',), '+OK'), (queued, '+QUEUED')])
        expect(check_name, other, [(('REPLICAOF', '127.0.0.1', 1), '+OK')])
        expect(check_name, s, [(('EXEC',), after)])
    # A script whose flags were not known when it was queued: scriptPrepareForRun() refuses it.
    expect(check_name, other, [(('REPLICAOF', 'NO', 'ONE'), '+OK')])
    expect(check_name, s, [(('MULTI',), '+OK'), (('SCRIPT', 'LOAD', loaded), '+QUEUED'),
                           (('EVALSHA', loaded_sha, 0), '+QUEUED')])
    expect(check_name, other, [(('REPLICAOF', '127.0.0.1', 1), '+OK')])
    expect(check_name, s, [(('EXEC',), [loaded_sha, "-MASTERDOWN Link with MASTER is down, "
                                        "replica-serve-stale-data is set to 'no' and 'allow-stale' "
                                        "flag is not set on the script."])])
    # MULTI on a stale replica, and the link no longer stale before the queued GET.
    expect(check_name, s, [(('MULTI',), '+OK')])
    expect(check_name, other, [(('REPLICAOF', 'NO', 'ONE'), '+OK')])
    expect(check_name, s, [(('GET', 'x'), '+QUEUED'), (('EXEC',), ['mv']),
                           (('CONFIG', 'SET', 'replica-serve-stale-data', 'yes'), '+OK')])
    other.close()


def check_engine(name, binary, work):
    mport, rport, sport = PORTS[name]
    # R renames two commands: a renamed command keeps Redis's flags.
    renames = ('--rename-command', 'HSET', 'MYHSET', '--rename-command', 'EVAL_RO', 'MYEVALRO')
    procs = [start(binary, port, os.path.join(work, name, role), extra)
             for port, role, extra in ((mport, 'm', ()), (rport, 'r', renames), (sport, 's', ()))]
    conns = []
    try:
        m, r, s = (Conn(port) for port in (mport, rport, sport))
        conns = [m, r, s]
        expect(name, m, [(('SET', 'x', 'mv'), '+OK'), (('FUNCTION', 'LOAD', LIBRARY), 'gates')])
        expect(name, s, [(('FUNCTION', 'LOAD', LIBRARY), 'gates')])
        expect(name, r, [(('REPLICAOF', '127.0.0.1', mport), '+OK')])
        wait_link(r, f'{name}: the replica link')
        wait_for(lambda: r('GET', 'x') == 'mv', f'{name}: the replica has the data')
        check_read_only_replica(name, m, r, sport)
        check_renamed(name, r)
        check_no_multi(name, m)
        check_noreplicas(name, m)
        check_demoted_in_transaction(name, s, mport)
        check_stale(name, s)
        if r('GET', 'x') != 'mv' or r('EXISTS', 'y', 'z', 'h', 's') != 0:
            raise AssertionError(f'{name}: the replica holds data its master does not have')
    finally:
        for c in conns:
            c.close()
        for p in procs:
            stop(p)


def main():
    work = tempfile.mkdtemp(prefix='peadb-replica-gates-')
    try:
        if REDIS:
            check_engine('redis', REDIS, work)
        check_engine('peadb', PEADB, work)
        print('replica gates parity: ok' + ('' if REDIS else ' (PeaDB side only)'))
        return 0
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python3
"""Exercise real timeout, group cleanup, scratch ownership and child readiness."""
import contextlib
import importlib.util
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

from _isolated_server import IsolatedServer
from _readiness import ObservedProcess, wait_until_ready
from _redis_path import redis_server_path
from test_config_set_get_parity import start as config_start

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / 'scripts/ci/run_integration_tests.sh'
PEADB = str(ROOT / 'peadb-server')


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@contextlib.contextmanager
def server(binary=PEADB, password=None):
    port = free_port()
    args = [str(binary), '--bind', '127.0.0.1', '--port', str(port), '--save', '',
            '--shutdown-timeout', '0', '--loglevel', 'warning']
    if password is not None:
        args += ['--requirepass', password]
    proc = IsolatedServer(args, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    try:
        wait_until_ready(port, process=proc, password=password)
        yield proc, port
    finally:
        stop(proc)
        assert not Path(proc.data_dir.name).exists()


def must_refuse(callback, text):
    try:
        callback()
    except RuntimeError as error:
        choices = (text,) if isinstance(text, str) else text
        assert any(choice in str(error) for choice in choices), str(error)
    else:
        raise AssertionError('readiness accepted the wrong/unready child')


def check_readiness(work):
    for binary in (PEADB, redis_server_path()):
        assert binary is not None, 'pinned Redis reference required'
        with server(binary, 'readiness-test-password') as (owned, port):
            must_refuse(lambda: wait_until_ready(port, process=owned, timeout=0.1),
                        'did not become ready')
            must_refuse(lambda: wait_until_ready(port, process=owned, password='wrong', timeout=0.1),
                        'did not become ready')
            wait_until_ready(port, process=owned, username='default',
                             password='readiness-test-password')
            wait_until_ready(port, process=ObservedProcess(owned.pid),
                             password='readiness-test-password')

    with server() as (owned, port):
        # Deterministically keep the replacement alive while the old server
        # answers PING. Polling child liveness alone cannot detect this race.
        delayed = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
        try:
            must_refuse(lambda: wait_until_ready(port, process=delayed), 'process_id mismatch')
        finally:
            stop(delayed)
        must_refuse(lambda: wait_until_ready(port, process=delayed), 'exited with code')
        must_refuse(lambda: wait_until_ready(port, process=ObservedProcess(delayed.pid)),
                    'exited with code')
        # The actual configuration fixture must refuse and reap a child that
        # loses its bind race, preserving the diagnostic log on disk.
        must_refuse(lambda: config_start(PEADB, port, str(work)),
                    ('process_id mismatch', 'exited with code'))
        assert (work / 'server.log').exists()
        wait_until_ready(port, process=owned)

    # A successful TCP accept / LOADING reply must not count as readiness.
    script = work / 'loading.py'
    script.write_text('''import os,socket,time
with socket.socket() as s:
 s.bind(('127.0.0.1',0)); s.listen(); print(s.getsockname()[1],flush=True)
 while True:
  c,_=s.accept()
  with c:
   c.recv(1024); c.sendall(b'-LOADING Redis is loading the dataset in memory\\r\\n')
''')
    loading = subprocess.Popen([sys.executable, str(script)], stdout=subprocess.PIPE, text=True)
    try:
        port = int(loading.stdout.readline())
        must_refuse(lambda: wait_until_ready(port, process=loading, timeout=0.1),
                    'did not become ready')
    finally:
        stop(loading)
        loading.stdout.close()
    print('readiness: authenticated PONG, PID ownership, bind failure, child exit and LOADING passed')


def wait_file(path):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if path.exists() and path.read_text().endswith('\n'):
            return path.read_text().splitlines()
        time.sleep(0.01)
    raise AssertionError(f'fixture did not publish {path}')


def assert_gone(pids, reap=None):
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if reap is not None:
            reap()
        # Orphan zombies may await launchd on macOS; they cannot hold ports or
        # data. Linux's supervisor must reap adopted children (no zombies).
        rows = subprocess.check_output(['ps', '-axo', 'pid=,stat='], text=True)
        states = {int(pid): state for pid, state in (line.split() for line in rows.splitlines())}
        live = [pid for pid in pids if pid in states and
                (sys.platform == 'linux' or not states[pid].startswith('Z'))]
        if not live:
            return
        time.sleep(0.02)
    raise AssertionError(f'owned children survived cleanup: {live}')


def check_runner(work):
    assert shutil.which('timeout') or shutil.which('gtimeout'), 'GNU timeout required for regression'
    spec = importlib.util.spec_from_file_location('integration_runner', RUNNER.with_suffix('.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.enable_subreaper()
    script = work / 'owned.py'
    record = work / 'owned.txt'
    port = free_port()
    script.write_text(f'''import os,sys,signal,subprocess,tempfile,time
sys.path.insert(0,{str(ROOT / 'tests/integration')!r})
from _isolated_server import IsolatedServer
from _readiness import wait_until_ready
p=IsolatedServer([{PEADB!r},'--bind','127.0.0.1','--port',{str(port)!r},'--save','','--loglevel','warning'])
wait_until_ready({port},process=p)
child=subprocess.Popen([sys.executable,'-u','-c','import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print("ready",flush=True); time.sleep(60)'],stdout=subprocess.PIPE)
assert child.stdout.readline()==b'ready\\n'
with open({str(record)!r},'w') as f:
 f.write(str(p.pid)+'\\n'+str(child.pid)+'\\n'+p.data_dir.name+'\\n'+tempfile.gettempdir()+'\\n')
print('owned fixture ready',flush=True)
time.sleep(60)
''')
    env = dict(os.environ, TIMEOUT_SECS='2', INTEGRATION_LOG_DIR=str(work / 'logs'))
    with server() as (preexisting, protected_port):
        # A concurrent runner owns a sibling server. Its group must survive
        # the timed-out invocation's cleanup just like the pre-existing one.
        sibling_record = work / 'sibling.txt'
        sibling = work / 'sibling.py'
        sibling_port = free_port()
        sibling.write_text(script.read_text().replace(str(record), str(sibling_record))
                           .replace(str(port), str(sibling_port)))
        sibling_runner = subprocess.Popen(['bash', str(RUNNER), str(sibling)],
                                         env=dict(env, TIMEOUT_SECS='20'),
                                         stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        try:
            sibling_rows = wait_file(sibling_record)
            result = subprocess.run(['bash', str(RUNNER), str(script)], env=env,
                                    capture_output=True, text=True, timeout=8)
            assert result.returncode == 1, result.stdout + result.stderr
            assert 'timeout after 2s' in result.stdout, result.stdout
            rows = wait_file(record)
            assert_gone([int(pid) for pid in rows[:2]])
            assert all(not Path(path).exists() for path in rows[2:]), rows
            logs = list((work / 'logs').glob('run-*/*owned.log'))
            assert len(logs) == 1 and 'owned fixture ready' in logs[0].read_text(), logs
            wait_until_ready(protected_port, process=preexisting)
            os.kill(int(sibling_rows[0]), 0)
            os.kill(int(sibling_rows[1]), 0)
            # Reusing the timed-out port succeeds before any later test runs.
            with IsolatedServer([PEADB, '--port', str(port), '--bind', '127.0.0.1', '--save', '',
                                 '--loglevel', 'warning'], stdout=subprocess.DEVNULL) as replacement:
                try:
                    wait_until_ready(port, process=replacement)
                finally:
                    stop(replacement)
        finally:
            # Exercise interruption cleanup while GNU timeout is still active.
            sibling_runner.terminate()
            assert sibling_runner.wait(timeout=5) == 128 + signal.SIGTERM
        assert_gone([int(pid) for pid in sibling_rows[:2]])
        assert all(not Path(path).exists() for path in sibling_rows[2:]), sibling_rows
        wait_until_ready(protected_port, process=preexisting)

    # No GNU timeout on PATH still has a Python-enforced deadline. Also check
    # cleanup after an ordinary successful exit which leaks a child.
    record.unlink()
    assert module.run_test(script, 2, None, work / 'fallback.log') == 124
    rows = wait_file(record)
    assert_gone([int(pid) for pid in rows[:2]])
    assert all(not Path(path).exists() for path in rows[2:])

    # Even SIGKILL of the supervisor must not strand its detached guardian
    # and servers. A private parent-liveness pipe drives guardian cleanup.
    record.unlink()
    killed = subprocess.Popen(['bash', str(RUNNER), str(script)],
                              env=dict(env, TIMEOUT_SECS='20'), stdout=subprocess.DEVNULL)
    identities = {}
    killed_rows = None
    try:
        rows = wait_file(record)
        killed_rows = rows
        guardian_pid = os.getpgid(int(rows[0]))
        for pid in (guardian_pid, *map(int, rows[:2])):
            identities[pid] = subprocess.check_output(
                ['ps', '-o', 'pgid=,lstart=', '-p', str(pid)], text=True).strip()
        killed.kill()
        assert killed.wait(timeout=2) == -signal.SIGKILL
        assert_gone([guardian_pid, *map(int, rows[:2])], reap=module.reap_adopted)
    finally:
        if killed.poll() is None:
            killed.terminate()
            killed.wait(timeout=5)
        # Retain ownership even if the watchdog assertion fails: rescue only
        # recorded PIDs whose group and creation time still match our fixture.
        for pid, identity in identities.items():
            current = subprocess.run(['ps', '-o', 'pgid=,lstart=', '-p', str(pid)],
                                     capture_output=True, text=True).stdout.strip()
            if current and current == identity:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        assert_gone(list(identities), reap=module.reap_adopted)
        # SIGKILL cannot run the supervisor's directory finally block. Remove
        # its verified scratch only after all recorded members are gone.
        if killed_rows is not None:
            shutil.rmtree(killed_rows[3])
    script.write_text(script.read_text().replace('time.sleep(60)\n', ''))
    record.unlink()
    assert module.run_test(script, 3, None, work / 'success.log') == 0
    rows = wait_file(record)
    assert_gone([int(pid) for pid in rows[:2]])
    assert all(not Path(path).exists() for path in rows[2:])
    print('runner: GNU/fallback timeout, TERM/KILL, interruption, supervisor death, scratch cleanup '
          'and unrelated groups passed')


def main():
    with tempfile.TemporaryDirectory(prefix='peadb-harness-regression-') as directory:
        work = Path(directory)
        check_readiness(work)
        check_runner(work)
    print('integration harness lifecycle passed')


if __name__ == '__main__':
    main()

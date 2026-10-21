#!/usr/bin/env python3
"""Run integration scripts in owned groups, including GNU timeout's descendants.

A guardian remains the group leader until cleanup finishes. This anchors the
PGID even when the test/timeout exits; we never signal a recycled group ID.
Tests must keep children in their inherited group or own their separate groups
and clean them on SIGTERM (as the migration acceptance harness does).
"""
import ctypes
import os
from pathlib import Path
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[2]


def guardian(result_fd, parent_fd, command):
    # A caught handler resets to SIG_DFL on exec, unlike SIG_IGN. Children
    # therefore receive normal TERM while the guardian remains a PGID anchor.
    signal.signal(signal.SIGTERM, lambda *_: None)

    def parent_lost():
        # A killed outer command can SIGKILL the supervisor before its finally
        # block runs. EOF on this private pipe still cleans our anchored group.
        os.read(parent_fd, 1)
        os.killpg(os.getpgrp(), signal.SIGTERM)
        time.sleep(1)
        os.killpg(os.getpgrp(), signal.SIGKILL)

    threading.Thread(target=parent_lost, daemon=True).start()
    result = subprocess.call(command)
    try:
        os.write(result_fd, f'{result}\n'.encode())
    except BrokenPipeError:
        # Keep the anchor/watchdog alive after parent death, even when TERM
        # already made the test exit and there is nobody to read its result.
        pass
    finally:
        os.close(result_fd)
    while True:
        signal.pause()


def enable_subreaper():
    # Linux otherwise leaves orphaned grandchildren for PID 1 (which in a
    # container need not reap). macOS launchd reaps orphans itself.
    if sys.platform == 'linux':
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
            raise OSError(ctypes.get_errno(), 'cannot enable child subreaper')


def reap_adopted():
    if sys.platform == 'linux':
        while True:
            try:
                pid, _ = os.waitpid(-1, os.WNOHANG)
                if not pid:
                    return
            except ChildProcessError:
                return


def group_members(pgid):
    # Exclude zombies: they hold neither ports nor files. Inspection tolerates
    # children disappearing; signaling uses only our still-anchored PGID.
    listing = subprocess.check_output(['ps', '-axo', 'pid=,pgid=,stat='], text=True)
    return [int(pid) for pid, group, state in (line.split() for line in listing.splitlines())
            if int(group) == pgid and int(pid) != pgid and not state.startswith('Z')]


def stop_group(proc):
    # Do not poll()/wait() the guardian before the last group signal: even its
    # unreaped zombie reserves the PID/PGID if it unexpectedly died.
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        deadline = time.monotonic() + 1
        while group_members(proc.pid) and time.monotonic() < deadline:
            time.sleep(0.02)
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()
    deadline = time.monotonic() + 2
    while True:
        reap_adopted()
        if not group_members(proc.pid):
            # A child can become a zombie during the ps snapshot, after the
            # preceding waitpid pass. Reap once more after observing no live
            # members so Linux does not retain that final adopted zombie.
            reap_adopted()
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(f'owned process group {proc.pid} did not stop')
        time.sleep(0.02)


def run_test(test, budget, timeout_cmd, log):
    with tempfile.TemporaryDirectory(prefix='peadb-integration-') as scratch:
        read_fd, write_fd = os.pipe()
        parent_read, parent_write = os.pipe()
        proc = None
        try:
            command = [sys.executable, str(test)]
            if timeout_cmd:
                # --foreground keeps timeout and its children in our group;
                # the supervisor handles descendants and TERM-resistant tests.
                command = [timeout_cmd, '--foreground', '--kill-after=1s',
                           f'{budget}s', *command]
            with log.open('wb') as output:
                proc = subprocess.Popen(
                    [sys.executable, str(Path(__file__).resolve()), '--guardian',
                     str(write_fd), str(parent_read), *command], cwd=ROOT, stdout=output,
                    stderr=subprocess.STDOUT, start_new_session=True,
                    pass_fds=(write_fd, parent_read), env=dict(os.environ, TMPDIR=scratch,
                                                             TMP=scratch, TEMP=scratch))
                os.close(write_fd)
                write_fd = None
                os.close(parent_read)
                parent_read = None
                ready, _, _ = select.select([read_fd], [], [], budget + 2 if timeout_cmd else budget)
                if not ready:
                    return 124
                result = os.read(read_fd, 64)
                if not result:
                    raise RuntimeError('integration guardian exited without a result')
                return int(result)
        finally:
            # Defer repeated interrupts until the owned group is dead and its
            # direct/adopted children are reaped, then remove scratch data.
            previous = {sig: signal.signal(sig, signal.SIG_IGN)
                        for sig in (signal.SIGINT, signal.SIGTERM)}
            try:
                if proc is not None:
                    stop_group(proc)
            finally:
                os.close(read_fd)
                os.close(parent_write)
                if parent_read is not None:
                    os.close(parent_read)
                if write_fd is not None:
                    os.close(write_fd)
                for sig, handler in previous.items():
                    signal.signal(sig, handler)


def interrupted(signum, _frame):
    raise SystemExit(128 + signum)


def main():
    os.chdir(ROOT)
    budget = float(os.environ.get('TIMEOUT_SECS', '30'))
    if not 0 < budget < float('inf'):
        raise ValueError('TIMEOUT_SECS must be positive and finite')
    tests = [Path(arg).resolve() for arg in sys.argv[1:]] if len(sys.argv) > 1 else sorted(
        (ROOT / 'tests/integration').glob('test_*.py'))
    if not tests or any(not test.is_file() for test in tests):
        raise ValueError('no integration tests found or an explicit test file does not exist')
    enable_subreaper()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, interrupted)
    timeout_cmd = shutil.which('timeout') or shutil.which('gtimeout')
    log_root = Path(os.environ.get('INTEGRATION_LOG_DIR', ROOT / 'artifacts/integration'))
    log_root.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix='run-', dir=log_root))
    print(f'Integration logs: {logs}', flush=True)
    passed = failed = 0
    for index, test in enumerate(tests):
        log = logs / f'{index:03d}-{test.stem}.log'
        # This Redis-first test observes several real one-second cron/TTL boundaries on both
        # engines. Keep its assertions active and allow for slower sanitizer/CI scheduling.
        test_budget = (60.0 if test.name == 'test_replication_backlog_ttl_parity.py' and
                       'TIMEOUT_SECS' not in os.environ else budget)
        code = run_test(test, test_budget, timeout_cmd, log)
        if code == 0:
            passed += 1
        else:
            failed += 1
            reason = f'timeout after {test_budget:g}s' if code == 124 else f'exit {code}'
            print(f'FAIL: {test.stem} ({reason})\nLog: {log}', flush=True)
            print('\n'.join(log.read_text(errors='replace').splitlines()[-80:]), flush=True)
            print('---', flush=True)
    print(f'PASSED: {passed}\nFAILED: {failed}', flush=True)
    return bool(failed)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--guardian':
        guardian(int(sys.argv[2]), int(sys.argv[3]), sys.argv[4:])
    else:
        sys.exit(main())

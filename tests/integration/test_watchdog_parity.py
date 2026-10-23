#!/usr/bin/env python3
"""Redis-first watchdog lifecycle and real interrupted-command diagnostics (delta 60)."""
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time

from _isolated_server import IsolatedServer
from _readiness import wait_until_ready
from test_config_rewrite_parity import Conn, free_port, reference

ROOT = Path(__file__).resolve().parents[2]
MARKER = '--- WATCHDOG TIMER EXPIRED ---'


def eventually(predicate, description):
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.02)
    raise AssertionError(description)


def run(binary):
    with tempfile.TemporaryDirectory(prefix='peadb-watchdog-') as td:
        logfile = Path(td) / 'stdout.log'
        errors = Path(td) / 'stderr.log'
        port = free_port()
        with logfile.open('wb') as output, errors.open('wb') as error:
            proc = IsolatedServer(
                [binary, '--port', str(port), '--bind', '127.0.0.1', '--save', '',
                 '--shutdown-timeout', '0', '--watchdog-period', '1000',
                 '--enable-debug-command', 'yes'],
                stdout=output, stderr=error)
        text = lambda: logfile.read_text(errors='replace')
        count = lambda: text().count(MARKER)
        try:
            wait_until_ready(port, process=proc)
            with Conn(port) as c:
                assert c.get('watchdog-period') == b'1000'
                assert c('DEBUG', 'SLEEP', '1.2') == b'+OK'
                eventually(lambda: count() == 1, text())
                assert c('CONFIG', 'SET', 'watchdog-period', '0') == b'+OK'
                assert c.get('watchdog-period') == b'0'
                for value in ('-1', '2147483648', 'nan'):
                    reply = c('CONFIG', 'SET', 'watchdog-period', value)
                    assert reply.startswith(b'-ERR CONFIG SET failed'), reply
                    assert c.get('watchdog-period') == b'0'
                # Invalid batches must not arm the timer or change a preceding setting.
                assert c('CONFIG', 'SET', 'watchdog-period', '200', 'timeout', '-1').startswith(
                    b'-ERR CONFIG SET failed')
                assert c.get('watchdog-period') == b'0'
                assert c('DEBUG', 'SLEEP', '.4') == b'+OK'
                assert count() == 1, text()
                # At Redis's default 10 Hz, two cron ticks are the minimum period.
                assert c('CONFIG', 'SET', 'watchdog-period', '1') == b'+OK'
                assert c.get('watchdog-period') == b'200'
                # Healthy idle and active service rearm the one-shot timer.
                for _ in range(6):
                    time.sleep(.1)
                    assert c('PING') == b'+PONG'
                assert count() == 1, text()
                for expected in (2, 3):
                    assert c('DEBUG', 'SLEEP', '.8') == b'+OK'
                    eventually(lambda: count() == expected, text())
                    # A blocked command emits one report, not a periodic signal storm.
                    assert c('SET', 'recovered', expected) == b'+OK'
                    assert c('GET', 'recovered') == str(expected).encode()
                eventually(lambda: bool(re.search(r'debugCommand|handle_command', text())),
                           'missing real command-dispatch stack frames: ' + text())
                assert re.search(r'nanosleep|usleep|sleep_for', text()), text()
                # Replace a short timer with a long one, then disable an armed timer.
                assert c('CONFIG', 'SET', 'watchdog-period', '2000') == b'+OK'
                assert c('DEBUG', 'SLEEP', '.4') == b'+OK'
                assert count() == 3, text()
                assert c('CONFIG', 'SET', 'watchdog-period', '200') == b'+OK'
                assert c('CONFIG', 'SET', 'watchdog-period', '0') == b'+OK'
                assert c('DEBUG', 'SLEEP', '.4') == b'+OK'
                assert count() == 3, text()
                # Enabling after disable must reinstall the handler and timer.
                assert c('CONFIG', 'SET', 'watchdog-period', '200') == b'+OK'
                assert c('DEBUG', 'SLEEP', '.5') == b'+OK'
                eventually(lambda: count() == 4, text())
                assert c('PING') == b'+PONG'
                # A different real stall must capture the persistence call chain.
                assert c('CONFIG', 'SET', 'rdb-key-save-delay', '100000') == b'+OK'
                assert c('MSET', 'a', '1', 'b', '2', 'c', '3', 'd', '4', 'e', '5') == b'+OK'
                assert c('SAVE') == b'+OK'
                eventually(lambda: count() == 5, text())
                eventually(lambda: 'rdbSave' in text() or 'rdb_save' in text(),
                           'missing real persistence stack: ' + text())
                assert c('CONFIG', 'SET', 'rdb-key-save-delay', '0') == b'+OK'
                assert c('BGSAVE').startswith(b'+Background saving started')
                eventually(lambda: b'rdb_bgsave_in_progress:0' in c('INFO', 'persistence'),
                           'owned background child did not complete')
                assert c('PING') == b'+PONG'
                # Exit while enabled: the reporter must be joined without hanging.
            proc.terminate()
            assert proc.wait(timeout=5) == 0, text()
            diagnostics = errors.read_text(errors='replace')
            assert 'ERROR: AddressSanitizer' not in diagnostics, diagnostics
            assert 'runtime error:' not in diagnostics, diagnostics
            print('PASS watchdog configuration, real stacks, rearm/update/disable/recovery:',
                  binary, flush=True)
        except BaseException:
            print(text(), flush=True)
            print(errors.read_text(errors='replace'), flush=True)
            raise
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
            else:
                proc.wait()


if __name__ == '__main__':
    run(reference())
    run(os.environ.get('PEADB_TEST_BIN', str(ROOT / 'peadb-server')))

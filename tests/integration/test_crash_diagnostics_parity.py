#!/usr/bin/env python3
"""Owned Redis-first fatal-signal reports, settings and DEBUG crash restrictions."""
import os
from pathlib import Path
import re
import resource
import signal
import struct
import subprocess
import sys
import tempfile

from _isolated_server import IsolatedServer
from _readiness import wait_until_ready
from test_config_rewrite_parity import Conn, free_port, reference

ROOT = Path(__file__).resolve().parents[2]
DENIED = (b'-ERR DEBUG command not allowed. If the enable-debug-command option is set to "local", '
          b'you can run it from a local connection, otherwise you need to set this option in the '
          b'configuration file, and then restart the server.')


def no_core():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def verify_debug_instruction(binary, text):
    # Resolve against this exact binary offline; no resolver runs in the handler.
    symbols = []
    for line in subprocess.check_output(['nm', '-n', binary], text=True).splitlines():
        parts = line.split()
        if len(parts) >= 3:
            try:
                symbols.append((int(parts[0], 16), parts[-1]))
            except ValueError:
                pass
    data = Path(binary).read_bytes()[:64]
    if data[:4] == b'\x7fELF':
        offset = struct.unpack_from('<Q', data, 32)[0]
        size, count = struct.unpack_from('<HH', data, 54)
        with open(binary, 'rb') as executable:
            executable.seek(offset)
            headers = executable.read(size * count)
        image_base = min(struct.unpack_from('<Q', headers, i * size + 16)[0]
                         for i in range(count)
                         if struct.unpack_from('<I', headers, i * size)[0] == 1)
    else:
        image_base = next(address for address, name in symbols if name == '__mh_execute_header')
    index = next(i for i, (_, name) in enumerate(symbols) if 'debug_segfault' in name)
    start = symbols[index][0]
    end = next(address for address, _ in symbols[index + 1:] if address > start)
    loaded = int(re.search(r'Executable load address: 0x([0-9a-f]+)', text)[1], 16)
    pc = int(re.search(r'frame 0: 0x([0-9a-f]+)', text)[1], 16)
    assert start <= pc - loaded + image_base < end, (hex(pc), hex(loaded), start, end, text)
    frames = [int(address, 16) - loaded + image_base
              for address in re.findall(r'frame(?: 0)?: 0x([0-9a-f]+)', text)]
    callers = [(address, next(value for value, _ in symbols[i + 1:] if value > address))
               for i, (address, name) in enumerate(symbols) if 'debugCommand' in name]
    assert any(lo <= frame < hi for lo, hi in callers for frame in frames), text


def case(binary, action, settings=(), runtime=None, expected_log=True, expected_signal=None,
         command='DEBUG', password=None):
    with tempfile.TemporaryDirectory(prefix='peadb-crash-') as td:
        output, errors = Path(td) / 'stdout', Path(td) / 'stderr'
        port = free_port()
        with output.open('wb') as out, errors.open('wb') as err:
            proc = IsolatedServer(
                [binary, '--port', str(port), '--bind', '127.0.0.1', '--save', '',
                 '--shutdown-timeout', '0', '--crash-memcheck-enabled', 'no', *settings],
                stdout=out, stderr=err, preexec_fn=no_core)
        try:
            wait_until_ready(port, process=proc, password=password)
            with Conn(port) as c:
                if password:
                    assert c(command, 'SEGFAULT') == b'-NOAUTH Authentication required.'
                    assert c('AUTH', password) == b'+OK'
                if runtime is not None:
                    assert c('CONFIG', 'SET', 'crash-log-enabled', runtime) == b'+OK'
                    assert c.get('crash-log-enabled') == runtime.encode()
                assert c('CONFIG', 'SET', 'crash-log-enabled', 'invalid').startswith(b'-ERR')
                before = c.get('crash-log-enabled')
                assert c('CONFIG', 'SET', 'crash-log-enabled', 'no', 'timeout', '-1').startswith(b'-ERR')
                assert c.get('crash-log-enabled') == before
                assert c('CONFIG', 'SET', 'enable-debug-command', 'yes').endswith(
                    b"can't set immutable config")
                if action == 'denied':
                    assert c(command, 'SEGFAULT') == DENIED
                    assert c('PING') == b'+PONG'
                    proc.terminate()
                    assert proc.wait(timeout=5) == 0
                    return
                if action == 'abort':
                    os.kill(proc.pid, expected_signal)
                else:
                    try:
                        reply = c(command, 'SEGFAULT')
                    except (AssertionError, ConnectionError, OSError):
                        pass  # A crash closes the connection without a protocol reply.
                    else:
                        raise AssertionError(f'DEBUG SEGFAULT returned {reply!r}')
            rc = proc.wait(timeout=5)
            assert rc == -expected_signal, (rc, output.read_text(), errors.read_text())
            text = output.read_text(errors='replace')
            diagnostics = errors.read_text(errors='replace')
            if expected_log:
                assert 'crashed by signal' in text, text
                assert 'STACK TRACE' in text, text
                # Actual instruction/backtrace addresses, not just a success marker.
                assert len(re.findall(r'0x[0-9a-fA-F]{4,}', text)) >= 2, text
                if 'peadb' in Path(binary).name:
                    assert 'Executable load address:' in text, text
                    assert re.search(r'frame 0: 0x[0-9a-f]+', text), text
                    if action == 'segfault':
                        verify_debug_instruction(binary, text)
            else:
                assert 'STACK TRACE' not in text and 'crashed by signal' not in text, text
            assert 'PeaDB crashed:' not in diagnostics, diagnostics
            assert 'ERROR: AddressSanitizer' not in diagnostics, diagnostics
            assert 'runtime error:' not in diagnostics, diagnostics
            print('PASS fatal report', action, settings, runtime, binary, flush=True)
        except BaseException:
            print(output.read_text(errors='replace'), errors.read_text(errors='replace'), flush=True)
            raise
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=5)


def run(binary, reference_engine=False):
    fault = signal.SIGBUS if sys.platform == 'darwin' else signal.SIGSEGV
    case(binary, 'denied')
    if reference_engine:
        case(binary, 'denied', ('--enable-debug-command', 'no'))
    for sig in (signal.SIGABRT, signal.SIGSEGV, signal.SIGBUS, signal.SIGFPE, signal.SIGILL):
        case(binary, 'abort', expected_signal=sig)
    case(binary, 'abort', ('--crash-log-enabled', 'no'), expected_log=False,
         expected_signal=signal.SIGABRT)
    case(binary, 'abort', runtime='no', expected_log=False, expected_signal=signal.SIGABRT)
    case(binary, 'abort', ('--crash-log-enabled', 'no'), runtime='yes',
         expected_signal=signal.SIGABRT)
    case(binary, 'segfault', ('--enable-debug-command', 'yes'), expected_signal=fault)
    case(binary, 'denied', ('--rename-command', 'DEBUG', 'fault-test'), command='fault-test')
    case(binary, 'segfault', ('--enable-debug-command', 'yes', '--rename-command', 'DEBUG',
                            'fault-test', '--requirepass', 'owned-test-password'),
         command='fault-test', password='owned-test-password', expected_signal=fault)
    if reference_engine:
        case(binary, 'segfault', ('--enable-debug-command', 'local'), expected_signal=fault)
    case(binary, 'segfault', ('--enable-debug-command', 'yes'), runtime='no',
         expected_log=False, expected_signal=fault)


if __name__ == '__main__':
    run(reference(), reference_engine=True)
    run(os.environ.get('PEADB_TEST_BIN', str(ROOT / 'peadb-server')))

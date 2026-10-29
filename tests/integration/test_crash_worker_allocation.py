#!/usr/bin/env python3
"""Cold worker fatal reporting must not lazily allocate TLS on macOS."""
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]


def main():
    with tempfile.TemporaryDirectory(prefix='peadb-crash-worker-') as td:
        binary = str(Path(td) / 'worker')
        command = shlex.split(os.environ.get('CXX', 'c++')) + [
            '-std=c++20', '-O2', '-fno-omit-frame-pointer', '-pthread',
            '-I' + str(ROOT / 'include'),
            '-DPEADB_VERSION="' + (ROOT / 'VERSION').read_text().strip() + '"',
            str(ROOT / 'src/crash.cpp'),
            str(ROOT / 'tests/integration/fixtures/crash_worker.cpp'), '-o', binary]
        if sys.platform != 'darwin':
            command.append('-ldl')
        subprocess.run(command, check=True, timeout=20)
        result = subprocess.run([binary], capture_output=True, text=True, timeout=5)
        assert result.returncode == -signal.SIGABRT, result
        assert 'PEADB BUG REPORT END' in result.stdout, result
        assert re.search(r'frame 0: 0x[0-9a-f]+', result.stdout), result
        assert len(re.findall(r'^frame', result.stdout, re.M)) == 1, result
        if sys.platform == 'darwin':
            canary, diagnostics = result.stderr.split('CANARY END\n')
            assert 'ALLOC\n' in canary, result
            assert diagnostics == '', result
            print('PASS cold-worker allocation audit (macOS malloc_logger)')
        else:
            assert result.stderr == '', result
            print('PASS cold-worker signal/PC-only fallback; macOS allocator audit is platform-specific')


if __name__ == '__main__':
    main()

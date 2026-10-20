#!/usr/bin/env python3
"""Exercise every release migration path cheaply; this is explicitly a smoke run."""
import json
import pathlib
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]


def main():
    with tempfile.TemporaryDirectory(prefix='peadb-migration-smoke-') as directory:
        work = pathlib.Path(directory) / 'gate'
        result = subprocess.run([
            sys.executable, str(ROOT / 'tests/acceptance/migration.py'),
            '--smoke', '--keys', '120', '--timeout', '8', '--budget-seconds', '18',
            '--work-dir', str(work),
        ], capture_output=True, text=True)
        assert result.returncode == 0, result.stdout + result.stderr
        report = json.loads((work / 'report.json').read_text())
        assert report['status'] == 'smoke_passed', report
        assert report['redis_command_aof_reference'] == 'passed', report
        assert len(report['paths']) == 6, report
        assert len(report['aof_restart_digests']) == 2, report
        print(result.stdout, end='')


if __name__ == '__main__':
    main()

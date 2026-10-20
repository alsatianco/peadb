#!/usr/bin/env python3
"""The CI runner must finish as children exit during group cleanup."""
import os
import pathlib
import shlex
import shutil
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]


def main():
    with tempfile.TemporaryDirectory(prefix='peadb-runner-cleanup-') as tmp:
        root = pathlib.Path(tmp)
        scripts = root / 'scripts' / 'ci'
        tests = root / 'tests' / 'integration'
        binaries = root / 'bin'
        for directory in (scripts, tests, binaries):
            directory.mkdir(parents=True)
        runner = scripts / 'run_integration_tests.sh'
        shutil.copyfile(ROOT / 'scripts/ci/run_integration_tests.sh', runner)
        shutil.copyfile(ROOT / 'scripts/ci/run_integration_tests.py', runner.with_suffix('.py'))
        for name in ('first', 'second'):
            (tests / f'test_{name}.py').write_text(
                'import subprocess,sys\n'
                'subprocess.Popen([sys.executable,"-c","pass"])\n'
                f'print({name!r})\n')

        # Global name scans and per-candidate PGID reads are no longer needed.
        # Preserve their old failure diagnostic and assert the new supervisor
        # does not depend on them while a real child exits during cleanup.
        state = shlex.quote(str(root / 'snapshot-taken'))
        (binaries / 'pgrep').write_text(
            '#!/bin/sh\n'
            f'if [ -f {state} ]; then echo 9999999; else touch {state}; fi\n')
        real_ps = shlex.quote(shutil.which('ps'))
        (binaries / 'ps').write_text(
            '#!/bin/sh\n'
            'case "$*" in *9999999*) exit 1;; esac\n'
            f'exec {real_ps} "$@"\n')
        for binary in binaries.iterdir():
            binary.chmod(0o755)
        env = dict(os.environ, PATH=str(binaries) + os.pathsep + os.environ['PATH'])
        result = subprocess.run(['bash', str(runner)], env=env, capture_output=True,
                                text=True, timeout=15)
        assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)
        assert 'PASSED: 2' in result.stdout, result.stdout
        assert 'FAILED: 0' in result.stdout, result.stdout
        assert not (root / 'snapshot-taken').exists(), 'runner used a global name scan'
    print('CI runner cleanup race: ok')


if __name__ == '__main__':
    main()

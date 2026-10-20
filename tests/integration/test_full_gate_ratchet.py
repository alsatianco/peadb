#!/usr/bin/env python3
"""Exercise the full runner's parity stage with isolated logs and a real ratchet.

No servers are started. A copied runner uses the real parser/checker, a small
ledger in a private Git repository, and a fake sweep/differential stage. This
checks failure propagation and freshness without changing project baselines.
"""
import json
import os
import pathlib
import shutil
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
OTHER_STAGES = 'unit,integration,coverage,portcheck,compat,fuzz,stability,perf,sentinel'


def main():
    with tempfile.TemporaryDirectory(prefix='peadb-full-gate-ratchet-') as tmp:
        root = pathlib.Path(tmp).resolve()
        for name in ('scripts/redis/skipfiles', 'scripts/ci', 'tests/diff', 'compat', 'build'):
            (root / name).mkdir(parents=True)
        for name in ('scripts/run_all_tests.sh', 'scripts/redis/parse_tcl_results.py',
                     'scripts/ci/check_suite_ratchet.py'):
            shutil.copyfile(ROOT / name, root / name)
        (root / 'compat/delta.md').write_text('Fixture delta registry\n')
        baseline = {'last_counts': {'ok': 2, 'err': 0, 'ignored': 0},
                    'mode': 'native', 'skipfile_lines': 0}
        (root / 'compat/redis_suite_ledger.json').write_text(json.dumps({
            'suites': {'unit/first': baseline, 'unit/second': baseline}}))
        # The checker's delta exception must see this fixture's Git state, not
        # the real project's changed documentation or an inherited override.
        for args in (['init', '-q'], ['add', 'compat/delta.md'],
                     ['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                      '-c', 'commit.gpgsign=false', 'commit', '-qm', 'fixture']):
            subprocess.run(['git', *args], cwd=root, check=True, capture_output=True)
        sweep = root / 'scripts/redis/run_redis_tests.sh'
        sweep.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
assert os.environ['SUITE_MODE'] == 'native'
root = pathlib.Path.cwd()
out = pathlib.Path(os.environ['OUT_DIR']).resolve()
assert out == root / 'artifacts/redis-native'
(root / 'sweep-ran').touch()
fixture = json.loads((root / 'fixture.json').read_text())
for name, content in fixture['logs'].items():
    (out / name).write_text(content)
sys.exit(fixture.get('exit', 0))
''')
        sweep.chmod(0o755)
        (root / 'tests/diff/run_diff_tests.py').write_text(
            "import pathlib\npathlib.Path('diff-ran').touch()\n")
        env = dict(os.environ, SKIP=OTHER_STAGES, BUILD_DIR=str(root / 'build'),
                   PEADB_BIN='/usr/bin/true')
        for name in ('RATCHET_ALLOW_REGRESSION', 'GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE'):
            env.pop(name, None)
        logs = root / 'artifacts/redis-native'
        reports = root / 'artifacts/redis-parity'
        complete = '\x1b[32mAll tests passed without errors!\x1b[0m\n'
        good = '[ok]: first\n[ok]: second\n' + complete
        good_logs = {'unit_first.log': good, 'unit_second.log': good}

        def run(label, fixture_logs, expected, diagnostic, sweep_exit=0, skip_redis=False):
            logs.mkdir(parents=True, exist_ok=True)
            reports.mkdir(parents=True, exist_ok=True)
            # Previous successful data must neither fill a missing current log
            # nor survive a failed sweep as a current report/coverage artifact.
            for name, content in good_logs.items():
                (logs / name).write_text(content)
            (logs / 'coverage_old.txt').write_text('SET\n')
            (logs / 'failed_suites.txt').write_text('old failure\n')
            (logs / 'repro_commands.txt').write_text('old repro\n')
            for suffix in ('json', 'md'):
                (reports / f'pass_rate.{suffix}').write_text('stale report')
            for name in ('sweep-ran', 'diff-ran'):
                (root / name).unlink(missing_ok=True)
            (root / 'fixture.json').write_text(json.dumps({
                'logs': fixture_logs, 'exit': sweep_exit}))
            run_env = dict(env, SKIP=OTHER_STAGES + (',redis' if skip_redis else ''))
            result = subprocess.run(['bash', str(root / 'scripts/run_all_tests.sh')],
                                    env=run_env, capture_output=True, text=True, timeout=10)
            output = result.stdout + result.stderr
            assert result.returncode == expected, (label, result.returncode, output)
            assert diagnostic in output, (label, output)
            assert (root / 'diff-ran').exists(), (label, 'later stage did not run', output)
            assert ('FAILED:  0' if expected == 0 else 'FAILED:  1') in output, (label, output)
            if skip_redis:
                assert not (root / 'sweep-ran').exists(), output
                assert (reports / 'pass_rate.json').read_text() == 'stale report'
            else:
                assert (root / 'sweep-ran').exists(), output
                assert not (logs / 'coverage_old.txt').exists(), label
                assert not (logs / 'failed_suites.txt').exists(), label
                assert not (logs / 'repro_commands.txt').exists(), label
            print(f'{label}: ok')

        run('green sweep and ratchet', good_logs, 0, 'suite ratchet OK')
        report = json.loads((reports / 'pass_rate.json').read_text())
        assert report['totals'] == {'ok': 4, 'err': 0, 'skipfile_lines': 0}, report
        assert '| unit/first | 2 |' in (reports / 'pass_rate.md').read_text()
        run('dropped count', {**good_logs, 'unit_first.log': '[ok]: only one\n' + complete},
            1, 'ok count regressed 2 -> 1')
        skipfile = root / 'scripts/redis/skipfiles/unit_first.skip'
        skipfile.write_text('new skip\n')
        run('grown skipfile', good_logs, 1, 'skipfile grew 0 -> 1')
        skipfile.unlink()
        run('missing suite cannot reuse old log', {'unit_first.log': good},
            1, 'unit/second: adopted in ledger but missing from results')
        run('failed sweep stops parsing', good_logs, 1,
            '--- FAIL: Upstream Redis Tcl tests and suite ratchet ---', sweep_exit=7)
        assert not (reports / 'pass_rate.json').exists()
        assert not (reports / 'pass_rate.md').exists()
        run('no current logs', {}, 1, 'no runtest logs found')

        # A documented count/skip change is allowed by the real checker, but
        # this exception must never allow unfinished or failed harness output.
        (root / 'compat/delta.md').write_text('Fixture delta registry\nDocument a change\n')
        subprocess.run(['git', 'add', 'compat/delta.md'], cwd=root, check=True)
        run('documented count change', {**good_logs, 'unit_first.log': complete},
            0, 'suite ratchet OK')
        for label, bad in (('partial log', good.replace(complete, '')),
                           ('failed assertion', good + '[err]: failure\n'),
                           ('harness exception', good + '[exception]: failure\n')):
            run(label, {**good_logs, 'unit_first.log': bad},
                1, 'incomplete or failed runtest log')
            assert not (reports / 'pass_rate.json').exists(), label
            assert not (reports / 'pass_rate.md').exists(), label
        run('missing suite despite documented change', {'unit_first.log': good},
            1, 'adopted in ledger but missing from results')
        run('explicit redis skip', good_logs, 0,
            '=== SKIP: Upstream Redis Tcl tests and suite ratchet ===', skip_redis=True)
    print('Full gate parity freshness and failure propagation: ok')


if __name__ == '__main__':
    main()

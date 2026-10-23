#!/usr/bin/env python3
"""Timed PSYNC2 cycles retain raw evidence but contribute a stable parity count."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
PARSER = ROOT / 'scripts/redis/parse_tcl_results.py'
CHECKER = ROOT / 'scripts/ci/check_suite_ratchet.py'
POLICY = 'psync2-one-cycle-v1'
COMPLETE = 'All tests passed without errors!\n'


def cycle(number):
    return [f'PSYNC2: --- CYCLE {number} ---',
            'PSYNC2: [NEW LAYOUT] Set #4 as master',
            *[f'PSYNC2: Set #{node} to replicate from #4' for node in range(4)],
            'PSYNC2: cluster is consistent after failover',
            'PSYNC2: generate load while killing replication links',
            f'PSYNC2: cluster is consistent after load (x = {number * 12345})',
            'PSYNC2: total sum of full synchronizations is exactly 4']


FINALS = ['PSYNC2: Bring the master back again for next test',
          'PSYNC2: Partial resync after restart using RDB aux fields']


def log(names):
    return ''.join(f'[\x1b[32mok\x1b[0m]: {name} (5 ms)\n' for name in names) + (
        '[ignore]: leak check\n' * 6) + COMPLETE


def main():
    with tempfile.TemporaryDirectory(prefix='peadb-native-counting-') as tmp:
        root = Path(tmp)
        logs = root / 'logs'
        logs.mkdir()
        report = root / 'report.json'
        ledger = root / 'ledger.json'
        ledger.write_text(json.dumps({'suites': {'integration/psync2': {
            'mode': 'native', 'counting_policy': POLICY,
            'last_counts': {'ok': 12, 'err': 0, 'ignored': 6}, 'skipfile_lines': 0}}}))
        def parse(content, expected=0, require_complete=True):
            (logs / 'integration_psync2.log').write_text(content)
            command = [sys.executable, str(PARSER), str(logs), '--out-json', str(report)]
            if require_complete:
                command.append('--require-complete')
            result = subprocess.run(command,
                                    capture_output=True, text=True, timeout=5)
            assert result.returncode == expected, (result.stdout, result.stderr)
            if expected == 0:
                return json.loads(report.read_text())

        # Different numbers of complete cycles must not become different baselines.
        for cycles in (4, 1, 5, 6):
            names = sum((cycle(n) for n in range(1, cycles + 1)), []) + FINALS
            got = parse(log(names))
            row = got['suites']['integration/psync2']
            assert row['ok'] == 12, row
            assert row['raw_ok'] == cycles * 10 + 2, row
            assert row['cycles'] == cycles and row['counting_policy'] == POLICY, row
            assert row['ignored'] == 6 and row['err'] == 0, row
            assert got['totals']['ok'] == 12, got
            checked = subprocess.run([sys.executable, str(CHECKER), '--results', str(report),
                                      '--ledger', str(ledger), '--update'],
                                     capture_output=True, text=True, timeout=5)
            assert checked.returncode == 0, checked.stdout + checked.stderr
            assert json.loads(ledger.read_text())['suites']['integration/psync2'][
                'last_counts']['ok'] == 12

        valid = cycle(1) + cycle(2) + FINALS
        invalid = [valid[:-1], valid[1:], cycle(2) + FINALS,
                   cycle(1) + cycle(1) + FINALS, cycle(1)[:-1] + cycle(2) + FINALS,
                   cycle(1) + FINALS + ['PSYNC2: unknown new assertion'],
                   cycle(1)[:3] + [cycle(1)[2]] + cycle(1)[4:] + FINALS]
        for names in invalid:
            parse(log(names), 1)
        for bad in (log(valid).replace(COMPLETE, ''), log(valid) + '[err]: failure\n',
                    log(valid) + '[exception]: failure\n', log(valid) + '[ok]:unknown\n',
                    log(valid) + '[skip]: PSYNC2: skipped phase\n'):
            parse(bad, 1)
            parse(bad, 1, require_complete=False)
        # A documented-delta override must not permit missing/mismatched accounting.
        got = parse(log(valid))
        got['suites']['integration/psync2'].pop('counting_policy')
        report.write_text(json.dumps(got))
        checked = subprocess.run([sys.executable, str(CHECKER), '--results', str(report),
                                  '--ledger', str(ledger)],
                                 env=dict(os.environ, RATCHET_ALLOW_REGRESSION='1'),
                                 capture_output=True, text=True, timeout=5)
        assert checked.returncode == 1 and 'counting policy' in checked.stderr, checked
        # Ordinary suites keep their existing counts and report shape.
        spec = importlib.util.spec_from_file_location('parser', PARSER)
        parser = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(parser)
        ordinary = logs / 'unit_first.log'
        ordinary.write_text('[ok]: a\n[ok]: b\n[ignore]: c\n' + COMPLETE)
        assert parser.count_results(ordinary, True) == {
            'ok': 2, 'err': 0, 'ignored': 1, 'exceptions': 0}
    print('Native timed-suite accounting, raw evidence and rejection checks: ok')


if __name__ == '__main__':
    main()

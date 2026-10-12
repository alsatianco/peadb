#!/usr/bin/env python3
"""Exercise the Codex loop in disposable repositories without model requests.

Run: python3 AI-TEMP/test-auto-improve-codex.py
Requires git, jq, Python 3, and /bin/bash (macOS bash 3.2 is supported).
The real CLI/model smoke test is documented in auto-improve-codex-validation.md.
"""
import json
import os
from pathlib import Path
import select
import shutil
import signal
import subprocess
import tempfile
import time
import unittest

SCRIPT = Path(__file__).with_name('auto-improve-codex.sh')
MOCK = r'''#!/usr/bin/env python3
import json, os, pathlib, signal, subprocess, sys
if sys.argv[1:] == ['exec', '--help']:
    print('fixture help' if os.environ.get('MOCK_OLD_CLI') else '--approve-for-me --output-schema --json'); sys.exit(0)
if sys.argv[1:] == ['--version']:
    print('codex-cli fixture'); sys.exit(0)
args = sys.argv[1:]
assert args[0] == 'exec', args
assert '--approve-for-me' in args, args
assert 'approval_policy="on-request"' in args, args
assert 'approvals_reviewer="auto_review"' in args, args
assert 'model_reasoning_effort="high"' in args, args
assert args[args.index('--model') + 1] == 'gpt-6.1-sol', args
assert '--json' in args, args
state = pathlib.Path('AI-TEMP/mock-calls.json')
calls = json.loads(state.read_text()) if state.exists() else []
scenario = json.loads(os.environ['MOCK_SCENARIO'])
step = scenario[min(len(calls), len(scenario) - 1)]
calls.append(args); state.write_text(json.dumps(calls))
def option(name): return pathlib.Path(args[args.index(name) + 1])
assert json.loads(option('--output-schema').read_text())['additionalProperties'] is False
assert 'Rules for this session:' in args[-1]
assert 'never git push' in args[-1]
assert 'Never infer completion' in args[-1]
if 'resume' in args:
    assert args.index('--approve-for-me') < args.index('resume')
    assert args[args.index('resume') + 1] == '00000000-0000-4000-8000-000000000001'
    assert 'Do not start a new task' in args[-1]
def event(**data): print(json.dumps(data), flush=True)
if step.get('sid', True):
    event(type='thread.started', thread_id='00000000-0000-4000-8000-000000000001')
event(type='turn.started')
if step.get('hang'):
    child = subprocess.Popen(['sleep', '120'])
    pathlib.Path('AI-TEMP/child.pid').write_text(str(child.pid))
    def stop(*_):
        child.wait(timeout=5); sys.exit(143)
    signal.signal(signal.SIGTERM, stop)
    child.wait(); sys.exit(0)
if step.get('dirty'):
    pathlib.Path('tracked.txt').write_text('uncommitted change\n')
if step.get('commit'):
    pathlib.Path('tracked.txt').write_text('task committed\n')
    subprocess.run(['git', 'add', 'tracked.txt'], check=True)
    subprocess.run(['git', 'commit', '-qm', 'test: finish fixture task'], check=True)
if step.get('empty_plan'):
    pathlib.Path('AI-TEMP/improve-plan.md').write_text('')
if step.get('tick'):
    plan = pathlib.Path('AI-TEMP/improve-plan.md')
    plan.write_text(plan.read_text().replace('[ ]', '[x]'))
if step.get('rewrite'):
    subprocess.run(['git', 'checkout', '--orphan', 'replacement'], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    pathlib.Path('tracked.txt').write_text('rewritten\n')
    subprocess.run(['git', 'add', '.'], check=True)
    subprocess.run(['git', 'commit', '-qm', 'test: replace fixture history'], check=True)
    subprocess.run(['git', 'branch', '-M', 'main'], check=True)
result = {'status': step.get('status', 'task_done'), 'task': 'fixture task', 'commits': [], 'note': 'fixture result'}
if step.get('malformed'):
    result['commits'] = 'invalid'
if step.get('result', True):
    option('--output-last-message').write_text(json.dumps(result))
    event(type='item.completed', item={'type':'agent_message', 'text':json.dumps(result)})
if 'error' in step:
    event(type='turn.failed', error={'message':step['error']})
    print(step['error'], file=sys.stderr)
    sys.exit(1)
if step.get('completed', True):
    event(type='turn.completed', usage={'input_tokens':100, 'cached_input_tokens':50, 'output_tokens':10})
sys.exit(step.get('rc', 0))
'''


class LoopTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='codex-loop-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.git('init', '-q', '-b', 'main')
        self.git('config', 'user.name', 'Loop fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        (self.root / '.gitignore').write_text('AI-TEMP/\n')
        (self.root / 'tracked.txt').write_text('initial\n')
        self.git('add', '.')
        self.git('commit', '-qm', 'test: initialize fixture')
        (self.root / 'AI-TEMP/bin').mkdir(parents=True)
        shutil.copy(SCRIPT, self.root / 'AI-TEMP/auto-improve-codex.sh')
        (self.root / 'AI-TEMP/improve-plan.md').write_text('- [ ] fixture task\n')
        mock = self.root / 'AI-TEMP/bin/codex'
        mock.write_text(MOCK)
        mock.chmod(0o755)
        self.env = {k:v for k,v in os.environ.items() if k not in ('MODEL', 'EFFORT', 'MAX_BUDGET_USD', 'MAX_RUNS', 'MAX_FAILS', 'MAX_IDLE', 'LIMIT_WAIT', 'MAX_LIMIT_WAITS', 'ALLOW_UNTRACKED')}
        self.env.update(PATH=str(mock.parent) + os.pathsep + os.environ['PATH'], AUTO_IMPROVE_AWAKE='1', MAX_RUNS='1', MAX_FAILS='1', LIMIT_WAIT='0', MAX_LIMIT_WAITS='2')

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, text=True).strip()

    def run_loop(self, scenario=None, expected=2, **overrides):
        env = {**self.env, **overrides, 'MOCK_SCENARIO':json.dumps(scenario or [{}])}
        proc = subprocess.run(['/bin/bash', 'AI-TEMP/auto-improve-codex.sh'], cwd=self.root, env=env, capture_output=True, text=True, timeout=20)
        self.assertEqual(proc.returncode, expected, proc.stdout + proc.stderr)
        self.assertFalse((self.root / 'AI-TEMP/auto-improve/lock').exists())
        return proc.stdout + proc.stderr

    def calls(self):
        path = self.root / 'AI-TEMP/mock-calls.json'
        return json.loads(path.read_text()) if path.exists() else []

    def test_task_commit_plan_and_token_summary(self):
        out = self.run_loop([{'commit':True, 'tick':True}])
        self.assertIn('completion has not been audited twice', out)
        self.assertEqual(len(self.calls()), 1)
        self.assertEqual(self.git('status', '--porcelain'), '')
        summary = (self.root / 'AI-TEMP/auto-improve-codex/summary.tsv').read_text()
        self.assertIn('\ttask_done\t1\t100\t50\t10\t', summary)
        latest = self.root / 'AI-TEMP/auto-improve-codex/latest'
        for name in ('schema.json', 'instructions.txt', 'stream.jsonl', 'result.json', 'usage.json', 'plan-before.md', 'plan-after.md', 'commits.txt', 'stderr.txt'):
            self.assertTrue((latest / name).is_file(), name)
        self.assertEqual(json.loads((latest / 'usage.json').read_text())['input_tokens'], 100)

    def test_all_done_requires_two_fresh_sessions(self):
        out = self.run_loop([{'status':'all_done'}], expected=0, MAX_RUNS='0')
        self.assertIn('independent completion audit', self.calls()[1][-1])
        self.assertIn('two fresh sessions', out)
        self.assertEqual(len(self.calls()), 2)
        self.assertTrue(all('resume' not in c for c in self.calls()))

    def test_rate_limit_resumes_the_same_task_even_if_dirty(self):
        out = self.run_loop([{'error':'You have hit your usage limit', 'dirty':True, 'result':False}, {'commit':True, 'tick':True}])
        self.assertIn('usage limit', out)
        self.assertIn('resume', self.calls()[1])
        self.assertEqual(self.git('status', '--porcelain'), '')

    def test_prestart_429_retries_fresh(self):
        self.run_loop([{'error':'HTTP 429 Too Many Requests', 'sid':False, 'result':False}, {'tick':True, 'commit':True}])
        self.assertNotIn('resume', self.calls()[1])
        self.assertEqual(self.calls()[0][-1], self.calls()[1][-1])

    def test_non_limit_failure_resumes_once(self):
        out = self.run_loop([{'error':'connection reset', 'result':False}, {'commit':True, 'tick':True}])
        self.assertIn('resuming it once', out)
        self.assertEqual(len(self.calls()), 2)

    def test_stale_result_not_reused_after_retry(self):
        out = self.run_loop([{'error':'connection reset'}, {'result':False, 'completed':False}], expected=1)
        self.assertIn('status=none', out)
        self.assertEqual(len(self.calls()), 2)

    def test_malformed_result_rejected(self):
        out = self.run_loop([{'malformed':True}], expected=1)
        self.assertIn('status=none', out)
        self.assertEqual(len(self.calls()), 2)

    def test_nonzero_exit_cannot_report_success(self):
        out = self.run_loop([{'rc':1}], expected=1)
        self.assertIn('status=none', out)

    def test_limit_wait_cap(self):
        out = self.run_loop([{'error':'rate limit reached', 'result':False}], expected=1)
        self.assertIn('2 usage-limit waits', out)
        self.assertEqual(len(self.calls()), 3)

    def test_dirty_result_stops_immediately(self):
        out = self.run_loop([{'dirty':True}], expected=1, MAX_RUNS='0')
        self.assertIn('working tree has changes', out)
        self.assertEqual(len(self.calls()), 1)

    def test_idle_cap(self):
        out = self.run_loop(expected=1, MAX_RUNS='0', MAX_IDLE='2')
        self.assertIn('without both a commit and an updated plan', out)
        self.assertEqual(len(self.calls()), 2)

    def test_failed_status_cap(self):
        out = self.run_loop([{'status':'failed'}], expected=1, MAX_RUNS='0', MAX_FAILS='2')
        self.assertIn('2 sessions in a row failed', out)
        self.assertEqual(len(self.calls()), 2)

    def test_history_rewrite_stops(self):
        out = self.run_loop([{'rewrite':True}], expected=1)
        self.assertIn('history was rewritten', out)

    def test_wrong_branch_and_dirty_start_never_launch(self):
        self.git('checkout', '-qb', 'fixture-branch')
        self.assertIn('not main', self.run_loop(expected=1))
        self.assertEqual(self.calls(), [])
        self.git('checkout', '-q', 'main')
        (self.root / 'unexpected.txt').write_text('dirty\n')
        self.assertIn('working tree has changes', self.run_loop(expected=1))
        self.assertEqual(self.calls(), [])

    def test_allowed_untracked_notes_after_history_change(self):
        (self.root / 'todo.md').write_text('owner notes\n')
        (self.root / 'auto_prompt.md').write_text('owner prompt\n')
        self.git('commit', '--amend', '-qm', 'test: rewrite fixture history before startup')
        self.run_loop([{'status':'all_done'}])
        self.assertEqual(len(self.calls()), 1)

    def test_custom_untracked_allowance_overrides_defaults(self):
        (self.root / 'notes.md').write_text('custom notes\n')
        self.run_loop([{'status':'all_done'}], ALLOW_UNTRACKED='notes.md')
        self.assertEqual(len(self.calls()), 1)
        (self.root / 'auto_prompt.md').write_text('owner prompt\n')
        out = self.run_loop(expected=1, ALLOW_UNTRACKED='notes.md')
        self.assertIn('?? auto_prompt.md', out)
        self.assertEqual(len(self.calls()), 1)

    def test_allowed_notes_still_block_tracked_edits(self):
        (self.root / 'auto_prompt.md').write_text('tracked prompt\n')
        self.git('add', 'auto_prompt.md')
        self.git('commit', '-qm', 'test: track owner prompt')
        (self.root / 'auto_prompt.md').write_text('edited prompt\n')
        out = self.run_loop(expected=1)
        self.assertIn(' M auto_prompt.md', out)
        self.assertEqual(self.calls(), [])

    def test_no_checkboxes_still_requires_audit(self):
        (self.root / 'AI-TEMP/improve-plan.md').write_text('Owner still needs to publish.\n')
        self.run_loop([{'status':'all_done'}], expected=0, MAX_RUNS='0')
        self.assertEqual(len(self.calls()), 2)

    def test_empty_plan_is_an_error(self):
        (self.root / 'AI-TEMP/improve-plan.md').write_text('')
        self.run_loop(expected=2)
        self.assertEqual(self.calls(), [])

    def test_session_empties_plan(self):
        out = self.run_loop([{'empty_plan':True}], expected=1)
        self.assertIn('removed or emptied', out)

    def test_commit_without_plan_update_hits_idle_cap(self):
        out = self.run_loop([{'commit':True}, {}], expected=1, MAX_RUNS='0')
        self.assertIn('without both a commit and an updated plan', out)

    def test_blocked_stops_without_retry(self):
        self.run_loop([{'status':'blocked'}], expected=2, MAX_RUNS='0')
        self.assertEqual(len(self.calls()), 1)

    def test_older_cli_fails_before_launch(self):
        out = self.run_loop(expected=2, MOCK_OLD_CLI='1')
        self.assertIn('lacks --approve-for-me', out)
        self.assertEqual(self.calls(), [])

    def test_help_without_session(self):
        proc = subprocess.run(['/bin/bash', 'AI-TEMP/auto-improve-codex.sh', '--help'], cwd=self.root, env=self.env, capture_output=True, text=True, timeout=10)
        self.assertEqual(proc.returncode, 0)
        self.assertIn('MODEL=gpt-6.1-sol', proc.stdout)
        self.assertEqual(self.calls(), [])

    def test_watch_renders_codex_events(self):
        self.run_loop([{'status':'all_done'}])
        stream = self.root / 'AI-TEMP/auto-improve-codex/latest/stream.jsonl'
        with stream.open('a') as out:
            out.write(json.dumps({'type':'item.started', 'item':{'type':'command_execution', 'command':'echo fixture-watch'}}) + '\n')
        proc = subprocess.Popen(['/bin/bash', 'AI-TEMP/auto-improve-codex.sh', 'watch'], cwd=self.root, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        try:
            self.assertTrue(select.select([proc.stdout], [], [], 10)[0], 'watch produced no output')
            # Give jq time to flush the short fixture transcript before stopping tail.
            time.sleep(0.2)
        finally:
            os.killpg(proc.pid, signal.SIGTERM)
            out, err = proc.communicate(timeout=10)
        self.assertIn(b'fixture result', out, err)
        self.assertIn(b'fixture-watch', out, err)
        self.assertIn(b'== token usage:', out, err)

    def test_live_shared_lock_never_launches(self):
        lock = self.root / 'AI-TEMP/auto-improve/lock'
        lock.mkdir(parents=True)
        (lock / 'pid').write_text(str(os.getpid()))
        proc = subprocess.run(['/bin/bash', 'AI-TEMP/auto-improve-codex.sh'], cwd=self.root, env=self.env, capture_output=True, text=True, timeout=10)
        self.assertEqual(proc.returncode, 2)
        self.assertIn('loop lock exists', proc.stderr)
        self.assertTrue(lock.exists())
        self.assertEqual(self.calls(), [])

    def test_stale_lock_requires_review(self):
        lock = self.root / 'AI-TEMP/auto-improve/lock'
        lock.mkdir(parents=True)
        (lock / 'pid').write_text('999999999')
        proc = subprocess.run(['/bin/bash', 'AI-TEMP/auto-improve-codex.sh'], cwd=self.root, env=self.env, capture_output=True, text=True, timeout=10)
        self.assertEqual(proc.returncode, 2)
        self.assertTrue(lock.exists())
        self.assertEqual(self.calls(), [])

    def test_leading_zeros_normalized(self):
        self.run_loop([{'status':'all_done'}], expected=0, MAX_RUNS='003')
        self.assertEqual(len(self.calls()), 2)

    def test_invalid_settings_and_unsupported_cost_cap(self):
        for overrides in ({'MAX_RUNS':'oops'}, {'MAX_FAILS':'0'}, {'MAX_IDLE':'0'}, {'EFFORT':'ultra'}, {'MAX_BUDGET_USD':'1'}):
            with self.subTest(overrides=overrides):
                self.run_loop(expected=2, **overrides)
        self.assertEqual(self.calls(), [])

    def test_signal_cleans_lock_and_child(self):
        env = {**self.env, 'MOCK_SCENARIO':json.dumps([{'hang':True}])}
        proc = subprocess.Popen(['/bin/bash', 'AI-TEMP/auto-improve-codex.sh'], cwd=self.root, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            child_file = self.root / 'AI-TEMP/child.pid'
            deadline = time.monotonic() + 10
            while not child_file.exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertTrue(child_file.exists())
            child = int(child_file.read_text())
            proc.send_signal(signal.SIGTERM)
            out, err = proc.communicate(timeout=10)
            self.assertEqual(proc.returncode, 130, out + err)
            self.assertFalse((self.root / 'AI-TEMP/auto-improve/lock').exists())
            with self.assertRaises(ProcessLookupError):
                os.kill(child, 0)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()


if __name__ == '__main__':
    unittest.main(verbosity=2)

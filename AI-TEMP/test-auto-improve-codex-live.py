#!/usr/bin/env python3
"""Run a real model-backed task and resume in a disposable repository.

Uses your saved Codex login; makes real model requests. Logs stay ignored.
Run from the repository root: python3 AI-TEMP/test-auto-improve-codex-live.py
"""
import json, os, pathlib, shutil, subprocess, tempfile
source = pathlib.Path(__file__).with_name('auto-improve-codex.sh')
validation = source.parent / 'auto-improve-codex-validation'
validation.mkdir(exist_ok=True)
root = pathlib.Path(tempfile.mkdtemp(prefix='auto-improve-codex-live-'))
(validation / 'live-root.txt').write_text(str(root))
def git(*args):
    return subprocess.check_output(['git', *args], cwd=root, text=True).strip()
git('init', '-q', '-b', 'main')
git('config', 'user.name', 'Codex CLI test')
git('config', 'user.email', 'codex-test@example.invalid')
(root / 'AI-TEMP').mkdir()
(root / 'scripts/dev').mkdir(parents=True)
(root / 'compat').mkdir()
(root / '.gitignore').write_text('AI-TEMP/\n')
(root / 'README.md').write_text('Disposable Codex CLI smoke-test repository.\n')
(root / 'compat/delta.md').write_text('No Redis implementation in this CLI fixture.\n')
(root / 'scripts/run_all_tests.sh').write_text('#!/bin/bash\ntest "$(cat smoke.txt)" = codex-ok\n')
(root / 'scripts/dev/redis_vs_peadb_benchmark.sh').write_text('#!/bin/bash\ntest "$(cat smoke.txt)" = codex-ok\n')
git('add', '.')
git('commit', '-qm', 'test: initialize disposable CLI fixture')
shutil.copy(source, root / 'AI-TEMP/auto-improve-codex.sh')
# Keep this fixture independent of PeaDB's remote CI and any GitHub credentials.
(root / 'AI-TEMP/bin').mkdir()
gh_fixture = root / 'AI-TEMP/bin/gh'
gh_fixture.write_text("#!/bin/sh\nprintf '%s\\n' '{\"workflow_runs\":[]}'\n")
gh_fixture.chmod(0o755)
(root / 'AI-TEMP/improve-plan.md').write_text('''# Disposable CLI smoke test
## Part 0
This is a disposable fixture for testing the automation script, not the PeaDB implementation.
- [ ] Create smoke.txt containing exactly codex-ok followed by a newline. Update README.md to document it and compat/delta.md to record that the fixture has no compatibility gaps. Run bash scripts/run_all_tests.sh and bash scripts/dev/redis_vs_peadb_benchmark.sh (both are tiny local fixture checks), then commit these changes with one concise message. Tick this plan checkbox after success. No other task is authorized in this fixture.
### 0.4 Rule for every item
Use only the tiny fixture checks described above; do not access or modify another repository.
### 0.5 Owner decisions
None.
''')
env = {**os.environ, 'AUTO_IMPROVE_AWAKE':'1', 'MODEL':'gpt-6.1-sol', 'EFFORT':'high', 'MAX_RUNS':'1', 'MAX_FAILS':'1', 'MAX_IDLE':'2', 'MAX_LIMIT_WAITS':'0'}
env.pop('MAX_BUDGET_USD', None)
env['GH_TOKEN'] = 'fixture-read-token'
env['PATH'] = str(gh_fixture.parent) + os.pathsep + os.environ['PATH']
print(f'Live fixture: {root}', flush=True)
with (validation / 'live-loop.log').open('w') as log:
    completed = subprocess.run(['/bin/bash', str(root / 'AI-TEMP/auto-improve-codex.sh')], cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=300)
print(f'Loop exit: {completed.returncode}', flush=True)
print((validation / 'live-loop.log').read_text(), flush=True)
assert completed.returncode == 2
run = (root / 'AI-TEMP/auto-improve-codex/latest').resolve()
result = json.loads((run / 'result.json').read_text())
assert result['status'] in ('task_done', 'all_done'), result
assert (root / 'smoke.txt').read_text() == 'codex-ok\n'
assert git('status', '--porcelain') == ''
assert '[ ]' not in (root / 'AI-TEMP/improve-plan.md').read_text()
events = [json.loads(line) for line in (run / 'stream.jsonl').read_text().splitlines()]
sid = next(e['thread_id'] for e in events if e['type'] == 'thread.started')
print(f'Live task, git commit, plan update, schema, JSONL, usage: PASS; thread={sid}', flush=True)
# Match the script's exact argument order; resume accepts inherited parent exec flags.
args = ['codex', 'exec', '--model', 'gpt-6.1-sol', '-c', 'model_reasoning_effort="high"', '--approve-for-me', '-c', 'approval_policy="on-request"', '-c', 'approvals_reviewer="auto_review"', '--output-schema', str(run / 'schema.json'), '--output-last-message', str(run / 'resume-result.json'), '--json', 'resume', sid, 'The smoke-test task is committed and the plan is complete. Verify the plan and report all_done. Do not edit files or make another commit.\n\n' + (run / 'instructions.txt').read_text()]
with (run / 'resume.jsonl').open('w') as out, (run / 'resume-stderr.txt').open('w') as err:
    resumed = subprocess.run(args, cwd=root, env=env, stdin=subprocess.DEVNULL, stdout=out, stderr=err, timeout=180)
assert resumed.returncode == 0, (run / 'resume-stderr.txt').read_text()
assert json.loads((run / 'resume-result.json').read_text())['status'] == 'all_done'
assert git('status', '--porcelain') == ''
print('Live resume with the same model, effort, Auto review, schema, and instructions: PASS', flush=True)

# Verify configuration and rule delivery from the saved rollout, rather than CLI
# acceptance alone: unknown -c keys can otherwise be silently ignored.
codex_dir = pathlib.Path(os.environ.get('CODEX_HOME', str(pathlib.Path.home() / '.codex')))
rollouts = list((codex_dir / 'sessions').rglob(f'*{sid}.jsonl'))
assert rollouts, 'No saved rollout for the smoke-test thread'
records = [json.loads(line) for line in rollouts[0].read_text().splitlines()]
contexts = [r['payload'] for r in records if r.get('type') == 'turn_context']
assert len(contexts) >= 2, 'Expected initial and resumed turn contexts'
for ctx in contexts:
    assert ctx['model'] == 'gpt-6.1-sol'
    assert ctx['effort'] == 'high'
    assert ctx['approval_policy'] == 'on-request'
    assert ctx['approvals_reviewer'] == 'auto_review'
    assert ctx['sandbox_policy']['type'] == 'workspace-write'
rule_messages = [r for r in records if r.get('type') == 'response_item' and r.get('payload', {}).get('role') == 'user' and 'Rules for this session:' in json.dumps(r)]
assert len(rule_messages) >= 2, 'Loop rules must be delivered on both turns'
print('Saved session confirms model, effort, Auto review, sandbox, and loop-rule delivery: PASS', flush=True)

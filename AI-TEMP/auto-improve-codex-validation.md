# Codex auto-improve loop

`auto-improve-codex.sh` uses `gpt-6.1-sol`, high reasoning effort, and automatic approval review. It keeps the original PeaDB task rules, resumes interrupted sessions, waits on usage limits, and records Codex JSONL events and structured results.

Start from the repository root:

```bash
bash AI-TEMP/auto-improve-codex.sh
bash AI-TEMP/auto-improve-codex.sh watch  # another terminal
```

The initial working tree must be clean and on `main`, except for untracked local notes `todo.md` and `auto_prompt.md`, which are allowed by default. `ALLOW_UNTRACKED` overrides that list; tracked edits always stop the loop. For example, to allow another notes file:

```bash
ALLOW_UNTRACKED='todo.md auto_prompt.md notes.md' bash AI-TEMP/auto-improve-codex.sh
```

Logs, schemas, instructions, results, token counts, plan snapshots, and commit lists live under `AI-TEMP/auto-improve-codex/`. The shared lock remains at `AI-TEMP/auto-improve/lock`, preventing simultaneous Claude and Codex loops. An existing lock is never automatically deleted: inspect its PID and running processes before removing a stale lock.

The CLI invocation uses:

```bash
codex exec --model gpt-6.1-sol \
  -c 'model_reasoning_effort="high"' \
  --approve-for-me \
  -c 'approval_policy="on-request"' \
  -c 'approvals_reviewer="auto_review"' \
  --output-schema /absolute/path/schema.json \
  --output-last-message /absolute/path/result.json \
  --json "$session_prompt"
```

All loop rules are included in the prompt, including resumed turns. Resume uses the same parent `exec` flags before `resume SESSION_ID PROMPT`; the resume subcommand itself does not expose `--approve-for-me`. Codex's automatic review keeps the workspace sandbox and reviews escalation requests. Denied actions produce a `blocked` result when no permitted alternative remains. This mode differs from disabling approval review or disabling the sandbox.

The changes adopted from `../kawasan/auto-improve.sh` are prompt-based loop rules, explicit fresh completion audits, CLI capability checks, unique run directories, validation of nonempty plans, stronger progress detection, conservative lock handling, a blocked status, and clear exit codes. PeaDB's original distinction between agent work and owner-only publication actions remains in the prompt. Two fresh sessions must agree that no authorized agent task remains; unchecked-box counts are diagnostic only.

Exit codes are `0` for audited completion, `1` for failure or stalled progress, and `2` for a configuration error, lock, blocked result, or `MAX_RUNS` cap. A bounded run can finish a task successfully and still exit `2` because completion has not been audited twice. `MAX_RUNS=0` is unlimited. Other environment settings are listed by `--help`.

Codex JSONL reports token usage rather than Claude's dollar-cost fields. `MAX_BUDGET_USD` is rejected because Codex has no corresponding CLI budget cap. Usage-limit events do not provide Claude's reset timestamp, so the loop waits `LIMIT_WAIT` seconds (default 1800) before retrying.

## Research and validation

Checked against installed `codex-cli 0.162.1` and macOS `/bin/bash` 3.2 on 2026-10-12. Official references:

- [Non-interactive mode](https://learn.chatgpt.com/docs/non-interactive-mode): `exec`, JSONL events, output schemas, last-message files, and resume.
- [Auto-review](https://learn.chatgpt.com/docs/sandboxing/auto-review): workspace sandbox, `on-request`, and `auto_review`.
- [Configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference): `model_reasoning_effort`.
- [GPT-6.1 Sol](https://developers.openai.com/api/docs/models/gpt-6.1-sol): exact model ID and supported high effort.

Run deterministic integration checks without model requests:

```bash
/bin/bash -n AI-TEMP/auto-improve-codex.sh
shellcheck --shell=bash AI-TEMP/auto-improve-codex.sh
python3 AI-TEMP/test-auto-improve-codex.py
```

The disposable-repository tests cover commits, plan updates, completion audits, token summaries, watcher output, rate-limit waits, resume, stale or malformed results, dirty trees, default and custom untracked-file allowances, tracked notes edits, branch and history guards, active and stale locks, blocked results, idle/failure caps, unsupported CLI versions, settings validation, and signal cleanup. A rate-limit test caught a real `grep -q`/`pipefail` SIGPIPE bug; the loop now consumes the full error stream.

A real-model test is available separately; it makes model requests using the saved Codex login:

```bash
python3 AI-TEMP/test-auto-improve-codex-live.py
```

It creates an isolated Git repository, gives the loop one small file/documentation task with tiny fixture checks, verifies the commit and plan update, then resumes the same session. Saved session records verify the effective model, effort, approval policy, reviewer, workspace sandbox, and delivery of loop rules on both turns. The repository and logs remain available for inspection; their path is printed and recorded in the ignored validation directory. These checks test the automation and do not run PeaDB's real improvement plan or its full server suite.

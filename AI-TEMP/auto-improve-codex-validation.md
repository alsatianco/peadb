# Codex auto-improve loop

`auto-improve-codex.sh` uses `gpt-6.1-sol`, high reasoning effort, and automatic approval review. It keeps the original PeaDB task rules, resumes interrupted sessions, waits on usage limits, and records Codex JSONL events and structured results.

Start from the repository root:

```bash
bash AI-TEMP/auto-improve-codex.sh
bash AI-TEMP/auto-improve-codex.sh watch  # another terminal
```

The checkout must be on `main`. When it has changes, a separate Codex cleanup session inspects staged/unstaged diffs, untracked files and plan notes. It commits complete, validated changes or preserves unfinished/unrelated work in a named stash with explicit paths, including untracked files. Stash reasons, hashes and recovery commands belong in the plan notes. The loop verifies a clean tree and a new commit or stash before continuing; failed cleanup stops it. Cleanup runs before the first task and after sessions that leave changes, and does not count toward `MAX_RUNS` or task progress. Cleanup after an `all_done` result requires new completion audits. Interrupted cleanup resumes the same cleanup session.

Untracked local notes `todo.md` and `auto_prompt.md` remain allowed by default. `ALLOW_UNTRACKED` overrides that list; tracked edits to those files still require cleanup. For example, to allow another notes file:

```bash
ALLOW_UNTRACKED='todo.md auto_prompt.md notes.md' bash AI-TEMP/auto-improve-codex.sh
```

Before every fresh task session (including completion audits), the script uses `gh api --paginate` to check [GitHub Actions](https://github.com/alsatianco/peadb/actions). It saves `actions-pages.json` and `actions.json` in the run directory and attaches the report to the session prompt. The latest completed result for each workflow/branch/event determines outstanding failures; newer pending, cancelled, skipped or neutral runs do not hide a failure, and a later successful run supersedes it. Failure, timeout, startup failure and action-required results need investigation. Running/cancelled/skipped checks are not reported as passed. An API/authentication error or malformed response stops the loop before plan work.

The session must inspect failed jobs/logs, fix applicable failures, validate and commit fixes, then proceed to the next plan task. Because the loop never pushes, remote failures may remain red after a local fix: subsequent sessions verify the existing fix and record local evidence rather than repeating it. Failures exclusive to another branch require an explanation. Unreadable logs or unresolved relevant failures prevent plan progress. `gh` is required for normal runs; it uses existing authentication or process-local `GH_TOKEN` from `~/.secrets/github_token`. Token contents are never printed or saved in logs.

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

Update verified on 2026-10-13: all 43 disposable-repository checks passed, macOS Bash syntax and ShellCheck passed, and a read-only check against the real GitHub API identified the latest failing workflow. The full repository gate passed all 11 stages with zero failures/skips using the local dependency setup documented in the improvement plan (`AI-TEMP/tools`, the pinned Redis cache, and `LUA_PKG=peadb-bundled-lua51`); log: `AI-TEMP/auto-improve-codex-fix-tests-final.log`. The Linux-only Docker comparison was not run on macOS. The updated real-model fixture was syntax-checked; no new model-backed smoke run was made.

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

The disposable-repository tests cover commits, plan updates, completion audits, token summaries, watcher output, rate-limit waits, resume, stale or malformed results, dirty-tree commits and stashes, failed cleanup, cleanup resume, default and custom untracked-file allowances, tracked notes edits, paginated Actions reports, superseded failures, run attempts, pending runs, fresh per-iteration checks, API failures, malformed responses, branch and history guards, active and stale locks, blocked results, idle/failure caps, unsupported CLI versions, settings validation, and signal cleanup. A rate-limit test caught a real `grep -q`/`pipefail` SIGPIPE bug; the loop now consumes the full error stream.

A real-model test is available separately; it makes model requests using the saved Codex login:

```bash
python3 AI-TEMP/test-auto-improve-codex-live.py
```

It creates an isolated Git repository with an empty Actions fixture (no remote CI repairs), gives the loop one small file/documentation task with tiny fixture checks, verifies the commit and plan update, then resumes the same session. Saved session records verify the effective model, effort, approval policy, reviewer, workspace sandbox, and delivery of loop rules on both turns. The repository and logs remain available for inspection; their path is printed and recorded in the ignored validation directory. These checks test the automation and do not run PeaDB's real improvement plan or its full server suite.

# Autonomous Codex plan runner

The maintained runner is `scripts/dev/auto-improve-codex.sh`; its plan and runtime records remain ignored under `AI-TEMP/`. It targets this repository's `main` branch and Redis 7.2.5 improvement plan. Prerequisites are Bash (including macOS Bash 3.2), Git, jq, gh with read access to Actions, a logged-in Codex CLI supporting `--approve-for-me`, and the plan's build/test tools. The existing loop policy authorizes Bash to commit validated changes and push to `origin/main`; the model requests explicit file paths and never runs Git mutations itself.

Start or restart with the same command:

```bash
bash scripts/dev/auto-improve-codex.sh
bash scripts/dev/auto-improve-codex.sh watch  # another terminal
```

The original local entry point can link to the maintained runner (preserve any older local copy before replacing it):

```bash
ln -s ../scripts/dev/auto-improve-codex.sh AI-TEMP/auto-improve-codex.sh
bash AI-TEMP/auto-improve-codex.sh
```

## Recovery and change review

Before invoking Codex, the runner saves a plan snapshot, starting commit, phase, transcript and an atomic `active` pointer under `AI-TEMP/auto-improve-codex/`. Ctrl-C stops the session or usage-limit sleep, cleans up owned child processes and releases the shared loop lock; the recovery record remains. A restart checks branch/history and resumes that phase by the exact session ID from its transcript, before cleanup or new plan work. A clean working tree does not cancel a pending task. The original runner's latest unreported run is recognized automatically, including a run stopped during a usage-limit wait. Completed legacy runs are identified by their summary records and are not resumed.

Recovery re-reads the current plan, linked task plans, Git status, diffs and recent commits. It refreshes the Actions report and requests only remaining changes after validation. Saved result files are never replayed: if a commit or stash already succeeded before interruption, the resumed session reconciles the current state. When a saved session explicitly reports that it no longer exists, a fresh session reconstructs the same task from the plan and run records. An interruption before the CLI emits a session ID uses the same reconstruction path.

When no task record exists and Git has changes, a separate review session reads the plan and all staged, unstaged and untracked files:

| Finding | Action |
| --- | --- |
| Unfinished current/next plan task | Preserve its implementation, helpers, tests and docs, record the remaining work in the plan, and continue it. Failing or unrun checks are work to finish. |
| Complete, useful unrelated changes | Validate and request a coherent commit with explicit paths. |
| Unrelated work that should be put aside | Request a named stash with explicit paths, including untracked files. Save its hash and recovery command in the plan. |
| Mixed task and unrelated changes | Commit/stash only the unrelated paths, then continue the preserved task. |

The review's structured `resume_task` status tells Bash to continue the identified task rather than requiring a clean tree. Classification and validation are model decisions, subject to the same repository instructions as task implementation. The runner checks result shape, path validity, branch/history, command exits and cleanup outcomes; it cannot independently prove that a model's interpretation of the plan or validation claim is correct.

Failed and blocked task results retain the task record and working files. Ordinary failures retry the same task up to `MAX_FAILS`; restarting later continues that task. An interrupted cleanup resumes cleanup, including reconciliation of earlier Git requests, rather than starting a plan item. New tasks must first inspect plan checkpoints even when Git is clean, and must write a dated in-progress checkpoint before implementation. Stashing never marks a task complete.

## Existing unattended safeguards

Every task attempt gets a fresh paginated Actions report. Applicable failures must be investigated and repaired before plan progress; pending, cancelled and skipped checks are never credited as passes. The model must run the required gates on final contents before requesting commits. Bash accepts commit/stash requests only from completed, schema-valid successful turns, stages literal file paths, rejects ignored untracked files and local notes, checks whitespace and commits each group separately. Each commit is pushed immediately; a push failure preserves local commits, and startup retries publication. No force-push, history rewrite, stash drop/pop, or discard is performed.

Usage limits wait and resume the same session indefinitely by default. Ordinary error retries, failure caps and idle detection prevent endless unsuccessful work. Two fresh sessions must independently audit completion; a recovered session cannot supply one of those two audits. Plan prose, deferred tasks and acceptance gates count as work alongside checkboxes. Owner-only actions remain excluded by the loop rules.

`--help` lists settings. Common overrides are `MAX_RUNS=1` for a bounded run, `LIMIT_WAIT=1800`, `MAX_LIMIT_WAITS=0` for unlimited usage-limit waits, `MAX_FAILS=2`, `MAX_IDLE=2`, and `ALLOW_UNTRACKED='todo.md auto_prompt.md'`. Allowed untracked notes stay untouched; tracked edits to those paths still require review. `MAX_BUDGET_USD` is rejected because Codex events do not expose a dollar-cost cap.

Exit codes are `0` for completion audited twice, `1` for failure/stall, and `2` for configuration, lock, blocked work or a session cap. `MAX_RUNS=1` normally exits `2` even after a successful task because completion still needs audits. Session caps and failure/idle counters apply to each invocation; durable task recovery survives restarts. Logs, JSONL transcripts, result/schema files, plan snapshots, summaries and stash recovery records stay in the ignored state directory.

The lock at `AI-TEMP/auto-improve/lock` is shared with the original loop. A stale lock is not automatically deleted: inspect its PID and any surviving owned child processes before removing it. SIGKILL or a machine crash can leave this lock and children behind. Authentication failures, unreadable Actions reports, missing prerequisites, divergent history, denied approvals and unresolved external blockers stop the runner with its work preserved. Running under a service supervisor can restart after an outage, but requires a policy for these blockers; blindly restarting permanent failures or removing locks is not a completion strategy. Model quality, external availability and owner-only work prevent a promise that every plan item can finish without intervention.

## Verification

The deterministic suite runs the actual Bash runner in disposable repositories with mock Codex/Actions responses and local bare remotes. It makes no model requests and never touches the real remote or improvement tasks:

```bash
/bin/bash -n scripts/dev/auto-improve-codex.sh
shellcheck --shell=bash scripts/dev/auto-improve-codex.sh
python3 scripts/dev/test_auto_improve_codex.py
```

Verified on 2026-10-31: all 68 checks passed in macOS Bash 3.2 and native ARM64 Linux Bash, with final summaries and exit code 0. Bash syntax, ShellCheck and Python compilation also passed. The initial 55-check run failed one old assertion expecting failed plan work to be stashed; its replacement verifies preservation and resumption. The following expanded 65-check run and final 68-check runs passed. Logs remain local under `AI-TEMP/auto-improve-codex-recovery-*.log`. No model-backed plan session or PeaDB server gate was run for this automation change.

Coverage includes process-level stop/restart during active work and usage waits, original-run adoption, clean-tree recovery, missing-session reconstruction, cleanup recovery, partial Git delivery without result replay, rewritten-history rejection, plan-driven/mixed-file review, failed-task preservation, two fresh audits after recovery, and the existing commit, push, stash, CLI, Actions, lock and progress guards. This suite validates automation; it does not execute PeaDB's server gates or establish the quality of model-generated changes.

Codex resume syntax is checked against installed CLI help and [official non-interactive mode documentation](https://learn.chatgpt.com/docs/non-interactive-mode). Parent `exec` flags precede `resume SESSION_ID PROMPT`. The invocation retains workspace sandboxing and automatic approval review; see [official auto-review documentation](https://learn.chatgpt.com/docs/sandboxing/auto-review).

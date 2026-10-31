#!/usr/bin/env bash
# auto-improve-codex.sh: run unattended Codex CLI sessions, one task of AI-TEMP/improve-plan.md per session, until no task is left that an agent can do.
#
# Usage:
#   bash AI-TEMP/auto-improve-codex.sh         start the loop (from anywhere inside the repo; Ctrl-C stops it)
#   bash AI-TEMP/auto-improve-codex.sh watch   follow the current session's text and tool calls in another terminal
#   bash AI-TEMP/auto-improve-codex.sh --help  show usage without starting a session
# Exit codes: 0 = completion audited twice; 1 = failed/stalled; 2 = blocked/config/run limit.
#
# Optional settings (environment variables):
#   MODEL=gpt-6.1-sol         exact Codex model ID
#   EFFORT=high              low | medium | high | xhigh | max
#   MAX_RUNS=0                 stop after this many sessions (0 = no limit)
#   MAX_FAILS=2                stop after this many failed sessions in a row
#   MAX_IDLE=2                 stop after this many "task_done" sessions without both a commit and a plan update
#   LIMIT_WAIT=1800            seconds to wait after a usage limit when the reset time is unknown
#   MAX_LIMIT_WAITS=0          usage-limit wait cap (0 = wait until usage returns)
#   ALLOW_UNTRACKED="todo.md auto_prompt.md"  untracked files (space-separated) that do not count as a dirty working tree
#
# Records live in AI-TEMP/auto-improve-codex/run-*/ and cleanup-*/ (JSONL transcript, result, usage, stderr, plans, commits). Task sessions also go to summary.tsv; cleanup records include stash hashes.
#
# The loop stops when two fresh sessions agree no agent task remains, work is blocked, cleanup fails, the repo leaves main, history is rewritten, or sessions repeatedly fail or stall.
#
# Written for macOS's bash 3.2 (no mapfile, no ${var,,}, no associative arrays).

set -u -o pipefail

case "${1:-}" in
  --help|-h) sed -n '2,26p' "$0"; exit 0 ;;
  ''|watch) ;;
  *) echo "Usage: bash AI-TEMP/auto-improve-codex.sh [watch|--help]" >&2; exit 2 ;;
esac
[ "$#" -le 1 ] || { echo "auto-improve-codex: too many arguments" >&2; exit 2; }

# Keep the Mac awake while the loop runs: a sleeping laptop stops the 40-minute test gate halfway.
if [ "${1:-}" != watch ] && [ "$(uname -s)" = Darwin ] && [ -z "${AUTO_IMPROVE_AWAKE:-}" ] && command -v caffeinate >/dev/null 2>&1; then
  export AUTO_IMPROVE_AWAKE=1
  exec caffeinate -is "$BASH" "$0" "$@"
fi

REPO=$(git -C "$(dirname "$0")" rev-parse --show-toplevel) || exit 2
cd "$REPO" || exit 2

PLAN=AI-TEMP/improve-plan.md
STATE_DIR=AI-TEMP/auto-improve-codex
# Share the original loop's lock: both agents edit the same checkout.
LOCK=AI-TEMP/auto-improve/lock

MODEL=${MODEL:-gpt-6.1-sol}
EFFORT=${EFFORT:-high}
MAX_RUNS=${MAX_RUNS:-0}
MAX_FAILS=${MAX_FAILS:-2}
MAX_IDLE=${MAX_IDLE:-2}
LIMIT_WAIT=${LIMIT_WAIT:-1800}
MAX_LIMIT_WAITS=${MAX_LIMIT_WAITS:-0}
ALLOW_UNTRACKED=${ALLOW_UNTRACKED:-"todo.md auto_prompt.md"}

# Codex has no Claude --max-budget-usd equivalent; never silently ignore a cap.
if [ -n "${MAX_BUDGET_USD:-}" ]; then
  echo "auto-improve-codex: MAX_BUDGET_USD is not supported by Codex CLI" >&2
  exit 2
fi
for setting in MAX_RUNS MAX_FAILS MAX_IDLE LIMIT_WAIT MAX_LIMIT_WAITS; do
  value=${!setting}
  case "$value" in
    ''|*[!0-9]*) echo "auto-improve-codex: $setting must be a nonnegative integer" >&2; exit 2 ;;
  esac
  printf -v "$setting" '%d' "$((10#$value))"
done
[ "$MAX_FAILS" -gt 0 ] && [ "$MAX_IDLE" -gt 0 ] || {
  echo "auto-improve-codex: MAX_FAILS and MAX_IDLE must be positive" >&2; exit 2;
}
case "$EFFORT" in
  low|medium|high|xhigh|max) ;;
  *) echo "auto-improve-codex: unsupported EFFORT=$EFFORT" >&2; exit 2 ;;
esac

PROMPT='Before doing the next plan task, review the fresh GitHub Actions report attached below for https://github.com/alsatianco/peadb/actions. Inspect the failed jobs and logs with gh run view --repo alsatianco/peadb RUN_ID --log-failed (use --attempt RUN_ATTEMPT). Fix every outstanding failure that applies to this checkout, run the relevant checks, and include each coherent fix in the commits requested in your final result. If unpublished local commits already fix a remote failure, verify that with the failed log and local checks; record the evidence in AI-TEMP/improve-plan.md instead of making a duplicate fix. For failures on other branches, determine whether their cause applies here and record the reason if it does not. Do not treat running, cancelled or skipped checks as passed. If logs cannot be read or a relevant failure cannot be fixed, report blocked or failed and do not advance the plan. Then read AI-TEMP/improve-plan.md, its linked task plans and progress checkpoints, and git status/log. Resume unfinished work before selecting a new task, even when the working tree is clean. Reconcile any completion notes with actual commits and final validation evidence before advancing. Write a dated in-progress checkpoint with task identity, files, checks run and remaining work before implementing it. After finishing, update that file so the next session knows what to do.'

CLEANUP_PROMPT='CLEANUP SESSION: First read AI-TEMP/improve-plan.md (including linked task plans and checkpoints), git log, and all staged, unstaged and untracked changes. If changes belong to an unfinished current/next plan task, preserve the coherent implementation, helpers, tests and docs together and report resume_task so the loop continues that task before selecting another. This includes work with failing or unrun checks: finish it rather than stashing it merely because it is incomplete. Only unrelated changes should be committed after validation if worth keeping, or stashed with reasons otherwise. If mixed, request commits/stashes for unrelated paths only and report resume_task for the remaining task paths. Never mark a task complete just because files were stashed. Review the existing working-tree and index changes before any CI repair or plan task. Read git status, staged and unstaged diffs, untracked files and the plan notes to understand their purpose. For complete, meaningful changes, run the relevant validation and request commits with explicit file paths and concise one-line messages in your final result. For unrelated work that is incomplete, broken or uncertain, request stashes with explicit file paths and reasons. The Bash script executes all git mutations and records stash hashes and recovery commands in the plan; do not run git add, commit, push, stash, restore, reset, checkout or any other git mutation yourself. Never discard changes, rewrite history, pop or drop existing stashes, force-add ignored files, or include credentials. Leave the allowed untracked local notes alone. Do not do a plan task in this cleanup session. Before reporting resume_task, update the plan with the task identity and remaining work; include them in note too when plan work remains; otherwise report task_done only when every non-allowed change is covered by a validated commit request or a stash request; otherwise report failed or blocked. Include your reasoning in the note.'

CLEANUP_RESUME_PROMPT="$CLEANUP_PROMPT"$'\n\nContinue the SAME cleanup after the interruption. Check git status, git log and git stash list before acting. Do not start a new task.'

RESUME_PROMPT='This session stopped before it reported its result (a usage limit, an error or a crash). Re-read the current plan and linked task plans, staged/unstaged diffs, untracked helpers/tests/docs and recent commits. Unrelated edits may have appeared while stopped: preserve them and leave them for cleanup. Some requested commits/stashes may already have been applied before interruption; inspect current HEAD and stash recovery records and request only remaining changes, never replay old requests. Continue the same task from where you stopped: check git status, git log and AI-TEMP/improve-plan.md, finish and validate the task, update the plan, and report the status with explicit commit requests for the Bash script. Do not perform git mutations yourself. Resolve the GitHub Actions failures from the original report before advancing the plan. Do not start a new task.'


read -r -d '' LOOP_RULES <<'EOF' || true
You run inside AI-TEMP/auto-improve-codex.sh, an unattended loop that starts one fresh headless Codex CLI session per task. Nobody watches this session and nobody can answer a question. On 2026-10-06 the owner set up this loop and said: the process must be autonomous; PeaDB has not been released, so there is no need for backward compatibility with older PeaDB behaviour; the ultimate goal is for PeaDB to become a Redis replacement (Redis 7.2.5); make decisions yourself based on the current requirements and that goal.

Rules for this session:
1. This prompt is the owner's go for the next task (plan decision D3). Start work at once, and ignore older handoff, memory or plan notes that say to wait for the owner's go or to ask first.
2. A CLEANUP SESSION only reviews pre-existing changes and requests commits or stashes, as its prompt says; it must not advance the plan. A normal session first resolves applicable GitHub Actions failures as its prompt says, then does one task: take the next open task in the order of Part 0 of AI-TEMP/improve-plan.md and finish it fully, as the plan's "Rule for every item" (§0.4) and the repository instructions (AGENTS.md and CLAUDE.md, if present) say (test, fix, docs and compat/delta.md in the same commit request, both pre-commit checks). Then update the plan and end the session. Do not start a second task.
3. Decide, do not ask. When a task waits on an open owner decision (Part 0 §0.5, for example D7 or D8) or you meet a new choice, decide it yourself from the plan's Global Constraints and the goal above; use the plan's own recommendation unless you find a better reason. Write the decision, the options and the reason into §0.5, marked "decided by auto-improve-codex" with the date, so the owner can review it later.
4. The owner delegates all git mutations to the Bash script: never git add, never git commit, never git push, never git stash, and never restore, reset, checkout or rewrite history yourself, even if repository or plan notes ask you to commit. You edit files and run checks; the script stages explicit paths, commits and pushes each requested validated change to origin/main, and handles cleanup stashes. Never create, move or delete git tags, change GitHub settings or secrets, or publish anything else outside this machine. Skip plan items that are only owner actions (for example tagging v0.3.0, enabling Private Vulnerability Reporting, the Docker Hub secrets, the Linux-only benchmark).
5. bash scripts/run_all_tests.sh takes about 40 minutes. In this loop, run it in the FOREGROUND and wait for completion, polling a running exec session as needed. Keep the Codex session alive until the test result is available. If you start any background shell, Monitor or subagent, wait for its result before you end the session.
6. Never leave a broken state behind. If you cannot finish the task in this session (for example a check fails and you cannot fix it), do not request a commit of broken work: leave the changes in the working tree, write in the plan what is done, what is left and why, and report status "failed". The loop resumes this same task before selecting another. Cleanup stashes only unrelated work, with recovery notes. Never mark unfinished work complete just because it was stashed.
7. If automatic approval review denies an action, use a permitted alternative or report status "blocked" with the reason; nobody can answer an approval prompt. Never bypass a denial or weaken the permission settings.
8. Read prose, tables, deferred tasks and acceptance gates as well as checkboxes. Never infer completion from an absence of unchecked boxes. Do not weaken tests or call skipped, interrupted, unrun or failed checks successful. If the previous session reported all_done, independently audit its claim against the source and plan and resume authorized work if anything remains. Keep ignored plans and runtime logs local; never force-add them. Never display or commit credentials.
9. At the end, report: status "task_done" when you finished and validated the task, updated the plan and requested its commits; "all_done" when the plan has no open task left that an agent can do (only owner actions remain; list them in note); "blocked" when unfinished agent work requires permissions or external prerequisites and no independent authorized work remains; "failed" otherwise. In "task", name the task; in "commits", list objects with a concise one-line "message" and explicit repository-relative file "paths" covering only validated work (include both old and new paths for renames; no directories, globs or ignored untracked files). Cleanup may report "resume_task" to preserve and continue unfinished plan work; commit/stash requests in that result must cover only unrelated changes. In "stashes", list objects with "reason" and explicit file "paths" for cleanup only, or [] for normal sessions. Use [] for both on failed/blocked results. Never repeat a path across requests. All checks must pass on the final contents before requesting commits; the script will use those contents, without rerunning checks; in "note", say in one or two sentences what changed and what the next task is.
EOF

SCHEMA='{"type":"object","properties":{"status":{"type":"string","enum":["task_done","all_done","blocked","failed","resume_task"]},"task":{"type":"string"},"commits":{"type":"array","items":{"type":"object","properties":{"message":{"type":"string"},"paths":{"type":"array","items":{"type":"string"}}},"required":["message","paths"],"additionalProperties":false}},"stashes":{"type":"array","items":{"type":"object","properties":{"reason":{"type":"string"},"paths":{"type":"array","items":{"type":"string"}}},"required":["reason","paths"],"additionalProperties":false}},"note":{"type":"string"}},"required":["status","task","commits","stashes","note"],"additionalProperties":false}'


# Verified against codex-cli 0.162.1 and official documentation:
# https://learn.chatgpt.com/docs/non-interactive-mode
# https://learn.chatgpt.com/docs/sandboxing/auto-review
# --approve-for-me selects workspace-write + on-request + auto_review.
# Do not use the removed/deprecated --full-auto or Claude's --effort flags.
# Logs go to AI-TEMP/auto-improve-codex; there is no dollar-cost field in Codex events.

for tool in codex jq git; do
  command -v "$tool" >/dev/null 2>&1 || { echo "auto-improve-codex: $tool not found on PATH" >&2; exit 2; }
done
case "${1:-}" in
  watch)
    tail -n +1 -F "$STATE_DIR/latest/stream.jsonl" | jq -R -r --unbuffered '
      fromjson? |
      if .type == "item.completed" and .item.type == "agent_message" then .item.text
      elif .type == "item.started" and .item.type == "command_execution" then
        "  > command: \(.item.command | .[0:200])"
      elif .type == "item.completed" and .item.type == "command_execution" then
        "  < exit \(.item.exit_code): \((.item.aggregated_output // "") | .[0:500])"
      elif .type == "item.completed" and .item.type == "file_change" then
        .item.changes[]? | "  > \(.kind): \(.path)"
      elif .type == "turn.completed" then "== token usage: \(.usage | tojson)"
      elif .type == "error" or .type == "turn.failed" then
        "== error: \(.message // .error.message // (.error | tostring))"
      else empty end'
    exit $?
    ;;
  '') ;;
  *) echo "Usage: bash AI-TEMP/auto-improve-codex.sh [watch]" >&2; exit 2 ;;
esac

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$STATE_DIR/loop.log"
}

fmt_time() {
  date -r "$1" '+%Y-%m-%d %H:%M' 2>/dev/null || date -d "@$1" '+%Y-%m-%d %H:%M' 2>/dev/null || printf '%s' "$1"
}

# Prints tracked changes and untracked files, leaving out the files in ALLOW_UNTRACKED.
dirty_files() {
  local line path allowed skip
  git status --porcelain --untracked-files=all | while IFS= read -r line; do
    if [ "${line:0:3}" = '?? ' ]; then
      path=${line:3}
      skip=0
      for allowed in $ALLOW_UNTRACKED; do
        [ "$path" = "$allowed" ] && skip=1
      done
      [ "$skip" = 1 ] && continue
    fi
    printf '%s\n' "$line"
  done
}

# Branch guards also apply before and after cleanup.
check_repo() {
  local branch
  branch=$(git symbolic-ref --quiet --short HEAD) || branch="(detached HEAD)"
  if [ "$branch" != main ]; then
    log "STOP: the repo is on $branch, not main"
    return 1
  fi
  return 0
}

open_boxes() {
  grep -c -E '^[[:space:]]*[-*] \[ \]' "$PLAN" || true
}

# Use the owner's read token only in this process, without printing it.
github() (
  if [ -z "${GH_TOKEN:-}${GITHUB_TOKEN:-}" ] && [ -r "$HOME/.secrets/github_token" ]; then
    GH_TOKEN=$(cat "$HOME/.secrets/github_token") || exit 1
    export GH_TOKEN
  fi
  gh "$@"
)

# Fetch every page, then retain the latest completed run per workflow/branch/event.
# A newer pending run does not hide an unresolved failure; a later success does.
check_actions() {
  if ! github api --paginate 'repos/alsatianco/peadb/actions/runs?per_page=100' >"$RUN_DIR/actions-pages.json" 2>"$RUN_DIR/actions-stderr.txt"; then
    log "STOP: cannot check GitHub Actions; see $RUN_DIR/actions-stderr.txt"
    return 1
  fi
  if ! jq -s -e '
    if length == 0 or any(.[]; (.workflow_runs | type) != "array") then
      error("invalid Actions response")
    else
      [.[].workflow_runs[]] as $runs | $runs |
      {pending: map(select(.status != "completed") |
        {id, run_attempt, name, head_branch, head_sha, status, html_url}),
       latest_completed: (map(select(.status == "completed")) |
         group_by([.workflow_id, .head_branch, .event]) |
         map(sort_by(.run_number, .run_attempt) | last |
           {id, run_attempt, name, head_branch, head_sha, event, conclusion, html_url}))} |
      . + {failures: [$runs[] | select(.status == "completed" and
        .conclusion != "cancelled" and .conclusion != "skipped" and .conclusion != "neutral")] |
        group_by([.workflow_id, .head_branch, .event]) |
        map(sort_by(.run_number, .run_attempt) | last) | map(select(
        .conclusion == "failure" or .conclusion == "timed_out" or
        .conclusion == "startup_failure" or .conclusion == "action_required") |
          {id, run_attempt, name, head_branch, head_sha, event, conclusion, html_url})}
    end' "$RUN_DIR/actions-pages.json" >"$RUN_DIR/actions.json"; then
    log "STOP: invalid GitHub Actions response; see $RUN_DIR/actions-pages.json"
    return 1
  fi
  log "session $runs: GitHub Actions checked; $(jq '.failures | length' "$RUN_DIR/actions.json") outstanding failed workflow(s)"
}

# Parent exec flags must precede `resume`: resume has no --approve-for-me flag.
# Include the loop rules in every prompt, following ../kawasan/auto-improve.sh.
run_codex() {
  local stream=$1 prompt=$2 resume=${3:-}
  local args=(exec
    --model "$MODEL"
    -c "model_reasoning_effort=\"$EFFORT\""
    --approve-for-me
    -c 'approval_policy="on-request"'
    -c 'approvals_reviewer="auto_review"'
    --output-schema "$REPO/$RUN_DIR/schema.json"
    --output-last-message "$REPO/$RUN_DIR/result.json"
    --json)
  [ -n "$resume" ] && args+=(resume "$resume")
  args+=("$prompt"$'\n\n'"$LOOP_RULES")
  # An interrupted attempt must not reuse the previous attempt's final message.
  rm -f "$RUN_DIR/result.json"
  : >"$RUN_DIR/attempt-stderr.txt"
  codex "${args[@]}" </dev/null >>"$stream" 2>"$RUN_DIR/attempt-stderr.txt" &
  CODEX_PID=$!
  wait "$CODEX_PID"
  local rc=$?
  CODEX_PID=""
  cat "$RUN_DIR/attempt-stderr.txt" >>"$RUN_DIR/stderr.txt"
  return "$rc"
}

# Accept only a valid result from a completed turn, never a partial/intermediate message.
read_attempt() {
  local stream=$1 from=$2 sid completed
  tail -n +"$((from + 1))" "$stream" >"$RUN_DIR/attempt.jsonl"
  RESULT=""
  STATUS=""
  completed=$(jq -r -R 'fromjson? | select(.type == "turn.completed") | .type' "$RUN_DIR/attempt.jsonl" | tail -n 1)
  if [ -n "$completed" ] && [ "$rc" -eq 0 ] && [ -s "$RUN_DIR/result.json" ]; then
    RESULT=$(jq -c -e '
      select(type == "object" and (keys == ["commits", "note", "stashes", "status", "task"])) |
      select(.status == "task_done" or .status == "all_done" or .status == "blocked" or .status == "failed" or .status == "resume_task") |
      select((.task | type) == "string" and (.note | type) == "string") |
      def paths_ok: type == "array" and length > 0 and all(.[]; type == "string" and length > 0 and (contains("\u0000") | not));
      select((.commits | type) == "array" and all(.commits[];
        type == "object" and keys == ["message", "paths"] and
        (.message | type == "string" and test("[^[:space:]]") and (test("[\r\n\u0000]") | not)) and (.paths | paths_ok))) |
      select((.stashes | type) == "array" and all(.stashes[];
        type == "object" and keys == ["paths", "reason"] and
        (.reason | type == "string" and test("[^[:space:]]") and (contains("\u0000") | not)) and (.paths | paths_ok)))
      ' "$RUN_DIR/result.json" 2>/dev/null) || RESULT=""
    STATUS=$(printf '%s' "$RESULT" | jq -r '.status // empty' 2>/dev/null)
  fi
  sid=$(jq -r -R 'fromjson? | select(.type == "thread.started") | .thread_id // empty' "$RUN_DIR/attempt.jsonl" | head -n 1)
  [ -n "$sid" ] && SESSION_ID=$sid
  USAGE=$(jq -c -R 'fromjson? | select(.type == "turn.completed") | .usage // {}' "$RUN_DIR/attempt.jsonl" | tail -n 1)
}

# Codex exec errors do not expose Claude's rate_limit_event/resetsAt fields.
# Search only this attempt's error events and stderr, then use LIMIT_WAIT.
attempt_errors() {
  jq -r -R 'fromjson? | select(.type == "error" or .type == "turn.failed") |
    .message // .error.message // (.error | tostring)' "$RUN_DIR/attempt.jsonl"
  cat "$RUN_DIR/attempt-stderr.txt"
}

hit_limit() {
  [ -z "$STATUS" ] || return 1
  # Read the entire pipe: grep -q can SIGPIPE the producer under `pipefail`.
  attempt_errors | grep -Ei 'usage[ _-]*limit|session[ _-]*limit|weekly[ _-]*limit|rate[ _-]*limit|too many requests|quota[ _-]*(exceeded|exhausted|exhaustion)|hit your .*limit|(^|[^0-9])429([^0-9]|$)' >/dev/null
}

missing_session() {
  attempt_errors | grep -Ei '(session|thread).*(not found|does not exist|no longer exists)|no (stored |saved )?(session|thread).*found' >/dev/null
}

# Cleanup and plan sessions share retry/usage-limit handling, with phase-specific prompts.
execute_session() {
  local initial_prompt=$1 resume_prompt=$2 prompt=$1 retries limit_waits from now pause reconstructed=0
  SESSION_ID=${3:-}
  [ -z "$SESSION_ID" ] || prompt=$resume_prompt
  retries=0
  limit_waits=0
  while :; do
    from=$(wc -l <"$STREAM" | tr -d ' ')
    run_codex "$STREAM" "$prompt" "$SESSION_ID"
    rc=$?
    read_attempt "$STREAM" "$from"
    [ -n "$STATUS" ] && break

    if hit_limit; then
      limit_waits=$((limit_waits + 1))
      if [ "$MAX_LIMIT_WAITS" -gt 0 ] && [ "$limit_waits" -gt "$MAX_LIMIT_WAITS" ]; then
        log "STOP: $MAX_LIMIT_WAITS usage-limit waits in a row"
        return 1
      fi
      now=$(date +%s)
      pause=$LIMIT_WAIT
      log "session $runs: usage limit; waiting until $(fmt_time $((now + pause))), then resuming the same session"
      sleep "$pause" &
      CODEX_PID=$!
      wait "$CODEX_PID"
      CODEX_PID=""
    elif [ -n "$SESSION_ID" ] && [ "$reconstructed" -eq 0 ] && missing_session; then
      log "session $runs: saved session unavailable; reconstructing the same task from its plan and run records"
      reconstructed=1
      SESSION_ID=""
      initial_prompt=$resume_prompt
      retries=0
    elif [ "$retries" -lt 1 ] && [ -n "$SESSION_ID" ]; then
      retries=$((retries + 1))
      log "session $runs: ended without a status (exit $rc); resuming it once"
    else
      log "session $runs: ended without a status (exit $rc); see $RUN_DIR/stderr.txt and $STREAM"
      break
    fi
    # A session that stopped before it started has nothing to resume, so it gets the first prompt again.
    if [ -n "$SESSION_ID" ]; then prompt=$resume_prompt; else prompt=$initial_prompt; fi
  done
  return 0
}

# Publishing is owned by Bash, never by the model. Also retry unpublished commits
# at startup, so restarting after a push failure cannot silently abandon them.
push_commits() {
  check_repo || return 1
  log "pushing local main to origin/main"
  if ! GIT_TERMINAL_PROMPT=0 git push origin HEAD:refs/heads/main; then
    log "STOP: git push failed; local commits are preserved. Fix the remote/authentication issue and restart the script"
    return 1
  fi
}

# Git add without -u can stage a tracked file inside an ignored directory yet
# return failure. Update tracked paths separately; new paths were checked above.
stage_paths() {
  local path
  for path in "$@"; do
    if git --literal-pathspecs ls-files --error-unmatch -- "$path" >/dev/null 2>&1; then
      git --literal-pathspecs add -u -- "$path" || return 1
    else
      git --literal-pathspecs add -- "$path" || return 1
    fi
  done
}

# Execute only explicit, validated requests from a successful completed turn.
# --only prevents unrelated pre-existing staged changes leaking into a commit.
apply_result() {
  local expected_head=$1 phase=$2 path allowed request message stash_hash
  local paths=()
  check_repo || return 1
  [ "$(git rev-parse HEAD)" = "$expected_head" ] || {
    log "STOP: session changed git history; git mutations belong to the script"; return 1;
  }
  [ -s "$PLAN" ] || { log "STOP: session removed or emptied $PLAN"; return 1; }
  case "$STATUS" in task_done|all_done|resume_task) ;; *) return 0 ;; esac
  [ "$phase" != cleanup ] || [ "$STATUS" = task_done ] || [ "$STATUS" = resume_task ] || return 0
  if [ "$phase" != cleanup ] && [ "$STATUS" = resume_task ]; then
    log "STOP: resume_task is only valid in cleanup"; return 1
  fi
  if [ "$phase" != cleanup ] && [ "$(printf '%s' "$RESULT" | jq '.stashes | length')" -ne 0 ]; then
    log "STOP: only cleanup sessions may request stashes"; return 1
  fi
  if ! printf '%s' "$RESULT" | jq -e '
    [.commits[].paths[], .stashes[].paths[]] | length == (unique | length)' >/dev/null; then
    log "STOP: duplicate paths in git requests"; return 1
  fi
  # Validate every request before mutating the index or creating any commit.
  while IFS= read -r -d '' path; do
    case "$path" in
      /*|.|..|./*|../*|*/../*|*/..|*/./*|*/.|.git|.git/*)
        log "STOP: invalid requested git path: $path"; return 1 ;;
    esac
    if [ -d "$path" ] && [ ! -L "$path" ]; then
      log "STOP: git requests must name files, not directories: $path"; return 1
    fi
    if ! git --literal-pathspecs ls-files --error-unmatch -- "$path" >/dev/null 2>&1; then
      if git check-ignore -q -- "$path"; then
        log "STOP: refusing ignored untracked file: $path"; return 1
      fi
      for allowed in $ALLOW_UNTRACKED; do
        if [ "$path" = "$allowed" ]; then
          log "STOP: refusing allowed untracked local note: $path"; return 1
        fi
      done
    fi
    if [ -z "$(git --literal-pathspecs status --porcelain --untracked-files=all -- "$path")" ]; then
      log "STOP: requested path has no changes: $path"; return 1
    fi
  done < <(printf '%s' "$RESULT" | jq -j '.commits[].paths[], .stashes[].paths[] | ., "\u0000"')
  while IFS= read -r request; do
    paths=()
    while IFS= read -r -d '' path; do paths+=("$path"); done < <(printf '%s' "$request" | jq -j '.paths[] | ., "\u0000"')
    message=$(printf '%s' "$request" | jq -r '.message')
    if ! git --literal-pathspecs diff --check HEAD -- "${paths[@]}" ||
       ! stage_paths "${paths[@]}" ||
       ! git --literal-pathspecs commit --only -m "$message" -- "${paths[@]}"; then
      log "STOP: could not commit requested changes; working tree and index preserved"; return 1
    fi
    push_commits || return 1
  done < <(printf '%s' "$RESULT" | jq -c '.commits[]')
  while IFS= read -r request; do
    paths=()
    while IFS= read -r -d '' path; do paths+=("$path"); done < <(printf '%s' "$request" | jq -j '.paths[] | ., "\u0000"')
    message=$(printf '%s' "$request" | jq -r '.reason')
    if ! git --literal-pathspecs stash push --include-untracked -m "auto-improve-codex: $message" -- "${paths[@]}"; then
      log "STOP: could not stash requested changes"; return 1
    fi
    stash_hash=$(git rev-parse refs/stash) || return 1
    # shellcheck disable=SC2016 # Markdown recovery command is literal text.
    printf '\n- auto-improve-codex cleanup: %s. Stash `%s`; recover with `git stash apply %s`.\n' "$message" "$stash_hash" "$stash_hash" >>"$PLAN"
    printf '%s\n' "$stash_hash" >>"$RUN_DIR/stashes.txt"
  done < <(printf '%s' "$RESULT" | jq -c '.stashes[]')
}

# Persist before invoking Codex, so SIGINT, a crash or an interrupted git request
# can all restart the same phase. Never replay result.json after a restart.
activate_session() {
  rm -f "$RUN_DIR/finished"
  printf '%s\n' "$SCHEMA" >"$RUN_DIR/schema.json" || return 1
  printf '%s\n' "$LOOP_RULES" >"$RUN_DIR/instructions.txt" || return 1
  printf '%s\n' "${RUN_DIR##*/}" >"$STATE_DIR/active.tmp" &&
    mv "$STATE_DIR/active.tmp" "$STATE_DIR/active"
}

prepare_session() {
  local phase=$1
  STREAM=$RUN_DIR/stream.jsonl
  : >"$STREAM"
  git rev-parse HEAD >"$RUN_DIR/head-before.txt" || return 1
  printf '%s\n' "$phase" >"$RUN_DIR/phase.txt"
  printf '%s\n' "$SCHEMA" >"$RUN_DIR/schema.json"
  printf '%s\n' "$LOOP_RULES" >"$RUN_DIR/instructions.txt"
  cp "$PLAN" "$RUN_DIR/plan-before.md" || return 1
  activate_session || return 1
  ln -sfn "${RUN_DIR##*/}" "$STATE_DIR/latest"
}

finish_session() {
  : >"$RUN_DIR/finished"
  rm -f "$STATE_DIR/active"
}

saved_session_id() {
  jq -r -R 'fromjson? | select(.type == "thread.started") | .thread_id // empty' "$RUN_DIR/stream.jsonl" | tail -n 1
}

load_recovery() {
  local name original_head
  RECOVERY_DIR=""
  RECOVERY_PHASE=""
  if [ -f "$STATE_DIR/active" ]; then
    name=$(cat "$STATE_DIR/active")
  else
    # Upgrade from the original runner: its latest run has a transcript but no
    # summary row if it stopped during an attempt or a usage-limit wait.
    name=$(readlink "$STATE_DIR/latest" 2>/dev/null) || return 0
    [ ! -f "$STATE_DIR/$name/finished" ] || return 0
    if [ -f "$STATE_DIR/summary.tsv" ] && grep -F "$(printf '\t%s\t' "$name")" "$STATE_DIR/summary.tsv" >/dev/null; then
      return 0
    fi
    [ ! -f "$STATE_DIR/$name/plan-after.md" ] || return 0
  fi
  case "$name" in
    run-*|cleanup-*) ;;
    *) log "STOP: invalid recovery record"; return 1 ;;
  esac
  case "$name" in */*) log "STOP: invalid recovery directory"; return 1 ;; esac
  RECOVERY_DIR=$STATE_DIR/$name
  [ -d "$RECOVERY_DIR" ] && [ -f "$RECOVERY_DIR/stream.jsonl" ] && [ -s "$RECOVERY_DIR/plan-before.md" ] || {
    log "STOP: incomplete recovery record: $RECOVERY_DIR"; return 1;
  }
  if [ -f "$RECOVERY_DIR/finished" ]; then
    rm -f "$STATE_DIR/active"
    RECOVERY_DIR=""
    return 0
  fi
  if [ -f "$RECOVERY_DIR/head-before.txt" ]; then
    original_head=$(cat "$RECOVERY_DIR/head-before.txt")
    git merge-base --is-ancestor "$original_head" HEAD || {
      log "STOP: interrupted session's history is no longer an ancestor of HEAD"; return 1;
    }
  else
    git rev-parse HEAD >"$RECOVERY_DIR/head-before.txt" || return 1
  fi
  case "$name" in cleanup-*) RECOVERY_PHASE=cleanup ;; *) RECOVERY_PHASE=task ;; esac
  log "recovery: continuing interrupted $RECOVERY_PHASE in $RECOVERY_DIR before cleanup or new plan work"
}

# Local variables keep cleanup results separate from the preceding plan session.
reconcile_tree() {
  local dirty cleanup_head stash_before stash_after RUN_DIR RUN_NAME STREAM
  local SESSION_ID STATUS RESULT USAGE rc resume_id=""
  TREE_RECONCILED=0
  TREE_RESUME=0
  TREE_TASK_NOTE=""
  check_repo || return 1
  dirty=$(dirty_files) || { log "STOP: cannot inspect working-tree changes"; return 1; }
  [ -n "$dirty" ] || [ "$RECOVERY_PHASE" = cleanup ] || return 0
  cleanup_head=$(git rev-parse HEAD)
  stash_before=$(git rev-parse --verify -q refs/stash || true)
  if [ "$RECOVERY_PHASE" = cleanup ]; then
    RUN_DIR=$RECOVERY_DIR
    STREAM=$RUN_DIR/stream.jsonl
    resume_id=$(saved_session_id)
    activate_session || return 1
    RECOVERY_DIR=""
    RECOVERY_PHASE=""
  else
    RUN_DIR=$(mktemp -d "$STATE_DIR/cleanup-$(date '+%Y%m%d-%H%M%S').XXXXXX") || return 1
    prepare_session cleanup || return 1
  fi
  RUN_NAME=${RUN_DIR##*/}
  printf '%s\n' "$dirty" >"$RUN_DIR/dirty-before.txt"
  log "cleanup ($RUN_NAME): analysing working-tree changes for a meaningful commit or stash"
  printf '%s\n' "$dirty" | tee -a "$STATE_DIR/loop.log"
  execute_session "$CLEANUP_PROMPT"$'\n\nAllowed untracked notes: '"$ALLOW_UNTRACKED" "$CLEANUP_RESUME_PROMPT"$'\n\nAllowed untracked notes: '"$ALLOW_UNTRACKED" "$resume_id" || return 1
  check_repo || return 1
  git merge-base --is-ancestor "$cleanup_head" HEAD || {
    log "STOP: cleanup rewrote history"; return 1;
  }
  [ -s "$PLAN" ] || { log "STOP: cleanup removed or emptied $PLAN"; return 1; }
  apply_result "$cleanup_head" cleanup || return 1
  cp "$PLAN" "$RUN_DIR/plan-after.md"
  git log --oneline "$cleanup_head..HEAD" >"$RUN_DIR/commits.txt"
  stash_after=$(git rev-parse --verify -q refs/stash || true)
  printf '%s\n' "$stash_after" >"$RUN_DIR/stash-after.txt"
  log "cleanup: status=${STATUS:-none}; $(printf '%s' "$RESULT" | jq -r '.note // empty')"
  dirty=$(dirty_files) || { log "STOP: cannot inspect working-tree changes"; return 1; }
  if [ "$STATUS" = resume_task ]; then
    TREE_RESUME=1
    TREE_TASK_NOTE=$(printf '%s' "$RESULT" | jq -r '[.task, .note] | join(": ")')
    log "cleanup: unfinished plan task preserved; resume it before selecting new work"
  elif [ "$STATUS" != task_done ] || [ -n "$dirty" ]; then
    log "STOP: cleanup did not commit or stash all changes; see $RUN_DIR"
    return 1
  elif [ "$cleanup_head" = "$(git rev-parse HEAD)" ] && [ "$stash_before" = "$stash_after" ] && [ -z "$resume_id" ]; then
    log "STOP: cleanup left no commit or new stash; see $RUN_DIR"
    return 1
  fi
  finish_session
  TREE_RECONCILED=1
  [ -n "$USAGE" ] || USAGE='{}'
  printf '%s\n' "$USAGE" >"$RUN_DIR/usage.json"
  log "cleanup: ready to continue; commits in $RUN_DIR/commits.txt, stash=${stash_after:-none}"
}

# Run Codex as a background job so the shell handles Ctrl-C promptly.
# Stop its child commands as well; a killed test must not keep running in this checkout.
stop_children() {
  local parent=$1 child
  for child in $(pgrep -P "$parent" 2>/dev/null); do
    stop_children "$child"
    kill -TERM "$child" 2>/dev/null || true
  done
}
CODEX_PID=""
on_signal() {
  log "STOP: got a signal; ending the running session"
  if [ -n "$CODEX_PID" ]; then
    stop_children "$CODEX_PID"
    kill -TERM "$CODEX_PID" 2>/dev/null
    wait "$CODEX_PID" 2>/dev/null
  fi
  exit 130
}

command -v gh >/dev/null 2>&1 || { echo "auto-improve-codex: gh not found on PATH" >&2; exit 2; }

[ -s "$PLAN" ] || { echo "auto-improve-codex: $PLAN missing or empty" >&2; exit 2; }
mkdir -p "$STATE_DIR" "$(dirname "$LOCK")" || exit 2
git check-ignore -q "$STATE_DIR" || { echo "auto-improve-codex: $STATE_DIR is not gitignored" >&2; exit 2; }

if ! mkdir "$LOCK" 2>/dev/null; then
  echo "auto-improve-codex: loop lock exists at $REPO/$LOCK; check its pid before removing a stale lock" >&2
  exit 2
fi
echo $$ >"$LOCK/pid"
trap 'rm -f "$LOCK/pid"; rmdir "$LOCK"' EXIT
trap on_signal INT TERM

[ -f "$STATE_DIR/summary.tsv" ] || printf 'time\trun\tstatus\tcommits\tinput_tokens\tcached_input_tokens\toutput_tokens\tsession_id\ttask\n' >"$STATE_DIR/summary.tsv"

log "start: $(codex --version 2>/dev/null), model=$MODEL effort=$EFFORT permissions=approve-for-me, HEAD $(git rev-parse --short HEAD), $(open_boxes) open boxes in the plan"
check_repo || exit 1
cli_help=$(codex exec --help) || exit 2
[[ "$cli_help" == *--approve-for-me* ]] || {
  log "STOP: this Codex CLI lacks --approve-for-me; update it first"
  exit 2
}

load_recovery || exit 1
push_commits || exit 1

runs=0
fails=0
idle=0
all_done_streak=0

while :; do
  if [ "$MAX_RUNS" -gt 0 ] && [ "$runs" -ge "$MAX_RUNS" ]; then
    log "STOP: MAX_RUNS=$MAX_RUNS sessions done; completion has not been audited twice"
    exit 2
  fi
  TREE_RESUME=0
  resume_id=""
  if [ "$RECOVERY_PHASE" != task ]; then
    reconcile_tree || exit 1
  fi

  runs=$((runs + 1))
  if [ "$RECOVERY_PHASE" = task ]; then
    RUN_DIR=$RECOVERY_DIR
    STREAM=$RUN_DIR/stream.jsonl
    resume_id=$(saved_session_id)
    activate_session || exit 1
    recovering=1
  else
    RUN_DIR=$(mktemp -d "$STATE_DIR/run-$(date '+%Y%m%d-%H%M%S')-$runs.XXXXXX") || exit 2
    prepare_session task || exit 1
    recovering=$TREE_RESUME
    printf '%s\n' "${TREE_TASK_NOTE:-}" >"$RUN_DIR/recovery-context.txt"
  fi
  RUN_NAME=${RUN_DIR##*/}
  ln -sfn "$RUN_NAME" "$STATE_DIR/latest"
  # New baseline after restart: earlier commits may already have been applied.
  head_before=$(git rev-parse HEAD)
  log "session $runs ($RUN_NAME): starting; watch it with: bash AI-TEMP/auto-improve-codex.sh watch"

  check_actions || exit 2
  report=$'\n\nFresh GitHub Actions report (also saved in '"$RUN_DIR/actions.json"$'):\n'"$(cat "$RUN_DIR/actions.json")"$'\n\nFor read-only GitHub commands, use gh with the existing GH_TOKEN/GITHUB_TOKEN or process-local GH_TOKEN loaded from ~/.secrets/github_token. Never print token contents.'
  prompt="$PROMPT$report"
  resume_prompt="$RESUME_PROMPT$report"$'\n\nOriginal plan snapshot: '"$RUN_DIR/plan-before.md. Current HEAD: $head_before. Check current contents and validation evidence before requesting any remaining commits. Never reuse an old result.json."
  if [ -s "$RUN_DIR/recovery-context.txt" ]; then
    resume_prompt="$resume_prompt"$'\n\nRecovery review: '"$(cat "$RUN_DIR/recovery-context.txt")"
  fi
  if [ "$recovering" -eq 1 ]; then
    prompt="$resume_prompt"
  elif [ "$all_done_streak" -gt 0 ]; then
    prompt="$prompt"$'\n\nThe previous session reported all_done. This is the independent completion audit.'
  fi
  execute_session "$prompt" "$resume_prompt" "$resume_id" || exit 1

  [ -s "$PLAN" ] || { log "STOP: session removed or emptied $PLAN"; exit 1; }
  apply_result "$head_before" task || exit 1
  cp "$PLAN" "$RUN_DIR/plan-after.md"
  head_after=$(git rev-parse HEAD)
  if ! git merge-base --is-ancestor "$head_before" "$head_after"; then
    log "STOP: history was rewritten (HEAD was $head_before, is now $head_after)"
    exit 1
  fi
  new_commits=$(git rev-list --count "$head_before..$head_after")
  git log --oneline "$head_before..$head_after" >"$RUN_DIR/commits.txt"
  [ -n "$USAGE" ] || USAGE='{}'
  printf '%s\n' "$USAGE" >"$RUN_DIR/usage.json"
  input_tokens=$(printf '%s' "$USAGE" | jq -r '.input_tokens // 0' 2>/dev/null)
  cached_tokens=$(printf '%s' "$USAGE" | jq -r '.cached_input_tokens // 0' 2>/dev/null)
  output_tokens=$(printf '%s' "$USAGE" | jq -r '.output_tokens // 0' 2>/dev/null)
  task=$(printf '%s' "$RESULT" | jq -r '.task // "" | gsub("[\t\n]"; " ")' 2>/dev/null)
  note=$(printf '%s' "$RESULT" | jq -r '.note // "" | gsub("[\t\n]"; " ")' 2>/dev/null)
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$RUN_NAME" "${STATUS:-none}" "$new_commits" "${input_tokens:-0}" "${cached_tokens:-0}" "${output_tokens:-0}" "${SESSION_ID:-?}" "$task" >>"$STATE_DIR/summary.tsv"
  log "session $runs: status=${STATUS:-none}, $new_commits new commit(s), tokens in=${input_tokens:-0} cached=${cached_tokens:-0} out=${output_tokens:-0}, session ${SESSION_ID:-?}"
  [ -n "$task" ] && log "  task: $task"
  [ -n "$note" ] && log "  note: $note"
  if [ "$STATUS" = task_done ] && cmp -s "$RUN_DIR/plan-before.md" "$PLAN"; then
    log "  warning: the session did not change $PLAN"
  fi

  if [ "$STATUS" = task_done ] || [ "$STATUS" = all_done ]; then
    finish_session
    RECOVERY_DIR=""
    RECOVERY_PHASE=""
    reconcile_tree || exit 1
    if [ "$TREE_RESUME" -eq 1 ]; then
      RECOVERY_DIR=$RUN_DIR
      RECOVERY_PHASE=task
      printf '%s\n' "$TREE_TASK_NOTE" >"$RUN_DIR/recovery-context.txt"
      activate_session || exit 1
    fi
  else
    RECOVERY_DIR=$RUN_DIR
    RECOVERY_PHASE=task
  fi

  case "$STATUS" in
    task_done)
      fails=0
      all_done_streak=0
      if [ "$new_commits" -eq 0 ] || cmp -s "$RUN_DIR/plan-before.md" "$PLAN"; then
        idle=$((idle + 1))
        if [ "$idle" -ge "$MAX_IDLE" ]; then
          log "STOP: $idle sessions in a row said task_done without both a commit and an updated plan"
          exit 1
        fi
      else
        idle=0
      fi
      ;;
    all_done)
      if [ "$recovering" -eq 1 ]; then
        all_done_streak=0
        log "session $runs: recovered work is complete; require two fresh completion audits"
        continue
      fi
      if [ "$TREE_RECONCILED" -eq 1 ]; then
        all_done_streak=0
        log "session $runs: cleanup changed the checkout; require fresh completion audits"
        continue
      fi
      fails=0
      idle=0
      all_done_streak=$((all_done_streak + 1))
      if [ "$all_done_streak" -ge 2 ]; then
        log "DONE: two fresh sessions in a row found no task an agent can do; $(open_boxes) open boxes remain for the owner"
        exit 0
      fi
      log "session $runs: says no agent task is left; one more fresh session checks that"
      ;;
    blocked)
      log "STOP: unfinished work requires permissions or external prerequisites; see $RUN_DIR/result.json"
      exit 2
      ;;
    *)
      all_done_streak=0
      fails=$((fails + 1))
      if [ "$fails" -ge "$MAX_FAILS" ]; then
        log "STOP: $fails sessions in a row failed"
        exit 1
      fi
      ;;
  esac
done

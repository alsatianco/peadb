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
#   MAX_LIMIT_WAITS=48         stop after this many usage-limit waits in a row
#   ALLOW_UNTRACKED="todo.md auto_prompt.md"  untracked files (space-separated) that do not count as a dirty working tree
#
# Each session keeps a full record in AI-TEMP/auto-improve-codex/run-*/ (Codex JSONL transcript, structured result, token usage, stderr, the plan before and after, the new commits), and one line per session goes to AI-TEMP/auto-improve-codex/summary.tsv.
#
# The loop stops when two fresh sessions agree no agent task remains, work is blocked, changes are left uncommitted, the repo leaves main, history is rewritten, or sessions repeatedly fail or stall.
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
MAX_LIMIT_WAITS=${MAX_LIMIT_WAITS:-48}
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

PROMPT='please read AI-TEMP/improve-plan.md and do the next task. after done, pls update that file AI-TEMP/improve-plan.md so we know what to do next in the next session'

RESUME_PROMPT='This session stopped before it reported its result (a usage limit, an error or a crash). Continue the same task from where you stopped: check git status, git log and AI-TEMP/improve-plan.md, finish the task, commit it, update the plan, and report the status. Do not start a new task.'

read -r -d '' LOOP_RULES <<'EOF' || true
You run inside AI-TEMP/auto-improve-codex.sh, an unattended loop that starts one fresh headless Codex CLI session per task. Nobody watches this session and nobody can answer a question. On 2026-10-06 the owner set up this loop and said: the process must be autonomous; PeaDB has not been released, so there is no need for backward compatibility with older PeaDB behaviour; the ultimate goal is for PeaDB to become a Redis replacement (Redis 7.2.5); make decisions yourself based on the current requirements and that goal.

Rules for this session:
1. This prompt is the owner's go for the next task (plan decision D3). Start work at once, and ignore older handoff, memory or plan notes that say to wait for the owner's go or to ask first.
2. Do one task: take the next open task in the order of Part 0 of AI-TEMP/improve-plan.md and finish it fully, as the plan's "Rule for every item" (§0.4) and the repository instructions (AGENTS.md and CLAUDE.md, if present) say (test, fix, docs and compat/delta.md in the same commit, both pre-commit checks, commit). Then update the plan and end the session. Do not start a second task.
3. Decide, do not ask. When a task waits on an open owner decision (Part 0 §0.5, for example D7 or D8) or you meet a new choice, decide it yourself from the plan's Global Constraints and the goal above; use the plan's own recommendation unless you find a better reason. Write the decision, the options and the reason into §0.5, marked "decided by auto-improve-codex" with the date, so the owner can review it later.
4. Some actions stay with the owner: never git push, never create, move or delete git tags, never change GitHub settings or secrets, never publish anything outside this machine. Skip plan items that are only such actions (for example pushing, tagging v0.3.0, enabling Private Vulnerability Reporting, the Docker Hub secrets, the Linux-only benchmark) and do not count them as work left for an agent.
5. bash scripts/run_all_tests.sh takes about 40 minutes. In this loop, run it in the FOREGROUND and wait for completion, polling a running exec session as needed. Keep the Codex session alive until the test result is available. If you start any background shell, Monitor or subagent, wait for its result before you end the session.
6. Never leave a broken state behind. If you cannot finish the task in this session (for example a check fails and you cannot fix it), do not commit the broken work: leave the changes in the working tree, write in the plan what is done, what is left and why, and report status "failed". The loop then stops so the owner can look.
7. If automatic approval review denies an action, use a permitted alternative or report status "blocked" with the reason; nobody can answer an approval prompt. Never bypass a denial or weaken the permission settings.
8. Read prose, tables, deferred tasks and acceptance gates as well as checkboxes. Never infer completion from an absence of unchecked boxes. Do not weaken tests or call skipped, interrupted, unrun or failed checks successful. If the previous session reported all_done, independently audit its claim against the source and plan and resume authorized work if anything remains. Keep ignored plans and runtime logs local; never force-add them. Never display or commit credentials.
9. At the end, report: status "task_done" when you committed the task and updated the plan; "all_done" when the plan has no open task left that an agent can do (only owner actions remain; list them in note); "blocked" when unfinished agent work requires permissions or external prerequisites and no independent authorized work remains; "failed" otherwise. In "task", name the task; in "commits", list the commit hashes you made; in "note", say in one or two sentences what changed and what the next task is.
EOF

SCHEMA='{"type":"object","properties":{"status":{"type":"string","enum":["task_done","all_done","blocked","failed"]},"task":{"type":"string"},"commits":{"type":"array","items":{"type":"string"}},"note":{"type":"string"}},"required":["status","task","commits","note"],"additionalProperties":false}'

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

# Fails (and says why) when the repo is not on main or has changes that a session left behind.
check_repo() {
  local branch dirty
  branch=$(git symbolic-ref --quiet --short HEAD) || branch="(detached HEAD)"
  if [ "$branch" != main ]; then
    log "STOP: the repo is on $branch, not main"
    return 1
  fi
  dirty=$(dirty_files)
  if [ -n "$dirty" ]; then
    log "STOP: the working tree has changes; look at them (and at the plan's notes) before you start the loop again:"
    printf '%s\n' "$dirty" | tee -a "$STATE_DIR/loop.log"
    return 1
  fi
}

open_boxes() {
  grep -c -E '^[[:space:]]*[-*] \[ \]' "$PLAN" || true
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
      select(type == "object" and (keys == ["commits", "note", "status", "task"])) |
      select(.status == "task_done" or .status == "all_done" or .status == "blocked" or .status == "failed") |
      select((.task | type) == "string" and (.note | type) == "string") |
      select((.commits | type) == "array" and all(.commits[]; type == "string"))
      ' "$RUN_DIR/result.json" 2>/dev/null) || RESULT=""
    STATUS=$(printf '%s' "$RESULT" | jq -r '.status // empty' 2>/dev/null)
  fi
  sid=$(jq -r -R 'fromjson? | select(.type == "thread.started") | .thread_id // empty' "$RUN_DIR/attempt.jsonl" | head -n 1)
  [ -n "$sid" ] && SESSION_ID=$sid
  USAGE=$(jq -c -R 'fromjson? | select(.type == "turn.completed") | .usage // {}' "$RUN_DIR/attempt.jsonl" | tail -n 1)
}

# Codex exec errors do not expose Claude's rate_limit_event/resetsAt fields.
# Search only this attempt's error events and stderr, then use LIMIT_WAIT.
hit_limit() {
  [ -z "$STATUS" ] || return 1
  {
    jq -r -R 'fromjson? | select(.type == "error" or .type == "turn.failed") |
      .message // .error.message // (.error | tostring)' "$RUN_DIR/attempt.jsonl"
    cat "$RUN_DIR/attempt-stderr.txt"
  # Read the entire pipe: grep -q can SIGPIPE the producer under `pipefail`.
  } | grep -Ei 'usage limit|session limit|weekly limit|rate.?limit|too many requests|hit your .*limit|(^|[^0-9])429([^0-9]|$)' >/dev/null
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

runs=0
fails=0
idle=0
all_done_streak=0

while :; do
  if [ "$MAX_RUNS" -gt 0 ] && [ "$runs" -ge "$MAX_RUNS" ]; then
    log "STOP: MAX_RUNS=$MAX_RUNS sessions done; completion has not been audited twice"
    exit 2
  fi
  check_repo || exit 1

  runs=$((runs + 1))
  RUN_DIR=$(mktemp -d "$STATE_DIR/run-$(date '+%Y%m%d-%H%M%S')-$runs.XXXXXX") || exit 2
  RUN_NAME=${RUN_DIR##*/}
  ln -sfn "$RUN_NAME" "$STATE_DIR/latest"
  STREAM=$RUN_DIR/stream.jsonl
  : >"$STREAM"
  printf '%s\n' "$SCHEMA" >"$RUN_DIR/schema.json"
  printf '%s\n' "$LOOP_RULES" >"$RUN_DIR/instructions.txt"
  cp "$PLAN" "$RUN_DIR/plan-before.md"
  head_before=$(git rev-parse HEAD)
  log "session $runs ($RUN_NAME): starting; watch it with: bash AI-TEMP/auto-improve-codex.sh watch"

  SESSION_ID=""
  prompt=$PROMPT
  if [ "$all_done_streak" -gt 0 ]; then
    prompt="$PROMPT"$'\n\nThe previous session reported all_done. This is the independent completion audit.'
  fi
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
      if [ "$limit_waits" -gt "$MAX_LIMIT_WAITS" ]; then
        log "STOP: $MAX_LIMIT_WAITS usage-limit waits in a row"
        exit 1
      fi
      now=$(date +%s)
      pause=$LIMIT_WAIT
      log "session $runs: usage limit; waiting until $(fmt_time $((now + pause))), then resuming the same session"
      sleep "$pause" &
      CODEX_PID=$!
      wait "$CODEX_PID"
      CODEX_PID=""
    elif [ "$retries" -lt 1 ] && [ -n "$SESSION_ID" ]; then
      retries=$((retries + 1))
      log "session $runs: ended without a status (exit $rc); resuming it once"
    else
      log "session $runs: ended without a status (exit $rc); see $RUN_DIR/stderr.txt and $STREAM"
      break
    fi
    # A session that stopped before it started has nothing to resume, so it gets the first prompt again.
    if [ -n "$SESSION_ID" ]; then prompt=$RESUME_PROMPT; else prompt=$PROMPT; fi
  done

  [ -s "$PLAN" ] || { log "STOP: session removed or emptied $PLAN"; exit 1; }
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

  check_repo || exit 1

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

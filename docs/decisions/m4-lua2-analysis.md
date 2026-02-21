# M4.LUA2 Analysis (Atomic execution + replication-oriented determinism)

Date: 2026-02-17

## Scope implemented now

Given current project state (no replication engine yet), this milestone is implemented as:

1. Script execution remains single-command atomic with respect to server command dispatch.
2. Script behavior is deterministic for supported script subset.
3. Script side-effects are applied in-order as a single script operation.
4. EVAL and EVALSHA share identical execution semantics for the same script body.

Replication-stream correctness is deferred to M6 replication tasks.

## Supported script subset for this milestone

- `return ARGV[1]`
- `return KEYS[1]`
- `return <integer>`
- `return '<string-literal>'`
- `redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])`
- `redis.call('INCR', KEYS[1]); return redis.call('GET', KEYS[1])`

This is intentionally narrow, but deterministic and testable.

## Validation approach

- Add `tests/integration/test_m4_lua2_atomic.py` first.
- Verify:
  - side-effect path works for both EVAL and EVALSHA.
  - repeated script invocations produce deterministic outputs.
  - script writes are visible immediately after execution.

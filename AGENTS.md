# Working agreements

- After every completed, meaningful change, make a commit with a concise, one-line message.
- Do not run `git push` unless the user explicitly asks for it.
- For authorized GitHub operations, use existing CLI authentication or a process-local `GH_TOKEN` with the required scope; never display or commit credentials.

# Lessons from CI and unfinished work

- Inspect staged, unstaged and untracked changes before starting. Existing edits may be useful, tested work from an earlier session. Review their purpose and evidence before recommending a stash or removal. Keep a coherent implementation, new helper files, tests and documentation together; keep personal prompts and old planning notes separate.
- Validate the code actually being delivered. Tests of a committed snapshot do not also validate pending edits excluded from that snapshot. Read final test summaries and exit codes before reporting success; an unfinished, skipped, interrupted or timed-out check is not a pass. Record successful reruns without hiding the original failure or claiming an aggregate run exited successfully when it did not.
- Reproduce a failure and check the relevant specification before changing an assertion. For Redis parity, use the project's pinned Redis reference on the same platform. Compiler argument evaluation order, allocator behavior, socket error timing and floating-point precision can differ across platforms. In allocation tests, sample counters before constructing diagnostic strings and avoid allocating literal diagnostics.
- Server readiness means a successful protocol round trip, using authentication when required. A listening socket or fixed sleep does not prove that AOF/RDB loading has finished. Synchronize background work using observable state and bounded waits.
- Give sanitizer builds appropriate execution budgets. Keep functional allocator, eviction and persistence assertions active; evaluate production RSS limits in Release builds because sanitizer overhead changes process memory usage. Diagnose the reason for a failure before adjusting a bound or expectation.
- Test harnesses own their processes and temporary data. Clean up by owned PID or verified process group, tolerate children exiting during inspection, and reap children after stopping them. Avoid global process-name kills that can stop unrelated servers or development tools.
- Distinguish process exit from PID disappearance: `kill(pid, 0)` succeeds for zombies. Observe non-child liveness with the shared zombie-aware helper and leave reaping to the owner. Changes to supervisors or orphan adoption need tests through the actual runner on Linux; a macOS pass does not exercise Linux subreaper behavior.
- Check test prerequisites and pinned tool versions before a long run. Run adopted upstream Redis suites in native mode and check their ledger ratchet. Keep failure logs useful. For import/export tools, check server replies and report errors instead of printing success after refused operations or copying no data unexpectedly.

# Room for development

These notes prevent known correctness, portability and workflow mistakes; they are not a feature freeze. New features, experiments, refactors, version upgrades and intentional behavior changes are welcome. Update the relevant tests, documentation and compatibility expectations when making such a change, and describe experimental limits and verification honestly. Apply the lessons to the current design rather than treating today's implementation or test expectations as permanent restrictions.

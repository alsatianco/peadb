---
name: clean-code
description: "Remove unused code throughout a repository, across all languages and frameworks. Use for dead-code elimination, unused files, functions, imports, dependencies, obsolete configuration, and redundant implementations. Perform and verify the cleanup, rather than stopping at an audit."
---

# Clean Code

Remove all confirmed unused code in the requested scope while preserving used
behavior. Default to the current repository and every first-party language;
infer suitable tools from its actual stack. This is an implementation task,
not a request for a style review or a list of possible deletions. Respect an
explicit request for an audit only, narrower paths, or other exclusions.

## Establish what is actually used

- Read applicable repository instructions and inspect staged, unstaged and
  untracked changes. Preserve existing work; filenames such as `temp`, `old`
  or `legacy` do not establish that a file is disposable.
- Inventory tracked source and identify entry points, build targets, package
  manifests, imports, test discovery, workflows, generators and deployment
  configuration. Separate shipped code, tests, tooling, generated outputs and
  vendored dependencies. Do not edit third-party internals as routine cleanup.
- Use compiler diagnostics, language-aware analyzers, dependency tools and
  reference searches appropriate to each language. Use installed tools or an
  isolated temporary environment; avoid adding project dependencies merely
  to perform the audit.
- Trace each candidate through its callers and discovery mechanisms. A text
  search with no matches, an analyzer warning, or missing test coverage is a
  lead, not sufficient evidence by itself. Check dynamic imports, reflection,
  registries, framework conventions, plugin exports, callbacks, templates,
  generated references, migrations and platform/build variants as relevant.
  A published API can have consumers outside the checkout.

## Remove the dead code

- Act on confirmed findings: delete unused files, functions, classes, methods,
  imports, variables, unreachable branches, obsolete compatibility paths,
  abandoned stubs and redundant implementations. Remove code made dead by
  those deletions as well. Follow dependency chains until no further confirmed
  unused code remains in scope.
- Check side effects before removing imports, initializers or bindings.
  Preserve a needed call even when its return value is unused. An unused
  callback parameter may be required by its calling convention.
- Remove unused dependencies only after checking build-time, runtime, test,
  development, optional and platform-specific consumers. Update manifests
  and lockfiles with the project's tooling; preserve required transitive
  dependencies and intentional public re-exports.
- Update build registrations, configuration, wrappers, exports, documentation
  and examples in the same change. Retain generators for generated files
  still consumed by the project. Remove a feature flag only after checking
  the supported variants and the implementation it controls.
- For redundant implementations, establish equivalent behavior and move
  callers to the surviving implementation before deleting the extra copy.
  Repeated test plumbing is a consolidation opportunity; distinct behavioral
  assertions remain valuable even when their setup looks similar. Prefer
  existing shared helpers over introducing another abstraction.
- Keep changes focused on removal and the wiring it requires. Do not replace
  findings with warning suppressions or sweeping formatter/style changes.
  Language statistics are diagnostic, not a target: do not hide tests or
  relabel files to make a language percentage smaller.

## Verify and finish

- Check test prerequisites and pinned reference/tool versions before long
  runs. Build and test the actual edited worktree. Use a fresh build or clean
  rebuild when cached outputs could hide deleted dependencies or source.
- Run checks proportionate to the cleanup: affected builds, behavioral tests,
  package/dependency validation and existing CI gates. Broaden coverage when
  shared code or entry points changed. Do not weaken assertions to make a
  deletion pass, and distinguish platform-specific evidence from a general
  portability claim.
- Rerun the relevant unused-code checks and inspect the final diff. Continue
  removing newly exposed dead code until no further confirmed findings
  remain. Keep genuinely uncertain candidates with their exact usage question
  and the evidence needed to resolve it; uncertainty about one candidate must
  not stop independent, confirmed cleanup.
- Read final summaries and exit codes. Report failures, skipped checks and
  successful reruns accurately. Follow repository rules for commits; never
  infer authorization to push or publish from this skill.
- Finish with a concise account of what was removed, why it was unused, the
  verification performed and any remaining uncertain candidates. Write or
  update a repository audit report when the findings need a durable record.
  Do not claim every unused path is proven absent when dynamic consumers or
  untested variants remain unresolved.

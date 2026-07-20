---
name: documentation-hygiene
description: Use when cleaning up, auditing, reorganizing, or de-duplicating a project's Markdown documentation, or when docs have drifted from the code. Verifies every .md against the actual code, consolidates to a CLAUDE.md + README.md + docs/ layout with hub/index links, deletes outdated / irrelevant / stale-generated files, and installs a rule that keeps docs in sync. Invoke before large doc edits or when docs have sprawled, gone stale, or contradict the code.
---

# Documentation Hygiene

Docs rot because the same fact is written in several places (so only one gets updated), because they drift from the code no one re-checked, and because scratch/one-off files never get deleted. The cure is: **one canonical home per fact, everything reachable from a hub, every claim verified against the code, and everything else deleted.**

This skill is general — it assumes no particular framework or layout. Adapt names to the repo; keep the shape.

## Core principles

1. **Verify against the code.** A doc is only as good as its agreement with the code *right now*. Every non-trivial claim must be checked against the source, config, or a live run — not trusted because it's written down.
2. **Single source of truth.** Every fact lives in exactly one place. If code, config, or a generated file already owns a fact, **link to it — never restate it**, or the copy will drift.
3. **Two anchors + one folder.** The repo root holds only `README.md` (for users) and `CLAUDE.md` (for contributors/agents) — plus standard meta files. Every other doc lives under `docs/`.
4. **Everything is reachable from a hub.** `README.md` and `CLAUDE.md` link to the docs a reader needs; nothing under `docs/` is orphaned.
5. **Delete fearlessly — but read first.** Outdated, irrelevant, scratch, and hand-drifted generated files are liabilities. Before deleting, confirm the file's *unique* facts have a home elsewhere; git history is the archive, so never archive-in-tree.
6. **Docs are part of the change.** Any change to behavior, commands, config, or build updates the relevant doc *in the same commit*. Encode this as a rule so the cleanup doesn't decay.

## Canonical layout

```
README.md            # user-facing: what it is, quick start, install/config, links out (user hub)
CLAUDE.md            # contributor/agent: build, test, run, conventions, architecture summary, docs index (contributor hub)
CHANGELOG.md         # optional, root by convention
LICENSE, CONTRIBUTING.md, SECURITY.md, CODE_OF_CONDUCT.md   # standard meta, stay at root
docs/
  README.md          # index for docs/ when it has several files (sub-hub)
  architecture.md    # how it works internally, design, roadmap
  <topic>.md         # guides, compatibility/ops, references, ...
  adr/ | guides/     # subfolder once a category reaches ~3+ files
```

Audience map — decide where a fact goes by *who reads it*:

| Reader | Home |
|--------|------|
| End user / adopter | `README.md`, then `docs/` guides |
| Contributor / AI agent | `CLAUDE.md`, then `docs/architecture.md` + deeper docs |
| Operator | `docs/` (ops / compatibility / migration) |
| Everyone (status/reference) | a single registry or generated file, linked from the hubs |

Sub-structure rule: keep `docs/` flat while small; give a category its own subfolder once it hits ~3+ files, with a one-line pointer from `docs/README.md`.

## The audit loop (the core of a run)

For **every** `.md` in scope, and every non-trivial claim in it:

1. **Find the backing code** — grep/read the source, config, or generated artifact the claim describes.
2. **Reconcile** — the claim is *correct* (leave, or replace with a link if code/config owns it), *stale* (fix to match the code), or *dead* (the feature is gone → delete the claim, and the doc if that's all it was).
3. **Run it** — for quick-start / install / example commands, actually execute them. A quick start that doesn't run is worse than none.

High-drift facts to re-verify first (these go wrong most): version numbers, counts ("N commands/endpoints"), config flags & env vars (renamed/removed?), file/path/module references (moved?), command & API names/signatures, benchmark numbers, screenshots, and any "coming soon / TODO / not yet" for things that shipped.

## Process (make each a todo)

1. **Inventory.** List every `.md` in scope (exclude the do-not-touch set below). For each: audience, and status — authoritative / duplicated / stale / generated / dead.
2. **Verify against code.** Run the audit loop above on each doc; note every correction and deletion.
3. **Define homes + hubs.** Confirm the two anchors and the `docs/` set; decide what each owns; plan the README and CLAUDE.md indexes.
4. **Consolidate.** Move each live fact to its one home; merge overlapping files into `docs/`; replace any prose that restates code/config/generated facts with a link.
5. **Prune.** Delete outdated, irrelevant, and scratch files, and committed generated files (see delete policy). Read before deleting.
6. **Wire hubs + links.** Add/refresh the docs index in `README.md` (users) and `CLAUDE.md` (contributors); ensure every `docs/` file is reachable and every link resolves.
7. **Install the anti-drift rule.** Put the "docs are part of the change" rule + the ownership/hub map in `CLAUDE.md`; add a CI link-check and a generated-doc regenerate-and-diff where possible.
8. **Report + verify invariants.** Emit a change report (below) and confirm the end-state invariants (below).

## Delete policy

Delete these:

- **Scratch / working files** — LLM feedback dumps, `notes.md`, `scratch.md`, `fix-*.md`, analysis exports, throwaway planning docs.
- **Superseded plans / requirements** — `TODO.md`, `requirements.md`, per-milestone design notes whose feature shipped (the code is now the truth).
- **Duplicate overviews** — an `FAQ.md` or second "intro" that re-answers what README/architecture already cover; fold in anything unique, delete the rest.
- **Restated-from-code prose** — a hand-maintained count, status table, flag list, or version that a generated file / registry / config already owns.
- **Committed generated docs that aren't reader-facing** — regenerate on demand instead (see nuance).

**Read before deleting**, and keep these:

- **Decision records / ADRs.** They capture *why* — which code cannot. They can look "outdated" yet still be the only record of a rationale. Keep (and link from a hub); don't auto-nuke.
- **Anything whose unique facts have no other home** — give the fact a home first, then delete the file.

**Generated-doc nuance.** Never hand-edit a generated doc. Then choose one stance and apply it consistently: **(a)** delete it from the tree, gitignore it, and document the generator command (best when it's only needed at build/CI time); or **(b)** keep it committed but add CI that regenerates and diffs to catch drift (justified only when it's reader-facing, e.g. an API/command reference people read on the repo host). Either way a generator must exist — "deleted" must never mean "lost."

## Do not touch

Vendored/third-party docs (`node_modules/`, `vendor/`, `deps/`, `third_party/`), docs generated by an external tool into their own output dir, standard meta files (`LICENSE`, `CONTRIBUTING`, `SECURITY`, `CODE_OF_CONDUCT`, issue/PR templates), and this skill's own files. Restructure your project's own authored docs only.

## Link, don't restate — the test

Before writing any fact as prose, ask: *does code, config, or a generated file already own this?* If yes, write a link instead. Examples of drift bait → fix:

- "Supports 42 commands" → link to the generated command list.
- Per-feature "done/partial/planned" scattered across README → one status registry, linked.
- Build steps duplicated in README *and* CLAUDE.md → keep in one, link from the other.
- Version `1.4.0` hardcoded in prose → link to the version source (`package.json`, `VERSION`, tag).

Distinguish **reference/registry** docs (status tables, command/API lists — single source of truth, always linked, never paraphrased) from **narrative** docs (architecture, guides — they explain, and link out to registries).

## End-state invariants ("clean")

The run is done — and re-running it is a no-op — when all hold:

- Root contains only `README.md`, `CLAUDE.md`, and standard meta files; all other authored docs are under `docs/`.
- Every fact has exactly one home; nothing restates what code/config/generated owns.
- Every `docs/` file is reachable from a hub (`README.md` or `CLAUDE.md`); every link resolves.
- No doc contradicts the code; documented quick-start/commands actually run.
- No scratch, superseded, or hand-drifted-generated files remain.
- `CLAUDE.md` states the docs ownership/hub map and the "docs are part of the change" rule.

## Change report

End every run with a short report so a human can sanction the deletions:

- **Deleted** — file → one-line reason (superseded by X / scratch / stale-generated).
- **Moved / merged** — from → to.
- **Corrected** — doc:section → what was wrong vs the code.
- **Left as-is (notable)** — e.g. ADRs kept for rationale.

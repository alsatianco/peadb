# Releasing PeaDB

This document is the runbook for cutting a PeaDB release and publishing its Docker images. Follow it in order — the sequencing exists because something pulls the published image daily (a scanner, a real deployment, or both — unresolved, but real), and any of those pullers running with `imagePullPolicy: Always` would ship an untested build into someone's system on its next restart if `latest` moved too soon.

## Tag convention

Tags are `v<VERSION>`, where `<VERSION>` is exactly the contents of the repo-root `VERSION` file — for example `v0.3.0` for `VERSION` containing `0.3.0`. The repository has zero tags as of this release, so `v0.3.0` establishes the convention rather than continuing one. `.github/workflows/release.yml` enforces that the pushed tag matches `VERSION` and fails the whole workflow if they disagree, so a tag can never be pushed against the wrong `VERSION`.

## Sequence

1. **Push `v<VERSION>`.** Tagging and pushing the tag triggers `.github/workflows/release.yml`, which builds both architectures natively, assembles a multi-arch manifest, and pushes it to Docker Hub as `alsatianco/peadb-server:<X.Y.Z>` and `alsatianco/peadb-server:<X.Y>` — for example `0.3.0` and `0.3`. **Leave `latest` pointing at the previous release (0.2)** — the workflow never touches `latest`, on purpose.
2. **Publish the release notes, breaking change first.** Copy the new `CHANGELOG.md` section for this version into the GitHub release notes, with the breaking change as the very first line — do not bury it under feature additions. Anyone scanning only the first line of the release notes must see it.
3. **Wait a week. Watch for issues.** Give the new image time to surface problems in real deployments before anyone's `imagePullPolicy: Always` pod could pick it up as `latest`. Watch the issue tracker and the Docker Hub pull/download metrics for the new tags during this window.
4. **Only then promote `latest`.** This is a deliberate manual step, not automated by any workflow: `docker buildx imagetools create --tag alsatianco/peadb-server:latest alsatianco/peadb-server:<X.Y.Z>` (or re-tag through the Docker Hub UI), run only after the waiting period has passed with no unresolved regressions.

## Operational notes

- **Multi-arch changes the pull-count baseline.** Once CI publishes both `linux/amd64` and `linux/arm64` images under one manifest, a single client pull now counts as one pull per architecture actually fetched — Docker Hub's `pull_count` metric climbs faster than it did for the amd64-only images this project shipped before. Record the pre-multi-arch pull-count baseline before switching CI publishing on, so a later jump in the metric is not misread as a sudden increase in real usage.
- **`INCRBYFLOAT`/`HINCRBYFLOAT`/`ZINCRBY` use `long double` formatted with `%.17Lf`.** `long double` is 80-bit extended precision (x87) on x86-64 Linux and IEEE binary128 on aarch64 Linux, so the 17th decimal digit of these commands' replies can differ between the `amd64` and `arm64` images for the same input. This is **not a PeaDB-specific divergence** — real Redis, built from the same C source on the same two architectures, exhibits the identical difference, because it uses the same `long double` type and the same `%.17Lf` formatting (`ld2string` in `util.c`). Treat any observed difference here as architecture parity with upstream Redis, not a PeaDB bug. One consequence for this project's own test infrastructure: `compat/redis_suite_ledger.json` baselines were recorded on one architecture (x86-64 Linux); if the upstream TCL suite is ever run on the other architecture, re-baseline the ledger for that architecture rather than treating a 17th-digit mismatch as a regression.

## Manual prerequisites

Two things cannot be verified or set up from inside this repository, and must be done by a repository administrator before the sequence above works:

- **Docker Hub credentials.** `.github/workflows/release.yml` logs in with `secrets.DOCKERHUB_USERNAME` and `secrets.DOCKERHUB_TOKEN`. If these secrets are not configured on the repository, the workflow fails loudly at the login step — which is the correct failure mode, not a silent skip.
- **Private Vulnerability Reporting.** Enable **Settings → Code security → Private vulnerability reporting** on `alsatianco/peadb` so the reporting link in [`SECURITY.md`](../SECURITY.md) actually works.

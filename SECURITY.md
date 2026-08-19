# Security Policy

## Status

PeaDB is pre-1.0 and has not been through an independent security audit. Do not store data you cannot afford to lose, and do not expose an instance to an untrusted network, until you have reviewed the known gaps below against your own threat model.

## Supported versions

Only the latest tagged release receives security fixes. There is no long-term support branch — upgrade to the newest tag to get a fix. See [`docs/RELEASING.md`](docs/RELEASING.md) for how releases are tagged and published.

## Reporting a vulnerability

Please report privately through GitHub's advisory form rather than opening a public issue: [https://github.com/alsatianco/peadb/security/advisories/new](https://github.com/alsatianco/peadb/security/advisories/new). You should receive an acknowledgement within 7 days. Please do not disclose the issue publicly until a fix has shipped.

## Known gaps

These are verified facts about the current codebase, not a guess at what might be missing. Weigh each one against your deployment before you rely on PeaDB for sensitive data.

- **No TLS.** There is no TLS/SSL support anywhere in the server — zero references to TLS, SSL, or OpenSSL in `src/`, `include/`, or the build (`CMakeLists.txt`). Traffic between clients and PeaDB, and between a master and its replicas, is unencrypted. If you need encryption in transit, terminate TLS at a proxy in front of PeaDB (for example stunnel, Envoy, or your cloud load balancer's TLS listener) — do not rely on network trust alone for anything crossing an untrusted boundary.
- **No multi-user ACL engine.** PeaDB enforces a single `default` user's password (`requirepass`); there is no per-user rule engine, so you cannot scope one credential to a subset of keys, commands, or pub/sub channels. `ACL SETUSER`, `DELUSER`, `LOAD`, `SAVE`, and `DRYRUN` all refuse with an explicit error rather than silently accepting a rule they cannot enforce — a prior version of `ACL SETUSER` returned `OK` while discarding the rule, which is now fixed; the command tells you plainly that it did nothing. See `compat/delta.md` items 1 and 7 for the full detail.
- **No Sentinel.** There is no `SENTINEL` command and no automatic-failover control plane. If you need automated master promotion, you must build it on top of `REPLICAOF` yourself, or run PeaDB behind an orchestrator that already owns that decision (Kubernetes or an equivalent).
- **No cluster failover.** PeaDB's Cluster protocol surface (slot routing, `MOVED`/`ASK` redirects, `CLUSTER SLOTS`/`SHARDS`/`NODES`) is real, but the topology is operator-configured, not gossiped — there is no failure detector and no automatic failover, so a node going down does not cause another node to take over its slots. See `compat/delta.md` item 6 and the [cluster addressing decision record](docs/decisions/p5-cluster-addressing-and-version-provenance.md).

Given these gaps, treat TLS termination and multi-tenant isolation as the operator's responsibility: put a TLS-terminating proxy in front of every instance that crosses a network boundary you do not fully trust, and enforce tenant isolation (separate instances, network policies, or an orchestrator's namespace boundaries) rather than relying on PeaDB's own access control to separate tenants.

For the full, always-current compatibility and feature-status picture, see [`docs/compatibility.md`](docs/compatibility.md) and the canonical gap registry [`compat/delta.md`](compat/delta.md) — this file does not restate per-feature status that those documents already own.

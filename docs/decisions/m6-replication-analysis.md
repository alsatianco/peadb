# M6 Replication Analysis

Date: 2026-02-17

Current implementation provides a replication bridge utility for interop testing:
- one-shot sync from Redis master to PeaDB.

This does not yet implement full PSYNC backlog/offset protocol in-process.

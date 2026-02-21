# M4 Streams Analysis

Date: 2026-02-17

## Phase 1 (M4.X1)

Implement minimal stream model with:
- XADD
- XRANGE
- XREVRANGE
- XLEN

Semantics covered:
- Server-generated IDs via `*` with monotonic `(ms-seq)` progression per key.
- Ordered append and ordered range reads.
- Basic range selectors: `-`, `+`, and exact id boundaries.

## Phase 2 (M4.X2)

Implement consumer-group basics with:
- XGROUP CREATE
- XREADGROUP GROUP <group> <consumer> STREAMS <key> >
- XACK
- XPENDING (summary form)

Simplifications for this milestone:
- single-node in-memory state only
- minimal pending-entry tracking
- no claim/idle-time advanced management

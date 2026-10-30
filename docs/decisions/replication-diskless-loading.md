# Diskless replication and loading lifecycle

Date: 2026-10-30

Status: accepted design; bounded RDB output is implemented (R4a), while diskless transport, loading and their acceptance remain pending. This record does not enable diskless replication or close a compatibility gap. Current support and verification belong to [delta items 8, 24, 46 and 70](../../compat/delta.md).

## Problem and specification

Diskless synchronization must send a snapshot while it is being generated, without a temporary RDB file or a complete serialized snapshot in memory. A replica using `swapdb` must load into separate databases and a separate function registry, retain the old state on a short read or cancellation, and publish the new state only after the entire snapshot and its framing validate. Merely storing the configuration values or receiving a whole snapshot before setting a loading flag does not provide these behaviors.

The specification is [Redis 7.2.5, commit f60370ce28b946c1146dcea77c9c399d39601aaa](https://github.com/redis/redis/tree/f60370ce28b946c1146dcea77c9c399d39601aaa). In that revision, `replication.c` owns `syncCommand()`, `shouldStartChildReplication()`, `startBgsaveForReplication()`, the `rdbPipe*` handlers, `useDisklessLoad()` and `readSyncBulkPayload()`; `rdb.c` owns `rdbSaveRioWithEOFMark()`, `rdbSaveToSlavesSockets()`, the explicit `rdbLoadingCtx` and loading progress; `server.c` owns command admission and `processEventsWhileBlocked()`. The complete [integration/replication suite](https://github.com/redis/redis/blob/f60370ce28b946c1146dcea77c9c399d39601aaa/tests/integration/replication.tcl) is an acceptance input, including the cases after writable-replica setup that the older PeaDB baseline did not reach.

The audited implementation has useful boundaries to preserve: [master snapshot transfers](../../src/server.cpp) keep file bytes outside `WriteBuf`, [the replication supervisor](../../src/command.cpp) tags handoffs with a generation and acknowledges applied command offsets, and [the RDB loader contract](../../include/rdb.hpp) validates structure before replacing a full-sync dataset. Its function compilation and apply-time allocations are not transactional. The [codec](../../src/rdb.cpp) now streams snapshot output through a bounded sink but still reads a complete string and addresses the singleton datastore and global function hooks; the current loading handoff blocks the event loop. These interfaces must change before a streaming load can safely serve clients.

## Decisions

Keep mutation and Lua execution on the event-loop thread. Use explicit snapshot input/output and load-target contexts, a bounded parent-side pipe relay, and a controlled event pump during loading. Here, asynchronous loading means serving the old dataset while loading another one; it does not require a background thread to mutate databases or compile Lua.

Rejected alternatives are keeping the whole serialized RDB in a string and calling that diskless, running the existing singleton loader on a worker, and temporarily redirecting `store()` or the global function hooks while processing clients. The first hides unbounded serialization memory, and the others introduce concurrent access or let commands see and modify the staging state. A standalone resumable decoder can replace the controlled pump later; it is not necessary to reproduce Redis's single-threaded loading model.

All names below describe proposed ownership and interfaces, not existing classes. Implementation must retain the existing wire format, codec validations, command propagation, persistent snapshot publication and applied-offset rules unless a documented Redis parity fix requires a change.

## Master admission and snapshot ownership

Track EOF capability independently from PSYNC2 capability on each outgoing replica session. Advertise both only when the receiving implementation can consume both length-framed and EOF-framed snapshots. A legacy SYNC client or a peer without EOF capability continues to receive the disk-backed length format. `repl-diskless-sync` selects the master transport; `repl-diskless-load` independently selects the replica's load strategy. The transport of one endpoint does not imply the other's strategy.

Pending recipients retain their wait-start time, capability set and request requirements. Match Redis's immediate `syncCommand()` path as well as its cron path: an EOF-capable peer may wait for the configured delay; reaching a nonzero `repl-diskless-sync-max-replicas` starts the compatible waiting batch early; zero means no threshold. Disk-backed synchronization starts without a diskless delay on the immediate path. The cron uses the oldest compatible wait and the live settings as Redis does, including its capability intersection and active-child exclusion. Test a mixed-capability cohort rather than assuming every pending session can share an EOF transfer.

A `ReplicationSnapshot` owns one immutable fork boundary: snapshot identity, transport, replication ID, applied offset, stream database, dirty epoch and recipient identities `(fd, client_id)`. No post-fork command may precede the RDB, and none may be lost if the replication ring wraps. Preserve the existing command-buffer limit for subsequent stream bytes; exclude the bounded snapshot relay from that buffer. Snapshot bytes and delimiters never advance replication offsets.

Late diskless arrivals wait for another snapshot because earlier pipe bytes cannot be replayed. Disk-backed late-arrival reuse is permitted only when a compatible recipient retains the complete command suffix from the original fork boundary; copy or share that suffix and the same snapshot identity. A standalone BGSAVE without retained replication bytes cannot be reused merely because a file exists. Keep the old wait-for-next behavior documented until this reuse condition is implemented and tested.

One child slot still excludes concurrent BGSAVE/AOF rewrite children. Distinguish a disk-target save from a socket-target transfer when updating dirty baselines, LASTSAVE, save status and persistence scheduling; sending a diskless snapshot is not a successful persistent save. A diskless transfer must not create or rename `dbfilename`. The normal disk path retains file fsync, directory fsync and the open-inode transfer guarantees.

### Master state machine

| State | Ownership and exit condition |
| --- | --- |
| Waiting | Session waits for admission; keepalive newlines are permitted outside partial headers. |
| Generating to disk | Child owns its temporary file; parent retains the fork boundary and command suffix. Child success opens the completed inode for transfer. |
| Relaying pipe | Child writes RDB bytes; parent owns the read registration, one bounded chunk and a cursor per surviving recipient. |
| Waiting for snapshot ACK | EOF trailer is completely sent; wait for the replica's post-load acknowledgement before releasing its buffered command stream, following Redis's EOF handshake. |
| Online | Normal stream output and ACK timeouts apply. |
| Aborting | Detach registrations and recipients, stop/reap the owned child when required, and release all transfer resources exactly once. |

Use a shared bounded relay chunk. Do not read another chunk while any surviving recipient still needs bytes from the current one. Enable socket write interest only for recipients with pending bytes, suspend pipe reads under backpressure, and restore them when all cursors drain or the blocking recipient disappears. A slow recipient therefore bounds the batch until it drains, disconnects or reaches the full-sync inactivity timeout. Normal clients must continue to run; a permanently writable socket or paused pipe must not create a busy loop.

Register/remove pipe events before closing/reusing their FDs. The existing [Poller](../../include/poller.hpp) primarily exposes client read/read-write operations; implement explicit pipe read suspend/resume behavior correctly for both epoll and kqueue instead of treating a pipe as a client socket. Every ready event and completion must validate its transfer/session identity after commands that can remove clients. Defer closes triggered inside command dispatch as the existing snapshot completion path does.

Separate pipe EOF, trailer delivery, child exit and child reaping. Redis uses an additional parent-to-child exit pipe so success cannot be reported before the parent drains the RDB; preserve that ordering with an owned completion pipe. Poll/reap children even while pipe reads are suspended. When the last useful recipient leaves, stop a socket-target child; a disk-target child may remain useful for configured saves, following Redis's save-point rule. Parent loss must close the completion/consumer ends so the child exits; a long disk writer must also observe parent loss between write/key boundaries. Exercise this through actual Linux process ownership, not only `kill(pid, 0)` on a possibly zombie child.

## Codec and framing boundaries

The common RDB writer now has a blocking sink interface with 64 KiB buffering, incremental CRC64 and propagated write errors; key names and function sources are visited without copying their registries. Individual values/compact encodings may still be materialized. File saves keep their atomic publication wrapper; a socket-target child writes to its pipe. DUMP and FUNCTION DUMP may retain an in-memory sink because their API returns a bulk payload. No pipe path may call `rdb_build_string()` to construct the entire snapshot first. Keep the existing key-save delay on the actual common serialization path.

Introduce a reader/source interface with bounded read-ahead and incremental checksum accounting, then make the common value decoders consume it. Parse each byte once; retrying an entire large value after every short socket read can become quadratic. Memory may include the active dataset, a staging dataset, the largest value being decoded and bounded I/O buffers, but must not include another whole serialized RDB. Stream groups, consumers, PEL, function sources and all existing legacy encodings stay on the common decoder.

Recognize keepalive newlines before FULLRESYNC and before the bulk header. A length frame owns exactly the declared RDB bytes, without consuming the next RESP command. An EOF frame has `$EOF:` plus a 40-byte delimiter, the RDB through its EOF opcode and checksum, then the matching delimiter. A direct loader parses the RDB end and verifies that exact trailer; it must not search arbitrary value bytes for a delimiter and truncate a valid value. The disk-spooling EOF path needs rolling delimiter detection across arbitrary read boundaries and must preserve any read-ahead command bytes for the stream parser. Invalid headers, premature EOF, malformed values, CRC failures and a wrong/missing trailer all fail synchronization.

Both frame types must work with every load strategy. Do not advertise EOF after implementing only direct socket loading: a replica configured with diskless loading disabled still needs to spool an EOF-framed master's snapshot safely. Likewise a direct loader must handle a length-framed disk-backed master. EINTR, EAGAIN, timeout, EOF and generation cancellation have distinct outcomes; a signal or a single interrupted read is not a corrupt snapshot.

## Replica loading and publication

Create an event-loop-owned `ReplicaLoadSession` identified by upstream generation and attempt sequence. It owns input framing, source/read-ahead, staging databases and functions where applicable, validation/progress state, the prospective stream database/ID/offset, and cleanup. Extend the existing thread handoff with exclusive socket-read ownership: the link thread pauses before direct loading and resumes only after the event loop returns the socket and any unread stream bytes. Two readers must never race on a replication socket. A stale completion must neither clear a newer attempt's loading flag nor publish its data.

Give decoders an explicit `RdbLoadTarget` containing a datastore reference, function-library context, role/expiry policy, load counters and replication AUX output. Preserve the stable address of the live `store()` object and swap its dataset payload at publication; do not swap unrelated hooks, runtime policy or worker ownership. Stage functions with callback references owned by their own context, using the same main-thread Lua engine with explicit registry selection. Compile and validate every library and collision before publication. Rebuilding the old registry from sources after failure is not transactional rollback: it can allocate or fail again and changes callback identities.

| Replica mode | Loading behavior | Failure and publication |
| --- | --- | --- |
| `disabled` | Receive to an owned temporary file, then use the file loader and normal loading admission. | Preserve the established durable rename/AOF sequence; do not claim `swapdb` rollback guarantees for an in-place load. |
| `on-empty-db`, all DBs empty | Read directly into the empty live target with normal loading admission. | Discard partial keys/functions on failure; there was no old dataset to preserve. |
| `on-empty-db`, any DB nonempty | Use the disk-spooling path. | Emptiness is across all databases, not just the selected DB. |
| `swapdb`, different master replication ID | Read directly into a separate dataset and function context; INFO reports `loading:1`, `async_loading:0`. | Refuse non-loading commands; keep old state hidden until success or failure. On failure discard staging and expose the old state again. |
| `swapdb`, matching master replication ID | Use the same separate target; INFO reports `loading:0`, `async_loading:1`. | Serve the old dataset/functions under normal replica rules while loading; discard them only on validated success. |

Matching ID means comparing the existing local replication history with the incoming FULLRESYNC ID, as Redis does. A hostname match, a prior connection to the same port, or accepting FULLRESYNC must not authorize asynchronous service. Keep the pre-attempt history and downstream replicas until `replicationAttachToNewMaster()`'s equivalent boundary; an aborted attempt before that point must not unnecessarily discard a usable cached master or chain.

At a successful swap, first ensure the generation still matches and finish all fallible decoding, function compilation and EOF/CRC verification. In one event-loop publication boundary, invalidate the replaced keyspace's watches/blocking dependencies as Redis does, detach downstream history, exchange dataset and function contexts, clear local writable-replica TTL ownership, initialize the master's stream session/database/offset and backlog, and transfer socket ownership back. Wake/recheck affected clients only against the published state. Never acknowledge bytes received as though their commands had been applied. Discard pre-snapshot held commands and incomplete transactions without replaying effects already included in the snapshot.

Retirement is part of the design: free old/staging data in bounded event-loop work or through an existing safe lazy-free ownership transfer, with function unrefs performed on the Lua-owning thread. Destroying a multi-million-key old dataset synchronously after a nominally fast swap can still stall every client. Allocation failure before publication aborts staging without replacing old state; no fallible work should be hidden inside a purported atomic exchange.

### Persistence during full sync

Coordinate AOF suspension/restart and old persistence children at Redis's load boundary, including the failure path where AOF remains suspended until a later successful sync or promotion. Never restart an AOF from a partially decoded staging dataset or let an old child overwrite a new snapshot. A diskless successful load changes in-memory state without saving an RDB; it must mark the appropriate dirty state and rebuild an enabled AOF from the published dataset. Disk-spooled loading retains its durable file publication. Test success, abort, reconnect, promotion and crash/restart separately; preserving the old in-memory state after an aborted swap does not by itself prove crash durability.

## Loading admission and event servicing

Represent startup loading, replication loading and asynchronous replication loading in one event-loop-owned load state, while keeping link transfer progress distinct. Receiving a snapshot to disk is not yet loading it. Start direct-loading state when the RDB parser starts, so a deliberately slow producer makes it externally observable. INFO loading/progress must remain truthful while a client can actually request it; setting a flag around an uninterruptible call is insufficient.

Use the generated Redis command flags, including `LOADING` and `NO_ASYNC_LOADING`, and Redis's EXEC aggregate/inverse flags. Preserve authentication, stale/readonly, busy-script, OOM and error-counter precedence. During asynchronous loading, Redis still applies normal replica and script rules; it does not reduce all admission to a blanket readonly check. CONFIG SET additionally refuses settings marked `DENY_LOADING_CONFIG`, even for a no-change update such as `appendonly no`; other settings follow Redis's normal checks. REPLICAOF is refused by `NO_ASYNC_LOADING`. Test direct, pipelined, MULTI/EXEC, renamed-command and script/function paths. The exact error is `LOADING Redis is loading the dataset in memory`.

Factor a controlled loading event pump from the existing server loop. It may accept clients, parse/dispatch admitted commands, flush replies, update progress/timeouts, notice disconnects and shutdown signals, and service a stalled load source. It must exclude recursively loading another snapshot, applying the master's command queue before publication, or starting a competing persistence child. Save and restore command-local selected DB, propagation/replay flags and execution context around pump calls. Do not yield while holding a datastore iterator, partially modifying a function registry, or running Lua compilation. Keep owned clients safe when a command closes a connection during loading.

Use Redis's byte-interval setting to bound servicing opportunities and time-based polling while waiting for input. Large strings must not prevent input progress/INFO forever. The real key-load delay belongs in the decoder, and the producer delay belongs in the writer; neither should be a scripted test-name or payload-text trigger. Delta item 54's general Lua event-service defect remains separate: returning BUSY for a text pattern does not satisfy nested script/kill behavior during asynchronous loading.

For startup, initialize listeners and the loading pump before RDB or AOF replay; begin configured replication only after loading succeeds. Preserve AOF-over-RDB precedence, authenticating clients, no-save shutdown during load and immediate second-SIGINT behavior. Startup should answer permitted INFO/AUTH and LOADING for other commands, then require a successful authenticated protocol round trip for readiness. Moving the listener alone does not fix startup event servicing or prompt signal cancellation.

## Acceptance and delivery boundaries

Implement in dependency order, with a failing-before pinned-reference regression, documentation/delta update and the full local gates for each coherent delivery. First land common streaming codec/load contexts while retaining existing transport behavior; then master delay/admission plus bounded pipe/EOF transport; then shared loading admission/event servicing including startup; then direct load/swapdb and publication. Transport negotiation must stay disabled until its matching receive capability is implemented. The local work plan tracks exact delivery steps; this record owns the architectural rationale and invariants.

| Area | Required evidence before claiming it implemented |
| --- | --- |
| Framing | Both header forms, fragmented trailer, keepalive newlines, binary values, exact stream boundary, malformed/short/checksum/trailer failures, EINTR and cancellation. |
| Transport/admission | Delay honored, threshold releases a batch, zero threshold, mixed EOF capabilities, legacy SYNC, concurrent and late recipients, same fork boundary, continuous post-fork writes and ring wrap, no disk file for socket-target sync. |
| Backpressure/lifecycle | Fast/slow/all/timeout recipient drops, no busy spin, pipe-registration reuse after FLUSHALL, external child kill while reads are paused, parent death in disk and pipe modes, owned PID cleanup and actual Linux child reaping. |
| Swap and failure | Different/matching IDs; old keys, absolute TTLs, functions, stream metadata and local TTL ownership preserved on abort and replaced together on success; invalid functions cannot partially publish. |
| Command service | Observable INFO states, old GET/FCALL during async load, CONFIG/REPLICAOF rejection, transactions/renames, blocked clients, watched keys, controlled script kill and authenticated startup RDB/AOF loading. |
| Persistence/history | Enabled/disabled AOF, save-point children, crash/restart after success/failure, promotion and changed role, superseded generations, cached-master reuse and chained replicas, applied-offset ACKs. |
| Scale/platform | Bounded relay/serialization memory beyond socket buffers, no whole-RDB serialized copy, retirement latency, macOS kqueue and native Linux epoll Release/sanitizer checks. |

The Linux CPU/backpressure evidence needs a dedicated measurement: the pinned suite's `set os [catch {exec uname}]` stores the catch status, so its subsequent comparison with `Linux` does not activate those CPU assertions. A green native suite alone does not establish the absence of pipe busy loops.

Re-run pinned Redis and repeated complete unmodified native `integration/replication` after all prerequisites; retain failures and investigate later cases rather than declaring this matrix exhaustive. Required later cases include short-read stress, child/parent death, read-pipe cleanup, chain/cache preservation, watchdog EINTR, replica-generated reply suppression and lazy expiry. Keep the larger suite unadopted until it finishes green repeatedly; never add blanket skips or weaken existing ratchet baselines to declare the architecture complete.

# M8 Modules ABI Boundary (Initial)

Date: 2026-02-17
Redis compatibility pin: Redis 7.2.5

## Scope

This milestone defines a minimal, explicit Redis module ABI boundary for PeaDB.
The goal is to lock the first symbol surface and validate it end-to-end with
test modules before expanding support.

## Command surface

- `MODULE LOAD <path>`
- `MODULE UNLOAD <name>`
- `MODULE LIST`

## Entry symbols

- `RedisModule_OnLoad`
- `RedisModule_OnUnload` (optional)

## Initial API symbols exposed to modules

- `RedisModule_OpenKey`
- `RedisModule_StringSet`
- `RedisModule_StringDMA`

## Versioning note

The ABI target for this phase is source-level compatibility with the above
subset against the Redis 7.2.5 behavior model. Full binary compatibility and
broader `RedisModule_*` coverage remain tracked for follow-up milestones.

# Remaining Issues In TigerBeetle Integration

This document tracks the remaining gaps between EloqKV's TigerBeetle integration and the local TigerBeetle reference in `external/tigerbeetle/`.

It is a fresh audit of the current codebase state.

## Scope

Audited components:

- [tigerbeetle-integration.md](/home/lintaoz/work/eloqkv/docs/tigerbeetle-integration.md)
- [tb_types.h](/home/lintaoz/work/eloqkv/include/tb_types.h)
- [tb_types.cpp](/home/lintaoz/work/eloqkv/src/tb_types.cpp)
- [tb_handler.cpp](/home/lintaoz/work/eloqkv/src/tb_handler.cpp)
- `external/lua_beetle/scripts/*.lua`
- local TigerBeetle reference in `external/tigerbeetle/`

Reference sources used:

- `external/tigerbeetle/src/tigerbeetle.zig`
- `external/tigerbeetle/src/state_machine.zig`

## Current Status

1. Native `TB` / `TB_BIN` command surface:

- Implemented.
- The command handlers, binary layouts, storage keys, and query/update paths exist.

2. Native TigerBeetle semantics:

- Incomplete.
- The implementation is usable, but it is not yet semantically equivalent to the local TigerBeetle reference.

3. `lua_beetle` compatibility/reference path:

- Partially aligned.
- The storage layout matches EloqKV, but the Lua semantics are still incomplete.

## Open Issues

### EloqKV Native C++ Path

1. Imported-event semantics are incomplete.

Files:

- [tb_handler.cpp](/home/lintaoz/work/eloqkv/src/tb_handler.cpp)

Issue type:

- Incomplete.

Problem:

- Imported accounts are rejected up front instead of being implemented.
- Imported transfers are rejected up front instead of being implemented.
- The implementation therefore lacks the current TigerBeetle imported-event rules, including:
  - imported/non-imported batch expectations
  - imported timestamp ordering and regression checks
  - imported transfer postdating checks against account timestamps
  - imported timeout restrictions

Evidence:

- [`tb_handler.cpp`](/home/lintaoz/work/eloqkv/src/tb_handler.cpp#L122)
- [`tb_handler.cpp`](/home/lintaoz/work/eloqkv/src/tb_handler.cpp#L129)
- [`tb_handler.cpp`](/home/lintaoz/work/eloqkv/src/tb_handler.cpp#L599)
- [`tb_handler.cpp`](/home/lintaoz/work/eloqkv/src/tb_handler.cpp#L742)
- TigerBeetle imported-event result family in `external/tigerbeetle/src/tigerbeetle.zig`

Impact:

- Imported-event workflows are unsupported.

2. Advanced transfer flags are incomplete.

Files:

- [tb_types.h](/home/lintaoz/work/eloqkv/include/tb_types.h)
- [tb_handler.cpp](/home/lintaoz/work/eloqkv/src/tb_handler.cpp)

Issue type:

- Incomplete.

Problem:

- The API surface defines:
  - `balancing_debit`
  - `balancing_credit`
  - `closing_debit`
  - `closing_credit`
- The execution path rejects those semantics instead of implementing TigerBeetle behavior for them.

Evidence:

- [`tb_types.h`](/home/lintaoz/work/eloqkv/include/tb_types.h#L106)
- [`tb_handler.cpp`](/home/lintaoz/work/eloqkv/src/tb_handler.cpp#L129)
- [`tb_handler.cpp`](/home/lintaoz/work/eloqkv/src/tb_handler.cpp#L744)

Impact:

- These flags are visible but not functionally supported.

3. Transfer idempotency is incomplete.

Files:

- [tb_types.h](/home/lintaoz/work/eloqkv/include/tb_types.h)
- [tb_handler.cpp](/home/lintaoz/work/eloqkv/src/tb_handler.cpp)

Issue type:

- Incomplete, with semantic mismatch against TigerBeetle.

Problem:

- Existing-transfer checks now distinguish several field mismatches, but they still do not cover the full TigerBeetle result space.
- `id_already_failed` is not modeled.
- The imported-event idempotency result family is not modeled.
- `overflows_debits` and `overflows_credits` are missing even though TigerBeetle defines them.

Evidence:

- Native comparison path: [`tb_handler.cpp`](/home/lintaoz/work/eloqkv/src/tb_handler.cpp#L321)
- Native result enum: [`tb_types.h`](/home/lintaoz/work/eloqkv/include/tb_types.h#L162)
- TigerBeetle result enum: `external/tigerbeetle/src/tigerbeetle.zig:240`
- TigerBeetle `id_already_failed`: `external/tigerbeetle/src/tigerbeetle.zig:253`
- TigerBeetle `overflows_debits` / `overflows_credits`: `external/tigerbeetle/src/tigerbeetle.zig:308`

Impact:

- Retry and duplicate-submission behavior are still not TigerBeetle-complete.

4. Closed-account transfer behavior is still only partially correct.

Files:

- [tb_handler.cpp](/home/lintaoz/work/eloqkv/src/tb_handler.cpp)

Issue type:

- Semantic mismatch.

Problem:

- Closed-account-specific statuses are present.
- The current logic allows `void_pending` on a closed account, which matches TigerBeetle.
- The remaining closed-account semantics tied to closing transfers are still not implemented because closing transfer flags themselves are not implemented.

Evidence:

- Native closed-account gate: [`tb_handler.cpp`](/home/lintaoz/work/eloqkv/src/tb_handler.cpp#L376)
- TigerBeetle closed-account rule: `external/tigerbeetle/src/state_machine.zig:4068`
- TigerBeetle closing-transfer status: `external/tigerbeetle/src/tigerbeetle.zig:269`

Impact:

- Basic closed-account enforcement exists, but full TigerBeetle closing-account behavior is still incomplete.

5. Post/void pending semantics are only partially current-TigerBeetle correct.

Files:

- [tb_handler.cpp](/home/lintaoz/work/eloqkv/src/tb_handler.cpp)

Issue type:

- Semantic mismatch.

Problem:

- The native path supports partial `post_pending` by using the explicit amount when provided.
- It does not implement TigerBeetle's `amount = maxInt(u128)` sentinel behavior for posting the full pending amount.
- It does not implement `pending_transfer_expired`.
- It currently requires exact account IDs, ledger, and code on post/void requests, while TigerBeetle allows those fields to be omitted and resolved from the pending transfer.

Evidence:

- Native post/void path: [`tb_handler.cpp`](/home/lintaoz/work/eloqkv/src/tb_handler.cpp#L389)
- TigerBeetle post/void rules: `external/tigerbeetle/src/state_machine.zig:3969`
- TigerBeetle `pending_transfer_expired`: `external/tigerbeetle/src/tigerbeetle.zig:294`

Impact:

- Two-phase transfer behavior is close, but not yet TigerBeetle-equivalent.

### `lua_beetle`

6. Transfer semantics in Lua are incomplete.

Files:

- [create_transfer.lua](/home/lintaoz/work/eloqkv/external/lua_beetle/scripts/create_transfer.lua)
- [create_linked_transfers.lua](/home/lintaoz/work/eloqkv/external/lua_beetle/scripts/create_linked_transfers.lua)

Issue type:

- Incomplete, with semantic mismatch.

Problem:

- Imported-event semantics are still rejected instead of implemented.
- Advanced transfer flags are still rejected instead of implemented.
- Full idempotency result distinctions are not implemented.
- Current TigerBeetle post/void sentinel and expiry semantics are not implemented.

Impact:

- The Lua transfer path is still not a full TigerBeetle-equivalent reference.

7. Lua account creation semantics are incomplete.

Files:

- [create_account.lua](/home/lintaoz/work/eloqkv/external/lua_beetle/scripts/create_account.lua)
- [create_linked_accounts.lua](/home/lintaoz/work/eloqkv/external/lua_beetle/scripts/create_linked_accounts.lua)

Issue type:

- Incomplete.

Problem:

- Imported-account semantics are still rejected instead of implemented.
- Full current TigerBeetle account result coverage is not implemented.

Impact:

- The Lua account path is still only a partial reference implementation.

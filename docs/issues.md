# Open Issues In TigerBeetle Integration

This document lists the currently open issues in EloqKV's TigerBeetle-related implementation and in the companion `lua_beetle` reference scripts.

It is a fresh audit of the current codebase state.

## Scope

Audited components:

- [tigerbeetle-integration.md](/home/lintaoz/work/eloqkv/docs/tigerbeetle-integration.md)
- [tb_types.h](/home/lintaoz/work/eloqkv/include/tb_types.h)
- [tb_types.cpp](/home/lintaoz/work/eloqkv/src/tb_types.cpp)
- [tb_handler.cpp](/home/lintaoz/work/eloqkv/src/tb_handler.cpp)
- `external/lua_beetle/scripts/*.lua`
- `external/lua_beetle/tests/*`
- local TigerBeetle reference in `external/tigerbeetle/`

Reference sources used:

- `external/tigerbeetle/src/tigerbeetle.zig`
- `external/tigerbeetle/src/state_machine.zig`
- `external/tigerbeetle/src/clients/c/tb_client.h`
- `external/tigerbeetle/src/clients/dotnet/TigerBeetle/Bindings.cs`

## Current Status

Goal-by-goal, the current state is:

1. Native `TB` / `TB_BIN` commands in EloqKV:

- Partially implemented.
- The command surface exists, but the implementation is still not current-TigerBeetle-correct.

2. TigerBeetle native wire protocol compatibility:

- Not implemented.

3. `lua_beetle` correctness audit and fixes:

- Partially complete.
- The Lua implementation is improved, but still not equivalent to current TigerBeetle semantics.

## Open Issues

### EloqKV

1. Imported-event semantics are not implemented.

Files:

- [tb_handler.cpp](/home/lintaoz/work/eloqkv/src/tb_handler.cpp)

Problem:

- Imported account semantics are rejected up front instead of being implemented.
- Imported transfer semantics are rejected up front instead of being implemented.
- The implementation still lacks the current TigerBeetle imported-event rules, including:
  - imported/non-imported batch expectations
  - imported timestamp ordering checks
  - imported timestamp regression checks
  - imported transfer postdating checks against account timestamps
  - imported timeout restrictions

Impact:

- Imported account and transfer workflows are unsupported.

2. Advanced transfer flags are not implemented.

Files:

- [tb_types.h](/home/lintaoz/work/eloqkv/include/tb_types.h)
- [tb_handler.cpp](/home/lintaoz/work/eloqkv/src/tb_handler.cpp)

Problem:

- The type layer defines:
  - `balancing_debit`
  - `balancing_credit`
  - `closing_debit`
  - `closing_credit`
- The execution path rejects these semantics rather than implementing TigerBeetle behavior for them.

Impact:

- These flags are visible at the API level but not supported functionally.

3. Transfer idempotency is incomplete.

Files:

- [tb_handler.cpp](/home/lintaoz/work/eloqkv/src/tb_handler.cpp)
- [tb_types.h](/home/lintaoz/work/eloqkv/include/tb_types.h)

Problem:

- Existing transfers still collapse to `EXISTS` rather than the full `exists_with_different_*` result family.
- `id_already_failed` is not modeled.
- Some current transfer result codes are still missing entirely, including:
  - `exists_with_different_ledger`
  - closed-account-specific statuses
  - imported-event status family
  - `overflows_debits`
  - `overflows_credits`

Impact:

- Duplicate handling and retry behavior are not TigerBeetle-compatible.

4. Closed-account transfer behavior is not implemented.

Files:

- [tb_handler.cpp](/home/lintaoz/work/eloqkv/src/tb_handler.cpp)

Problem:

- Closed accounts are represented in flags.
- Transfer execution does not enforce TigerBeetle's closed-account rules.

Impact:

- Closed-account movement rules are incorrect.

5. Account creation is still not fully current-TigerBeetle complete.

Files:

- [tb_handler.cpp](/home/lintaoz/work/eloqkv/src/tb_handler.cpp)

Problem:

- Imported account semantics are rejected rather than implemented.
- Current TigerBeetle imported-account behavior is therefore still missing.

Impact:

- Account creation is only complete for the non-imported subset.

### `lua_beetle`

6. Transfer semantics are still incomplete and not fully current-TigerBeetle correct.

Files:

- [create_transfer.lua](/home/lintaoz/work/eloqkv/external/lua_beetle/scripts/create_transfer.lua)
- [create_linked_transfers.lua](/home/lintaoz/work/eloqkv/external/lua_beetle/scripts/create_linked_transfers.lua)

Problem:

- Imported-event semantics are still not implemented and are rejected instead.
- Advanced transfer flags are still not implemented and are rejected instead.
- Full current idempotency result distinctions are not implemented.
- Some post/void amount handling still does not match current TigerBeetle behavior exactly.

Impact:

- The Lua transfer implementation remains only partially correct.

7. The single-transfer and linked-transfer Lua scripts are not fully aligned.

Files:

- [create_transfer.lua](/home/lintaoz/work/eloqkv/external/lua_beetle/scripts/create_transfer.lua)
- [create_linked_transfers.lua](/home/lintaoz/work/eloqkv/external/lua_beetle/scripts/create_linked_transfers.lua)

Problem:

- The linked-transfer script still lags the single-transfer script in validation completeness.
- The two paths are not guaranteed to return the same result for the same logical error case.

Impact:

- Batch and single-operation behavior can diverge.

8. Account creation is improved but still not fully current-TigerBeetle complete.

Files:

- [create_account.lua](/home/lintaoz/work/eloqkv/external/lua_beetle/scripts/create_account.lua)
- [create_linked_accounts.lua](/home/lintaoz/work/eloqkv/external/lua_beetle/scripts/create_linked_accounts.lua)

Problem:

- Imported account semantics are still not implemented and are rejected instead.
- Full current account status coverage is not implemented.

Impact:

- The Lua account path is still only partially aligned with current TigerBeetle behavior.

9. Lua history/index storage is incompatible with EloqKV history storage.

Problem:

- `lua_beetle` uses `APPEND`-based string indexes for:
  - transfer history
  - balance history
- EloqKV uses:
  - sorted sets for transfer history
  - lists for balance history

Impact:

- The two implementations cannot share history/query storage structures.


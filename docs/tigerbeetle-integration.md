# TigerBeetle Integration into EloqKV

## Overview

This document records the goals, design decisions, implementation plan, and all work executed for adding TigerBeetle financial transaction semantics natively to EloqKV, along with an audit of the companion `lua_beetle` reference implementation.

---

## Original Requirements (verbatim)

> I want to implement the tigerbeetle functionalities in eloqkv. Look at https://docs.tigerbeetle.com/ for API and information, and https://github.com/tigerbeetle/tigerbeetle for actual implementation. In this project. I want to:
>
> 1. Augment the Redis API to have a special command (e.g. `TB`) and then run tiger beetle commands.
> 2. Augment the network so that the tiger_beetle's native repl can directly interact with the brpc layer (i.e. make eloqkv wire protocol compatible with tigerbeetle).
> 3. Look at symbolic link `external/lua_beetle`, which tries to implement this (but with lua script). This project may be faulty (but have a rough skeleton correctness). See if there are any issues with the lua script if run under eloqkv (or redis, for that matter, but without persistency and transaction part).
> 4. I have a symbolic `external/tiger_beetle` that checked out the git repo of tigerbeetle, you can use it to examine the code locally.
> 5. I also have a `external/bin` that have the executables of tigerbeetle and dragonflydb, as well as redis. You can use these as the official release of the executables.

### Clarifications Made During Planning

The following decisions were reached through Q&A before implementation began:

| Question | Decision |
|---|---|
| Reuse lua_beetle or implement C++ natively? | **Native C++ in EloqKV.** lua_beetle is reference only. |
| Wire protocol encoding for TB command? | **Both.** `TB` = text key=value format; `TB_BIN` = binary 128-byte blobs. |
| Goal 2 (VSR wire protocol) — when? | **Deferred.** Too large for this iteration; requires a separate brpc listener and VSR framing. |
| Storage: dedicated data structure vs. Redis-style string keys? | **Option A: Redis string keys** (`account:{id}` → 128-byte blob). Simple, compatible with lua_beetle's schema, reuses EloqKV's existing string infrastructure. |
| Transfer history index: string APPEND vs. sorted set? | **Sorted set** (score = timestamp in microseconds). Enables O(log N) range queries for `GET_ACCOUNT_TRANSFERS`, unlike lua_beetle's O(N) APPEND approach. |
| Atomicity model: EloqKV BEGIN/COMMIT or TB-native linked batches? | **TB-native linked batches.** Each `TB`/`TB_BIN` call is self-contained; atomicity is per linked chain, not exposed as a BEGIN/COMMIT to the caller. |
| Redis vs. DragonflyDB as correctness standard for lua_beetle audit? | **Redis is the golden standard.** DragonflyDB is secondary. |

---

## Reference Codebases and Executables

All reference material lives under `/home/lintaoz/work/eloqkv/external/` and `/home/lintaoz/work/lua_beetle/`. This section is a self-contained map so any reader can find the canonical source of truth for every part of the system.

### TigerBeetle — Canonical Financial Engine

> Original brief refers to this as `external/tiger_beetle` (with underscore). On disk it is `external/tigerbeetle` (no underscore).

**Source tree:** `/home/lintaoz/work/eloqkv/external/tigerbeetle/` (Zig codebase, version 0.16.78+c3d9b09)

This is the reference implementation for all data layouts, semantics, error codes, and API behaviour. Key paths within it:

| Path | What it defines |
|---|---|
| `src/state_machine.zig` | Core create_accounts / create_transfers business logic — the ground truth for validation rules, flag semantics, two-phase transfer flow, linked-batch atomicity |
| `src/tigerbeetle/` | CLI entry point (`main.zig`), REPL, benchmarks |
| `src/vsr/` | VSR consensus protocol (relevant to Goal 2, currently deferred) |
| `src/clients/dotnet/TigerBeetle/Bindings.cs` | **Most useful for error codes** — C# enum with exact numeric values for every `CreateAccountResult` and `CreateTransferResult` error code, with doc links |
| `src/clients/go/` | Go client (`bindings.go`, `errors.go`) — useful for binary framing and batch semantics |
| `src/clients/c/` | C ABI client — defines the 128-byte struct layouts in portable C structs |
| `src/clients/python/`, `node/`, `java/`, `rust/` | Additional client bindings for reference |

**Binary:** `/home/lintaoz/work/eloqkv/external/bin/tigerbeetle` (v0.16.78)

Used to run a live TigerBeetle node for golden-standard correctness testing.

### Redis — Golden Standard for lua_beetle Testing

**Binary:** `/home/lintaoz/work/eloqkv/external/bin/redis-server` (v8.6.2)

Redis is the primary correctness target for the lua_beetle Lua scripts. "Works on Redis" is the definition of correct for lua_beetle. Also available:
- `/home/lintaoz/work/eloqkv/external/bin/redis-cli` — interactive client
- `/home/lintaoz/work/eloqkv/external/bin/redis-benchmark` — benchmarking tool

### DragonflyDB — Secondary Compatibility Target

**Binary:** `/home/lintaoz/work/eloqkv/external/bin/dragonfly-x86_64` (v1.37.2)

Redis-compatible drop-in replacement. Used to verify lua_beetle scripts work beyond just Redis. Tertiary compatibility target after Redis and EloqKV.

### lua_beetle — Lua Reference Implementation

> Original brief refers to this as `external/lua_beetle`. On disk it is a symlink: `external/lua_beetle` → `/home/lintaoz/work/lua_beetle/`.

**Location:** `/home/lintaoz/work/lua_beetle/` (also accessible as `external/lua_beetle` from within the eloqkv repo)

Pure Lua scripts that implement TigerBeetle semantics on top of standard Redis commands (`GET`, `SET`, `EXISTS`, `APPEND`). Each operation is a self-contained `EVAL` script. Intended as a readable reference for the binary format, validation logic, and two-phase transfer handling — not as a production implementation.

```
lua_beetle/
├── scripts/
│   ├── common.lua                  # Shared helpers: lb_slice16, lb_hex16, lb_has_flag,
│   │                               #   lb_add_u128, lb_compare_u128, lb_encode_u64,
│   │                               #   lb_decode_u64, lb_result (result encoder), lb_zero_16
│   ├── create_account.lua          # Single account creation (128-byte binary input)
│   ├── create_linked_accounts.lua  # Batch account creation with LINKED chain support
│   ├── create_transfer.lua         # Single transfer (direct / pending / post / void)
│   ├── create_linked_transfers.lua # Batch transfer creation with LINKED chain support
│   ├── lookup_account.lua          # Lookup one or more accounts by ID
│   ├── lookup_transfer.lua         # Lookup one or more transfers by ID
│   ├── get_account_transfers.lua   # Filtered transfer history for an account
│   └── get_account_balances.lua    # Balance snapshot history for an account
└── tests/
    ├── common.go                   # Go test constants: error codes, flag values
    ├── functional_tests.go         # Go functional test suite
    ├── functional_tests.py         # Python functional test suite
    ├── tigerbeetle_stress.go       # Stress test against native TigerBeetle
    └── luabeetle_stress.go         # Stress test against lua_beetle
```

### EloqKV — This Codebase

**Location:** `/home/lintaoz/work/eloqkv/`

Redis-compatible distributed ACID database built on [brpc](https://github.com/apache/brpc). The TB/TB_BIN commands are implemented here in C++.

```
eloqkv/
├── include/
│   ├── tb_types.h          # NEW: TigerBeetle type definitions
│   ├── tb_handler.h        # NEW: TB/TB_BIN command handler declarations
│   ├── redis_command.h     # MODIFIED: added TB, TB_BIN to RedisCommandType enum
│   └── redis_handler.h     # Existing handler base classes (unchanged)
├── src/
│   ├── tb_types.cpp        # NEW: Binary encode/decode, key builders, text parsers
│   ├── tb_handler.cpp      # NEW: Full TB/TB_BIN command logic
│   ├── redis_service.cpp   # MODIFIED: registered TbCommandHandler and TbBinCommandHandler
│   └── redis_handler.cpp   # Existing handlers (unchanged)
├── external/
│   ├── bin/                # Executables: redis-server, redis-cli, dragonfly-x86_64, tigerbeetle
│   └── tigerbeetle/        # TigerBeetle source tree (Zig, v0.16.78)
└── CMakeLists.txt          # MODIFIED: added tb_types.cpp, tb_handler.cpp
```

---

## Goals

### Goal 1 — Native TB/TB_BIN Commands in EloqKV (implemented)

Add two new Redis-compatible commands to EloqKV:

- **`TB`** — text key=value format (human-readable, `TB CREATE_ACCOUNT id=1 ledger=700 code=10`)
- **`TB_BIN`** — binary format (128-byte TigerBeetle-native struct blobs)

Both expose TigerBeetle's full double-entry bookkeeping API:
- Account lifecycle: `CREATE_ACCOUNT`, `CREATE_ACCOUNTS`, `LOOKUP_ACCOUNT`
- Transfer lifecycle: `CREATE_TRANSFER`, `CREATE_TRANSFERS`, `LOOKUP_TRANSFER`
- History queries: `GET_ACCOUNT_TRANSFERS`, `GET_ACCOUNT_BALANCES`

Implementation is native C++ inside EloqKV — not a Lua script bridge.

### Goal 2 — TigerBeetle VSR Wire Protocol (deferred)

Making EloqKV's brpc layer wire-protocol-compatible with TigerBeetle's native REPL is deferred; requires a separate listening port and VSR framing.

### Goal 3 — lua_beetle Correctness Audit (implemented)

Audit `/home/lintaoz/work/lua_beetle/scripts/` for bugs. Redis (`external/bin/redis-server`) is the golden standard — "correct" means "matches Redis behaviour". TigerBeetle's source (`external/tigerbeetle/src/state_machine.zig` and `Bindings.cs`) is authoritative for error codes and API semantics. Fix all confirmed bugs.

### Goal 4 — Reference Material (passive, no implementation required)

The external references are read-only inputs:
- **`external/tigerbeetle/`** — canonical TigerBeetle Zig source for semantics and error codes
- **`external/bin/tigerbeetle`** — live binary for integration testing
- **`external/bin/redis-server`** — golden standard for lua_beetle correctness
- **`external/bin/dragonfly-x86_64`** — secondary compatibility target
- **`/home/lintaoz/work/lua_beetle/`** — Lua reference scripts (audited and fixed in Goal 3)

---

## Project Context

### EloqKV Architecture

EloqKV is a Redis-compatible distributed ACID database built on [brpc](https://github.com/apache/brpc). Its command dispatch model:

```
brpc Redis listener
    └─ RedisServiceImpl::ProcessCommand()
           └─ looks up handler by command name
                  └─ RedisCommandHandler::Run(ctx, args, output, flush_batched)
```

Key patterns used throughout the codebase:

| Pattern | Description |
|---|---|
| `RedisCommandHandler` | Base class for all command handlers, subclassed per command |
| `AddCommandHandler(name, handler*)` | Registers a handler in `RedisServiceImpl::Init()` |
| `redis_impl->NewTxm(iso, cc)` | Opens an internal transaction |
| `redis_impl->GenericCommand(ctx, txm, args, &output)` | Executes a Redis sub-command within a txm (auto_commit=false) |
| `CommitTx(txm)` / `AbortTx(txm)` | Commits or rolls back a txm |
| `brpc::RedisReply` | Reply object with `SetString`, `SetNullString`, `SetArray`, `SetError`, `SetStatusCode` |
| `OutputHandler` | Abstract result capture class used with `GenericCommand` |

Existing handlers follow this pattern (e.g., `GetCommandHandler`, `SetCommandHandler`, `EvalCommandHandler`).

### lua_beetle Reference Implementation

Located at `/home/lintaoz/work/lua_beetle/`. Pure Lua scripts loaded into Redis via `EVAL`/`EVALSHA`. Each operation is a self-contained atomic Lua script. The scripts use binary 128-byte blobs identical to TigerBeetle's native struct layout (little-endian).

---

## Design Decisions

### Storage Layout

Accounts and transfers are stored as binary blobs using EloqKV's existing string key infrastructure. Key formats are compatible with lua_beetle so the two implementations can share data.

| Data | Key | Value |
|---|---|---|
| Account | `account:{16 raw bytes of ID}` | 128-byte TbAccount blob (little-endian) |
| Transfer | `transfer:{32 hex chars of ID}` | 128-byte TbTransfer blob |
| Transfer history index | `account:{16 raw bytes}:transfers` | Redis sorted set (score=timestamp_μs, member=32-hex-ID) |
| Balance history | `account:{16 raw bytes}:balance_history` | Redis list (RPUSH 128-byte TbAccountBalance blobs) |

**Why sorted set for transfer history?** The lua_beetle reference uses string APPEND (O(1) write, O(N) range scan). A sorted set gives O(log N) range queries by timestamp, which is required for `GET_ACCOUNT_TRANSFERS` with `timestamp_min`/`timestamp_max` filters and `limit`.

**Why microsecond ZADD scores?** Nanosecond timestamps (~1.7×10¹⁸) exceed IEEE 754 double precision (2⁵³ ≈ 9×10¹⁵). Dividing by 1000 gives microsecond scores (~1.7×10¹²), safely representable without loss.

### Atomicity

Each `TB`/`TB_BIN` call opens its own internal EloqKV transaction (`NewTxm` + `CommitTx`/`AbortTx`). This gives atomic semantics for an entire linked-chain batch without exposing `BEGIN`/`COMMIT` to callers. Linked batches where any item fails are fully rolled back via `AbortTx`.

### Command Encoding

| Command | Format |
|---|---|
| `TB CREATE_ACCOUNT id=1 ledger=700 code=10 [flags=linked\|history] ...` | Space-separated key=value tokens; flags are pipe-separated names |
| `TB_BIN CREATE_ACCOUNT <128-byte-blob>` | Raw binary blob in ARGV |

Both commands share the same internal `tb_exec_*` functions.

---

## New Files Created

### `include/tb_types.h` — Type definitions (392 lines)

All TigerBeetle data types:

```
TbU128                   — 128-bit unsigned integer {lo, hi uint64_t}; arithmetic operators via __int128
AccountFlags namespace   — LINKED=0x0001, DEBITS_MUST_NOT_EXCEED_CREDITS=0x0002,
                           CREDITS_MUST_NOT_EXCEED_DEBITS=0x0004, HISTORY=0x0008,
                           IMPORTED=0x0100, CLOSED=0x0200
TransferFlags namespace  — LINKED=0x0001, PENDING=0x0002, POST_PENDING=0x0004,
                           VOID_PENDING=0x0008, IMPORTED=0x0100
TbError enum (uint32_t)  — ~43 error codes matching TigerBeetle spec
TbAccount struct         — 128-byte layout matching TigerBeetle binary format
TbTransfer struct        — 128-byte layout matching TigerBeetle binary format
TbAccountBalance struct  — 128-byte layout for balance history snapshots
TbAccountFilter struct   — 128-byte layout for GET_ACCOUNT_TRANSFERS/BALANCES queries
```

Declarations for all encode/decode, key-builder, timestamp, and text-parser functions.

### `src/tb_types.cpp` — Serialization and parsing (904 lines)

- **`tb_encode_u16/32/64/128`** — little-endian write helpers
- **`tb_decode_u64/u128`** — little-endian read helpers
- **`tb_encode_account` / `tb_decode_account`** — Account ↔ 128-byte blob
- **`tb_encode_transfer` / `tb_decode_transfer`** — Transfer ↔ 128-byte blob
- **`tb_encode_account_balance`** — AccountBalance → 128-byte blob
- **`tb_decode_account_filter`** — AccountFilter ← 128-byte blob
- **`tb_account_key`** — builds `"account:" + 16 raw bytes`
- **`tb_transfer_key`** — builds `"transfer:" + 32-char hex`
- **`tb_account_transfers_key`** / **`tb_account_balances_key`** — index key builders
- **`tb_current_timestamp_ns`** — `system_clock::now()` in nanoseconds
- **`tb_parse_u128`** — decimal string → TbU128 using `unsigned __int128` for full precision
- **`tb_parse_account_text`** / **`tb_parse_transfer_text`** / **`tb_parse_account_filter_text`** — key=value token parsers
- **`tb_split_batch`** — splits arg vector at `","` separator tokens into sub-batches

### `include/tb_handler.h` — Handler declarations (219 lines)

```cpp
class CaptureOutputHandler : public OutputHandler {
    // Captures the first result from GenericCommand (string/int/nil/error)
    // Used for GET, EXISTS, SET return values
};

class ArrayCaptureOutputHandler : public OutputHandler {
    // Captures array of strings from GenericCommand
    // Used for ZRANGEBYSCORE, LRANGE return values
};

class TbCommandHandler : public RedisCommandHandler {
    brpc::RedisCommandHandlerResult Run(...) override;
    // Parses text key=value args, dispatches to tb_exec_* functions
};

class TbBinCommandHandler : public RedisCommandHandler {
    brpc::RedisCommandHandlerResult Run(...) override;
    // Decodes binary blob args, dispatches to same tb_exec_* functions
};
```

### `src/tb_handler.cpp` — Core logic (1291 lines)

Key internal functions:

```
tb_run_cmd(redis_impl, ctx, txm, args)
    Wraps GenericCommand, returns CaptureOutputHandler result

tb_do_create_account(redis_impl, ctx, txm, account)
    Validates account (id≠0, ledger≠0, code≠0, reserved=0, zero balances unless IMPORTED)
    Checks existence via GET → ERR_EXISTS if found
    Assigns server timestamp if not IMPORTED
    Writes via SET

tb_do_create_transfer(redis_impl, ctx, txm, transfer)
    Validates transfer fields (id≠0, debit≠credit, amount≠0, ledger≠0, code≠0)
    Checks transfer existence → ERR_EXISTS_WITH_DIFFERENT_FLAGS
    Loads both accounts → ERR_DEBIT/CREDIT_ACCOUNT_NOT_FOUND
    Validates ledgers match → ERR_LEDGER_MUST_MATCH
    Dispatches by flags:
      PENDING:      debits_pending+=amount, credits_pending+=amount
      POST_PENDING: looks up pending transfer, debits_pending-=amount,
                    debits_posted+=amount, credits_pending-=amount, credits_posted+=amount
      VOID_PENDING: debits_pending-=amount, credits_pending-=amount
      direct:       debits_posted+=amount, credits_posted+=amount
    Enforces balance constraints:
      DEBITS_MUST_NOT_EXCEED_CREDITS on debit account's own totals
      CREDITS_MUST_NOT_EXCEED_DEBITS on credit account's own totals
    Persists: SET transfer, SET debit account, SET credit account
    Indexes:  ZADD transfers sorted set (score=ns/1000)
    History:  RPUSH balance snapshots if HISTORY flag set

tb_exec_create_accounts(redis_impl, ctx, output, accounts)
    Iterates batch, splits at LINKED boundaries
    Each linked chain gets its own txm; failure → AbortTx + ERR_LINKED_EVENT_FAILED
    Non-linked items each get their own txm

tb_exec_create_transfers(redis_impl, ctx, output, transfers)
    Same linked-chain pattern as create_accounts

tb_exec_lookup_accounts / tb_exec_lookup_transfers
    Read-only txm, GET each ID, SetNullString() for missing, CommitTx

tb_exec_get_account_transfers(redis_impl, ctx, output, filter)
    ZRANGEBYSCORE or ZREVRANGEBYSCORE on the transfers sorted set
    Filters by debit/credit account flags, user_data, code

tb_exec_get_account_balances(redis_impl, ctx, output, filter)
    LRANGE on balance_history list
    Filters by timestamp range
```

---

## Modified Files

### `include/redis_command.h`

Added at end of `RedisCommandType` enum:

```cpp
// TigerBeetle financial commands
TB,
TB_BIN,
```

### `src/redis_service.cpp`

Added include:
```cpp
#include "tb_handler.h"
```

Added at end of `AddHandlers()`:
```cpp
auto &tb_hd = hd_vec_.emplace_back(std::make_unique<TbCommandHandler>(this));
AddCommandHandler("tb", tb_hd.get());
auto &tb_bin_hd = hd_vec_.emplace_back(std::make_unique<TbBinCommandHandler>(this));
AddCommandHandler("tb_bin", tb_bin_hd.get());
```

### `CMakeLists.txt`

Added to the source file list:
```cmake
src/tb_types.cpp
src/tb_handler.cpp
```

---

## lua_beetle Audit (Goal 3)

### Files Audited

| File | Lines | Purpose |
|---|---|---|
| `scripts/create_account.lua` | ~74 | Create single account |
| `scripts/create_linked_accounts.lua` | ~126 | Create batch of accounts with LINKED support |
| `scripts/create_transfer.lua` | ~311 | Create single transfer (all types) |
| `scripts/create_linked_transfers.lua` | ~446 | Create batch of transfers with LINKED support |
| `tests/common.go` | ~55 | Go test constants and helpers |

### Bugs Found and Fixed

#### Bug 1 — Hardcoded timestamp in all create scripts

**Files:** `create_account.lua:50`, `create_linked_accounts.lua:37`, `create_transfer.lua:51`, `create_linked_transfers.lua:37`

**Problem:** All non-IMPORTED records were stamped with the fixed value `1000000000000000000` ns (= 2001-09-09). This makes every record appear simultaneous, breaks time-ordered queries (`GET_ACCOUNT_TRANSFERS` with `timestamp_min/max`), and produces nonsensical balance history timelines.

The `create_linked_transfers.lua` even contained an acknowledged TODO comment:
```lua
-- TODO: EloqKV doesn't support TIME command in Lua scripts, using arbitrary timestamp
-- local timestamp = redis.call('TIME')
```

**Fix:** All four scripts now call `redis.call('TIME')` to obtain the actual server wall clock, converted to nanoseconds:

```lua
-- Before:
local DEFAULT_TIMESTAMP = lb_encode_u64(1000000000000000000)

-- After:
local time = redis.call('TIME')
local DEFAULT_TIMESTAMP = lb_encode_u64(tonumber(time[1]) * 1000000000 + tonumber(time[2]) * 1000)
```

Note: `redis.call('TIME')` returns `{seconds, microseconds}`. Multiplying seconds by 10⁹ and microseconds by 10³ yields nanosecond precision.

#### Bug 2 — Wrong error code for LINKED flag on single transfer

**File:** `create_transfer.lua:91`

**Problem:** When a single transfer (not part of a batch) has the `LINKED` flag set, the chain is unclosed. The script returned error code `1` with the comment `ERR_LINKED_EVENT_CHAIN_OPEN`. However, per the TigerBeetle spec (confirmed from `external/tigerbeetle/src/clients/dotnet/TigerBeetle/Bindings.cs`):

```
LinkedEventFailed    = 1   (a transfer failed because a linked predecessor failed)
LinkedEventChainOpen = 2   (LINKED flag on last item — chain has no terminator)
```

Code `1` is `LinkedEventFailed`, not `LinkedEventChainOpen`. A single transfer with LINKED flag should return `2`.

**Fix:**
```lua
-- Before:
return lb_result(1) -- ERR_LINKED_EVENT_CHAIN_OPEN

-- After:
return lb_result(2) -- ERR_LINKED_EVENT_CHAIN_OPEN
```

#### Bug 3 — Wrong test constant in common.go

**File:** `tests/common.go:17`

**Problem:** `ErrLinkedEventChainOpen = 1` was incorrect per TigerBeetle's spec. `LinkedEventChainOpen` is code `2`; code `1` is `LinkedEventFailed`.

**Fix:**
```go
// Before:
ErrLinkedEventChainOpen = 1

// After:
ErrLinkedEventFailed    = 1
ErrLinkedEventChainOpen = 2
```

### Non-Bugs Investigated

The following were investigated and confirmed correct (not bugs):

- **Balance constraint variable usage:** `create_transfer.lua` lines 213–247 correctly read each account's own balance fields — `new_debit_account` is used for the `DEBITS_MUST_NOT_EXCEED_CREDITS` check, and `new_credit_account` for `CREDITS_MUST_NOT_EXCEED_DEBITS`. The plan initially flagged these as bugs; they are not.

- **POST_PENDING/VOID_PENDING account assembly:** The binary string slice-and-concatenate operations correctly update only the relevant 16-byte fields within each 128-byte account blob.

- **Linked chain rollback in create_linked_transfers.lua:** The rollback mechanism — `DEL` transfer records, restore account blobs via `modified_accounts` dict, truncate index strings via `GETRANGE+SET` — is logically correct for the Redis string-APPEND index design.

- **encode_account_balance timestamp:** Uses the transfer's timestamp for the balance snapshot, which is the correct TigerBeetle semantic (snapshot timestamp = timestamp of the causative transfer).

- **Unclosed chain at end of batch (create_linked_accounts.lua:121, create_linked_transfers.lua:440):** Both correctly return code `2` for `ERR_LINKED_EVENT_CHAIN_OPEN`.

---

## Build Verification

After all changes, the build was run:

```
cmake --build build --target eloqkv -j4
```

**Result:** `tb_types.cpp.o` and `tb_handler.cpp.o` compiled without errors or warnings. Final link failed on `libtxservice.a` — this is a pre-existing build dependency issue (txservice must be built separately first) unrelated to the new code.

---

## Command Reference (TB text format)

```redis
# Account operations
TB CREATE_ACCOUNT id=1 ledger=700 code=10
TB CREATE_ACCOUNT id=2 ledger=700 code=10 flags=history|debits_must_not_exceed_credits
TB CREATE_ACCOUNTS id=1 ledger=700 code=10 , id=2 ledger=700 code=10
TB LOOKUP_ACCOUNT id=1
TB LOOKUP_ACCOUNT id=1,id=2

# Transfer operations
TB CREATE_TRANSFER id=100 debit_account_id=1 credit_account_id=2 amount=500 ledger=700 code=1
TB CREATE_TRANSFER id=101 debit_account_id=1 credit_account_id=2 amount=200 ledger=700 code=1 flags=pending
TB CREATE_TRANSFER id=102 debit_account_id=1 credit_account_id=2 amount=200 ledger=700 code=1 flags=post_pending pending_id=101
TB CREATE_TRANSFER id=103 debit_account_id=1 credit_account_id=2 amount=200 ledger=700 code=1 flags=void_pending pending_id=101
TB LOOKUP_TRANSFER id=100

# History queries
TB GET_ACCOUNT_TRANSFERS account_id=1 flags=debits limit=10
TB GET_ACCOUNT_TRANSFERS account_id=1 flags=credits|reversed limit=5
TB GET_ACCOUNT_BALANCES account_id=1 limit=10
```

---

## File Summary

| File | Status | Lines | Purpose |
|---|---|---|---|
| `include/tb_types.h` | New | 392 | TigerBeetle type definitions, error codes, function declarations |
| `src/tb_types.cpp` | New | 904 | Binary encode/decode, key builders, timestamp, text parsers |
| `include/tb_handler.h` | New | 219 | Handler class declarations, CaptureOutputHandler |
| `src/tb_handler.cpp` | New | 1291 | Full command logic for all TB subcommands |
| `include/redis_command.h` | Modified | +2 lines | Added TB, TB_BIN to RedisCommandType enum |
| `src/redis_service.cpp` | Modified | +5 lines | Registered TbCommandHandler and TbBinCommandHandler |
| `CMakeLists.txt` | Modified | +2 lines | Added tb_types.cpp and tb_handler.cpp to source list |
| `lua_beetle/scripts/create_account.lua` | Modified | ±2 lines | Fix hardcoded timestamp |
| `lua_beetle/scripts/create_linked_accounts.lua` | Modified | ±2 lines | Fix hardcoded timestamp |
| `lua_beetle/scripts/create_transfer.lua` | Modified | ±3 lines | Fix hardcoded timestamp + wrong LINKED error code |
| `lua_beetle/scripts/create_linked_transfers.lua` | Modified | ±2 lines | Fix hardcoded timestamp |
| `lua_beetle/tests/common.go` | Modified | +1 line | Fix ErrLinkedEventChainOpen constant value |

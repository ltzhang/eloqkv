# Lua Beetle Integration Tests

Tests TigerBeetle semantics across four backends:
**Redis**, **DragonflyDB**, **TigerBeetle** (native), and **EloqKV**.

Two test suites:
1. **lua_beetle** — Lua EVALSHA scripts implementing TigerBeetle semantics on Redis
2. **EloqKV native TB/TB_BIN** — EloqKV's built-in commands (no Lua layer)

## Directory Layout

```
tests/lua_beetle/
├── sanity_test.py              Python sanity test via lua_beetle EVALSHA
├── sanity_test_eloqkv_tb.py   Python sanity test via EloqKV TB/TB_BIN commands
├── stress_test_eloqkv_tb.py   Python stress test: TB_BIN vs EVALSHA comparison
├── tb_sanity/
│   ├── main.go                 Go sanity test (TigerBeetle native API)
│   └── go.mod
├── run_tests.sh                Master test runner (all backends, sanity + stress)
├── results/                    Per-run output logs (auto-created)
└── README.md                   This file
```

The lua_beetle stress test reuses the existing Go framework in `lua_beetle/tests/`.

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Python 3.8+ with `redis` package | `pip install redis` |
| Go 1.22+ | For TB sanity test and stress binary |
| `redis-server` in `$PATH` | Standard Redis |
| `external/bin/dragonfly-x86_64` | DragonflyDB binary |
| `external/bin/tigerbeetle` | TigerBeetle binary |
| `build/eloqkv` | EloqKV binary (built with `make -j6` in `build/`) |
| `lua_beetle/scripts/` | Lua beetle scripts |

---

## Sanity Test

Tests 20 semantic cases covering the full TigerBeetle feature surface.

### Against Redis / DragonflyDB / EloqKV

```bash
# Redis (port 6379)
python3 sanity_test.py --host localhost --port 6379 --backend Redis

# DragonflyDB (port 6380)
python3 sanity_test.py --host localhost --port 6380 --backend DragonflyDB

# EloqKV (port 6379)
python3 sanity_test.py --host localhost --port 6379 --backend EloqKV

# Custom scripts path
python3 sanity_test.py --port 6379 --backend Redis \
    --scripts /path/to/lua_beetle/scripts
```

**All options:**

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `localhost` | Server host |
| `--port` | `6379` | Server port |
| `--backend` | `Redis` | Label for display |
| `--scripts` | `/home/lintaoz/work/lua_beetle/scripts` | Path to lua scripts |

### Against TigerBeetle (Go native client)

```bash
cd tb_sanity
go mod download
go run . --address 3000
```

| Flag | Default | Description |
|------|---------|-------------|
| `--address` | `3000` | TigerBeetle address (port or host:port) |

### Sanity Test Cases

| # | Test Case |
|---|-----------|
| 1 | Create account — basic fields and server-assigned timestamp |
| 2 | Duplicate account rejected (ID_ALREADY_EXISTS) |
| 3 | Linked accounts — success path (all-or-nothing) |
| 4 | Linked accounts — rollback on duplicate in chain |
| 5 | Simple direct transfer, balance verification |
| 6 | Transfer with nonexistent debit account |
| 7 | Transfer with nonexistent credit account |
| 8 | Two-phase: PENDING (amounts in pending, not posted) |
| 9 | Two-phase: PENDING → POST (amounts move to posted) |
| 10 | Two-phase: PENDING → VOID (amounts cleared, nothing posted) |
| 11 | Double-post rejected (PENDING_TRANSFER_ALREADY_POSTED) |
| 12 | Void-after-post rejected (PENDING_TRANSFER_ALREADY_POSTED) |
| 13 | Balance constraint DEBITS_MUST_NOT_EXCEED_CREDITS enforced |
| 14 | Balance constraint CREDITS_MUST_NOT_EXCEED_DEBITS enforced |
| 15 | Linked transfers — success path |
| 16 | Linked transfers — rollback on failure mid-chain |
| 17 | Lookup transfer by ID |
| 18 | GET_ACCOUNT_TRANSFERS — count, filter by debit/credit, limit |
| 19 | GET_ACCOUNT_BALANCES — cumulative snapshots, HISTORY flag |
| 20 | Multiple transfers — cumulative balance accumulation |

---

## Stress Test

Uses the existing Go stress framework in `lua_beetle/tests/`.

```bash
# Build the stress binary (one-time)
cd /home/lintaoz/work/lua_beetle/tests
go build -o /tmp/stress_test .

# Redis
/tmp/stress_test -mode=redis -workload=mixed \
    -accounts=10000 -hot-accounts=100 \
    -workers=4 -duration=60 -batch=100 \
    -transfer-ratio=0.7 -twophase-ratio=0.2

# DragonflyDB
/tmp/stress_test -mode=dragonfly -workload=mixed \
    -accounts=10000 -hot-accounts=100 \
    -workers=4 -duration=60 -batch=100 \
    -transfer-ratio=0.7 -twophase-ratio=0.2

# EloqKV
/tmp/stress_test -mode=eloqkv -workload=mixed \
    -accounts=10000 -hot-accounts=100 \
    -workers=4 -duration=60 -batch=100 \
    -transfer-ratio=0.7 -twophase-ratio=0.2

# TigerBeetle
/tmp/stress_test -mode=tigerbeetle -tb-address=3000 \
    -workload=mixed \
    -accounts=10000 -hot-accounts=100 \
    -workers=4 -duration=60 -batch=100 \
    -transfer-ratio=0.7 -twophase-ratio=0.2
```

**All stress test flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `-mode` | `redis` | `redis`, `dragonfly`, `eloqkv`, `tigerbeetle`, `all` |
| `-accounts` | `10000` | Total accounts to create |
| `-hot-accounts` | `100` | Hot accounts (frequent access) |
| `-workers` | `10` | Concurrent goroutines |
| `-duration` | `60` | Test duration in seconds |
| `-batch` | `100` | Operations per batch |
| `-workload` | `transfer` | `transfer`, `lookup`, `twophase`, `mixed` |
| `-transfer-ratio` | `0.7` | Mixed: fraction that are transfers |
| `-twophase-ratio` | `0.1` | Mixed: fraction of transfers that are two-phase |
| `-tb-address` | `3000` | TigerBeetle port / address |
| `-verbose` | `false` | Enable verbose output |
| `-no-cleanup` | `false` | Skip FLUSHDB after run |

---

## Run All Tests (Master Script)

```bash
./run_tests.sh [options]
```

The script starts and cleanly stops each backend before moving to the next.

```bash
# Full run (sanity + stress, all backends, 30s stress)
./run_tests.sh

# Sanity only
./run_tests.sh --sanity-only

# Stress only, longer duration, more workers
./run_tests.sh --stress-only --duration 120 --workers 8

# Single backend
./run_tests.sh --backend redis
./run_tests.sh --backend dragonfly
./run_tests.sh --backend tigerbeetle
./run_tests.sh --backend eloqkv

# Custom stress workload
./run_tests.sh --workload transfer --duration 60 --workers 4
```

| Flag | Default | Description |
|------|---------|-------------|
| `--sanity-only` | — | Skip stress tests |
| `--stress-only` | — | Skip sanity tests |
| `--backend NAME` | all | Limit to one backend |
| `--duration N` | `30` | Stress duration (seconds) |
| `--workers N` | `4` | Stress concurrency |
| `--accounts N` | `10000` | Stress accounts |
| `--batch N` | `100` | Stress batch size |
| `--workload TYPE` | `mixed` | Stress workload type |

Results are saved in `results/run_<timestamp>/`.

---

## EloqKV Native TB / TB_BIN Tests

These tests exercise EloqKV's built-in `TB` (text format) and `TB_BIN` (binary
format) commands directly — no Lua scripts required.  They verify the same
TigerBeetle semantics as the lua_beetle suite, plus additional cases unique to
the native protocol.

### Sanity Test (24 cases)

```bash
# Run against a live EloqKV on port 6379
python3 sanity_test_eloqkv_tb.py

# Custom host/port
python3 sanity_test_eloqkv_tb.py --host localhost --port 6379
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `localhost` | EloqKV host |
| `--port` | `6379` | EloqKV port |

**Test cases (24):**

| # | Test |
|---|------|
| 1 | Create account — TB text format, TB_BIN lookup |
| 2 | Create account — TB_BIN format, TB text lookup |
| 3 | Duplicate account rejected (EXISTS=21) |
| 4 | Linked accounts — success path |
| 5 | Linked accounts — rollback when chain item fails |
| 6 | Simple direct transfer, balance verification |
| 7 | Transfer: nonexistent debit account (error 21) |
| 8 | Transfer: nonexistent credit account (error 22) |
| 9 | Two-phase: PENDING (amounts in pending) |
| 10 | Two-phase: PENDING → POST (amounts posted) |
| 11 | Two-phase: PENDING → VOID (amounts cleared) |
| 12 | Double-post rejected (PENDING_TRANSFER_ALREADY_POSTED=33) |
| 13 | Void-after-post rejected (33 or 34) |
| 14 | Balance constraint DEBITS_MUST_NOT_EXCEED_CREDITS (error 54) |
| 15 | Balance constraint CREDITS_MUST_NOT_EXCEED_DEBITS (error 55) |
| 16 | Linked transfers — success path |
| 17 | Linked transfers — rollback on failure mid-chain |
| 18 | Lookup transfer by ID via TB_BIN |
| 19 | GET_ACCOUNT_TRANSFERS — count, debit/credit filter |
| 20 | GET_ACCOUNT_BALANCES — HISTORY flag required |
| 21 | Multiple transfers — cumulative balance accumulation |
| 22 | Batch CREATE_TRANSFERS — 20 transfers in one TB_BIN call |
| 23 | TB text format: CREATE_TRANSFER with key=value args |
| 24 | TB text format: GET_ACCOUNT_TRANSFERS |

### Stress Test (TB_BIN vs EVALSHA)

Benchmarks EloqKV's native `TB_BIN CREATE_TRANSFERS` (batch mode) against
`lua_beetle` EVALSHA (pipelined, one call per transfer).

```bash
# Full comparison (both modes, 20s each)
python3 stress_test_eloqkv_tb.py \
    --duration 20 --workers 4 --batch-size 100 --accounts 1000 \
    --scripts /path/to/lua_beetle/scripts

# TB_BIN only
python3 stress_test_eloqkv_tb.py --mode tbbin --duration 30

# EVALSHA only
python3 stress_test_eloqkv_tb.py --mode evalsha \
    --scripts /path/to/lua_beetle/scripts
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `localhost` | EloqKV host |
| `--port` | `6379` | EloqKV port |
| `--duration` | `20` | Seconds per mode |
| `--workers` | `4` | Concurrent threads |
| `--batch-size` | `100` | Transfers per TB_BIN call (or pipeline) |
| `--accounts` | `1000` | Pre-created accounts |
| `--scripts` | `/home/lintaoz/work/lua_beetle/scripts` | lua_beetle scripts dir |
| `--mode` | `both` | `tbbin`, `evalsha`, or `both` |

**Example output:**

```
TB_BIN native batch mode  (batch=100, workers=4)
  Throughput:      3,470 transfers/s
  Latency p50:     108.8 ms  p99: 215.1 ms

lua_beetle EVALSHA mode   (batch=100 pipelined, workers=4)
  Throughput:      1,870 transfers/s
  Latency p50:     205.8 ms  p99: 303.5 ms

TB_BIN speedup:  1.86x
```

TB_BIN achieves higher throughput because each batch call processes 100
transfers in a single server-side transaction, while EVALSHA processes each
transfer independently (one EVALSHA per transfer) even when pipelined.

### Binary Protocol Reference

**Account blob (128 bytes, little-endian):**

| Offset | Size | Field |
|--------|------|-------|
| 0 | 16 | id (u128) |
| 16 | 16 | debits_pending (u128) |
| 32 | 16 | debits_posted (u128) |
| 48 | 16 | credits_pending (u128) |
| 64 | 16 | credits_posted (u128) |
| 80 | 16 | user_data_128 (u128) |
| 96 | 8 | user_data_64 (u64) |
| 104 | 4 | user_data_32 (u32) |
| 108 | 4 | reserved (must be 0) |
| 112 | 4 | ledger (u32) |
| 116 | 2 | code (u16) |
| 118 | 2 | flags (u16) |
| 120 | 8 | timestamp (u64) |

**Transfer blob (128 bytes, little-endian):** same layout with debit/credit
account IDs at offsets 16/32, amount at 48, pending_id at 64, timeout at 108.

**AccountFilter blob (128 bytes) for GET_ACCOUNT_TRANSFERS/BALANCES:**

| Offset | Size | Field |
|--------|------|-------|
| 0 | 16 | account_id (u128) |
| 16 | 16 | user_data_128 (u128) |
| 32 | 8 | user_data_64 (u64) |
| 40 | 4 | user_data_32 (u32) |
| 44 | 2 | code (u16) |
| 46 | 58 | reserved (zeros) |
| 104 | 8 | timestamp_min (u64) |
| 112 | 8 | timestamp_max (u64, 0=no limit) |
| 120 | 4 | limit (u32) |
| 124 | 4 | flags (u32: DEBITS=1, CREDITS=2, REVERSED=4) |

**TB_BIN LOOKUP_ACCOUNTS/LOOKUP_TRANSFERS:** each argument is a 16-byte ID blob
(`struct.pack("<QQ", id_val, 0)` for small IDs). Returns one slot per requested
ID: the 128-byte blob if found, or nil if not found.

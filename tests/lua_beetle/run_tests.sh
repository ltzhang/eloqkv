#!/usr/bin/env bash
# =============================================================================
# Lua Beetle Integration Test Runner
# =============================================================================
#
# Runs the sanity test and the stress test against all four backends:
#   Redis, DragonflyDB, TigerBeetle, EloqKV
#
# Usage:
#   ./run_tests.sh [options]
#
# Options:
#   --sanity-only     Run only the sanity test (skip stress)
#   --stress-only     Run only the stress test (skip sanity)
#   --backend NAME    Run only one backend: redis | dragonfly | tigerbeetle | eloqkv
#   --duration N      Stress test duration in seconds (default: 30)
#   --workers N       Number of concurrent workers for stress test (default: 4)
#   --accounts N      Number of accounts for stress test (default: 10000)
#   --batch N         Batch size for stress test (default: 100)
#   --workload TYPE   transfer | lookup | twophase | mixed (default: mixed)
#   --help            Show this help
#
# Requirements:
#   - Python 3.8+ with redis package  (pip install redis)
#   - Go 1.22+
#   - redis-server in PATH
#   - /home/lintaoz/work/eloqkv/external/bin/dragonfly-x86_64
#   - /home/lintaoz/work/eloqkv/external/bin/tigerbeetle
#   - /home/lintaoz/work/eloqkv/build/eloqkv
#
# Output:
#   Logs and results are written to ./results/run_<timestamp>/
#   Final summary is written to ../../test_report.md (tests/test_report.md)
# =============================================================================

set -euo pipefail

# ---- Paths -------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LUA_BEETLE_SCRIPTS="/home/lintaoz/work/lua_beetle/scripts"
LUA_BEETLE_TESTS="/home/lintaoz/work/lua_beetle/tests"
ELOQKV_BUILD="/home/lintaoz/work/eloqkv/build"
EXTERNAL_BIN="/home/lintaoz/work/eloqkv/external/bin"
TB_SANITY_DIR="$SCRIPT_DIR/tb_sanity"

REDIS_BIN="$(command -v redis-server)"
REDIS_CLI="$(command -v redis-cli)"
DRAGONFLY_BIN="$EXTERNAL_BIN/dragonfly-x86_64"
TIGERBEETLE_BIN="$EXTERNAL_BIN/tigerbeetle"
ELOQKV_BIN="$ELOQKV_BUILD/eloqkv"

TB_DATA_DIR="/tmp/tb_test_$$"
TB_FILE="$TB_DATA_DIR/0_0.tigerbeetle"
TB_PORT=3000
REDIS_PORT=6379
DRAGONFLY_PORT=6380

RESULTS_DIR="$SCRIPT_DIR/results/run_$(date +%Y%m%d_%H%M%S)"
REPORT_FILE="$SCRIPT_DIR/../../test_report.md"   # tests/test_report.md

# ---- Defaults ----------------------------------------------------------------
RUN_SANITY=true
RUN_STRESS=true
BACKEND_FILTER=""
STRESS_DURATION=30
STRESS_WORKERS=4
STRESS_ACCOUNTS=10000
STRESS_BATCH=100
STRESS_WORKLOAD="mixed"

# ---- Argument parsing --------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sanity-only) RUN_STRESS=false ;;
        --stress-only) RUN_SANITY=false ;;
        --backend)     BACKEND_FILTER="$2"; shift ;;
        --duration)    STRESS_DURATION="$2"; shift ;;
        --workers)     STRESS_WORKERS="$2"; shift ;;
        --accounts)    STRESS_ACCOUNTS="$2"; shift ;;
        --batch)       STRESS_BATCH="$2"; shift ;;
        --workload)    STRESS_WORKLOAD="$2"; shift ;;
        --help)
            head -40 "$0" | grep '^#' | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
    shift
done

mkdir -p "$RESULTS_DIR" "$TB_DATA_DIR"

# ---- Logging -----------------------------------------------------------------
LOG_FILE="$RESULTS_DIR/run.log"
log()  { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*"; }

# ---- Summary tracking --------------------------------------------------------
declare -A SANITY_RESULT   # backend → "PASS" | "FAIL" | "SKIP"
declare -A STRESS_OPS      # backend → ops/sec
declare -A STRESS_LATENCY  # backend → avg latency ms
declare -A STRESS_SUCCESS  # backend → success rate %

# ---- Process management ------------------------------------------------------
kill_port() {
    # Kill any process listening on a TCP port
    local port="$1"
    local pids
    pids=$(ss -tlnp "sport = :$port" 2>/dev/null | awk 'NR>1 {print $7}' \
          | grep -oP 'pid=\K[0-9]+' || true)
    if [[ -n "$pids" ]]; then
        kill -9 $pids 2>/dev/null || true
    fi
    sleep 0.5
}

kill_all_backends() {
    info "Killing all backends..."
    pkill -9 -x redis-server     2>/dev/null || true
    pkill -9 -x eloqkv           2>/dev/null || true
    pkill -9 -x dragonfly-x86_64 2>/dev/null || true
    pkill -9 -x tigerbeetle      2>/dev/null || true
    sleep 1
}

wait_for_port() {
    local host="$1" port="$2" timeout="${3:-10}" label="${4:-service}"
    local elapsed=0
    while ! $REDIS_CLI -h "$host" -p "$port" ping &>/dev/null 2>&1; do
        sleep 0.5
        elapsed=$((elapsed + 1))
        if [[ $elapsed -ge $((timeout * 2)) ]]; then
            err "$label did not become ready on $host:$port after ${timeout}s"
            return 1
        fi
    done
}

wait_for_tb() {
    local port="$1" timeout="${2:-15}"
    local elapsed=0
    # TigerBeetle speaks its own protocol, just check the port opens
    while ! bash -c "echo > /dev/tcp/localhost/$port" 2>/dev/null; do
        sleep 0.5
        elapsed=$((elapsed + 1))
        if [[ $elapsed -ge $((timeout * 2)) ]]; then
            err "TigerBeetle not ready on port $port after ${timeout}s"
            return 1
        fi
    done
}

# ---- Backend start/stop helpers ----------------------------------------------

start_redis() {
    info "Starting Redis on port $REDIS_PORT..."
    kill_port $REDIS_PORT
    $REDIS_BIN --port $REDIS_PORT --daemonize yes --logfile "$RESULTS_DIR/redis.log" \
        --dir /tmp >> "$LOG_FILE" 2>&1
    wait_for_port localhost $REDIS_PORT 10 "Redis"
    ok "Redis ready"
}

stop_redis() {
    info "Stopping Redis..."
    $REDIS_CLI -p $REDIS_PORT shutdown nosave 2>/dev/null || true
    pkill -9 -x redis-server 2>/dev/null || true
    sleep 1
}

start_dragonfly() {
    info "Starting DragonflyDB on port $DRAGONFLY_PORT..."
    kill_port $DRAGONFLY_PORT
    "$DRAGONFLY_BIN" --logtostderr --port=$DRAGONFLY_PORT \
        --default_lua_flags=allow-undeclared-keys \
        --dir /tmp \
        >> "$RESULTS_DIR/dragonfly.log" 2>&1 &
    wait_for_port localhost $DRAGONFLY_PORT 15 "DragonflyDB"
    ok "DragonflyDB ready"
}

stop_dragonfly() {
    info "Stopping DragonflyDB..."
    $REDIS_CLI -p $DRAGONFLY_PORT shutdown nosave 2>/dev/null || true
    pkill -9 -x dragonfly-x86_64 2>/dev/null || true
    sleep 1
}

start_tigerbeetle() {
    info "Starting TigerBeetle on port $TB_PORT..."
    pkill -9 -x tigerbeetle 2>/dev/null || true
    sleep 1
    rm -f "$TB_FILE"
    "$TIGERBEETLE_BIN" format --cluster=0 --replica=0 --replica-count=1 \
        --development "$TB_FILE" >> "$RESULTS_DIR/tigerbeetle.log" 2>&1
    "$TIGERBEETLE_BIN" start --addresses=$TB_PORT --development \
        "$TB_FILE" >> "$RESULTS_DIR/tigerbeetle.log" 2>&1 &
    wait_for_tb $TB_PORT 15
    ok "TigerBeetle ready"
}

stop_tigerbeetle() {
    info "Stopping TigerBeetle..."
    pkill -9 -x tigerbeetle 2>/dev/null || true
    sleep 1
    rm -f "$TB_FILE"
}

start_eloqkv() {
    info "Starting EloqKV on port $REDIS_PORT..."
    kill_port $REDIS_PORT
    pkill -9 -x redis-server 2>/dev/null || true
    pkill -9 -x eloqkv       2>/dev/null || true
    sleep 1
    "$ELOQKV_BIN" >> "$RESULTS_DIR/eloqkv.log" 2>&1 &
    wait_for_port localhost $REDIS_PORT 20 "EloqKV"
    ok "EloqKV ready"
}

stop_eloqkv() {
    info "Stopping EloqKV..."
    $REDIS_CLI -p $REDIS_PORT shutdown nosave 2>/dev/null || true
    pkill -9 -x eloqkv 2>/dev/null || true
    sleep 1
}

# ---- Sanity test runner ------------------------------------------------------

run_sanity_redis_compat() {
    local backend="$1" port="$2"
    local out="$RESULTS_DIR/sanity_${backend}.log"
    info "Running sanity test against $backend (port $port)..."
    if python3 "$SCRIPT_DIR/sanity_test.py" \
            --host localhost --port "$port" \
            --backend "$backend" \
            --scripts "$LUA_BEETLE_SCRIPTS" \
            2>&1 | tee "$out"; then
        SANITY_RESULT[$backend]="PASS"
        ok "$backend sanity: PASS"
    else
        SANITY_RESULT[$backend]="FAIL"
        err "$backend sanity: FAIL"
    fi
}

run_sanity_tigerbeetle() {
    local out="$RESULTS_DIR/sanity_TigerBeetle.log"
    info "Running TigerBeetle sanity test (Go)..."
    if (cd "$TB_SANITY_DIR" && go run . --address "$TB_PORT") \
            2>&1 | tee "$out"; then
        SANITY_RESULT[TigerBeetle]="PASS"
        ok "TigerBeetle sanity: PASS"
    else
        SANITY_RESULT[TigerBeetle]="FAIL"
        err "TigerBeetle sanity: FAIL"
    fi
}

# ---- Stress test runner ------------------------------------------------------

build_stress_binary() {
    local bin="$RESULTS_DIR/stress_test"
    if [[ ! -f "$bin" ]]; then
        info "Building stress test binary..."
        (cd "$LUA_BEETLE_TESTS" && go build -o "$bin" .) \
            >> "$LOG_FILE" 2>&1
        ok "Stress test binary built: $bin"
    fi
    echo "$bin"
}

run_stress_lua() {
    local backend="$1" mode="$2" port_flag=""
    local bin
    bin="$(build_stress_binary)"
    local out="$RESULTS_DIR/stress_${backend}.log"
    info "Running stress test against $backend (${STRESS_DURATION}s, ${STRESS_WORKERS} workers)..."

    local addr="localhost:$REDIS_PORT"
    [[ "$mode" == "dragonfly" ]] && addr="localhost:$DRAGONFLY_PORT"

    "$bin" \
        -mode="$mode" \
        -accounts="$STRESS_ACCOUNTS" \
        -hot-accounts=$(( STRESS_ACCOUNTS / 100 )) \
        -workers="$STRESS_WORKERS" \
        -duration="$STRESS_DURATION" \
        -batch="$STRESS_BATCH" \
        -workload="$STRESS_WORKLOAD" \
        -transfer-ratio=0.7 \
        -twophase-ratio=0.2 \
        2>&1 | tee "$out"

    # Parse metrics from output
    local ops lat
    ops=$(grep "Throughput:" "$out"    | awk '{print $2}' | tail -1)
    lat=$(grep "Average Latency:" "$out" | awk '{print $3}' | tail -1)
    STRESS_OPS[$backend]="${ops:-N/A}"
    STRESS_LATENCY[$backend]="${lat:-N/A}"
    ok "$backend stress: ${ops:-N/A} ops/sec, ${lat:-N/A} ms avg latency"
}

run_stress_tigerbeetle() {
    local bin
    bin="$(build_stress_binary)"
    local out="$RESULTS_DIR/stress_TigerBeetle.log"
    info "Running TigerBeetle stress test (${STRESS_DURATION}s, ${STRESS_WORKERS} workers)..."

    "$bin" \
        -mode=tigerbeetle \
        -tb-address="$TB_PORT" \
        -accounts="$STRESS_ACCOUNTS" \
        -hot-accounts=$(( STRESS_ACCOUNTS / 100 )) \
        -workers="$STRESS_WORKERS" \
        -duration="$STRESS_DURATION" \
        -batch="$STRESS_BATCH" \
        -workload="$STRESS_WORKLOAD" \
        -transfer-ratio=0.7 \
        -twophase-ratio=0.2 \
        2>&1 | tee "$out"

    local ops lat
    ops=$(grep "Throughput:" "$out"    | awk '{print $2}' | tail -1)
    lat=$(grep "Average Latency:" "$out" | awk '{print $3}' | tail -1)
    STRESS_OPS[TigerBeetle]="${ops:-N/A}"
    STRESS_LATENCY[TigerBeetle]="${lat:-N/A}"
    ok "TigerBeetle stress: ${ops:-N/A} ops/sec, ${lat:-N/A} ms avg latency"
}

# ---- Backend test sequences --------------------------------------------------

run_redis() {
    info "======================================="
    info "Backend: Redis"
    info "======================================="
    start_redis
    $REDIS_CLI -p $REDIS_PORT FLUSHALL > /dev/null 2>&1 || true
    $RUN_SANITY && run_sanity_redis_compat "Redis" "$REDIS_PORT"
    $REDIS_CLI -p $REDIS_PORT FLUSHALL > /dev/null 2>&1 || true
    $RUN_STRESS && run_stress_lua "Redis" "redis"
    stop_redis
}

run_dragonfly() {
    info "======================================="
    info "Backend: DragonflyDB"
    info "======================================="
    start_dragonfly
    $REDIS_CLI -p $DRAGONFLY_PORT FLUSHALL > /dev/null 2>&1 || true
    $RUN_SANITY && run_sanity_redis_compat "DragonflyDB" "$DRAGONFLY_PORT"
    $REDIS_CLI -p $DRAGONFLY_PORT FLUSHALL > /dev/null 2>&1 || true
    $RUN_STRESS && run_stress_lua "DragonflyDB" "dragonfly"
    stop_dragonfly
}

run_tigerbeetle() {
    info "======================================="
    info "Backend: TigerBeetle"
    info "======================================="
    start_tigerbeetle
    $RUN_SANITY && run_sanity_tigerbeetle
    # TigerBeetle is an immutable ledger; restart for a clean stress test
    stop_tigerbeetle
    start_tigerbeetle
    $RUN_STRESS && run_stress_tigerbeetle
    stop_tigerbeetle
}

run_eloqkv() {
    info "======================================="
    info "Backend: EloqKV"
    info "======================================="
    start_eloqkv
    $REDIS_CLI -p $REDIS_PORT FLUSHALL > /dev/null 2>&1 || true
    $RUN_SANITY && run_sanity_redis_compat "EloqKV" "$REDIS_PORT"
    $REDIS_CLI -p $REDIS_PORT FLUSHALL > /dev/null 2>&1 || true
    $RUN_STRESS && run_stress_lua "EloqKV" "eloqkv"
    stop_eloqkv
}

# ---- Main --------------------------------------------------------------------

main() {
    info "==================================================="
    info "Lua Beetle Integration Test Suite"
    info "==================================================="
    info "Results dir : $RESULTS_DIR"
    info "Sanity tests: $RUN_SANITY"
    info "Stress tests: $RUN_STRESS"
    if $RUN_STRESS; then
        info "Stress params: ${STRESS_DURATION}s, ${STRESS_WORKERS} workers, \
${STRESS_ACCOUNTS} accounts, batch=${STRESS_BATCH}, workload=${STRESS_WORKLOAD}"
    fi
    info "==================================================="

    # Kill any stale processes first
    kill_all_backends

    # Run backends
    local backends=("redis" "dragonfly" "tigerbeetle" "eloqkv")
    if [[ -n "$BACKEND_FILTER" ]]; then
        backends=("$BACKEND_FILTER")
    fi

    for b in "${backends[@]}"; do
        case "$b" in
            redis)       run_redis       ;;
            dragonfly)   run_dragonfly   ;;
            tigerbeetle) run_tigerbeetle ;;
            eloqkv)      run_eloqkv      ;;
            *) err "Unknown backend: $b" ;;
        esac
    done

    # ---- Print summary -------------------------------------------------------
    info ""
    info "==================================================="
    info "SUMMARY"
    info "==================================================="

    if $RUN_SANITY; then
        info "Sanity Test Results:"
        for b in Redis DragonflyDB TigerBeetle EloqKV; do
            local res="${SANITY_RESULT[$b]:-SKIP}"
            info "  $b: $res"
        done
    fi

    if $RUN_STRESS; then
        info ""
        info "Stress Test Results (workload=$STRESS_WORKLOAD, ${STRESS_DURATION}s, ${STRESS_WORKERS} workers):"
        info "  $(printf '%-15s %12s %18s' Backend Throughput 'Avg Latency (ms)')"
        for b in Redis DragonflyDB TigerBeetle EloqKV; do
            local ops="${STRESS_OPS[$b]:-N/A}"
            local lat="${STRESS_LATENCY[$b]:-N/A}"
            info "  $(printf '%-15s %12s %18s' "$b" "$ops" "$lat")"
        done
    fi

    info "==================================================="
    info "Full logs: $RESULTS_DIR"
    info "Report will be written to: $REPORT_FILE"
    info "==================================================="
}

main "$@"

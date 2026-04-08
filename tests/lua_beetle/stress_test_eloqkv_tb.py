#!/usr/bin/env python3
"""
Stress / performance test for EloqKV native TB_BIN commands.

Compares two modes:
  1. TB_BIN native batch  — one TB_BIN CREATE_TRANSFERS call per batch
  2. lua_beetle EVALSHA   — one EVALSHA call per transfer (pipelined)

Metrics reported:
  - Throughput (transfers/s)
  - Latency p50 / p99 (ms)
  - Error rate

Usage:
    python3 stress_test_eloqkv_tb.py [options]

    --host HOST        Redis host (default: localhost)
    --port PORT        Redis port (default: 6379)
    --duration SECS    Duration of each mode test (default: 20)
    --workers N        Number of concurrent worker threads (default: 4)
    --batch-size N     Transfers per TB_BIN batch (default: 100)
    --accounts N       Number of pre-created accounts (default: 1000)
    --scripts DIR      Path to lua_beetle scripts dir for EVALSHA mode
                       (default: /home/lintaoz/work/lua_beetle/scripts)
    --mode MODE        Which modes to run: "tbbin", "evalsha", or "both" (default: both)

Requires:
    pip install redis
"""

import argparse
import os
import struct
import threading
import time
import random
import math
from typing import List

import redis

# ---------------------------------------------------------------------------
# Struct helpers (same as sanity test)
# ---------------------------------------------------------------------------

_ACCOUNT_FMT = "<QQQQQQQQQQQQQIIIHHQ"
_TRANSFER_FMT = "<QQQQQQQQQQQQQIIIHHQ"

AF_HISTORY = 0x0008


def encode_account(id_val, ledger=700, code=10, flags=0):
    return struct.pack(_ACCOUNT_FMT,
                       id_val, 0,
                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                       0, 0, 0,
                       ledger, code, flags, 0)


def encode_transfer(id_val, debit_id, credit_id, amount,
                    ledger=700, code=1, flags=0):
    return struct.pack(_TRANSFER_FMT,
                       id_val, 0,
                       debit_id, 0,
                       credit_id, 0,
                       amount, 0,
                       0, 0, 0, 0,
                       0, 0, 0,
                       ledger, code, flags, 0)


# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

_id_lock = threading.Lock()
_id_counter = [0]


def next_id():
    with _id_lock:
        _id_counter[0] += 1
        return _id_counter[0]


# ---------------------------------------------------------------------------
# Worker: TB_BIN batch mode
# ---------------------------------------------------------------------------

class TbBinWorker(threading.Thread):
    def __init__(self, host, port, accounts, batch_size, duration):
        super().__init__(daemon=True)
        self.r = redis.Redis(host=host, port=port, decode_responses=False)
        self.accounts = accounts
        self.batch_size = batch_size
        self.duration = duration
        self.latencies: List[float] = []
        self.errors = 0
        self.total_transfers = 0

    def run(self):
        n = len(self.accounts)
        deadline = time.monotonic() + self.duration

        while time.monotonic() < deadline:
            # Build a batch of transfers between random accounts
            blobs = []
            pairs = []
            for _ in range(self.batch_size):
                i, j = random.sample(range(n), 2)
                debit_id = self.accounts[i]
                credit_id = self.accounts[j]
                txfr_id = next_id()
                blobs.append(encode_transfer(txfr_id, debit_id, credit_id, 1))
                pairs.append((debit_id, credit_id))

            t0 = time.monotonic()
            try:
                results = self.r.execute_command("TB_BIN", "CREATE_TRANSFERS", *blobs)
                elapsed = time.monotonic() - t0
                self.latencies.append(elapsed * 1000)
                if isinstance(results, list):
                    self.errors += sum(1 for r in results if r != 0)
                    self.total_transfers += self.batch_size
                else:
                    self.errors += self.batch_size
            except Exception:
                self.errors += self.batch_size


# ---------------------------------------------------------------------------
# Worker: lua_beetle EVALSHA (pipelined, one transfer per EVALSHA)
# ---------------------------------------------------------------------------

def load_lua_scripts(r, scripts_dir):
    """Load all lua_beetle scripts (with common.lua prepended) and return dict name->sha."""
    common_path = os.path.join(scripts_dir, "common.lua")
    with open(common_path, "rb") as f:
        common_src = f.read() + b"\n"

    shas = {}
    for fname in sorted(os.listdir(scripts_dir)):
        if not fname.endswith(".lua") or fname == "common.lua":
            continue
        path = os.path.join(scripts_dir, fname)
        with open(path, "rb") as f:
            src = common_src + f.read()
        sha = r.script_load(src)
        shas[fname[:-4]] = sha
    return shas


def encode_lb_account(id_val, ledger, code):
    """Encode an account as lua_beetle binary (same 128-byte format)."""
    return struct.pack(_ACCOUNT_FMT,
                       id_val, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                       0, 0, 0, ledger, code, 0, 0)


def encode_lb_transfer(id_val, debit_id, credit_id, amount,
                       ledger=700, code=1):
    return struct.pack(_TRANSFER_FMT,
                       id_val, 0,
                       debit_id, 0,
                       credit_id, 0,
                       amount, 0,
                       0, 0, 0, 0,
                       0, 0, 0,
                       ledger, code, 0, 0)


class LuaBeetleWorker(threading.Thread):
    def __init__(self, host, port, accounts, batch_size, duration, create_sha):
        super().__init__(daemon=True)
        self.r = redis.Redis(host=host, port=port, decode_responses=False)
        self.accounts = accounts
        self.batch_size = batch_size
        self.duration = duration
        self.create_sha = create_sha
        self.latencies: List[float] = []
        self.errors = 0
        self.total_transfers = 0

    def run(self):
        n = len(self.accounts)
        deadline = time.monotonic() + self.duration

        while time.monotonic() < deadline:
            pipe = self.r.pipeline(transaction=False)
            for _ in range(self.batch_size):
                i, j = random.sample(range(n), 2)
                debit_id = self.accounts[i]
                credit_id = self.accounts[j]
                txfr_id = next_id()
                blob = encode_lb_transfer(txfr_id, debit_id, credit_id, 1)
                pipe.evalsha(self.create_sha, 0, blob)

            t0 = time.monotonic()
            try:
                results = pipe.execute(raise_on_error=False)
                elapsed = time.monotonic() - t0
                self.latencies.append(elapsed * 1000)
                self.total_transfers += self.batch_size
                for res in results:
                    if isinstance(res, Exception):
                        self.errors += 1
                    elif isinstance(res, list) and res and res[0] != 0:
                        self.errors += 1
            except Exception:
                self.errors += self.batch_size


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------

def percentile(data, p):
    if not data:
        return 0.0
    s = sorted(data)
    idx = int(math.ceil(len(s) * p / 100)) - 1
    return s[max(0, idx)]


def aggregate(workers):
    total_txfr = sum(w.total_transfers for w in workers)
    total_err = sum(w.errors for w in workers)
    all_lats = []
    for w in workers:
        all_lats.extend(w.latencies)
    p50 = percentile(all_lats, 50)
    p99 = percentile(all_lats, 99)
    return total_txfr, total_err, p50, p99


# ---------------------------------------------------------------------------
# Setup: create accounts
# ---------------------------------------------------------------------------

def setup_accounts(r, n_accounts, flush=True):
    """Create n_accounts accounts via TB_BIN, return list of IDs."""
    if flush:
        r.execute_command("FLUSHDB")

    ids = []
    batch = []
    for _ in range(n_accounts):
        aid = next_id()
        ids.append(aid)
        batch.append(encode_account(aid, ledger=700, code=10))
        if len(batch) == 100:
            r.execute_command("TB_BIN", "CREATE_ACCOUNTS", *batch)
            batch = []
    if batch:
        r.execute_command("TB_BIN", "CREATE_ACCOUNTS", *batch)
    return ids


def setup_accounts_evalsha(r, scripts_dir, n_accounts):
    """Create accounts via lua_beetle CREATE_ACCOUNTS EVALSHA."""
    shas = load_lua_scripts(r, scripts_dir)
    create_sha = shas.get("create_account")
    if not create_sha:
        raise RuntimeError("create_account.lua not found in " + scripts_dir)
    ids = []
    pipe = r.pipeline(transaction=False)
    for _ in range(n_accounts):
        aid = next_id()
        ids.append(aid)
        blob = encode_lb_account(aid, 700, 10)
        pipe.evalsha(create_sha, 0, blob)
    pipe.execute(raise_on_error=False)
    return ids, shas


# ---------------------------------------------------------------------------
# Main benchmark
# ---------------------------------------------------------------------------

def run_tbbin_mode(args):
    print(f"\n{'='*60}")
    print(f"TB_BIN native batch mode  (batch={args.batch_size}, workers={args.workers})")
    print(f"{'='*60}")

    r = redis.Redis(host=args.host, port=args.port, decode_responses=False)
    accounts = setup_accounts(r, args.accounts)
    print(f"  Created {len(accounts)} accounts")

    workers = [TbBinWorker(args.host, args.port, accounts,
                           args.batch_size, args.duration)
               for _ in range(args.workers)]

    t_start = time.monotonic()
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    elapsed = time.monotonic() - t_start

    total, errs, p50, p99 = aggregate(workers)
    tps = total / elapsed if elapsed > 0 else 0
    err_pct = 100.0 * errs / total if total > 0 else 0

    print(f"  Duration:        {elapsed:.1f}s")
    print(f"  Transfers:       {total:,}")
    print(f"  Throughput:      {tps:,.0f} transfers/s")
    print(f"  Batch latency p50: {p50:.1f} ms  p99: {p99:.1f} ms")
    print(f"  Errors:          {errs:,} ({err_pct:.1f}%)")
    return {"mode": "TB_BIN", "tps": tps, "p50": p50, "p99": p99,
            "total": total, "errors": errs, "duration": elapsed}


def run_evalsha_mode(args):
    scripts_dir = args.scripts
    if not os.path.isdir(scripts_dir):
        print(f"\n[SKIP] lua_beetle scripts not found at {scripts_dir}")
        return None

    print(f"\n{'='*60}")
    print(f"lua_beetle EVALSHA mode   (batch={args.batch_size} pipelined, workers={args.workers})")
    print(f"{'='*60}")

    r = redis.Redis(host=args.host, port=args.port, decode_responses=False)
    r.execute_command("FLUSHDB")

    try:
        accounts, shas = setup_accounts_evalsha(r, scripts_dir, args.accounts)
    except Exception as e:
        print(f"  [SKIP] Could not set up lua_beetle accounts: {e}")
        return None

    create_sha = shas.get("create_transfer")
    if not create_sha:
        print(f"  [SKIP] create_transfer.lua not found in {scripts_dir}")
        return None

    print(f"  Created {len(accounts)} accounts")

    workers = [LuaBeetleWorker(args.host, args.port, accounts,
                               args.batch_size, args.duration, create_sha)
               for _ in range(args.workers)]

    t_start = time.monotonic()
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    elapsed = time.monotonic() - t_start

    total, errs, p50, p99 = aggregate(workers)
    tps = total / elapsed if elapsed > 0 else 0
    err_pct = 100.0 * errs / total if total > 0 else 0

    print(f"  Duration:        {elapsed:.1f}s")
    print(f"  Transfers:       {total:,}")
    print(f"  Throughput:      {tps:,.0f} transfers/s")
    print(f"  Batch latency p50: {p50:.1f} ms  p99: {p99:.1f} ms")
    print(f"  Errors:          {errs:,} ({err_pct:.1f}%)")
    return {"mode": "EVALSHA", "tps": tps, "p50": p50, "p99": p99,
            "total": total, "errors": errs, "duration": elapsed}


def print_comparison(tb_result, lb_result):
    if tb_result is None or lb_result is None:
        return
    print(f"\n{'='*60}")
    print(f"Comparison summary")
    print(f"{'='*60}")
    print(f"  {'Metric':<25} {'TB_BIN':>15} {'EVALSHA':>15}")
    print(f"  {'-'*55}")
    print(f"  {'Throughput (tx/s)':<25} {tb_result['tps']:>15,.0f} {lb_result['tps']:>15,.0f}")
    print(f"  {'Latency p50 (ms)':<25} {tb_result['p50']:>15.1f} {lb_result['p50']:>15.1f}")
    print(f"  {'Latency p99 (ms)':<25} {tb_result['p99']:>15.1f} {lb_result['p99']:>15.1f}")
    if lb_result['tps'] > 0:
        ratio = tb_result['tps'] / lb_result['tps']
        print(f"  {'TB_BIN speedup':<25} {ratio:>14.2f}x")


def main():
    ap = argparse.ArgumentParser(description="EloqKV TB_BIN vs EVALSHA stress test")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--duration", type=int, default=20,
                    help="Test duration per mode in seconds")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--accounts", type=int, default=1000)
    ap.add_argument("--scripts", default="/home/lintaoz/work/lua_beetle/scripts")
    ap.add_argument("--mode", choices=["tbbin", "evalsha", "both"], default="both")
    args = ap.parse_args()

    print(f"\nEloqKV TB_BIN stress test")
    print(f"  host={args.host}:{args.port}  workers={args.workers}"
          f"  batch={args.batch_size}  accounts={args.accounts}"
          f"  duration={args.duration}s")

    tb_result = lb_result = None

    if args.mode in ("tbbin", "both"):
        tb_result = run_tbbin_mode(args)

    if args.mode in ("evalsha", "both"):
        lb_result = run_evalsha_mode(args)

    if args.mode == "both":
        print_comparison(tb_result, lb_result)

    print()


if __name__ == "__main__":
    main()

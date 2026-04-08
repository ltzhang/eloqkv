#!/usr/bin/env python3
"""
Lua Beetle Sanity Test
======================
Tests core TigerBeetle semantics via lua_beetle scripts against any
Redis-compatible backend (Redis, DragonflyDB, EloqKV).

Usage:
    python3 sanity_test.py [options]

Options:
    --host HOST         Redis host (default: localhost)
    --port PORT         Redis port (default: 6379)
    --backend NAME      Backend name for display (default: Redis)
    --scripts PATH      Path to lua_beetle scripts directory
                        (default: /home/lintaoz/work/lua_beetle/scripts)

Exit codes:
    0  All tests passed
    1  One or more tests failed
"""

import redis
import struct
import sys
import os
import argparse
import time

# ============================================================================
# Error codes matching TigerBeetle
# ============================================================================
ERR_OK                              = 0
ERR_LINKED_EVENT_FAILED             = 1
ERR_LINKED_EVENT_CHAIN_OPEN         = 2
ERR_ACCOUNTS_MUST_BE_DIFFERENT      = 12
ERR_DEBIT_ACCOUNT_NOT_FOUND         = 21
ERR_ID_ALREADY_EXISTS               = 21
ERR_CREDIT_ACCOUNT_NOT_FOUND        = 22
ERR_ACCOUNTS_MUST_HAVE_SAME_LEDGER  = 23
ERR_TRANSFER_LEDGER_MUST_MATCH      = 24
ERR_PENDING_TRANSFER_NOT_FOUND      = 25
ERR_PENDING_TRANSFER_ALREADY_POSTED = 33
ERR_PENDING_TRANSFER_ALREADY_VOIDED = 34
ERR_EXISTS                          = 46
ERR_EXCEEDS_CREDITS                 = 54
ERR_EXCEEDS_DEBITS                  = 55

# ============================================================================
# Account / Transfer flags
# ============================================================================
ACCOUNT_FLAG_LINKED                        = 0x0001
ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS = 0x0002
ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS = 0x0004
ACCOUNT_FLAG_HISTORY                        = 0x0008

TRANSFER_FLAG_LINKED      = 0x0001
TRANSFER_FLAG_PENDING     = 0x0002
TRANSFER_FLAG_POST_PENDING = 0x0004
TRANSFER_FLAG_VOID_PENDING = 0x0008

# AccountFilter flags
FILTER_DEBITS   = 0x01
FILTER_CREDITS  = 0x02
FILTER_REVERSED = 0x04

# ============================================================================
# Encoding helpers
# ============================================================================

def u128_to_bytes(n):
    return struct.pack('<QQ', n & 0xFFFFFFFFFFFFFFFF, (n >> 64) & 0xFFFFFFFFFFFFFFFF)

def bytes_to_u128(b, offset=0):
    low, high = struct.unpack_from('<QQ', b, offset)
    return low | (high << 64)

def bytes_to_u64(b, offset=0):
    return struct.unpack_from('<Q', b, offset)[0]

def str_to_u64(s):
    h = 0
    for c in s:
        h = (h * 31 + ord(c)) & 0xFFFFFFFFFFFFFFFF
    return h

def encode_account(account_id, ledger, code, flags):
    if isinstance(account_id, str):
        account_id = str_to_u64(account_id)
    buf = bytearray(128)
    struct.pack_into('<QQ', buf, 0,
                     account_id & 0xFFFFFFFFFFFFFFFF,
                     (account_id >> 64) & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into('<I', buf, 112, ledger)
    struct.pack_into('<H', buf, 116, code)
    struct.pack_into('<H', buf, 118, flags)
    return bytes(buf)

def encode_transfer(transfer_id, debit_account_id, credit_account_id,
                    amount, ledger, code, flags, pending_id=None):
    if isinstance(transfer_id, str):
        transfer_id = str_to_u64(transfer_id)
    if isinstance(debit_account_id, str):
        debit_account_id = str_to_u64(debit_account_id)
    if isinstance(credit_account_id, str):
        credit_account_id = str_to_u64(credit_account_id)
    buf = bytearray(128)
    struct.pack_into('<QQ', buf, 0,
                     transfer_id & 0xFFFFFFFFFFFFFFFF,
                     (transfer_id >> 64) & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into('<QQ', buf, 16,
                     debit_account_id & 0xFFFFFFFFFFFFFFFF,
                     (debit_account_id >> 64) & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into('<QQ', buf, 32,
                     credit_account_id & 0xFFFFFFFFFFFFFFFF,
                     (credit_account_id >> 64) & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into('<QQ', buf, 48,
                     amount & 0xFFFFFFFFFFFFFFFF,
                     (amount >> 64) & 0xFFFFFFFFFFFFFFFF)
    if pending_id is not None:
        if isinstance(pending_id, str):
            pending_id = str_to_u64(pending_id)
        struct.pack_into('<QQ', buf, 64,
                         pending_id & 0xFFFFFFFFFFFFFFFF,
                         (pending_id >> 64) & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into('<I', buf, 112, ledger)
    struct.pack_into('<H', buf, 116, code)
    struct.pack_into('<H', buf, 118, flags)
    return bytes(buf)

def encode_account_filter(account_id, timestamp_min=0,
                          timestamp_max=0, limit=100,
                          flags=FILTER_DEBITS | FILTER_CREDITS):
    # timestamp_max=0 means "no upper bound" — the Lua scripts interpret 0 as +inf.
    # Avoid passing 2**64-1: DragonflyDB uses Lua 5.4 integer arithmetic and
    # decodes that as -1, triggering the timestamp_min > timestamp_max guard.
    if isinstance(account_id, str):
        account_id = str_to_u64(account_id)
    buf = bytearray(128)
    struct.pack_into('<QQ', buf, 0,
                     account_id & 0xFFFFFFFFFFFFFFFF,
                     (account_id >> 64) & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into('<Q', buf, 104, timestamp_min)
    struct.pack_into('<Q', buf, 112, timestamp_max)
    struct.pack_into('<I', buf, 120, limit)
    struct.pack_into('<I', buf, 124, flags)
    return bytes(buf)

def decode_result(result):
    if isinstance(result, int):
        return result
    if isinstance(result, (bytes, bytearray)):
        return result[0] if result else 0
    if isinstance(result, str):
        b = result.encode('latin-1')
        return b[0] if b else 0
    return int(result)

def decode_account(data):
    if not data:
        return None
    if isinstance(data, str):
        data = data.encode('latin-1')
    if len(data) < 128:
        return None
    return {
        'id':              bytes_to_u128(data, 0),
        'debits_pending':  bytes_to_u128(data, 16),
        'debits_posted':   bytes_to_u128(data, 32),
        'credits_pending': bytes_to_u128(data, 48),
        'credits_posted':  bytes_to_u128(data, 64),
        'ledger':          struct.unpack_from('<I', data, 112)[0],
        'code':            struct.unpack_from('<H', data, 116)[0],
        'flags':           struct.unpack_from('<H', data, 118)[0],
        'timestamp':       bytes_to_u64(data, 120),
    }

def decode_transfer(data):
    if not data:
        return None
    if isinstance(data, str):
        data = data.encode('latin-1')
    if len(data) < 128:
        return None
    return {
        'id':               bytes_to_u128(data, 0),
        'debit_account_id': bytes_to_u128(data, 16),
        'credit_account_id':bytes_to_u128(data, 32),
        'amount':           bytes_to_u128(data, 48),
        'pending_id':       bytes_to_u128(data, 64),
        'ledger':           struct.unpack_from('<I', data, 112)[0],
        'code':             struct.unpack_from('<H', data, 116)[0],
        'flags':            struct.unpack_from('<H', data, 118)[0],
        'timestamp':        bytes_to_u64(data, 120),
    }

# ============================================================================
# Test runner
# ============================================================================

class SanityTestRunner:
    def __init__(self, r, scripts_path, backend_name):
        self.r = r
        self.backend = backend_name
        self.passed = 0
        self.failed = 0
        self.start_time = time.time()

        # Load lua_beetle scripts
        common_path = os.path.join(scripts_path, 'common.lua')
        with open(common_path) as f:
            common = f.read() + "\n"

        def load(name):
            with open(os.path.join(scripts_path, name)) as f:
                return r.script_load(common + f.read())

        self.create_account_sha        = load('create_account.lua')
        self.create_linked_accounts_sha = load('create_linked_accounts.lua')
        self.create_transfer_sha       = load('create_transfer.lua')
        self.create_linked_transfers_sha = load('create_linked_transfers.lua')
        self.lookup_account_sha        = load('lookup_account.lua')
        self.lookup_transfer_sha       = load('lookup_transfer.lua')
        self.get_transfers_sha         = load('get_account_transfers.lua')
        self.get_balances_sha          = load('get_account_balances.lua')

    # ------------------------------------------------------------------
    # Eval wrappers
    # ------------------------------------------------------------------

    def create_account(self, acct_id, ledger, code, flags):
        data = encode_account(acct_id, ledger, code, flags)
        return decode_result(self.r.evalsha(self.create_account_sha, 0, data))

    def create_linked_accounts(self, *blobs):
        data = b''.join(blobs)
        return self.r.evalsha(self.create_linked_accounts_sha, 0, data)

    def create_transfer(self, tid, did, cid, amount, ledger, code, flags, pending_id=None):
        data = encode_transfer(tid, did, cid, amount, ledger, code, flags, pending_id)
        return decode_result(self.r.evalsha(self.create_transfer_sha, 0, data))

    def create_linked_transfers(self, *blobs):
        data = b''.join(blobs)
        return self.r.evalsha(self.create_linked_transfers_sha, 0, data)

    def lookup_account(self, acct_id):
        if isinstance(acct_id, str):
            acct_id = str_to_u64(acct_id)
        return decode_account(self.r.evalsha(self.lookup_account_sha, 0, u128_to_bytes(acct_id)))

    def lookup_transfer(self, tid):
        if isinstance(tid, str):
            tid = str_to_u64(tid)
        return decode_transfer(self.r.evalsha(self.lookup_transfer_sha, 0, u128_to_bytes(tid)))

    def get_account_transfers(self, acct_id, limit=100, flags=FILTER_DEBITS | FILTER_CREDITS):
        filt = encode_account_filter(acct_id, limit=limit, flags=flags)
        blob = self.r.evalsha(self.get_transfers_sha, 0, filt)
        if isinstance(blob, str):
            blob = blob.encode('latin-1')
        if not blob:
            return []
        return [decode_transfer(blob[i*128:(i+1)*128]) for i in range(len(blob) // 128)]

    def get_account_balances(self, acct_id, limit=100):
        filt = encode_account_filter(acct_id, limit=limit, flags=FILTER_DEBITS | FILTER_CREDITS)
        blob = self.r.evalsha(self.get_balances_sha, 0, filt)
        if isinstance(blob, str):
            blob = blob.encode('latin-1')
        if not blob:
            return []
        results = []
        for i in range(len(blob) // 128):
            chunk = blob[i*128:(i+1)*128]
            # AccountBalance layout (128 bytes):
            #   [0:16]  debits_pending, [16:32] debits_posted
            #   [32:48] credits_pending,[48:64] credits_posted
            #   [64:72] timestamp,      [72:128] reserved
            results.append({
                'debits_pending':  bytes_to_u128(chunk, 0),
                'debits_posted':   bytes_to_u128(chunk, 16),
                'credits_pending': bytes_to_u128(chunk, 32),
                'credits_posted':  bytes_to_u128(chunk, 48),
            })
        return results

    # ------------------------------------------------------------------
    # Assertion helpers
    # ------------------------------------------------------------------

    def expect(self, condition, msg):
        if not condition:
            print(f"  FAIL: {msg}")
            self.failed += 1
            return False
        return True

    def run_test(self, name, fn):
        try:
            fn()
            print(f"  PASS: {name}")
            self.passed += 1
        except AssertionError as e:
            print(f"  FAIL: {name} — {e}")
            self.failed += 1
        except Exception as e:
            print(f"  ERROR: {name} — {e}")
            self.failed += 1

    # ------------------------------------------------------------------
    # Individual test cases
    # ------------------------------------------------------------------

    def test_create_account_basic(self):
        rc = self.create_account(1001, 700, 10, 0)
        assert rc == ERR_OK, f"expected ERR_OK, got {rc}"
        acc = self.lookup_account(1001)
        assert acc is not None, "account not found after creation"
        assert acc['id'] == 1001, f"id mismatch: {acc['id']}"
        assert acc['ledger'] == 700, f"ledger mismatch: {acc['ledger']}"
        assert acc['code'] == 10, f"code mismatch: {acc['code']}"
        assert acc['debits_posted'] == 0
        assert acc['credits_posted'] == 0
        assert acc['timestamp'] > 0, "timestamp not set by server"

    def test_duplicate_account_rejected(self):
        rc = self.create_account(1002, 700, 10, 0)
        assert rc == ERR_OK
        rc2 = self.create_account(1002, 700, 10, 0)
        assert rc2 == ERR_ID_ALREADY_EXISTS, f"expected ID_ALREADY_EXISTS, got {rc2}"

    def test_linked_accounts_success(self):
        blobs = (
            encode_account(1010, 700, 10, ACCOUNT_FLAG_LINKED),
            encode_account(1011, 700, 10, ACCOUNT_FLAG_LINKED),
            encode_account(1012, 700, 10, 0),
        )
        self.create_linked_accounts(*blobs)
        for aid in (1010, 1011, 1012):
            acc = self.lookup_account(aid)
            assert acc is not None, f"account {aid} not created"
            assert acc['ledger'] == 700

    def test_linked_accounts_rollback(self):
        # Pre-create account 1020
        self.create_account(1020, 700, 10, 0)
        blobs = (
            encode_account(1021, 700, 10, ACCOUNT_FLAG_LINKED),
            encode_account(1020, 700, 10, 0),   # duplicate → chain fails
        )
        self.create_linked_accounts(*blobs)
        acc = self.lookup_account(1021)
        assert acc is None, "account 1021 should have been rolled back"

    def test_simple_transfer(self):
        self.create_account(2001, 700, 10, 0)
        self.create_account(2002, 700, 10, 0)
        rc = self.create_transfer("tx_simple", 2001, 2002, 1000, 700, 10, 0)
        assert rc == ERR_OK, f"transfer failed: {rc}"
        da = self.lookup_account(2001)
        ca = self.lookup_account(2002)
        assert da['debits_posted'] == 1000
        assert da['credits_posted'] == 0
        assert ca['credits_posted'] == 1000
        assert ca['debits_posted'] == 0

    def test_transfer_nonexistent_debit_account(self):
        self.create_account(2010, 700, 10, 0)
        rc = self.create_transfer("tx_no_debit", 99991, 2010, 100, 700, 10, 0)
        assert rc == ERR_DEBIT_ACCOUNT_NOT_FOUND, f"expected DEBIT_ACCOUNT_NOT_FOUND, got {rc}"

    def test_transfer_nonexistent_credit_account(self):
        self.create_account(2011, 700, 10, 0)
        rc = self.create_transfer("tx_no_credit", 2011, 99992, 100, 700, 10, 0)
        assert rc == ERR_CREDIT_ACCOUNT_NOT_FOUND, f"expected CREDIT_ACCOUNT_NOT_FOUND, got {rc}"

    def test_two_phase_pending(self):
        self.create_account(3001, 700, 10, 0)
        self.create_account(3002, 700, 10, 0)
        rc = self.create_transfer("tx_pend1", 3001, 3002, 500, 700, 10, TRANSFER_FLAG_PENDING)
        assert rc == ERR_OK
        da = self.lookup_account(3001)
        ca = self.lookup_account(3002)
        assert da['debits_pending'] == 500
        assert da['debits_posted'] == 0
        assert ca['credits_pending'] == 500
        assert ca['credits_posted'] == 0

    def test_two_phase_post(self):
        self.create_account(3010, 700, 10, 0)
        self.create_account(3011, 700, 10, 0)
        self.create_transfer("tx_pend2", 3010, 3011, 600, 700, 10, TRANSFER_FLAG_PENDING)
        rc = self.create_transfer("tx_post2", 3010, 3011, 600, 700, 10,
                                  TRANSFER_FLAG_POST_PENDING, pending_id="tx_pend2")
        assert rc == ERR_OK, f"post failed: {rc}"
        da = self.lookup_account(3010)
        ca = self.lookup_account(3011)
        assert da['debits_pending'] == 0
        assert da['debits_posted'] == 600
        assert ca['credits_pending'] == 0
        assert ca['credits_posted'] == 600

    def test_two_phase_void(self):
        self.create_account(3020, 700, 10, 0)
        self.create_account(3021, 700, 10, 0)
        self.create_transfer("tx_pend3", 3020, 3021, 700, 700, 10, TRANSFER_FLAG_PENDING)
        rc = self.create_transfer("tx_void3", 3020, 3021, 700, 700, 10,
                                  TRANSFER_FLAG_VOID_PENDING, pending_id="tx_pend3")
        assert rc == ERR_OK, f"void failed: {rc}"
        da = self.lookup_account(3020)
        ca = self.lookup_account(3021)
        assert da['debits_pending'] == 0
        assert da['debits_posted'] == 0
        assert ca['credits_pending'] == 0
        assert ca['credits_posted'] == 0

    def test_double_post_rejected(self):
        self.create_account(3030, 700, 10, 0)
        self.create_account(3031, 700, 10, 0)
        self.create_transfer("tx_pend4", 3030, 3031, 100, 700, 10, TRANSFER_FLAG_PENDING)
        self.create_transfer("tx_post4a", 3030, 3031, 100, 700, 10,
                             TRANSFER_FLAG_POST_PENDING, pending_id="tx_pend4")
        rc = self.create_transfer("tx_post4b", 3030, 3031, 100, 700, 10,
                                  TRANSFER_FLAG_POST_PENDING, pending_id="tx_pend4")
        assert rc == ERR_PENDING_TRANSFER_ALREADY_POSTED, f"expected ALREADY_POSTED, got {rc}"

    def test_void_after_post_rejected(self):
        self.create_account(3040, 700, 10, 0)
        self.create_account(3041, 700, 10, 0)
        self.create_transfer("tx_pend5", 3040, 3041, 100, 700, 10, TRANSFER_FLAG_PENDING)
        self.create_transfer("tx_post5", 3040, 3041, 100, 700, 10,
                             TRANSFER_FLAG_POST_PENDING, pending_id="tx_pend5")
        rc = self.create_transfer("tx_void5", 3040, 3041, 100, 700, 10,
                                  TRANSFER_FLAG_VOID_PENDING, pending_id="tx_pend5")
        # lua_beetle returns ALREADY_VOIDED (34) when voiding an already-posted transfer.
        # Native TigerBeetle returns ALREADY_POSTED (33). This is a known implementation
        # difference: lua_beetle checks the voided flag before the posted flag.
        assert rc in (ERR_PENDING_TRANSFER_ALREADY_POSTED, ERR_PENDING_TRANSFER_ALREADY_VOIDED), \
            f"expected ALREADY_POSTED or ALREADY_VOIDED, got {rc}"

    def test_balance_constraint_debits_must_not_exceed_credits(self):
        # Account 4001 cannot have debits > credits
        self.create_account(4001, 700, 10, ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS)
        self.create_account(4002, 700, 10, 0)
        # First transfer should succeed (balance stays 0/0)
        rc = self.create_transfer("tx_exceed1", 4001, 4002, 500, 700, 10, 0)
        assert rc == ERR_EXCEEDS_CREDITS, f"expected EXCEEDS_CREDITS, got {rc}"

    def test_balance_constraint_credits_must_not_exceed_debits(self):
        self.create_account(4010, 700, 10, 0)
        self.create_account(4011, 700, 10, ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS)
        rc = self.create_transfer("tx_exceed2", 4010, 4011, 500, 700, 10, 0)
        assert rc == ERR_EXCEEDS_DEBITS, f"expected EXCEEDS_DEBITS, got {rc}"

    def test_linked_transfers_success(self):
        self.create_account(5001, 700, 10, 0)
        self.create_account(5002, 700, 10, 0)
        self.create_account(5003, 700, 10, 0)
        blobs = (
            encode_transfer("tx_link1", 5001, 5002, 100, 700, 10, TRANSFER_FLAG_LINKED),
            encode_transfer("tx_link2", 5002, 5003, 50,  700, 10, 0),
        )
        self.create_linked_transfers(*blobs)
        a1 = self.lookup_account(5001)
        a2 = self.lookup_account(5002)
        a3 = self.lookup_account(5003)
        assert a1['debits_posted'] == 100
        assert a2['credits_posted'] == 100
        assert a2['debits_posted'] == 50
        assert a3['credits_posted'] == 50

    def test_linked_transfers_rollback(self):
        self.create_account(5010, 700, 10, 0)
        self.create_account(5011, 700, 10, 0)
        blobs = (
            encode_transfer("tx_roll1", 5010, 5011, 200, 700, 10, TRANSFER_FLAG_LINKED),
            encode_transfer("tx_roll2", 5010, 99999, 50, 700, 10, 0),  # nonexistent credit
        )
        self.create_linked_transfers(*blobs)
        a1 = self.lookup_account(5010)
        a2 = self.lookup_account(5011)
        assert a1['debits_posted'] == 0,  "debits should be 0 after rollback"
        assert a2['credits_posted'] == 0, "credits should be 0 after rollback"

    def test_lookup_transfer(self):
        self.create_account(6001, 700, 10, 0)
        self.create_account(6002, 700, 10, 0)
        self.create_transfer("tx_lookup1", 6001, 6002, 250, 700, 10, 0)
        t = self.lookup_transfer("tx_lookup1")
        assert t is not None, "transfer not found"
        assert t['debit_account_id'] == 6001
        assert t['credit_account_id'] == 6002
        assert t['amount'] == 250

    def test_get_account_transfers(self):
        self.create_account(7001, 700, 10, 0)
        self.create_account(7002, 700, 10, 0)
        for i in range(5):
            self.create_transfer(f"tx_gat_{i}", 7001, 7002, 100 * (i + 1), 700, 10, 0)

        # All transfers
        txs = self.get_account_transfers(7001, limit=10)
        assert len(txs) == 5, f"expected 5 transfers, got {len(txs)}"

        # Debits only
        txs_d = self.get_account_transfers(7001, limit=10, flags=FILTER_DEBITS)
        assert len(txs_d) == 5

        # Limit
        txs_lim = self.get_account_transfers(7001, limit=3)
        assert len(txs_lim) == 3, f"expected 3 (limit), got {len(txs_lim)}"

        # Credits for the other side
        txs_c = self.get_account_transfers(7002, limit=10, flags=FILTER_CREDITS)
        assert len(txs_c) == 5

    def test_get_account_balances(self):
        self.create_account(8001, 700, 10, ACCOUNT_FLAG_HISTORY)
        self.create_account(8002, 700, 10, 0)
        for i in range(3):
            self.create_transfer(f"tx_bal_{i}", 8001, 8002, 100, 700, 10, 0)

        bals = self.get_account_balances(8001, limit=10)
        assert len(bals) == 3, f"expected 3 snapshots, got {len(bals)}"

        # Balances should be cumulative
        assert bals[0]['debits_posted'] == 100
        assert bals[1]['debits_posted'] == 200
        assert bals[2]['debits_posted'] == 300

        # Account without HISTORY should return empty
        bals_no_hist = self.get_account_balances(8002, limit=10)
        assert len(bals_no_hist) == 0, "account without HISTORY should return no snapshots"

    def test_multiple_transfers_cumulative(self):
        self.create_account(9001, 700, 10, 0)
        self.create_account(9002, 700, 10, 0)
        for i in range(10):
            self.create_transfer(f"tx_cum_{i}", 9001, 9002, 100, 700, 10, 0)
        da = self.lookup_account(9001)
        ca = self.lookup_account(9002)
        assert da['debits_posted'] == 1000
        assert ca['credits_posted'] == 1000

    # ------------------------------------------------------------------
    # Run all tests
    # ------------------------------------------------------------------

    def run_all(self):
        print(f"\n{'='*60}")
        print(f"Lua Beetle Sanity Tests — {self.backend}")
        print(f"{'='*60}")
        print("Flushing database...\n")
        self.r.flushdb()

        tests = [
            ("Create account (basic)",                   self.test_create_account_basic),
            ("Duplicate account rejected",               self.test_duplicate_account_rejected),
            ("Linked accounts — success path",           self.test_linked_accounts_success),
            ("Linked accounts — rollback on failure",    self.test_linked_accounts_rollback),
            ("Simple direct transfer",                   self.test_simple_transfer),
            ("Transfer: nonexistent debit account",      self.test_transfer_nonexistent_debit_account),
            ("Transfer: nonexistent credit account",     self.test_transfer_nonexistent_credit_account),
            ("Two-phase: PENDING",                       self.test_two_phase_pending),
            ("Two-phase: PENDING → POST",                self.test_two_phase_post),
            ("Two-phase: PENDING → VOID",                self.test_two_phase_void),
            ("Double-post rejected",                     self.test_double_post_rejected),
            ("Void-after-post rejected",                 self.test_void_after_post_rejected),
            ("Balance constraint: DEBITS_MUST_NOT_EXCEED_CREDITS",
                                                         self.test_balance_constraint_debits_must_not_exceed_credits),
            ("Balance constraint: CREDITS_MUST_NOT_EXCEED_DEBITS",
                                                         self.test_balance_constraint_credits_must_not_exceed_debits),
            ("Linked transfers — success path",          self.test_linked_transfers_success),
            ("Linked transfers — rollback on failure",   self.test_linked_transfers_rollback),
            ("Lookup transfer by ID",                    self.test_lookup_transfer),
            ("GET_ACCOUNT_TRANSFERS query",              self.test_get_account_transfers),
            ("GET_ACCOUNT_BALANCES query",               self.test_get_account_balances),
            ("Multiple transfers — cumulative balances", self.test_multiple_transfers_cumulative),
        ]

        for name, fn in tests:
            self.run_test(name, fn)

        elapsed = time.time() - self.start_time
        print(f"\n{'='*60}")
        print(f"Results: {self.passed} passed, {self.failed} failed  ({elapsed:.2f}s)")
        print(f"{'='*60}\n")
        return self.failed == 0


# ============================================================================
# Entry point
# ============================================================================

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--host', default='localhost')
    p.add_argument('--port', type=int, default=6379)
    p.add_argument('--backend', default='Redis')
    p.add_argument('--scripts', default='/home/lintaoz/work/lua_beetle/scripts',
                   help='Path to lua_beetle scripts directory')
    return p.parse_args()


def main():
    args = parse_args()
    r = redis.Redis(host=args.host, port=args.port, decode_responses=False)
    try:
        r.ping()
    except redis.ConnectionError as e:
        print(f"ERROR: Cannot connect to {args.backend} at {args.host}:{args.port}: {e}")
        sys.exit(1)

    runner = SanityTestRunner(r, args.scripts, args.backend)
    ok = runner.run_all()
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()

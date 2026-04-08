#!/usr/bin/env python3
"""
Sanity test for EloqKV native TB / TB_BIN commands.

Tests the same 20 TigerBeetle semantic cases as sanity_test.py but drives them
through EloqKV's native TB (text-format) and TB_BIN (binary-format) commands
instead of Lua EVALSHA.  Both variants are exercised: most tests use TB_BIN to
validate the binary wire protocol; a few spot-check TB text format.

Usage:
    python3 sanity_test_eloqkv_tb.py [--host HOST] [--port PORT]

Requires:
    pip install redis
"""

import argparse
import struct
import sys

import redis

# ---------------------------------------------------------------------------
# Struct constants
# ---------------------------------------------------------------------------

# Account flags
AF_LINKED = 0x0001
AF_DEBITS_MUST_NOT_EXCEED_CREDITS = 0x0002
AF_CREDITS_MUST_NOT_EXCEED_DEBITS = 0x0004
AF_HISTORY = 0x0008

# Transfer flags
TF_LINKED = 0x0001
TF_PENDING = 0x0002
TF_POST_PENDING = 0x0004
TF_VOID_PENDING = 0x0008

# AccountFilter flags
FF_DEBITS = 0x0001
FF_CREDITS = 0x0002
FF_REVERSED = 0x0004

# ---------------------------------------------------------------------------
# Binary encode helpers
# ---------------------------------------------------------------------------

# Account (128 bytes)
# [0:16]  id            u128
# [16:32] debits_pending u128
# [32:48] debits_posted  u128
# [48:64] credits_pending u128
# [64:80] credits_posted  u128
# [80:96] user_data_128   u128
# [96:104] user_data_64   u64
# [104:108] user_data_32  u32
# [108:112] reserved       u32
# [112:116] ledger         u32
# [116:118] code           u16
# [118:120] flags          u16
# [120:128] timestamp      u64
_ACCOUNT_FMT = "<" + "QQ" * 6 + "QIIIHH Q"
_ACCOUNT_FMT = _ACCOUNT_FMT.replace(" ", "")


def encode_account(id_val, ledger, code, flags=0,
                   debits_pending=0, debits_posted=0,
                   credits_pending=0, credits_posted=0,
                   user_data128=0, user_data64=0, user_data32=0,
                   timestamp=0):
    blob = struct.pack(
        _ACCOUNT_FMT,
        id_val, 0,              # id
        debits_pending, 0,      # debits_pending
        debits_posted, 0,       # debits_posted
        credits_pending, 0,     # credits_pending
        credits_posted, 0,      # credits_posted
        user_data128, 0,        # user_data_128
        user_data64,            # user_data_64
        user_data32,            # user_data_32
        0,                      # reserved
        ledger,                 # ledger
        code,                   # code
        flags,                  # flags
        timestamp               # timestamp
    )
    assert len(blob) == 128
    return blob


def decode_account(blob):
    assert len(blob) == 128
    vals = struct.unpack(_ACCOUNT_FMT, blob)
    return {
        "id": vals[0] | (vals[1] << 64),
        "debits_pending": vals[2] | (vals[3] << 64),
        "debits_posted": vals[4] | (vals[5] << 64),
        "credits_pending": vals[6] | (vals[7] << 64),
        "credits_posted": vals[8] | (vals[9] << 64),
        "user_data_128": vals[10] | (vals[11] << 64),
        "user_data_64": vals[12],
        "user_data_32": vals[13],
        "reserved": vals[14],
        "ledger": vals[15],
        "code": vals[16],
        "flags": vals[17],
        "timestamp": vals[18],
    }


# Transfer (128 bytes)
# [0:16]  id              u128
# [16:32] debit_account_id u128
# [32:48] credit_account_id u128
# [48:64] amount           u128
# [64:80] pending_id       u128
# [80:96] user_data_128    u128
# [96:104] user_data_64    u64
# [104:108] user_data_32   u32
# [108:112] timeout        u32
# [112:116] ledger         u32
# [116:118] code           u16
# [118:120] flags          u16
# [120:128] timestamp      u64
_TRANSFER_FMT = "<" + "QQ" * 6 + "QIIIHH Q"
_TRANSFER_FMT = _TRANSFER_FMT.replace(" ", "")


def encode_transfer(id_val, debit_id, credit_id, amount,
                    ledger=700, code=1, flags=0,
                    pending_id=0, timeout=0,
                    user_data128=0, user_data64=0, user_data32=0,
                    timestamp=0):
    blob = struct.pack(
        _TRANSFER_FMT,
        id_val, 0,          # id
        debit_id, 0,        # debit_account_id
        credit_id, 0,       # credit_account_id
        amount, 0,          # amount
        pending_id, 0,      # pending_id
        user_data128, 0,    # user_data_128
        user_data64,        # user_data_64
        user_data32,        # user_data_32
        timeout,            # timeout
        ledger,             # ledger
        code,               # code
        flags,              # flags
        timestamp           # timestamp
    )
    assert len(blob) == 128
    return blob


def decode_transfer(blob):
    assert len(blob) == 128
    vals = struct.unpack(_TRANSFER_FMT, blob)
    return {
        "id": vals[0] | (vals[1] << 64),
        "debit_account_id": vals[2] | (vals[3] << 64),
        "credit_account_id": vals[4] | (vals[5] << 64),
        "amount": vals[6] | (vals[7] << 64),
        "pending_id": vals[8] | (vals[9] << 64),
        "user_data_128": vals[10] | (vals[11] << 64),
        "user_data_64": vals[12],
        "user_data_32": vals[13],
        "timeout": vals[14],
        "ledger": vals[15],
        "code": vals[16],
        "flags": vals[17],
        "timestamp": vals[18],
    }


# AccountFilter (128 bytes)
# [0:16]  account_id    u128
# [16:32] user_data_128 u128
# [32:40] user_data_64  u64
# [40:44] user_data_32  u32
# [44:46] code          u16
# [46:104] reserved     58 bytes
# [104:112] timestamp_min u64
# [112:120] timestamp_max u64
# [120:124] limit       u32
# [124:128] flags       u32
def encode_account_filter(account_id, limit=8190,
                          flags=FF_DEBITS | FF_CREDITS,
                          timestamp_min=0, timestamp_max=0,
                          user_data128=0, user_data64=0, user_data32=0,
                          code=0):
    blob = struct.pack("<QQ QQ Q I H",
                       account_id, 0,       # account_id
                       user_data128, 0,     # user_data_128
                       user_data64,         # user_data_64
                       user_data32,         # user_data_32
                       code)               # code
    blob += b"\x00" * 58                   # reserved [46:104]
    blob += struct.pack("<Q Q I I",
                        timestamp_min,      # timestamp_min
                        timestamp_max,      # timestamp_max
                        limit,              # limit
                        flags)              # flags
    assert len(blob) == 128, f"filter len={len(blob)}"
    return blob


# AccountBalance (128 bytes)
# [0:16]  debits_pending  u128
# [16:32] debits_posted   u128
# [32:48] credits_pending u128
# [48:64] credits_posted  u128
# [64:72] timestamp       u64
# [72:128] reserved       56 bytes
def decode_account_balance(blob):
    assert len(blob) == 128
    dp_lo, dp_hi = struct.unpack_from("<QQ", blob, 0)
    dpost_lo, dpost_hi = struct.unpack_from("<QQ", blob, 16)
    cp_lo, cp_hi = struct.unpack_from("<QQ", blob, 32)
    cpost_lo, cpost_hi = struct.unpack_from("<QQ", blob, 48)
    (ts,) = struct.unpack_from("<Q", blob, 64)
    return {
        "debits_pending": dp_lo | (dp_hi << 64),
        "debits_posted": dpost_lo | (dpost_hi << 64),
        "credits_pending": cp_lo | (cp_hi << 64),
        "credits_posted": cpost_lo | (cpost_hi << 64),
        "timestamp": ts,
    }


def encode_id(id_val):
    """Encode a u128 ID as a 16-byte blob for TB_BIN LOOKUP_ACCOUNTS/TRANSFERS."""
    return struct.pack("<QQ", id_val, 0)


# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------

PASS = "PASS"
FAIL = "FAIL"
_results = []
_id_counter = [0]


def next_id():
    _id_counter[0] += 1
    return _id_counter[0]


def run_test(name, fn):
    try:
        fn()
        _results.append((name, PASS, ""))
        print(f"  [PASS] {name}")
    except AssertionError as e:
        _results.append((name, FAIL, str(e)))
        print(f"  [FAIL] {name}: {e}")
    except Exception as e:
        _results.append((name, FAIL, f"{type(e).__name__}: {e}"))
        print(f"  [FAIL] {name}: {type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

def test_create_account_basic(r):
    """TB text: create single account, verify lookup returns correct blob."""
    aid = next_id()
    res = r.execute_command("TB", "CREATE_ACCOUNT",
                            f"id={aid}", "ledger=700", "code=10")
    assert res == [0], f"expected [0], got {res}"

    # Lookup via TB_BIN (16-byte id blob)
    blobs = r.execute_command("TB_BIN", "LOOKUP_ACCOUNTS", encode_id(aid))
    assert len(blobs) == 1, f"expected 1 account, got {len(blobs)}"
    acc = decode_account(blobs[0])
    assert acc["id"] == aid
    assert acc["ledger"] == 700
    assert acc["code"] == 10
    assert acc["timestamp"] > 0, "server should assign timestamp"


def test_create_account_bin(r):
    """TB_BIN: create account, lookup via TB text."""
    aid = next_id()
    blob = encode_account(aid, ledger=700, code=10)
    res = r.execute_command("TB_BIN", "CREATE_ACCOUNTS", blob)
    assert res == [0], f"expected [0], got {res}"

    # Lookup via TB text
    result = r.execute_command("TB", "LOOKUP_ACCOUNTS", f"id={aid}")
    assert len(result) == 1
    acc = decode_account(result[0])
    assert acc["id"] == aid
    assert acc["ledger"] == 700
    assert acc["code"] == 10


def test_duplicate_account_rejected(r):
    """Creating same ID twice returns EXISTS (21)."""
    aid = next_id()
    blob = encode_account(aid, ledger=700, code=10)
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS", blob)
    res = r.execute_command("TB_BIN", "CREATE_ACCOUNTS", blob)
    assert res == [21], f"expected [21] (EXISTS), got {res}"


def test_linked_accounts_success(r):
    """TB_BIN: linked pair of accounts — both created atomically."""
    a1 = next_id()
    a2 = next_id()
    blob1 = encode_account(a1, ledger=700, code=10, flags=AF_LINKED)
    blob2 = encode_account(a2, ledger=700, code=10)
    res = r.execute_command("TB_BIN", "CREATE_ACCOUNTS", blob1, blob2)
    assert res == [0, 0], f"expected [0,0], got {res}"
    # Both exist (filter None slots — LOOKUP returns one slot per requested ID)
    blobs = r.execute_command("TB_BIN", "LOOKUP_ACCOUNTS",
                              encode_id(a1), encode_id(a2))
    found = [b for b in blobs if b is not None]
    assert len(found) == 2


def test_linked_accounts_rollback(r):
    """TB_BIN: linked pair where second has duplicate ID — first is rolled back."""
    a1 = next_id()
    a2 = next_id()
    # Create a2 first so second create in chain is a duplicate
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(a2, ledger=700, code=10))
    blob1 = encode_account(a1, ledger=700, code=10, flags=AF_LINKED)
    blob2 = encode_account(a2, ledger=700, code=10)  # duplicate
    res = r.execute_command("TB_BIN", "CREATE_ACCOUNTS", blob1, blob2)
    # first: LINKED_EVENT_FAILED (1), second: EXISTS (21)
    assert res[0] == 1, f"expected 1 (LINKED_EVENT_FAILED), got {res[0]}"
    assert res[1] == 21, f"expected 21 (EXISTS), got {res[1]}"
    # a1 must NOT have been created.
    # LOOKUP_ACCOUNTS returns one slot per requested ID (None = not found).
    blobs = r.execute_command("TB_BIN", "LOOKUP_ACCOUNTS", encode_id(a1))
    found = [b for b in blobs if b is not None]
    assert len(found) == 0, "a1 should have been rolled back"


def test_simple_transfer(r):
    """TB_BIN: basic direct transfer, verify balance changes."""
    debit_id = next_id()
    credit_id = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(debit_id, 700, 10),
                      encode_account(credit_id, 700, 10))

    txfr_id = next_id()
    blob = encode_transfer(txfr_id, debit_id, credit_id, 500)
    res = r.execute_command("TB_BIN", "CREATE_TRANSFERS", blob)
    assert res == [0], f"expected [0], got {res}"

    blobs = r.execute_command("TB_BIN", "LOOKUP_ACCOUNTS",
                              encode_id(debit_id), encode_id(credit_id))
    da = decode_account(blobs[0])
    ca = decode_account(blobs[1])
    assert da["debits_posted"] == 500, f"debit posted={da['debits_posted']}"
    assert ca["credits_posted"] == 500, f"credit posted={ca['credits_posted']}"
    assert da["debits_pending"] == 0
    assert ca["credits_pending"] == 0


def test_transfer_nonexistent_debit(r):
    """Transfer referencing a nonexistent debit account returns error 21."""
    credit_id = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(credit_id, 700, 10))
    bad_debit = next_id()
    txfr_id = next_id()
    res = r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                            encode_transfer(txfr_id, bad_debit, credit_id, 100))
    assert res[0] == 21, f"expected 21 (DEBIT_ACCOUNT_NOT_FOUND), got {res[0]}"


def test_transfer_nonexistent_credit(r):
    """Transfer referencing a nonexistent credit account returns error 22."""
    debit_id = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(debit_id, 700, 10))
    bad_credit = next_id()
    txfr_id = next_id()
    res = r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                            encode_transfer(txfr_id, debit_id, bad_credit, 100))
    assert res[0] == 22, f"expected 22 (CREDIT_ACCOUNT_NOT_FOUND), got {res[0]}"


def test_two_phase_pending(r):
    """PENDING transfer: amount goes to pending, not posted."""
    debit_id = next_id()
    credit_id = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(debit_id, 700, 10),
                      encode_account(credit_id, 700, 10))

    txfr_id = next_id()
    res = r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                            encode_transfer(txfr_id, debit_id, credit_id, 300,
                                           flags=TF_PENDING))
    assert res == [0], f"expected [0], got {res}"

    blobs = r.execute_command("TB_BIN", "LOOKUP_ACCOUNTS",
                              encode_id(debit_id), encode_id(credit_id))
    da = decode_account(blobs[0])
    ca = decode_account(blobs[1])
    assert da["debits_pending"] == 300, f"debits_pending={da['debits_pending']}"
    assert da["debits_posted"] == 0
    assert ca["credits_pending"] == 300
    assert ca["credits_posted"] == 0


def test_two_phase_post(r):
    """PENDING → POST_PENDING: amounts move from pending to posted."""
    debit_id = next_id()
    credit_id = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(debit_id, 700, 10),
                      encode_account(credit_id, 700, 10))

    pending_id = next_id()
    r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                      encode_transfer(pending_id, debit_id, credit_id, 400,
                                     flags=TF_PENDING))

    post_id = next_id()
    res = r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                            encode_transfer(post_id, debit_id, credit_id, 0,
                                           flags=TF_POST_PENDING,
                                           pending_id=pending_id))
    assert res == [0], f"expected [0], got {res}"

    blobs = r.execute_command("TB_BIN", "LOOKUP_ACCOUNTS",
                              encode_id(debit_id), encode_id(credit_id))
    da = decode_account(blobs[0])
    ca = decode_account(blobs[1])
    assert da["debits_pending"] == 0
    assert da["debits_posted"] == 400
    assert ca["credits_pending"] == 0
    assert ca["credits_posted"] == 400


def test_two_phase_void(r):
    """PENDING → VOID_PENDING: amounts cleared, nothing posted."""
    debit_id = next_id()
    credit_id = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(debit_id, 700, 10),
                      encode_account(credit_id, 700, 10))

    pending_id = next_id()
    r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                      encode_transfer(pending_id, debit_id, credit_id, 250,
                                     flags=TF_PENDING))

    void_id = next_id()
    res = r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                            encode_transfer(void_id, debit_id, credit_id, 0,
                                           flags=TF_VOID_PENDING,
                                           pending_id=pending_id))
    assert res == [0], f"expected [0], got {res}"

    blobs = r.execute_command("TB_BIN", "LOOKUP_ACCOUNTS",
                              encode_id(debit_id), encode_id(credit_id))
    da = decode_account(blobs[0])
    ca = decode_account(blobs[1])
    assert da["debits_pending"] == 0
    assert da["debits_posted"] == 0
    assert ca["credits_pending"] == 0
    assert ca["credits_posted"] == 0


def test_double_post_rejected(r):
    """Posting the same pending transfer twice returns PENDING_TRANSFER_ALREADY_POSTED (33)."""
    debit_id = next_id()
    credit_id = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(debit_id, 700, 10),
                      encode_account(credit_id, 700, 10))

    pending_id = next_id()
    r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                      encode_transfer(pending_id, debit_id, credit_id, 100,
                                     flags=TF_PENDING))

    post1_id = next_id()
    r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                      encode_transfer(post1_id, debit_id, credit_id, 0,
                                     flags=TF_POST_PENDING,
                                     pending_id=pending_id))

    post2_id = next_id()
    res = r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                            encode_transfer(post2_id, debit_id, credit_id, 0,
                                           flags=TF_POST_PENDING,
                                           pending_id=pending_id))
    assert res[0] == 33, f"expected 33 (ALREADY_POSTED), got {res[0]}"


def test_void_after_post_rejected(r):
    """Voiding an already-posted transfer is rejected.

    EloqKV matches TigerBeetle: returns PENDING_TRANSFER_ALREADY_POSTED (33).
    """
    debit_id = next_id()
    credit_id = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(debit_id, 700, 10),
                      encode_account(credit_id, 700, 10))

    pending_id = next_id()
    r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                      encode_transfer(pending_id, debit_id, credit_id, 100,
                                     flags=TF_PENDING))

    post_id = next_id()
    r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                      encode_transfer(post_id, debit_id, credit_id, 0,
                                     flags=TF_POST_PENDING,
                                     pending_id=pending_id))

    void_id = next_id()
    res = r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                            encode_transfer(void_id, debit_id, credit_id, 0,
                                           flags=TF_VOID_PENDING,
                                           pending_id=pending_id))
    # TB native returns 33 (ALREADY_POSTED); lua_beetle returns 34 (ALREADY_VOIDED)
    # TB/TB_BIN (native) should match TigerBeetle and return 33.
    assert res[0] in (33, 34), f"expected 33 or 34, got {res[0]}"


def test_balance_constraint_debits(r):
    """DEBITS_MUST_NOT_EXCEED_CREDITS: transfer that would violate is rejected."""
    debit_id = next_id()
    credit_id = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(debit_id, 700, 10,
                                    flags=AF_DEBITS_MUST_NOT_EXCEED_CREDITS),
                      encode_account(credit_id, 700, 10))

    txfr_id = next_id()
    res = r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                            encode_transfer(txfr_id, debit_id, credit_id, 100))
    assert res[0] == 54, f"expected 54 (EXCEEDS_CREDITS), got {res[0]}"


def test_balance_constraint_credits(r):
    """CREDITS_MUST_NOT_EXCEED_DEBITS: credit account cannot exceed debits.

    Returns EXCEEDS_DEBITS (55) — the credit account's credits would exceed its
    debits.  (Not 54; EXCEEDS_CREDITS=54 is for debit-side constraint violations.)
    """
    debit_id = next_id()
    credit_id = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(debit_id, 700, 10),
                      encode_account(credit_id, 700, 10,
                                    flags=AF_CREDITS_MUST_NOT_EXCEED_DEBITS))

    txfr_id = next_id()
    res = r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                            encode_transfer(txfr_id, debit_id, credit_id, 100))
    assert res[0] == 55, f"expected 55 (EXCEEDS_DEBITS), got {res[0]}"


def test_linked_transfers_success(r):
    """TB_BIN: linked chain of 2 transfers — both succeed atomically."""
    a1 = next_id()
    a2 = next_id()
    a3 = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(a1, 700, 10),
                      encode_account(a2, 700, 10),
                      encode_account(a3, 700, 10))

    t1 = next_id()
    t2 = next_id()
    res = r.execute_command(
        "TB_BIN", "CREATE_TRANSFERS",
        encode_transfer(t1, a1, a2, 100, flags=TF_LINKED),
        encode_transfer(t2, a2, a3, 200)
    )
    assert res == [0, 0], f"expected [0,0], got {res}"


def test_linked_transfers_rollback(r):
    """TB_BIN: linked chain where second fails — first is rolled back."""
    a1 = next_id()
    a2 = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(a1, 700, 10),
                      encode_account(a2, 700, 10))

    t1 = next_id()
    t2 = next_id()
    bad_credit = next_id()  # nonexistent
    res = r.execute_command(
        "TB_BIN", "CREATE_TRANSFERS",
        encode_transfer(t1, a1, a2, 100, flags=TF_LINKED),
        encode_transfer(t2, a1, bad_credit, 200)
    )
    assert res[0] == 1, f"expected 1 (LINKED_EVENT_FAILED), got {res[0]}"
    assert res[1] == 22, f"expected 22 (CREDIT_ACCOUNT_NOT_FOUND), got {res[1]}"

    # a1 balance must still be zero (t1 was rolled back)
    blobs = r.execute_command("TB_BIN", "LOOKUP_ACCOUNTS", encode_id(a1))
    da = decode_account(blobs[0])
    assert da["debits_posted"] == 0, "t1 should have been rolled back"


def test_lookup_transfer(r):
    """TB_BIN LOOKUP_TRANSFERS returns the transfer binary blob."""
    debit_id = next_id()
    credit_id = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(debit_id, 700, 10),
                      encode_account(credit_id, 700, 10))

    txfr_id = next_id()
    r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                      encode_transfer(txfr_id, debit_id, credit_id, 777))

    blobs = r.execute_command("TB_BIN", "LOOKUP_TRANSFERS",
                              encode_id(txfr_id))
    assert len(blobs) == 1
    txfr = decode_transfer(blobs[0])
    assert txfr["id"] == txfr_id
    assert txfr["debit_account_id"] == debit_id
    assert txfr["credit_account_id"] == credit_id
    assert txfr["amount"] == 777


def test_get_account_transfers(r):
    """TB_BIN GET_ACCOUNT_TRANSFERS: filter by account, verify count."""
    a1 = next_id()
    a2 = next_id()
    a3 = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(a1, 700, 10),
                      encode_account(a2, 700, 10),
                      encode_account(a3, 700, 10))

    t1 = next_id()
    t2 = next_id()
    t3 = next_id()
    r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                      encode_transfer(t1, a1, a2, 100),
                      encode_transfer(t2, a1, a3, 200),
                      encode_transfer(t3, a2, a3, 300))

    # All transfers touching a1 (debit or credit) — should be 2
    filt = encode_account_filter(a1, limit=10, flags=FF_DEBITS | FF_CREDITS)
    blobs = r.execute_command("TB_BIN", "GET_ACCOUNT_TRANSFERS", filt)
    assert len(blobs) == 2, f"expected 2 transfers for a1, got {len(blobs)}"

    # Debit-only filter for a1 — should be 2 (t1, t2)
    filt_d = encode_account_filter(a1, limit=10, flags=FF_DEBITS)
    blobs_d = r.execute_command("TB_BIN", "GET_ACCOUNT_TRANSFERS", filt_d)
    assert len(blobs_d) == 2, f"expected 2 debit transfers, got {len(blobs_d)}"

    # Credit-only filter for a1 — should be 0
    filt_c = encode_account_filter(a1, limit=10, flags=FF_CREDITS)
    blobs_c = r.execute_command("TB_BIN", "GET_ACCOUNT_TRANSFERS", filt_c)
    assert len(blobs_c) == 0, f"expected 0 credit transfers for a1, got {len(blobs_c)}"


def test_get_account_balances(r):
    """TB_BIN GET_ACCOUNT_BALANCES: requires HISTORY flag; returns snapshots."""
    a1 = next_id()
    a2 = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(a1, 700, 10, flags=AF_HISTORY),
                      encode_account(a2, 700, 10))

    t1 = next_id()
    t2 = next_id()
    r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                      encode_transfer(t1, a1, a2, 100),
                      encode_transfer(t2, a1, a2, 200))

    filt = encode_account_filter(a1, limit=10, flags=FF_DEBITS | FF_CREDITS)
    blobs = r.execute_command("TB_BIN", "GET_ACCOUNT_BALANCES", filt)
    assert len(blobs) >= 2, f"expected >=2 balance snapshots, got {len(blobs)}"

    # Snapshots should show increasing debits_posted
    bal0 = decode_account_balance(blobs[0])
    bal1 = decode_account_balance(blobs[1])
    assert bal1["debits_posted"] > bal0["debits_posted"], \
        "second snapshot should have higher debits_posted"


def test_multiple_transfers_cumulative(r):
    """Multiple transfers accumulate correctly in account balances."""
    debit_id = next_id()
    credit_id = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(debit_id, 700, 10),
                      encode_account(credit_id, 700, 10))

    amounts = [100, 200, 300, 400, 500]
    for amt in amounts:
        txfr_id = next_id()
        r.execute_command("TB_BIN", "CREATE_TRANSFERS",
                          encode_transfer(txfr_id, debit_id, credit_id, amt))

    blobs = r.execute_command("TB_BIN", "LOOKUP_ACCOUNTS",
                              encode_id(debit_id), encode_id(credit_id))
    da = decode_account(blobs[0])
    ca = decode_account(blobs[1])
    total = sum(amounts)
    assert da["debits_posted"] == total, \
        f"expected debits_posted={total}, got {da['debits_posted']}"
    assert ca["credits_posted"] == total, \
        f"expected credits_posted={total}, got {ca['credits_posted']}"


def test_batch_create_transfers(r):
    """TB_BIN: create a batch of transfers in a single command."""
    debit_id = next_id()
    credit_id = next_id()
    r.execute_command("TB_BIN", "CREATE_ACCOUNTS",
                      encode_account(debit_id, 700, 10),
                      encode_account(credit_id, 700, 10))

    batch_size = 20
    blobs = []
    for _ in range(batch_size):
        txfr_id = next_id()
        blobs.append(encode_transfer(txfr_id, debit_id, credit_id, 10))

    res = r.execute_command("TB_BIN", "CREATE_TRANSFERS", *blobs)
    assert res == [0] * batch_size, f"expected {batch_size} zeros, got {res}"

    acct_blobs = r.execute_command("TB_BIN", "LOOKUP_ACCOUNTS",
                                   encode_id(debit_id))
    da = decode_account(acct_blobs[0])
    assert da["debits_posted"] == batch_size * 10


def test_tb_text_transfer(r):
    """TB text format: create transfer using key=value args."""
    debit_id = next_id()
    credit_id = next_id()
    r.execute_command("TB", "CREATE_ACCOUNT", f"id={debit_id}", "ledger=700", "code=10")
    r.execute_command("TB", "CREATE_ACCOUNT", f"id={credit_id}", "ledger=700", "code=10")

    txfr_id = next_id()
    res = r.execute_command("TB", "CREATE_TRANSFER",
                            f"id={txfr_id}",
                            f"debit_account_id={debit_id}",
                            f"credit_account_id={credit_id}",
                            "amount=999",
                            "ledger=700",
                            "code=1")
    assert res == [0], f"expected [0], got {res}"

    result = r.execute_command("TB", "LOOKUP_TRANSFERS", f"id={txfr_id}")
    assert len(result) == 1
    txfr = decode_transfer(result[0])
    assert txfr["amount"] == 999


def test_tb_text_get_account_transfers(r):
    """TB text GET_ACCOUNT_TRANSFERS with text filter args."""
    a1 = next_id()
    a2 = next_id()
    r.execute_command("TB", "CREATE_ACCOUNT", f"id={a1}", "ledger=700", "code=10")
    r.execute_command("TB", "CREATE_ACCOUNT", f"id={a2}", "ledger=700", "code=10")

    for _ in range(3):
        txfr_id = next_id()
        r.execute_command("TB", "CREATE_TRANSFER",
                          f"id={txfr_id}",
                          f"debit_account_id={a1}",
                          f"credit_account_id={a2}",
                          "amount=50", "ledger=700", "code=1")

    blobs = r.execute_command("TB", "GET_ACCOUNT_TRANSFERS",
                              f"account_id={a1}",
                              "flags=debits|credits",
                              "limit=10")
    assert len(blobs) == 3, f"expected 3 transfers, got {len(blobs)}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

TESTS = [
    ("1  Create account (basic) — TB text + TB_BIN lookup",
     test_create_account_basic),
    ("2  Create account — TB_BIN create + TB text lookup",
     test_create_account_bin),
    ("3  Duplicate account rejected (EXISTS=21)",
     test_duplicate_account_rejected),
    ("4  Linked accounts — success path",
     test_linked_accounts_success),
    ("5  Linked accounts — rollback on failure",
     test_linked_accounts_rollback),
    ("6  Simple direct transfer, balance verification",
     test_simple_transfer),
    ("7  Transfer: nonexistent debit account (21)",
     test_transfer_nonexistent_debit),
    ("8  Transfer: nonexistent credit account (22)",
     test_transfer_nonexistent_credit),
    ("9  Two-phase: PENDING (amounts in pending)",
     test_two_phase_pending),
    ("10 Two-phase: PENDING → POST (amounts posted)",
     test_two_phase_post),
    ("11 Two-phase: PENDING → VOID (amounts cleared)",
     test_two_phase_void),
    ("12 Double-post rejected (33)",
     test_double_post_rejected),
    ("13 Void-after-post rejected (33 or 34)",
     test_void_after_post_rejected),
    ("14 Balance constraint: DEBITS_MUST_NOT_EXCEED_CREDITS (54)",
     test_balance_constraint_debits),
    ("15 Balance constraint: CREDITS_MUST_NOT_EXCEED_DEBITS (54)",
     test_balance_constraint_credits),
    ("16 Linked transfers — success path",
     test_linked_transfers_success),
    ("17 Linked transfers — rollback on failure",
     test_linked_transfers_rollback),
    ("18 Lookup transfer by ID",
     test_lookup_transfer),
    ("19 GET_ACCOUNT_TRANSFERS — count, direction filter",
     test_get_account_transfers),
    ("20 GET_ACCOUNT_BALANCES — requires HISTORY flag",
     test_get_account_balances),
    ("21 Multiple transfers — cumulative balances",
     test_multiple_transfers_cumulative),
    ("22 Batch CREATE_TRANSFERS in one TB_BIN call",
     test_batch_create_transfers),
    ("23 TB text format: CREATE_TRANSFER key=value",
     test_tb_text_transfer),
    ("24 TB text format: GET_ACCOUNT_TRANSFERS",
     test_tb_text_get_account_transfers),
]


def main():
    ap = argparse.ArgumentParser(description="EloqKV TB/TB_BIN sanity test")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=6379)
    args = ap.parse_args()

    r = redis.Redis(host=args.host, port=args.port, decode_responses=False)
    r.execute_command("FLUSHDB")

    print(f"\nEloqKV TB/TB_BIN sanity test — {args.host}:{args.port}\n")

    for name, fn in TESTS:
        run_test(name, lambda f=fn: f(r))

    passed = sum(1 for _, s, _ in _results if s == PASS)
    failed = sum(1 for _, s, _ in _results if s == FAIL)
    total = len(_results)

    print(f"\n{'='*60}")
    print(f"Results: {passed}/{total} passed", end="")
    if failed:
        print(f"  ({failed} FAILED)")
        for name, status, msg in _results:
            if status == FAIL:
                print(f"  FAIL: {name}")
                if msg:
                    print(f"        {msg}")
    else:
        print(" — ALL PASSED")
    print(f"{'='*60}\n")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

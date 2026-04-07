/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace EloqKV
{

// ---------------------------------------------------------------------------
// 128-bit unsigned integer (little-endian storage: lo = bytes 0-7, hi = bytes
// 8-15)
// ---------------------------------------------------------------------------
struct TbU128
{
    uint64_t lo{0};
    uint64_t hi{0};

    bool is_zero() const
    {
        return lo == 0 && hi == 0;
    }
    bool operator==(const TbU128 &o) const
    {
        return lo == o.lo && hi == o.hi;
    }
    bool operator!=(const TbU128 &o) const
    {
        return !(*this == o);
    }
    bool operator<(const TbU128 &o) const
    {
        if (hi != o.hi)
            return hi < o.hi;
        return lo < o.lo;
    }
    bool operator<=(const TbU128 &o) const
    {
        return !(o < *this);
    }
    bool operator>(const TbU128 &o) const
    {
        return o < *this;
    }
    TbU128 operator+(const TbU128 &o) const
    {
        TbU128 r;
        r.lo = lo + o.lo;
        r.hi = hi + o.hi + (r.lo < lo ? 1ULL : 0ULL);
        return r;
    }
    TbU128 operator-(const TbU128 &o) const
    {
        TbU128 r;
        r.lo = lo - o.lo;
        r.hi = hi - o.hi - (lo < o.lo ? 1ULL : 0ULL);
        return r;
    }
    // Returns true if adding `o` would overflow a u128
    bool would_overflow_add(const TbU128 &o) const
    {
        TbU128 r = *this + o;
        // overflow if result < either operand
        return r < *this;
    }
};

// ---------------------------------------------------------------------------
// Account flags (u16 bitmask)
// ---------------------------------------------------------------------------
namespace AccountFlags
{
constexpr uint16_t LINKED = 0x0001;
constexpr uint16_t DEBITS_MUST_NOT_EXCEED_CREDITS = 0x0002;
constexpr uint16_t CREDITS_MUST_NOT_EXCEED_DEBITS = 0x0004;
constexpr uint16_t HISTORY = 0x0008;
constexpr uint16_t IMPORTED = 0x0010;  // 1 << 4
constexpr uint16_t CLOSED = 0x0020;   // 1 << 5
}  // namespace AccountFlags

// ---------------------------------------------------------------------------
// Transfer flags (u16 bitmask)
// ---------------------------------------------------------------------------
namespace TransferFlags
{
constexpr uint16_t LINKED = 0x0001;
constexpr uint16_t PENDING = 0x0002;
constexpr uint16_t POST_PENDING = 0x0004;
constexpr uint16_t VOID_PENDING = 0x0008;
constexpr uint16_t BALANCING_DEBIT = 0x0010;   // 1 << 4
constexpr uint16_t BALANCING_CREDIT = 0x0020;  // 1 << 5
constexpr uint16_t CLOSING_DEBIT = 0x0040;     // 1 << 6
constexpr uint16_t CLOSING_CREDIT = 0x0080;    // 1 << 7
constexpr uint16_t IMPORTED = 0x0100;          // 1 << 8 (unchanged)
}  // namespace TransferFlags

// ---------------------------------------------------------------------------
// AccountFilter flags (u32 bitmask)
// ---------------------------------------------------------------------------
namespace AccountFilterFlags
{
constexpr uint32_t DEBITS = 0x0001;
constexpr uint32_t CREDITS = 0x0002;
constexpr uint32_t REVERSED = 0x0004;
}  // namespace AccountFilterFlags

// ---------------------------------------------------------------------------
// Account creation status codes (matching CreateAccountStatus from TigerBeetle)
// ---------------------------------------------------------------------------
enum class TbAccountStatus : uint32_t
{
    OK = 0,
    LINKED_EVENT_FAILED = 1,
    LINKED_EVENT_CHAIN_OPEN = 2,
    TIMESTAMP_MUST_BE_ZERO = 3,
    RESERVED_FIELD = 4,
    RESERVED_FLAG = 5,
    ID_MUST_NOT_BE_ZERO = 6,
    ID_MUST_NOT_BE_INT_MAX = 7,
    FLAGS_ARE_MUTUALLY_EXCLUSIVE = 8,
    DEBITS_PENDING_MUST_BE_ZERO = 9,
    DEBITS_POSTED_MUST_BE_ZERO = 10,
    CREDITS_PENDING_MUST_BE_ZERO = 11,
    CREDITS_POSTED_MUST_BE_ZERO = 12,
    LEDGER_MUST_NOT_BE_ZERO = 13,
    CODE_MUST_NOT_BE_ZERO = 14,
    EXISTS_WITH_DIFFERENT_FLAGS = 15,
    EXISTS_WITH_DIFFERENT_USER_DATA_128 = 16,
    EXISTS_WITH_DIFFERENT_USER_DATA_64 = 17,
    EXISTS_WITH_DIFFERENT_USER_DATA_32 = 18,
    EXISTS_WITH_DIFFERENT_LEDGER = 19,
    EXISTS_WITH_DIFFERENT_CODE = 20,
    EXISTS = 21,
};

// ---------------------------------------------------------------------------
// Transfer creation status codes (matching CreateTransferStatus from
// TigerBeetle)
// ---------------------------------------------------------------------------
enum class TbTransferStatus : uint32_t
{
    OK = 0,
    LINKED_EVENT_FAILED = 1,
    LINKED_EVENT_CHAIN_OPEN = 2,
    TIMESTAMP_MUST_BE_ZERO = 3,
    RESERVED_FLAG = 4,
    ID_MUST_NOT_BE_ZERO = 5,
    ID_MUST_NOT_BE_INT_MAX = 6,
    FLAGS_ARE_MUTUALLY_EXCLUSIVE = 7,
    DEBIT_ACCOUNT_ID_MUST_NOT_BE_ZERO = 8,
    DEBIT_ACCOUNT_ID_MUST_NOT_BE_INT_MAX = 9,
    CREDIT_ACCOUNT_ID_MUST_NOT_BE_ZERO = 10,
    CREDIT_ACCOUNT_ID_MUST_NOT_BE_INT_MAX = 11,
    ACCOUNTS_MUST_BE_DIFFERENT = 12,
    PENDING_ID_MUST_BE_ZERO = 13,
    PENDING_ID_MUST_NOT_BE_ZERO = 14,
    PENDING_ID_MUST_NOT_BE_INT_MAX = 15,
    PENDING_ID_MUST_BE_DIFFERENT = 16,
    TIMEOUT_RESERVED_FOR_PENDING_TRANSFER = 17,
    AMOUNT_MUST_NOT_BE_ZERO = 18,
    LEDGER_MUST_NOT_BE_ZERO = 19,
    CODE_MUST_NOT_BE_ZERO = 20,
    DEBIT_ACCOUNT_NOT_FOUND = 21,
    CREDIT_ACCOUNT_NOT_FOUND = 22,
    ACCOUNTS_MUST_HAVE_THE_SAME_LEDGER = 23,
    TRANSFER_MUST_HAVE_THE_SAME_LEDGER_AS_ACCOUNTS = 24,
    PENDING_TRANSFER_NOT_FOUND = 25,
    PENDING_TRANSFER_NOT_PENDING = 26,
    PENDING_TRANSFER_HAS_DIFFERENT_DEBIT_ACCOUNT_ID = 27,
    PENDING_TRANSFER_HAS_DIFFERENT_CREDIT_ACCOUNT_ID = 28,
    PENDING_TRANSFER_HAS_DIFFERENT_LEDGER = 29,
    PENDING_TRANSFER_HAS_DIFFERENT_CODE = 30,
    EXCEEDS_PENDING_TRANSFER_AMOUNT = 31,
    PENDING_TRANSFER_HAS_DIFFERENT_AMOUNT = 32,
    PENDING_TRANSFER_ALREADY_POSTED = 33,
    PENDING_TRANSFER_ALREADY_VOIDED = 34,
    PENDING_TRANSFER_EXPIRED = 35,
    EXISTS_WITH_DIFFERENT_FLAGS = 36,
    EXISTS_WITH_DIFFERENT_DEBIT_ACCOUNT_ID = 37,
    EXISTS_WITH_DIFFERENT_CREDIT_ACCOUNT_ID = 38,
    EXISTS_WITH_DIFFERENT_AMOUNT = 39,
    EXISTS_WITH_DIFFERENT_PENDING_ID = 40,
    EXISTS_WITH_DIFFERENT_USER_DATA_128 = 41,
    EXISTS_WITH_DIFFERENT_USER_DATA_64 = 42,
    EXISTS_WITH_DIFFERENT_USER_DATA_32 = 43,
    EXISTS_WITH_DIFFERENT_TIMEOUT = 44,
    EXISTS_WITH_DIFFERENT_CODE = 45,
    EXISTS = 46,
    OVERFLOWS_DEBITS_PENDING = 47,
    OVERFLOWS_CREDITS_PENDING = 48,
    OVERFLOWS_DEBITS_POSTED = 49,
    OVERFLOWS_CREDITS_POSTED = 50,
    OVERFLOWS_DEBITS = 51,
    OVERFLOWS_CREDITS = 52,
    OVERFLOWS_TIMEOUT = 53,
    EXCEEDS_CREDITS = 54,
    EXCEEDS_DEBITS = 55,
    IMPORTED_EVENT_EXPECTED = 56,
    IMPORTED_EVENT_NOT_EXPECTED = 57,
    IMPORTED_EVENT_TIMESTAMP_OUT_OF_RANGE = 58,
    IMPORTED_EVENT_TIMESTAMP_MUST_NOT_ADVANCE = 59,
    IMPORTED_EVENT_TIMESTAMP_MUST_NOT_REGRESS = 60,
    IMPORTED_EVENT_TIMESTAMP_MUST_POSTDATE_DEBIT_ACCOUNT = 61,
    IMPORTED_EVENT_TIMESTAMP_MUST_POSTDATE_CREDIT_ACCOUNT = 62,
    IMPORTED_EVENT_TIMEOUT_MUST_BE_ZERO = 63,
    CLOSING_TRANSFER_MUST_BE_PENDING = 64,
    DEBIT_ACCOUNT_ALREADY_CLOSED = 65,
    CREDIT_ACCOUNT_ALREADY_CLOSED = 66,
    EXISTS_WITH_DIFFERENT_LEDGER = 67,
    ID_ALREADY_FAILED = 68,
};

// ---------------------------------------------------------------------------
// Account (128 bytes, all fields little-endian)
// Layout:
//   [0:16]   id
//   [16:32]  debits_pending
//   [32:48]  debits_posted
//   [48:64]  credits_pending
//   [64:80]  credits_posted
//   [80:96]  user_data_128
//   [96:104] user_data_64
//   [104:108] user_data_32
//   [108:112] reserved (must be 0)
//   [112:116] ledger
//   [116:118] code
//   [118:120] flags
//   [120:128] timestamp
// ---------------------------------------------------------------------------
struct TbAccount
{
    TbU128 id;
    TbU128 debits_pending;
    TbU128 debits_posted;
    TbU128 credits_pending;
    TbU128 credits_posted;
    TbU128 user_data_128;
    uint64_t user_data_64{0};
    uint32_t user_data_32{0};
    uint32_t reserved{0};
    uint32_t ledger{0};
    uint16_t code{0};
    uint16_t flags{0};
    uint64_t timestamp{0};

    bool has_flag(uint16_t f) const
    {
        return (flags & f) != 0;
    }

    // Helpers for balance constraint checks
    TbU128 total_debits() const
    {
        return debits_pending + debits_posted;
    }
    TbU128 total_credits() const
    {
        return credits_pending + credits_posted;
    }
};

// ---------------------------------------------------------------------------
// Transfer (128 bytes, all fields little-endian)
// Layout:
//   [0:16]   id
//   [16:32]  debit_account_id
//   [32:48]  credit_account_id
//   [48:64]  amount
//   [64:80]  pending_id
//   [80:96]  user_data_128
//   [96:104] user_data_64
//   [104:108] user_data_32
//   [108:112] timeout
//   [112:116] ledger
//   [116:118] code
//   [118:120] flags
//   [120:128] timestamp
// ---------------------------------------------------------------------------
struct TbTransfer
{
    TbU128 id;
    TbU128 debit_account_id;
    TbU128 credit_account_id;
    TbU128 amount;
    TbU128 pending_id;
    TbU128 user_data_128;
    uint64_t user_data_64{0};
    uint32_t user_data_32{0};
    uint32_t timeout{0};
    uint32_t ledger{0};
    uint16_t code{0};
    uint16_t flags{0};
    uint64_t timestamp{0};

    bool has_flag(uint16_t f) const
    {
        return (flags & f) != 0;
    }
};

// ---------------------------------------------------------------------------
// AccountBalance (128 bytes) — balance snapshot for history
// Layout matches TigerBeetle ABI:
//   [0:16]   debits_pending
//   [16:32]  debits_posted
//   [32:48]  credits_pending
//   [48:64]  credits_posted
//   [64:72]  timestamp
//   [72:128] reserved (zeros)
// ---------------------------------------------------------------------------
struct TbAccountBalance
{
    TbU128 debits_pending;
    TbU128 debits_posted;
    TbU128 credits_pending;
    TbU128 credits_posted;
    uint64_t timestamp{0};
};

// ---------------------------------------------------------------------------
// AccountFilter (128 bytes)
// Layout:
//   [0:16]   account_id
//   [16:32]  user_data_128
//   [32:40]  user_data_64
//   [40:44]  user_data_32
//   [44:46]  code
//   [46:104] reserved
//   [104:112] timestamp_min
//   [112:120] timestamp_max
//   [120:124] limit
//   [124:128] flags
// ---------------------------------------------------------------------------
struct TbAccountFilter
{
    TbU128 account_id;
    TbU128 user_data_128;
    uint64_t user_data_64{0};
    uint32_t user_data_32{0};
    uint16_t code{0};
    uint64_t timestamp_min{0};
    uint64_t timestamp_max{0};
    uint32_t limit{0};
    uint32_t flags{0};
};

// ---------------------------------------------------------------------------
// Binary encode/decode primitives (little-endian)
// ---------------------------------------------------------------------------
void tb_encode_u16(uint8_t *buf, uint16_t val);
void tb_encode_u32(uint8_t *buf, uint32_t val);
void tb_encode_u64(uint8_t *buf, uint64_t val);
void tb_encode_u128(uint8_t *buf, const TbU128 &val);

uint16_t tb_decode_u16(const uint8_t *buf);
uint32_t tb_decode_u32(const uint8_t *buf);
uint64_t tb_decode_u64(const uint8_t *buf);
TbU128 tb_decode_u128(const uint8_t *buf);

// ---------------------------------------------------------------------------
// Struct <-> 128-byte binary string encode/decode
// ---------------------------------------------------------------------------
std::string tb_encode_account(const TbAccount &acc);
bool tb_decode_account(std::string_view data, TbAccount &acc);

std::string tb_encode_transfer(const TbTransfer &txfr);
bool tb_decode_transfer(std::string_view data, TbTransfer &txfr);

std::string tb_encode_account_balance(const TbAccountBalance &bal);
bool tb_decode_account_balance(std::string_view data, TbAccountBalance &bal);

bool tb_decode_account_filter(std::string_view data, TbAccountFilter &filter);

// ---------------------------------------------------------------------------
// Redis key helpers
// All account keys use the raw 16-byte binary id (matching lua_beetle schema)
// Transfer keys use lowercase hex id (matching lua_beetle schema)
// ---------------------------------------------------------------------------
std::string tb_account_key(const TbU128 &id);
std::string tb_transfer_key(const TbU128 &id);
std::string tb_account_transfers_key(const TbU128 &id);
std::string tb_account_balances_key(const TbU128 &id);

// Convert TbU128 to 32-char lowercase hex string
std::string tb_u128_to_hex(const TbU128 &id);

// Convert 16-byte binary to 32-char lowercase hex
std::string tb_bytes_to_hex(const uint8_t *buf, size_t len);

// Get a server-assigned monotonic nanosecond timestamp
uint64_t tb_current_timestamp_ns();

// ---------------------------------------------------------------------------
// Text parser: parse "key=val key=val ..." tokens into TB structs
// `tokens` is the full args vector; [start, end) is the slice to parse.
// Returns true on success; sets err_msg on failure.
// ---------------------------------------------------------------------------
bool tb_parse_account_text(const std::vector<std::string_view> &tokens,
                           size_t start,
                           size_t end,
                           TbAccount &acc,
                           std::string &err_msg);

bool tb_parse_transfer_text(const std::vector<std::string_view> &tokens,
                            size_t start,
                            size_t end,
                            TbTransfer &txfr,
                            std::string &err_msg);

bool tb_parse_account_filter_text(const std::vector<std::string_view> &tokens,
                                  size_t start,
                                  size_t end,
                                  TbAccountFilter &filter,
                                  std::string &err_msg);

// Parse a u128 integer from decimal or hex (0x...) string
bool tb_parse_u128(std::string_view val, TbU128 &out);

// Parse account flags from pipe-separated names (e.g. "linked|history")
bool tb_parse_account_flags(std::string_view val, uint16_t &out);

// Parse transfer flags from pipe-separated names (e.g. "pending|linked")
bool tb_parse_transfer_flags(std::string_view val, uint16_t &out);

// Parse account filter flags (e.g. "debits|credits|reversed")
bool tb_parse_filter_flags(std::string_view val, uint32_t &out);

// Split a token list at comma separators (args that are exactly ",")
// Returns ranges [start, end) for each object
std::vector<std::pair<size_t, size_t>> tb_split_batch(
    const std::vector<std::string_view> &tokens,
    size_t start);

}  // namespace EloqKV

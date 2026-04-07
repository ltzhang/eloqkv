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
#include "tb_types.h"

#include <charconv>
#include <chrono>
#include <cstring>

namespace EloqKV
{

// ---------------------------------------------------------------------------
// Primitive encode/decode (little-endian)
// ---------------------------------------------------------------------------

void tb_encode_u16(uint8_t *buf, uint16_t val)
{
    buf[0] = static_cast<uint8_t>(val);
    buf[1] = static_cast<uint8_t>(val >> 8);
}

void tb_encode_u32(uint8_t *buf, uint32_t val)
{
    buf[0] = static_cast<uint8_t>(val);
    buf[1] = static_cast<uint8_t>(val >> 8);
    buf[2] = static_cast<uint8_t>(val >> 16);
    buf[3] = static_cast<uint8_t>(val >> 24);
}

void tb_encode_u64(uint8_t *buf, uint64_t val)
{
    buf[0] = static_cast<uint8_t>(val);
    buf[1] = static_cast<uint8_t>(val >> 8);
    buf[2] = static_cast<uint8_t>(val >> 16);
    buf[3] = static_cast<uint8_t>(val >> 24);
    buf[4] = static_cast<uint8_t>(val >> 32);
    buf[5] = static_cast<uint8_t>(val >> 40);
    buf[6] = static_cast<uint8_t>(val >> 48);
    buf[7] = static_cast<uint8_t>(val >> 56);
}

void tb_encode_u128(uint8_t *buf, const TbU128 &val)
{
    tb_encode_u64(buf, val.lo);
    tb_encode_u64(buf + 8, val.hi);
}

uint16_t tb_decode_u16(const uint8_t *buf)
{
    return static_cast<uint16_t>(buf[0]) |
           (static_cast<uint16_t>(buf[1]) << 8);
}

uint32_t tb_decode_u32(const uint8_t *buf)
{
    return static_cast<uint32_t>(buf[0]) | (static_cast<uint32_t>(buf[1]) << 8) |
           (static_cast<uint32_t>(buf[2]) << 16) |
           (static_cast<uint32_t>(buf[3]) << 24);
}

uint64_t tb_decode_u64(const uint8_t *buf)
{
    return static_cast<uint64_t>(buf[0]) | (static_cast<uint64_t>(buf[1]) << 8) |
           (static_cast<uint64_t>(buf[2]) << 16) |
           (static_cast<uint64_t>(buf[3]) << 24) |
           (static_cast<uint64_t>(buf[4]) << 32) |
           (static_cast<uint64_t>(buf[5]) << 40) |
           (static_cast<uint64_t>(buf[6]) << 48) |
           (static_cast<uint64_t>(buf[7]) << 56);
}

TbU128 tb_decode_u128(const uint8_t *buf)
{
    TbU128 v;
    v.lo = tb_decode_u64(buf);
    v.hi = tb_decode_u64(buf + 8);
    return v;
}

// ---------------------------------------------------------------------------
// Account encode/decode
// ---------------------------------------------------------------------------

std::string tb_encode_account(const TbAccount &acc)
{
    std::string buf(128, '\0');
    auto *b = reinterpret_cast<uint8_t *>(buf.data());
    tb_encode_u128(b + 0, acc.id);
    tb_encode_u128(b + 16, acc.debits_pending);
    tb_encode_u128(b + 32, acc.debits_posted);
    tb_encode_u128(b + 48, acc.credits_pending);
    tb_encode_u128(b + 64, acc.credits_posted);
    tb_encode_u128(b + 80, acc.user_data_128);
    tb_encode_u64(b + 96, acc.user_data_64);
    tb_encode_u32(b + 104, acc.user_data_32);
    tb_encode_u32(b + 108, acc.reserved);
    tb_encode_u32(b + 112, acc.ledger);
    tb_encode_u16(b + 116, acc.code);
    tb_encode_u16(b + 118, acc.flags);
    tb_encode_u64(b + 120, acc.timestamp);
    return buf;
}

bool tb_decode_account(std::string_view data, TbAccount &acc)
{
    if (data.size() != 128)
        return false;
    const auto *b = reinterpret_cast<const uint8_t *>(data.data());
    acc.id = tb_decode_u128(b + 0);
    acc.debits_pending = tb_decode_u128(b + 16);
    acc.debits_posted = tb_decode_u128(b + 32);
    acc.credits_pending = tb_decode_u128(b + 48);
    acc.credits_posted = tb_decode_u128(b + 64);
    acc.user_data_128 = tb_decode_u128(b + 80);
    acc.user_data_64 = tb_decode_u64(b + 96);
    acc.user_data_32 = tb_decode_u32(b + 104);
    acc.reserved = tb_decode_u32(b + 108);
    acc.ledger = tb_decode_u32(b + 112);
    acc.code = tb_decode_u16(b + 116);
    acc.flags = tb_decode_u16(b + 118);
    acc.timestamp = tb_decode_u64(b + 120);
    return true;
}

// ---------------------------------------------------------------------------
// Transfer encode/decode
// ---------------------------------------------------------------------------

std::string tb_encode_transfer(const TbTransfer &txfr)
{
    std::string buf(128, '\0');
    auto *b = reinterpret_cast<uint8_t *>(buf.data());
    tb_encode_u128(b + 0, txfr.id);
    tb_encode_u128(b + 16, txfr.debit_account_id);
    tb_encode_u128(b + 32, txfr.credit_account_id);
    tb_encode_u128(b + 48, txfr.amount);
    tb_encode_u128(b + 64, txfr.pending_id);
    tb_encode_u128(b + 80, txfr.user_data_128);
    tb_encode_u64(b + 96, txfr.user_data_64);
    tb_encode_u32(b + 104, txfr.user_data_32);
    tb_encode_u32(b + 108, txfr.timeout);
    tb_encode_u32(b + 112, txfr.ledger);
    tb_encode_u16(b + 116, txfr.code);
    tb_encode_u16(b + 118, txfr.flags);
    tb_encode_u64(b + 120, txfr.timestamp);
    return buf;
}

bool tb_decode_transfer(std::string_view data, TbTransfer &txfr)
{
    if (data.size() != 128)
        return false;
    const auto *b = reinterpret_cast<const uint8_t *>(data.data());
    txfr.id = tb_decode_u128(b + 0);
    txfr.debit_account_id = tb_decode_u128(b + 16);
    txfr.credit_account_id = tb_decode_u128(b + 32);
    txfr.amount = tb_decode_u128(b + 48);
    txfr.pending_id = tb_decode_u128(b + 64);
    txfr.user_data_128 = tb_decode_u128(b + 80);
    txfr.user_data_64 = tb_decode_u64(b + 96);
    txfr.user_data_32 = tb_decode_u32(b + 104);
    txfr.timeout = tb_decode_u32(b + 108);
    txfr.ledger = tb_decode_u32(b + 112);
    txfr.code = tb_decode_u16(b + 116);
    txfr.flags = tb_decode_u16(b + 118);
    txfr.timestamp = tb_decode_u64(b + 120);
    return true;
}

// ---------------------------------------------------------------------------
// AccountBalance encode/decode
// ---------------------------------------------------------------------------

std::string tb_encode_account_balance(const TbAccountBalance &bal)
{
    std::string buf(128, '\0');
    auto *b = reinterpret_cast<uint8_t *>(buf.data());
    tb_encode_u128(b + 0, bal.debits_pending);
    tb_encode_u128(b + 16, bal.debits_posted);
    tb_encode_u128(b + 32, bal.credits_pending);
    tb_encode_u128(b + 48, bal.credits_posted);
    tb_encode_u64(b + 64, bal.timestamp);
    // bytes [72:128] remain zero
    return buf;
}

bool tb_decode_account_balance(std::string_view data, TbAccountBalance &bal)
{
    if (data.size() != 128)
        return false;
    const auto *b = reinterpret_cast<const uint8_t *>(data.data());
    bal.debits_pending = tb_decode_u128(b + 0);
    bal.debits_posted = tb_decode_u128(b + 16);
    bal.credits_pending = tb_decode_u128(b + 32);
    bal.credits_posted = tb_decode_u128(b + 48);
    bal.timestamp = tb_decode_u64(b + 64);
    return true;
}

bool tb_decode_account_filter(std::string_view data, TbAccountFilter &filter)
{
    if (data.size() != 128)
        return false;
    const auto *b = reinterpret_cast<const uint8_t *>(data.data());
    filter.account_id = tb_decode_u128(b + 0);
    filter.user_data_128 = tb_decode_u128(b + 16);
    filter.user_data_64 = tb_decode_u64(b + 32);
    filter.user_data_32 = tb_decode_u32(b + 40);
    filter.code = tb_decode_u16(b + 44);
    for (size_t i = 46; i < 104; ++i)
    {
        if (b[i] != 0)
            return false;
    }
    filter.timestamp_min = tb_decode_u64(b + 104);
    filter.timestamp_max = tb_decode_u64(b + 112);
    filter.limit = tb_decode_u32(b + 120);
    filter.flags = tb_decode_u32(b + 124);
    return true;
}

// ---------------------------------------------------------------------------
// Redis key helpers
// ---------------------------------------------------------------------------

std::string tb_bytes_to_hex(const uint8_t *buf, size_t len)
{
    static const char hex_chars[] = "0123456789abcdef";
    std::string result;
    result.reserve(len * 2);
    for (size_t i = 0; i < len; i++)
    {
        result.push_back(hex_chars[(buf[i] >> 4) & 0xF]);
        result.push_back(hex_chars[buf[i] & 0xF]);
    }
    return result;
}

std::string tb_u128_to_hex(const TbU128 &id)
{
    uint8_t buf[16];
    tb_encode_u128(buf, id);
    return tb_bytes_to_hex(buf, 16);
}

// Account key uses raw 16-byte binary id (compatible with lua_beetle)
std::string tb_account_key(const TbU128 &id)
{
    uint8_t buf[16];
    tb_encode_u128(buf, id);
    std::string key = "account:";
    key.append(reinterpret_cast<const char *>(buf), 16);
    return key;
}

// Transfer key uses hex id (compatible with lua_beetle)
std::string tb_transfer_key(const TbU128 &id)
{
    return "transfer:" + tb_u128_to_hex(id);
}

// Transfer history sorted set key
std::string tb_account_transfers_key(const TbU128 &id)
{
    uint8_t buf[16];
    tb_encode_u128(buf, id);
    std::string key = "account:";
    key.append(reinterpret_cast<const char *>(buf), 16);
    key.append(":transfers");
    return key;
}

// Balance history list key
std::string tb_account_balances_key(const TbU128 &id)
{
    uint8_t buf[16];
    tb_encode_u128(buf, id);
    std::string key = "account:";
    key.append(reinterpret_cast<const char *>(buf), 16);
    key.append(":balance_history");
    return key;
}

// ---------------------------------------------------------------------------
// Timestamp
// ---------------------------------------------------------------------------

uint64_t tb_current_timestamp_ns()
{
    using namespace std::chrono;
    auto sys_now = time_point_cast<nanoseconds>(system_clock::now());
    return static_cast<uint64_t>(sys_now.time_since_epoch().count());
}

// ---------------------------------------------------------------------------
// u128 text parser
// ---------------------------------------------------------------------------

bool tb_parse_u128(std::string_view val, TbU128 &out)
{
    if (val.empty())
        return false;

    out = TbU128{};

    // Hex: "0x..." or "0X..."
    if (val.size() >= 2 && val[0] == '0' && (val[1] == 'x' || val[1] == 'X'))
    {
        std::string_view hex = val.substr(2);
        if (hex.empty() || hex.size() > 32)
            return false;
        for (char c : hex)
        {
            uint8_t nibble;
            if (c >= '0' && c <= '9')
                nibble = c - '0';
            else if (c >= 'a' && c <= 'f')
                nibble = c - 'a' + 10;
            else if (c >= 'A' && c <= 'F')
                nibble = c - 'A' + 10;
            else
                return false;
            // shift out.hi:out.lo left by 4 bits
            out.hi = (out.hi << 4) | (out.lo >> 60);
            out.lo = (out.lo << 4) | nibble;
        }
        return true;
    }

    // Decimal
    for (char c : val)
    {
        if (c < '0' || c > '9')
            return false;
        uint8_t digit = c - '0';
        // result = result * 10 + digit (128-bit multiply by 10)
        uint64_t new_lo = out.lo * 10 + digit;
        uint64_t carry = (out.lo > (UINT64_MAX - digit) / 10) ? 1 : 0;
        // More precise carry: check if lo * 10 overflows
        // Use __uint128_t for accuracy
        unsigned __int128 tmp =
            static_cast<unsigned __int128>(out.lo) * 10 + digit;
        new_lo = static_cast<uint64_t>(tmp);
        carry = static_cast<uint64_t>(tmp >> 64);
        out.lo = new_lo;
        out.hi = out.hi * 10 + carry;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Flag parsers
// ---------------------------------------------------------------------------

bool tb_parse_account_flags(std::string_view val, uint16_t &out)
{
    out = 0;
    if (val == "0")
        return true;

    // Split by '|'
    size_t pos = 0;
    while (pos <= val.size())
    {
        size_t end = val.find('|', pos);
        if (end == std::string_view::npos)
            end = val.size();
        std::string_view flag = val.substr(pos, end - pos);
        // Trim whitespace
        while (!flag.empty() && flag.front() == ' ')
            flag.remove_prefix(1);
        while (!flag.empty() && flag.back() == ' ')
            flag.remove_suffix(1);

        if (flag == "linked")
            out |= AccountFlags::LINKED;
        else if (flag == "debits_must_not_exceed_credits")
            out |= AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS;
        else if (flag == "credits_must_not_exceed_debits")
            out |= AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS;
        else if (flag == "history")
            out |= AccountFlags::HISTORY;
        else if (flag == "imported")
            out |= AccountFlags::IMPORTED;
        else if (flag == "closed")
            out |= AccountFlags::CLOSED;
        else if (!flag.empty())
            return false;

        if (end == val.size())
            break;
        pos = end + 1;
    }
    return true;
}

bool tb_parse_transfer_flags(std::string_view val, uint16_t &out)
{
    out = 0;
    if (val == "0")
        return true;

    size_t pos = 0;
    while (pos <= val.size())
    {
        size_t end = val.find('|', pos);
        if (end == std::string_view::npos)
            end = val.size();
        std::string_view flag = val.substr(pos, end - pos);
        while (!flag.empty() && flag.front() == ' ')
            flag.remove_prefix(1);
        while (!flag.empty() && flag.back() == ' ')
            flag.remove_suffix(1);

        if (flag == "linked")
            out |= TransferFlags::LINKED;
        else if (flag == "pending")
            out |= TransferFlags::PENDING;
        else if (flag == "post_pending")
            out |= TransferFlags::POST_PENDING;
        else if (flag == "void_pending")
            out |= TransferFlags::VOID_PENDING;
        else if (flag == "balancing_debit")
            out |= TransferFlags::BALANCING_DEBIT;
        else if (flag == "balancing_credit")
            out |= TransferFlags::BALANCING_CREDIT;
        else if (flag == "closing_debit")
            out |= TransferFlags::CLOSING_DEBIT;
        else if (flag == "closing_credit")
            out |= TransferFlags::CLOSING_CREDIT;
        else if (flag == "imported")
            out |= TransferFlags::IMPORTED;
        else if (!flag.empty())
            return false;

        if (end == val.size())
            break;
        pos = end + 1;
    }
    return true;
}

bool tb_parse_filter_flags(std::string_view val, uint32_t &out)
{
    out = 0;
    if (val == "0")
        return true;

    size_t pos = 0;
    while (pos <= val.size())
    {
        size_t end = val.find('|', pos);
        if (end == std::string_view::npos)
            end = val.size();
        std::string_view flag = val.substr(pos, end - pos);
        while (!flag.empty() && flag.front() == ' ')
            flag.remove_prefix(1);
        while (!flag.empty() && flag.back() == ' ')
            flag.remove_suffix(1);

        if (flag == "debits")
            out |= AccountFilterFlags::DEBITS;
        else if (flag == "credits")
            out |= AccountFilterFlags::CREDITS;
        else if (flag == "reversed")
            out |= AccountFilterFlags::REVERSED;
        else if (!flag.empty())
            return false;

        if (end == val.size())
            break;
        pos = end + 1;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Batch splitter: find comma-separated ranges in args[start:]
// A comma is a standalone arg equal to ","
// ---------------------------------------------------------------------------

std::vector<std::pair<size_t, size_t>> tb_split_batch(
    const std::vector<std::string_view> &tokens,
    size_t start)
{
    std::vector<std::pair<size_t, size_t>> ranges;
    size_t obj_start = start;
    for (size_t i = start; i < tokens.size(); i++)
    {
        if (tokens[i] == ",")
        {
            if (i > obj_start)
                ranges.emplace_back(obj_start, i);
            obj_start = i + 1;
        }
    }
    if (obj_start < tokens.size())
        ranges.emplace_back(obj_start, tokens.size());
    return ranges;
}

// ---------------------------------------------------------------------------
// Text field parser: parse "key=value" token
// Returns {"key", "value"} or {"", ""} on failure
// ---------------------------------------------------------------------------

static std::pair<std::string_view, std::string_view> parse_kv(
    std::string_view token)
{
    size_t eq = token.find('=');
    if (eq == std::string_view::npos)
        return {"", ""};
    return {token.substr(0, eq), token.substr(eq + 1)};
}

// ---------------------------------------------------------------------------
// Account text parser
// ---------------------------------------------------------------------------

bool tb_parse_account_text(const std::vector<std::string_view> &tokens,
                           size_t start,
                           size_t end,
                           TbAccount &acc,
                           std::string &err_msg)
{
    acc = TbAccount{};
    for (size_t i = start; i < end; i++)
    {
        auto [key, val] = parse_kv(tokens[i]);
        if (key.empty())
        {
            err_msg = "ERR invalid token: " + std::string(tokens[i]);
            return false;
        }

        if (key == "id")
        {
            if (!tb_parse_u128(val, acc.id))
            {
                err_msg = "ERR invalid id: " + std::string(val);
                return false;
            }
        }
        else if (key == "ledger")
        {
            uint64_t v = 0;
            auto [ptr, ec] =
                std::from_chars(val.data(), val.data() + val.size(), v);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid ledger: " + std::string(val);
                return false;
            }
            acc.ledger = static_cast<uint32_t>(v);
        }
        else if (key == "code")
        {
            uint64_t v = 0;
            auto [ptr, ec] =
                std::from_chars(val.data(), val.data() + val.size(), v);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid code: " + std::string(val);
                return false;
            }
            acc.code = static_cast<uint16_t>(v);
        }
        else if (key == "flags")
        {
            if (!tb_parse_account_flags(val, acc.flags))
            {
                err_msg = "ERR invalid flags: " + std::string(val);
                return false;
            }
        }
        else if (key == "user_data_128")
        {
            if (!tb_parse_u128(val, acc.user_data_128))
            {
                err_msg = "ERR invalid user_data_128: " + std::string(val);
                return false;
            }
        }
        else if (key == "user_data_64")
        {
            auto [ptr, ec] = std::from_chars(
                val.data(), val.data() + val.size(), acc.user_data_64);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid user_data_64: " + std::string(val);
                return false;
            }
        }
        else if (key == "user_data_32")
        {
            uint64_t v = 0;
            auto [ptr, ec] =
                std::from_chars(val.data(), val.data() + val.size(), v);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid user_data_32: " + std::string(val);
                return false;
            }
            acc.user_data_32 = static_cast<uint32_t>(v);
        }
        else if (key == "timestamp")
        {
            auto [ptr, ec] = std::from_chars(
                val.data(), val.data() + val.size(), acc.timestamp);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid timestamp: " + std::string(val);
                return false;
            }
        }
        else
        {
            err_msg = "ERR unknown account field: " + std::string(key);
            return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// Transfer text parser
// ---------------------------------------------------------------------------

bool tb_parse_transfer_text(const std::vector<std::string_view> &tokens,
                            size_t start,
                            size_t end,
                            TbTransfer &txfr,
                            std::string &err_msg)
{
    txfr = TbTransfer{};
    for (size_t i = start; i < end; i++)
    {
        auto [key, val] = parse_kv(tokens[i]);
        if (key.empty())
        {
            err_msg = "ERR invalid token: " + std::string(tokens[i]);
            return false;
        }

        if (key == "id")
        {
            if (!tb_parse_u128(val, txfr.id))
            {
                err_msg = "ERR invalid id: " + std::string(val);
                return false;
            }
        }
        else if (key == "debit_account_id")
        {
            if (!tb_parse_u128(val, txfr.debit_account_id))
            {
                err_msg = "ERR invalid debit_account_id: " + std::string(val);
                return false;
            }
        }
        else if (key == "credit_account_id")
        {
            if (!tb_parse_u128(val, txfr.credit_account_id))
            {
                err_msg = "ERR invalid credit_account_id: " + std::string(val);
                return false;
            }
        }
        else if (key == "amount")
        {
            if (!tb_parse_u128(val, txfr.amount))
            {
                err_msg = "ERR invalid amount: " + std::string(val);
                return false;
            }
        }
        else if (key == "pending_id")
        {
            if (!tb_parse_u128(val, txfr.pending_id))
            {
                err_msg = "ERR invalid pending_id: " + std::string(val);
                return false;
            }
        }
        else if (key == "ledger")
        {
            uint64_t v = 0;
            auto [ptr, ec] =
                std::from_chars(val.data(), val.data() + val.size(), v);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid ledger: " + std::string(val);
                return false;
            }
            txfr.ledger = static_cast<uint32_t>(v);
        }
        else if (key == "code")
        {
            uint64_t v = 0;
            auto [ptr, ec] =
                std::from_chars(val.data(), val.data() + val.size(), v);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid code: " + std::string(val);
                return false;
            }
            txfr.code = static_cast<uint16_t>(v);
        }
        else if (key == "flags")
        {
            if (!tb_parse_transfer_flags(val, txfr.flags))
            {
                err_msg = "ERR invalid flags: " + std::string(val);
                return false;
            }
        }
        else if (key == "timeout")
        {
            uint64_t v = 0;
            auto [ptr, ec] =
                std::from_chars(val.data(), val.data() + val.size(), v);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid timeout: " + std::string(val);
                return false;
            }
            txfr.timeout = static_cast<uint32_t>(v);
        }
        else if (key == "user_data_128")
        {
            if (!tb_parse_u128(val, txfr.user_data_128))
            {
                err_msg = "ERR invalid user_data_128: " + std::string(val);
                return false;
            }
        }
        else if (key == "user_data_64")
        {
            auto [ptr, ec] = std::from_chars(
                val.data(), val.data() + val.size(), txfr.user_data_64);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid user_data_64: " + std::string(val);
                return false;
            }
        }
        else if (key == "user_data_32")
        {
            uint64_t v = 0;
            auto [ptr, ec] =
                std::from_chars(val.data(), val.data() + val.size(), v);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid user_data_32: " + std::string(val);
                return false;
            }
            txfr.user_data_32 = static_cast<uint32_t>(v);
        }
        else if (key == "timestamp")
        {
            auto [ptr, ec] = std::from_chars(
                val.data(), val.data() + val.size(), txfr.timestamp);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid timestamp: " + std::string(val);
                return false;
            }
        }
        else
        {
            err_msg = "ERR unknown transfer field: " + std::string(key);
            return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// AccountFilter text parser
// ---------------------------------------------------------------------------

bool tb_parse_account_filter_text(const std::vector<std::string_view> &tokens,
                                  size_t start,
                                  size_t end,
                                  TbAccountFilter &filter,
                                  std::string &err_msg)
{
    filter = TbAccountFilter{};
    for (size_t i = start; i < end; i++)
    {
        auto [key, val] = parse_kv(tokens[i]);
        if (key.empty())
        {
            err_msg = "ERR invalid token: " + std::string(tokens[i]);
            return false;
        }

        if (key == "account_id")
        {
            if (!tb_parse_u128(val, filter.account_id))
            {
                err_msg = "ERR invalid account_id: " + std::string(val);
                return false;
            }
        }
        else if (key == "timestamp_min")
        {
            auto [ptr, ec] = std::from_chars(
                val.data(), val.data() + val.size(), filter.timestamp_min);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid timestamp_min: " + std::string(val);
                return false;
            }
        }
        else if (key == "timestamp_max")
        {
            auto [ptr, ec] = std::from_chars(
                val.data(), val.data() + val.size(), filter.timestamp_max);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid timestamp_max: " + std::string(val);
                return false;
            }
        }
        else if (key == "limit")
        {
            uint64_t v = 0;
            auto [ptr, ec] =
                std::from_chars(val.data(), val.data() + val.size(), v);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid limit: " + std::string(val);
                return false;
            }
            filter.limit = static_cast<uint32_t>(v);
        }
        else if (key == "flags")
        {
            if (!tb_parse_filter_flags(val, filter.flags))
            {
                err_msg = "ERR invalid flags: " + std::string(val);
                return false;
            }
        }
        else if (key == "code")
        {
            uint64_t v = 0;
            auto [ptr, ec] =
                std::from_chars(val.data(), val.data() + val.size(), v);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid code: " + std::string(val);
                return false;
            }
            filter.code = static_cast<uint16_t>(v);
        }
        else if (key == "user_data_128")
        {
            if (!tb_parse_u128(val, filter.user_data_128))
            {
                err_msg = "ERR invalid user_data_128: " + std::string(val);
                return false;
            }
        }
        else if (key == "user_data_64")
        {
            auto [ptr, ec] = std::from_chars(
                val.data(), val.data() + val.size(), filter.user_data_64);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid user_data_64: " + std::string(val);
                return false;
            }
        }
        else if (key == "user_data_32")
        {
            uint64_t v = 0;
            auto [ptr, ec] =
                std::from_chars(val.data(), val.data() + val.size(), v);
            if (ec != std::errc{})
            {
                err_msg = "ERR invalid user_data_32: " + std::string(val);
                return false;
            }
            filter.user_data_32 = static_cast<uint32_t>(v);
        }
        else
        {
            err_msg = "ERR unknown filter field: " + std::string(key);
            return false;
        }
    }
    return true;
}

}  // namespace EloqKV

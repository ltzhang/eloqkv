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
#include "tb_handler.h"

#include <brpc/redis_reply.h>
#include <glog/logging.h>

#include <cassert>
#include <cstdarg>
#include <string>
#include <vector>

#include "redis_service.h"
#include "tb_types.h"
#include "tx_util.h"

namespace EloqKV
{
static constexpr TbU128 kU128Max{0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL};

static constexpr uint16_t kSupportedAccountFlagsMask = AccountFlags::LINKED |
                                                       AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS |
                                                       AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS |
                                                       AccountFlags::HISTORY |
                                                       AccountFlags::IMPORTED |
                                                       AccountFlags::CLOSED;

static constexpr uint16_t kSupportedTransferFlagsMask = TransferFlags::LINKED |
                                                        TransferFlags::PENDING |
                                                        TransferFlags::POST_PENDING |
                                                        TransferFlags::VOID_PENDING |
                                                        TransferFlags::BALANCING_DEBIT |
                                                        TransferFlags::BALANCING_CREDIT |
                                                        TransferFlags::CLOSING_DEBIT |
                                                        TransferFlags::CLOSING_CREDIT |
                                                        TransferFlags::IMPORTED;

// ---------------------------------------------------------------------------
// CaptureOutputHandler::OnFormatError
// ---------------------------------------------------------------------------

void CaptureOutputHandler::OnFormatError(const char *fmt, ...)
{
    has_error = true;
    va_list ap;
    va_start(ap, fmt);
    char buf[512];
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    error_msg = buf;
    type = Type::ERROR;
}

void ArrayCaptureOutputHandler::OnFormatError(const char *fmt, ...)
{
    has_error = true;
    va_list ap;
    va_start(ap, fmt);
    char buf[512];
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    error_msg = buf;
}

// ---------------------------------------------------------------------------
// Helper: run a Redis command within txm and return the captured result
// ---------------------------------------------------------------------------

static CaptureOutputHandler tb_run_cmd(RedisServiceImpl *redis_impl,
                                       RedisConnectionContext *ctx,
                                       txservice::TransactionExecution *txm,
                                       std::vector<std::string> args)
{
    CaptureOutputHandler cap;
    redis_impl->GenericCommand(ctx, txm, args, &cap);
    return cap;
}

static bool tb_filter_is_valid(const TbAccountFilter &filter)
{
    if (filter.account_id.is_zero() || filter.account_id == kU128Max)
        return false;
    if (filter.limit == 0)
        return false;

    bool want_debits = (filter.flags & AccountFilterFlags::DEBITS) != 0;
    bool want_credits = (filter.flags & AccountFilterFlags::CREDITS) != 0;
    if (!want_debits && !want_credits)
        return false;

    if (filter.timestamp_max != 0 &&
        filter.timestamp_min > filter.timestamp_max)
        return false;

    if ((filter.flags & ~(
             AccountFilterFlags::DEBITS | AccountFilterFlags::CREDITS |
             AccountFilterFlags::REVERSED)) != 0)
        return false;

    return true;
}

static bool tb_account_uses_unsupported_semantics(const TbAccount &acc)
{
    if ((acc.flags & ~kSupportedAccountFlagsMask) != 0)
        return true;
    return acc.has_flag(AccountFlags::IMPORTED);
}

static bool tb_transfer_uses_unsupported_semantics(const TbTransfer &txfr)
{
    if ((txfr.flags & ~kSupportedTransferFlagsMask) != 0)
        return true;
    if (txfr.has_flag(TransferFlags::IMPORTED))
        return true;
    if (txfr.has_flag(TransferFlags::BALANCING_DEBIT) ||
        txfr.has_flag(TransferFlags::BALANCING_CREDIT) ||
        txfr.has_flag(TransferFlags::CLOSING_DEBIT) ||
        txfr.has_flag(TransferFlags::CLOSING_CREDIT))
        return true;
    return false;
}

// ---------------------------------------------------------------------------
// Core TB operations
// These functions take a txm and perform all Redis sub-operations within it.
// They fill `results` with per-item status codes (one per account/transfer).
// ---------------------------------------------------------------------------

// Validate and create a single account within an open txm.
// Returns TbAccountStatus::OK or an appropriate error code.
static TbAccountStatus tb_do_create_account(RedisServiceImpl *redis_impl,
                                            RedisConnectionContext *ctx,
                                            txservice::TransactionExecution *txm,
                                            TbAccount &acc)
{
    // --- Basic field validation ---
    if (acc.id.is_zero())
        return TbAccountStatus::ID_MUST_NOT_BE_ZERO;
    if (acc.id == kU128Max)
        return TbAccountStatus::ID_MUST_NOT_BE_INT_MAX;
    if (acc.ledger == 0)
        return TbAccountStatus::LEDGER_MUST_NOT_BE_ZERO;
    if (acc.code == 0)
        return TbAccountStatus::CODE_MUST_NOT_BE_ZERO;
    if (acc.reserved != 0)
        return TbAccountStatus::RESERVED_FIELD;
    if ((acc.flags & ~kSupportedAccountFlagsMask) != 0)
        return TbAccountStatus::RESERVED_FLAG;

    // Non-imported accounts must have zero balances and zero timestamp
    if (!acc.has_flag(AccountFlags::IMPORTED))
    {
        if (!acc.debits_pending.is_zero())
            return TbAccountStatus::DEBITS_PENDING_MUST_BE_ZERO;
        if (!acc.debits_posted.is_zero())
            return TbAccountStatus::DEBITS_POSTED_MUST_BE_ZERO;
        if (!acc.credits_pending.is_zero())
            return TbAccountStatus::CREDITS_PENDING_MUST_BE_ZERO;
        if (!acc.credits_posted.is_zero())
            return TbAccountStatus::CREDITS_POSTED_MUST_BE_ZERO;
        if (acc.timestamp != 0)
            return TbAccountStatus::TIMESTAMP_MUST_BE_ZERO;
    }

    // Mutually exclusive flags: DEBITS_MUST_NOT_EXCEED_CREDITS and
    // CREDITS_MUST_NOT_EXCEED_DEBITS cannot both be set
    if (acc.has_flag(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS) &&
        acc.has_flag(AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS))
        return TbAccountStatus::FLAGS_ARE_MUTUALLY_EXCLUSIVE;

    // --- Assign server timestamp ---
    if (!acc.has_flag(AccountFlags::IMPORTED))
        acc.timestamp = tb_current_timestamp_ns();

    // --- Check for existing account ---
    std::string key = tb_account_key(acc.id);
    auto get_result = tb_run_cmd(redis_impl, ctx, txm, {"get", key});
    if (get_result.has_error)
    {
        LOG(WARNING) << "TB create_account GET error: " << get_result.error_msg;
        return TbAccountStatus::EXISTS;  // treat internal errors conservatively
    }
    if (!get_result.is_nil())
    {
        // Key exists — check for exact-match vs difference
        TbAccount existing{};
        if (tb_decode_account(get_result.str_val, existing))
        {
            if (existing.flags != acc.flags)
                return TbAccountStatus::EXISTS_WITH_DIFFERENT_FLAGS;
            if (existing.user_data_128 != acc.user_data_128)
                return TbAccountStatus::EXISTS_WITH_DIFFERENT_USER_DATA_128;
            if (existing.user_data_64 != acc.user_data_64)
                return TbAccountStatus::EXISTS_WITH_DIFFERENT_USER_DATA_64;
            if (existing.user_data_32 != acc.user_data_32)
                return TbAccountStatus::EXISTS_WITH_DIFFERENT_USER_DATA_32;
            if (existing.ledger != acc.ledger)
                return TbAccountStatus::EXISTS_WITH_DIFFERENT_LEDGER;
            if (existing.code != acc.code)
                return TbAccountStatus::EXISTS_WITH_DIFFERENT_CODE;
        }
        return TbAccountStatus::EXISTS;
    }

    // --- Store account ---
    std::string encoded = tb_encode_account(acc);
    auto set_result = tb_run_cmd(redis_impl, ctx, txm, {"set", key, encoded});
    if (set_result.has_error)
    {
        LOG(WARNING) << "TB create_account SET error: " << set_result.error_msg;
        return TbAccountStatus::EXISTS;
    }

    return TbAccountStatus::OK;
}

// Validate and create a single transfer within an open txm.
// On success, both account blobs are updated in-txm (not yet committed).
static TbTransferStatus tb_do_create_transfer(RedisServiceImpl *redis_impl,
                                              RedisConnectionContext *ctx,
                                              txservice::TransactionExecution *txm,
                                              TbTransfer &txfr)
{
    // --- Basic validation ---
    if (txfr.id.is_zero())
        return TbTransferStatus::ID_MUST_NOT_BE_ZERO;
    if (txfr.id == kU128Max)
        return TbTransferStatus::ID_MUST_NOT_BE_INT_MAX;
    if (txfr.debit_account_id.is_zero())
        return TbTransferStatus::DEBIT_ACCOUNT_ID_MUST_NOT_BE_ZERO;
    if (txfr.debit_account_id == kU128Max)
        return TbTransferStatus::DEBIT_ACCOUNT_ID_MUST_NOT_BE_INT_MAX;
    if (txfr.credit_account_id.is_zero())
        return TbTransferStatus::CREDIT_ACCOUNT_ID_MUST_NOT_BE_ZERO;
    if (txfr.credit_account_id == kU128Max)
        return TbTransferStatus::CREDIT_ACCOUNT_ID_MUST_NOT_BE_INT_MAX;
    if (txfr.debit_account_id == txfr.credit_account_id)
        return TbTransferStatus::ACCOUNTS_MUST_BE_DIFFERENT;
    if (txfr.ledger == 0)
        return TbTransferStatus::LEDGER_MUST_NOT_BE_ZERO;
    if (txfr.code == 0)
        return TbTransferStatus::CODE_MUST_NOT_BE_ZERO;
    if ((txfr.flags & ~kSupportedTransferFlagsMask) != 0)
        return TbTransferStatus::RESERVED_FLAG;

    // Mutually exclusive flags
    int phase_flags = 0;
    if (txfr.has_flag(TransferFlags::PENDING))
        phase_flags++;
    if (txfr.has_flag(TransferFlags::POST_PENDING))
        phase_flags++;
    if (txfr.has_flag(TransferFlags::VOID_PENDING))
        phase_flags++;
    if (phase_flags > 1)
        return TbTransferStatus::FLAGS_ARE_MUTUALLY_EXCLUSIVE;

    bool is_pending = txfr.has_flag(TransferFlags::PENDING);
    bool is_post = txfr.has_flag(TransferFlags::POST_PENDING);
    bool is_void = txfr.has_flag(TransferFlags::VOID_PENDING);
    bool is_direct = !is_pending && !is_post && !is_void;

    if (is_direct && txfr.amount.is_zero())
        return TbTransferStatus::AMOUNT_MUST_NOT_BE_ZERO;
    if (is_pending && txfr.amount.is_zero())
        return TbTransferStatus::AMOUNT_MUST_NOT_BE_ZERO;

    // For non-post/void transfers, pending_id must be zero
    if (is_direct || is_pending)
    {
        if (!txfr.pending_id.is_zero())
            return TbTransferStatus::PENDING_ID_MUST_BE_ZERO;
    }
    else
    {
        // post/void: pending_id must be set and not equal to transfer id
        if (txfr.pending_id.is_zero())
            return TbTransferStatus::PENDING_ID_MUST_NOT_BE_ZERO;
        if (txfr.pending_id == kU128Max)
            return TbTransferStatus::PENDING_ID_MUST_NOT_BE_INT_MAX;
        if (txfr.pending_id == txfr.id)
            return TbTransferStatus::PENDING_ID_MUST_BE_DIFFERENT;
    }

    // timeout is only valid for pending transfers
    if (!is_pending && txfr.timeout != 0)
        return TbTransferStatus::TIMEOUT_RESERVED_FOR_PENDING_TRANSFER;

    // Non-imported transfers must have zero timestamp
    if (!txfr.has_flag(TransferFlags::IMPORTED) && txfr.timestamp != 0)
        return TbTransferStatus::TIMESTAMP_MUST_BE_ZERO;

    // --- Check transfer doesn't already exist ---
    std::string txfr_key = tb_transfer_key(txfr.id);
    auto txfr_get = tb_run_cmd(redis_impl, ctx, txm, {"get", txfr_key});
    if (txfr_get.has_error)
    {
        LOG(WARNING) << "TB create_transfer GET transfer error: "
                     << txfr_get.error_msg;
        return TbTransferStatus::EXISTS;
    }
    if (!txfr_get.is_nil())
        return TbTransferStatus::EXISTS;

    // --- Load debit account ---
    std::string debit_key = tb_account_key(txfr.debit_account_id);
    auto debit_get = tb_run_cmd(redis_impl, ctx, txm, {"get", debit_key});
    if (debit_get.has_error || debit_get.is_nil())
        return TbTransferStatus::DEBIT_ACCOUNT_NOT_FOUND;
    TbAccount debit_acc{};
    if (!tb_decode_account(debit_get.str_val, debit_acc))
        return TbTransferStatus::DEBIT_ACCOUNT_NOT_FOUND;

    // --- Load credit account ---
    std::string credit_key = tb_account_key(txfr.credit_account_id);
    auto credit_get = tb_run_cmd(redis_impl, ctx, txm, {"get", credit_key});
    if (credit_get.has_error || credit_get.is_nil())
        return TbTransferStatus::CREDIT_ACCOUNT_NOT_FOUND;
    TbAccount credit_acc{};
    if (!tb_decode_account(credit_get.str_val, credit_acc))
        return TbTransferStatus::CREDIT_ACCOUNT_NOT_FOUND;

    // --- Ledger validation ---
    if (debit_acc.ledger != credit_acc.ledger)
        return TbTransferStatus::ACCOUNTS_MUST_HAVE_THE_SAME_LEDGER;
    if (txfr.ledger != debit_acc.ledger)
        return TbTransferStatus::TRANSFER_MUST_HAVE_THE_SAME_LEDGER_AS_ACCOUNTS;

    // --- Assign server timestamp ---
    if (!txfr.has_flag(TransferFlags::IMPORTED))
        txfr.timestamp = tb_current_timestamp_ns();

    TbU128 amount = txfr.amount;

    if (is_post || is_void)
    {
        // --- Two-phase: load pending transfer ---
        std::string pending_key = tb_transfer_key(txfr.pending_id);
        auto pending_get = tb_run_cmd(redis_impl, ctx, txm, {"get", pending_key});
        if (pending_get.has_error || pending_get.is_nil())
            return TbTransferStatus::PENDING_TRANSFER_NOT_FOUND;

        TbTransfer pending_txfr{};
        if (!tb_decode_transfer(pending_get.str_val, pending_txfr))
            return TbTransferStatus::PENDING_TRANSFER_NOT_FOUND;

        if (!pending_txfr.has_flag(TransferFlags::PENDING))
            return TbTransferStatus::PENDING_TRANSFER_NOT_PENDING;
        if (pending_txfr.debit_account_id != txfr.debit_account_id)
            return TbTransferStatus::PENDING_TRANSFER_HAS_DIFFERENT_DEBIT_ACCOUNT_ID;
        if (pending_txfr.credit_account_id != txfr.credit_account_id)
            return TbTransferStatus::PENDING_TRANSFER_HAS_DIFFERENT_CREDIT_ACCOUNT_ID;
        if (pending_txfr.ledger != txfr.ledger)
            return TbTransferStatus::PENDING_TRANSFER_HAS_DIFFERENT_LEDGER;
        if (pending_txfr.code != txfr.code)
            return TbTransferStatus::PENDING_TRANSFER_HAS_DIFFERENT_CODE;

        // Use the pending transfer's amount if none specified
        if (txfr.amount.is_zero())
            amount = pending_txfr.amount;
        else
        {
            if (txfr.amount > pending_txfr.amount)
                return TbTransferStatus::EXCEEDS_PENDING_TRANSFER_AMOUNT;
            amount = txfr.amount;
        }

        if (is_post)
        {
            // Move from pending to posted
            // debit: debits_pending -= amount, debits_posted += amount
            // credit: credits_pending -= amount, credits_posted += amount
            if (debit_acc.debits_pending < amount)
                return TbTransferStatus::EXCEEDS_DEBITS;
            if (credit_acc.credits_pending < amount)
                return TbTransferStatus::EXCEEDS_CREDITS;

            if (debit_acc.debits_posted.would_overflow_add(amount))
                return TbTransferStatus::OVERFLOWS_DEBITS_POSTED;
            if (credit_acc.credits_posted.would_overflow_add(amount))
                return TbTransferStatus::OVERFLOWS_CREDITS_POSTED;

            debit_acc.debits_pending = debit_acc.debits_pending - amount;
            debit_acc.debits_posted = debit_acc.debits_posted + amount;
            credit_acc.credits_pending = credit_acc.credits_pending - amount;
            credit_acc.credits_posted = credit_acc.credits_posted + amount;
        }
        else  // is_void
        {
            // Release pending amounts
            if (debit_acc.debits_pending < amount)
                return TbTransferStatus::EXCEEDS_DEBITS;
            if (credit_acc.credits_pending < amount)
                return TbTransferStatus::EXCEEDS_CREDITS;

            debit_acc.debits_pending = debit_acc.debits_pending - amount;
            credit_acc.credits_pending = credit_acc.credits_pending - amount;
        }
    }
    else if (is_pending)
    {
        // Reserve amounts in pending buckets
        if (debit_acc.debits_pending.would_overflow_add(amount))
            return TbTransferStatus::OVERFLOWS_DEBITS_PENDING;
        if (credit_acc.credits_pending.would_overflow_add(amount))
            return TbTransferStatus::OVERFLOWS_CREDITS_PENDING;

        debit_acc.debits_pending = debit_acc.debits_pending + amount;
        credit_acc.credits_pending = credit_acc.credits_pending + amount;

        // Balance constraints applied after update
        if (debit_acc.has_flag(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS))
        {
            if (debit_acc.total_debits() > debit_acc.total_credits())
                return TbTransferStatus::EXCEEDS_CREDITS;
        }
        if (credit_acc.has_flag(AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS))
        {
            if (credit_acc.total_credits() > credit_acc.total_debits())
                return TbTransferStatus::EXCEEDS_DEBITS;
        }
    }
    else  // is_direct
    {
        // Post directly
        if (debit_acc.debits_posted.would_overflow_add(amount))
            return TbTransferStatus::OVERFLOWS_DEBITS_POSTED;
        if (credit_acc.credits_posted.would_overflow_add(amount))
            return TbTransferStatus::OVERFLOWS_CREDITS_POSTED;

        debit_acc.debits_posted = debit_acc.debits_posted + amount;
        credit_acc.credits_posted = credit_acc.credits_posted + amount;

        // Balance constraints
        if (debit_acc.has_flag(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS))
        {
            if (debit_acc.total_debits() > debit_acc.total_credits())
                return TbTransferStatus::EXCEEDS_CREDITS;
        }
        if (credit_acc.has_flag(AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS))
        {
            if (credit_acc.total_credits() > credit_acc.total_debits())
                return TbTransferStatus::EXCEEDS_DEBITS;
        }
    }

    // --- Persist transfer ---
    std::string txfr_encoded = tb_encode_transfer(txfr);
    auto set_txfr = tb_run_cmd(
        redis_impl, ctx, txm, {"set", txfr_key, txfr_encoded});
    if (set_txfr.has_error)
    {
        LOG(WARNING) << "TB create_transfer SET transfer error: "
                     << set_txfr.error_msg;
        return TbTransferStatus::EXISTS;
    }

    // --- Update debit account ---
    std::string debit_encoded = tb_encode_account(debit_acc);
    auto set_debit =
        tb_run_cmd(redis_impl, ctx, txm, {"set", debit_key, debit_encoded});
    if (set_debit.has_error)
    {
        LOG(WARNING) << "TB create_transfer SET debit account error: "
                     << set_debit.error_msg;
        return TbTransferStatus::EXISTS;
    }

    // --- Update credit account ---
    std::string credit_encoded = tb_encode_account(credit_acc);
    auto set_credit =
        tb_run_cmd(redis_impl, ctx, txm, {"set", credit_key, credit_encoded});
    if (set_credit.has_error)
    {
        LOG(WARNING) << "TB create_transfer SET credit account error: "
                     << set_credit.error_msg;
        return TbTransferStatus::EXISTS;
    }

    // --- Update transfer history (sorted set, score = timestamp in μs) ---
    // Use microseconds to stay within double precision (ns timestamps ~1.7e18
    // exceed 2^53, microseconds ~1.7e12 are safely representable)
    std::string ts_score = std::to_string(txfr.timestamp / 1000);
    std::string txfr_hex = tb_u128_to_hex(txfr.id);

    std::string debit_xfers_key = tb_account_transfers_key(txfr.debit_account_id);
    tb_run_cmd(
        redis_impl, ctx, txm, {"zadd", debit_xfers_key, ts_score, txfr_hex});

    std::string credit_xfers_key =
        tb_account_transfers_key(txfr.credit_account_id);
    tb_run_cmd(
        redis_impl, ctx, txm, {"zadd", credit_xfers_key, ts_score, txfr_hex});

    // --- Update balance history if HISTORY flag set ---
    auto make_balance_snapshot = [&](const TbAccount &acc) -> TbAccountBalance {
        TbAccountBalance bal{};
        bal.timestamp = txfr.timestamp;
        bal.debits_pending = acc.debits_pending;
        bal.debits_posted = acc.debits_posted;
        bal.credits_pending = acc.credits_pending;
        bal.credits_posted = acc.credits_posted;
        return bal;
    };

    if (debit_acc.has_flag(AccountFlags::HISTORY))
    {
        TbAccountBalance bal = make_balance_snapshot(debit_acc);
        std::string bal_enc = tb_encode_account_balance(bal);
        std::string debit_bal_key = tb_account_balances_key(txfr.debit_account_id);
        tb_run_cmd(redis_impl, ctx, txm, {"rpush", debit_bal_key, bal_enc});
    }

    if (credit_acc.has_flag(AccountFlags::HISTORY))
    {
        TbAccountBalance bal = make_balance_snapshot(credit_acc);
        std::string bal_enc = tb_encode_account_balance(bal);
        std::string credit_bal_key =
            tb_account_balances_key(txfr.credit_account_id);
        tb_run_cmd(redis_impl, ctx, txm, {"rpush", credit_bal_key, bal_enc});
    }

    return TbTransferStatus::OK;
}

// ---------------------------------------------------------------------------
// Execute CREATE_ACCOUNTS batch (with linked-chain semantics)
// Returns a Redis array of u32 error codes (0 = success)
// ---------------------------------------------------------------------------
static void tb_exec_create_accounts(
    RedisServiceImpl *redis_impl,
    RedisConnectionContext *ctx,
    const std::vector<TbAccount> &accounts,
    brpc::RedisReply *output)
{
    const size_t n = accounts.size();
    if (n == 0)
    {
        output->SetError("ERR TB CREATE_ACCOUNT(S) requires at least one account");
        return;
    }

    std::vector<TbAccountStatus> results(n, TbAccountStatus::OK);

    for (const TbAccount &acc : accounts)
    {
        if (tb_account_uses_unsupported_semantics(acc))
        {
            output->SetError(
                "ERR TB imported account semantics are not supported yet");
            return;
        }
    }

    // Process accounts, handling linked chains.
    // A linked chain starts at index i where LINKED flag is set, and ends at
    // the first subsequent account without LINKED flag (that is the last of
    // the chain). If the last account still has LINKED, the chain is open.
    size_t i = 0;
    while (i < n)
    {
        // Find chain end
        size_t chain_start = i;
        size_t chain_end = i;
        while (chain_end < n &&
               (accounts[chain_end].has_flag(AccountFlags::LINKED) ||
                chain_end == chain_start))
        {
            if (!accounts[chain_end].has_flag(AccountFlags::LINKED) &&
                chain_end > chain_start)
                break;
            chain_end++;
            if (chain_end >= n || !accounts[chain_end - 1].has_flag(AccountFlags::LINKED))
                break;
        }

        // Check for open chain: last account in range still has LINKED set
        bool chain_open = (chain_end == n && chain_end > chain_start &&
                           accounts[chain_end - 1].has_flag(AccountFlags::LINKED));
        if (chain_open)
        {
            for (size_t j = chain_start; j < chain_end; j++)
                results[j] = (j == chain_end - 1)
                                  ? TbAccountStatus::LINKED_EVENT_CHAIN_OPEN
                                  : TbAccountStatus::LINKED_EVENT_FAILED;
            i = chain_end;
            continue;
        }

        // Determine if this is a linked chain (more than one, or first has LINKED)
        bool is_linked_chain =
            (chain_end - chain_start > 1) ||
            (chain_end - chain_start == 1 &&
             accounts[chain_start].has_flag(AccountFlags::LINKED));

        if (is_linked_chain)
        {
            // Execute entire chain in a single transaction
            txservice::TransactionExecution *txm = redis_impl->NewTxm(
                RedisCommandHandler::iso_level_,
                RedisCommandHandler::cc_protocol_);

            bool chain_failed = false;
            size_t failed_idx = chain_end;

            for (size_t j = chain_start; j < chain_end && !chain_failed; j++)
            {
                TbAccount acc = accounts[j];  // copy for mutation
                TbAccountStatus err =
                    tb_do_create_account(redis_impl, ctx, txm, acc);
                if (err != TbAccountStatus::OK)
                {
                    results[j] = err;
                    chain_failed = true;
                    failed_idx = j;
                }
            }

            if (chain_failed)
            {
                txservice::AbortTx(txm);
                // Mark all preceding items as LINKED_EVENT_FAILED
                for (size_t j = chain_start; j < failed_idx; j++)
                    results[j] = TbAccountStatus::LINKED_EVENT_FAILED;
                // Items after failed one also fail
                for (size_t j = failed_idx + 1; j < chain_end; j++)
                    results[j] = TbAccountStatus::LINKED_EVENT_FAILED;
            }
            else
            {
                auto [ok, err_code] = txservice::CommitTx(txm);
                if (!ok)
                {
                    for (size_t j = chain_start; j < chain_end; j++)
                        results[j] = TbAccountStatus::LINKED_EVENT_FAILED;
                }
            }
        }
        else
        {
            // Single (non-linked) account — its own transaction
            txservice::TransactionExecution *txm = redis_impl->NewTxm(
                RedisCommandHandler::iso_level_,
                RedisCommandHandler::cc_protocol_);
            TbAccount acc = accounts[chain_start];
            TbAccountStatus err =
                tb_do_create_account(redis_impl, ctx, txm, acc);
            results[chain_start] = err;
            if (err != TbAccountStatus::OK)
                txservice::AbortTx(txm);
            else
            {
                auto [ok, err_code] = txservice::CommitTx(txm);
                if (!ok)
                    results[chain_start] = TbAccountStatus::EXISTS;
            }
        }

        i = chain_end;
    }

    // Build response: array of u32 error codes
    output->SetArray(static_cast<int>(n));
    for (size_t j = 0; j < n; j++)
        output->operator[](static_cast<int>(j))
            .SetInteger(static_cast<int64_t>(static_cast<uint32_t>(results[j])));
}

// ---------------------------------------------------------------------------
// Execute CREATE_TRANSFERS batch (with linked-chain semantics)
// ---------------------------------------------------------------------------
static void tb_exec_create_transfers(
    RedisServiceImpl *redis_impl,
    RedisConnectionContext *ctx,
    const std::vector<TbTransfer> &transfers,
    brpc::RedisReply *output)
{
    const size_t n = transfers.size();
    if (n == 0)
    {
        output->SetError(
            "ERR TB CREATE_TRANSFER(S) requires at least one transfer");
        return;
    }

    std::vector<TbTransferStatus> results(n, TbTransferStatus::OK);

    for (const TbTransfer &txfr : transfers)
    {
        if (tb_transfer_uses_unsupported_semantics(txfr))
        {
            output->SetError(
                "ERR TB imported, balancing, and closing transfer semantics are not supported yet");
            return;
        }
    }

    size_t i = 0;
    while (i < n)
    {
        size_t chain_start = i;
        size_t chain_end = i;
        while (chain_end < n &&
               (transfers[chain_end].has_flag(TransferFlags::LINKED) ||
                chain_end == chain_start))
        {
            if (!transfers[chain_end].has_flag(TransferFlags::LINKED) &&
                chain_end > chain_start)
                break;
            chain_end++;
            if (chain_end >= n ||
                !transfers[chain_end - 1].has_flag(TransferFlags::LINKED))
                break;
        }

        bool chain_open =
            (chain_end == n && chain_end > chain_start &&
             transfers[chain_end - 1].has_flag(TransferFlags::LINKED));
        if (chain_open)
        {
            for (size_t j = chain_start; j < chain_end; j++)
                results[j] = (j == chain_end - 1)
                                  ? TbTransferStatus::LINKED_EVENT_CHAIN_OPEN
                                  : TbTransferStatus::LINKED_EVENT_FAILED;
            i = chain_end;
            continue;
        }

        bool is_linked_chain =
            (chain_end - chain_start > 1) ||
            (chain_end - chain_start == 1 &&
             transfers[chain_start].has_flag(TransferFlags::LINKED));

        if (is_linked_chain)
        {
            txservice::TransactionExecution *txm = redis_impl->NewTxm(
                RedisCommandHandler::iso_level_,
                RedisCommandHandler::cc_protocol_);

            bool chain_failed = false;
            size_t failed_idx = chain_end;

            for (size_t j = chain_start; j < chain_end && !chain_failed; j++)
            {
                TbTransfer txfr = transfers[j];
                TbTransferStatus err =
                    tb_do_create_transfer(redis_impl, ctx, txm, txfr);
                if (err != TbTransferStatus::OK)
                {
                    results[j] = err;
                    chain_failed = true;
                    failed_idx = j;
                }
            }

            if (chain_failed)
            {
                txservice::AbortTx(txm);
                for (size_t j = chain_start; j < failed_idx; j++)
                    results[j] = TbTransferStatus::LINKED_EVENT_FAILED;
                for (size_t j = failed_idx + 1; j < chain_end; j++)
                    results[j] = TbTransferStatus::LINKED_EVENT_FAILED;
            }
            else
            {
                auto [ok, err_code] = txservice::CommitTx(txm);
                if (!ok)
                {
                    for (size_t j = chain_start; j < chain_end; j++)
                        results[j] = TbTransferStatus::LINKED_EVENT_FAILED;
                }
            }
        }
        else
        {
            txservice::TransactionExecution *txm = redis_impl->NewTxm(
                RedisCommandHandler::iso_level_,
                RedisCommandHandler::cc_protocol_);
            TbTransfer txfr = transfers[chain_start];
            TbTransferStatus err =
                tb_do_create_transfer(redis_impl, ctx, txm, txfr);
            results[chain_start] = err;
            if (err != TbTransferStatus::OK)
                txservice::AbortTx(txm);
            else
            {
                auto [ok, err_code] = txservice::CommitTx(txm);
                if (!ok)
                    results[chain_start] = TbTransferStatus::EXISTS;
            }
        }

        i = chain_end;
    }

    output->SetArray(static_cast<int>(n));
    for (size_t j = 0; j < n; j++)
        output->operator[](static_cast<int>(j))
            .SetInteger(static_cast<int64_t>(static_cast<uint32_t>(results[j])));
}

// ---------------------------------------------------------------------------
// Execute LOOKUP_ACCOUNT(S) — returns array of binary account blobs or nil
// ---------------------------------------------------------------------------
static void tb_exec_lookup_accounts(RedisServiceImpl *redis_impl,
                                    RedisConnectionContext *ctx,
                                    const std::vector<TbU128> &ids,
                                    brpc::RedisReply *output)
{
    txservice::TransactionExecution *txm = redis_impl->NewTxm(
        RedisCommandHandler::iso_level_, RedisCommandHandler::cc_protocol_);

    const size_t n = ids.size();
    std::vector<std::string> results(n);
    std::vector<bool> found(n, false);

    for (size_t i = 0; i < n; i++)
    {
        std::string key = tb_account_key(ids[i]);
        auto cap = tb_run_cmd(redis_impl, ctx, txm, {"get", key});
        if (cap.is_string())
        {
            results[i] = cap.str_val;
            found[i] = true;
        }
    }

    txservice::CommitTx(txm);  // read-only, commit always ok

    output->SetArray(static_cast<int>(n));
    for (size_t i = 0; i < n; i++)
    {
        if (found[i])
            output->operator[](static_cast<int>(i)).SetString(results[i]);
        else
            output->operator[](static_cast<int>(i)).SetNullString();
    }
}

// ---------------------------------------------------------------------------
// Execute LOOKUP_TRANSFER(S)
// ---------------------------------------------------------------------------
static void tb_exec_lookup_transfers(RedisServiceImpl *redis_impl,
                                     RedisConnectionContext *ctx,
                                     const std::vector<TbU128> &ids,
                                     brpc::RedisReply *output)
{
    txservice::TransactionExecution *txm = redis_impl->NewTxm(
        RedisCommandHandler::iso_level_, RedisCommandHandler::cc_protocol_);

    const size_t n = ids.size();
    std::vector<std::string> results(n);
    std::vector<bool> found(n, false);

    for (size_t i = 0; i < n; i++)
    {
        std::string key = tb_transfer_key(ids[i]);
        auto cap = tb_run_cmd(redis_impl, ctx, txm, {"get", key});
        if (cap.is_string())
        {
            results[i] = cap.str_val;
            found[i] = true;
        }
    }

    txservice::CommitTx(txm);

    output->SetArray(static_cast<int>(n));
    for (size_t i = 0; i < n; i++)
    {
        if (found[i])
            output->operator[](static_cast<int>(i)).SetString(results[i]);
        else
            output->operator[](static_cast<int>(i)).SetNullString();
    }
}

// ---------------------------------------------------------------------------
// Execute GET_ACCOUNT_TRANSFERS
// ---------------------------------------------------------------------------
static void tb_exec_get_account_transfers(RedisServiceImpl *redis_impl,
                                          RedisConnectionContext *ctx,
                                          const TbAccountFilter &filter,
                                          brpc::RedisReply *output)
{
    txservice::TransactionExecution *txm = redis_impl->NewTxm(
        RedisCommandHandler::iso_level_, RedisCommandHandler::cc_protocol_);

    std::string xfers_key = tb_account_transfers_key(filter.account_id);

    if (!tb_filter_is_valid(filter))
    {
        txservice::AbortTx(txm);
        output->SetArray(0);
        return;
    }

    // Build ZRANGEBYSCORE / ZREVRANGEBYSCORE command
    // Score = timestamp in microseconds
    std::string min_score =
        (filter.timestamp_min == 0) ? "-inf"
                                    : std::to_string(filter.timestamp_min / 1000);
    std::string max_score =
        (filter.timestamp_max == 0) ? "+inf"
                                    : std::to_string(filter.timestamp_max / 1000);
    uint32_t limit = filter.limit;

    bool reversed = (filter.flags & AccountFilterFlags::REVERSED) != 0;

    std::vector<std::string> range_cmd;
    if (reversed)
    {
        range_cmd = {"zrevrangebyscore",
                     xfers_key,
                     max_score,
                     min_score,
                     "limit",
                     "0",
                     std::to_string(limit)};
    }
    else
    {
        range_cmd = {"zrangebyscore",
                     xfers_key,
                     min_score,
                     max_score,
                     "limit",
                     "0",
                     std::to_string(limit)};
    }

    ArrayCaptureOutputHandler arr_cap;
    redis_impl->GenericCommand(ctx, txm, range_cmd, &arr_cap);

    bool want_debits = (filter.flags & AccountFilterFlags::DEBITS) != 0;
    bool want_credits = (filter.flags & AccountFilterFlags::CREDITS) != 0;

    std::vector<std::string> matched;
    for (const std::string &txfr_hex : arr_cap.values)
    {
        // Fetch transfer
        std::string key = "transfer:" + txfr_hex;
        auto cap = tb_run_cmd(redis_impl, ctx, txm, {"get", key});
        if (cap.has_error || cap.is_nil())
            continue;

        TbTransfer txfr{};
        if (!tb_decode_transfer(cap.str_val, txfr))
            continue;

        // Filter by debit/credit direction
        bool is_debit = (txfr.debit_account_id == filter.account_id);
        bool is_credit = (txfr.credit_account_id == filter.account_id);
        if ((want_debits && is_debit) || (want_credits && is_credit))
        {
            // Optional user_data and code filters
            if (!filter.user_data_128.is_zero() &&
                txfr.user_data_128 != filter.user_data_128)
                continue;
            if (filter.user_data_64 != 0 &&
                txfr.user_data_64 != filter.user_data_64)
                continue;
            if (filter.user_data_32 != 0 &&
                txfr.user_data_32 != filter.user_data_32)
                continue;
            if (filter.code != 0 && txfr.code != filter.code)
                continue;

            matched.push_back(cap.str_val);
        }
    }

    txservice::CommitTx(txm);

    output->SetArray(static_cast<int>(matched.size()));
    for (size_t i = 0; i < matched.size(); i++)
        output->operator[](static_cast<int>(i)).SetString(matched[i]);
}

// ---------------------------------------------------------------------------
// Execute GET_ACCOUNT_BALANCES
// ---------------------------------------------------------------------------
static void tb_exec_get_account_balances(RedisServiceImpl *redis_impl,
                                         RedisConnectionContext *ctx,
                                         const TbAccountFilter &filter,
                                         brpc::RedisReply *output)
{
    txservice::TransactionExecution *txm = redis_impl->NewTxm(
        RedisCommandHandler::iso_level_, RedisCommandHandler::cc_protocol_);

    // First verify account exists and has HISTORY flag
    if (!tb_filter_is_valid(filter))
    {
        txservice::AbortTx(txm);
        output->SetArray(0);
        return;
    }

    std::string acc_key = tb_account_key(filter.account_id);
    auto acc_cap = tb_run_cmd(redis_impl, ctx, txm, {"get", acc_key});
    if (acc_cap.is_nil() || acc_cap.has_error)
    {
        txservice::AbortTx(txm);
        output->SetArray(0);
        return;
    }
    TbAccount acc{};
    if (!tb_decode_account(acc_cap.str_val, acc) ||
        !acc.has_flag(AccountFlags::HISTORY))
    {
        txservice::AbortTx(txm);
        output->SetArray(0);
        return;
    }

    // Fetch all balance history entries
    std::string bal_key = tb_account_balances_key(filter.account_id);
    ArrayCaptureOutputHandler arr_cap;
    redis_impl->GenericCommand(
        ctx, txm, {"lrange", bal_key, "0", "-1"}, &arr_cap);

    bool reversed = (filter.flags & AccountFilterFlags::REVERSED) != 0;
    uint32_t limit = filter.limit;

    std::vector<std::string> matched;

    // For reversed, scan from newest (end) to oldest (beginning) then apply
    // limit, so we get the latest `limit` entries rather than the earliest.
    auto try_add = [&](const std::string &blob)
    {
        TbAccountBalance bal{};
        if (!tb_decode_account_balance(blob, bal))
            return;
        if (filter.timestamp_min != 0 && bal.timestamp < filter.timestamp_min)
            return;
        if (filter.timestamp_max != 0 && bal.timestamp > filter.timestamp_max)
            return;
        matched.push_back(blob);
    };

    if (reversed)
    {
        for (auto it = arr_cap.values.rbegin();
             it != arr_cap.values.rend() && matched.size() < limit;
             ++it)
            try_add(*it);
    }
    else
    {
        for (const std::string &blob : arr_cap.values)
        {
            if (matched.size() >= limit)
                break;
            try_add(blob);
        }
    }

    txservice::CommitTx(txm);

    output->SetArray(static_cast<int>(matched.size()));
    for (size_t i = 0; i < matched.size(); i++)
        output->operator[](static_cast<int>(i)).SetString(matched[i]);
}

// ---------------------------------------------------------------------------
// Subcommand dispatch helpers (shared between TB and TB_BIN)
// ---------------------------------------------------------------------------

// Normalise a subcommand name to lowercase
static std::string to_lower(std::string_view sv)
{
    std::string s(sv);
    for (char &c : s)
        if (c >= 'A' && c <= 'Z')
            c += 'a' - 'A';
    return s;
}

// ---------------------------------------------------------------------------
// TbCommandHandler::Run — text format
// ---------------------------------------------------------------------------

brpc::RedisCommandHandlerResult TbCommandHandler::Run(
    RedisConnectionContext *ctx,
    const std::vector<butil::StringPiece> &args,
    brpc::RedisReply *output,
    bool /*flush_batched*/)
{
    assert(!args.empty() && (args[0] == "tb" || args[0] == "TB"));

    if (args.size() < 2)
    {
        output->SetError(
            "ERR TB requires a subcommand: CREATE_ACCOUNT(S), CREATE_TRANSFER(S), "
            "LOOKUP_ACCOUNT(S), LOOKUP_TRANSFER(S), GET_ACCOUNT_TRANSFERS, "
            "GET_ACCOUNT_BALANCES");
        return brpc::REDIS_CMD_HANDLED;
    }

    std::vector<std::string_view> sv_args;
    sv_args.reserve(args.size());
    for (const auto &a : args)
        sv_args.emplace_back(a.data(), a.size());

    std::string subcmd = to_lower(sv_args[1]);

    if (subcmd == "create_account" || subcmd == "create_accounts")
    {
        // Parse batch of accounts from key=value tokens
        auto ranges = tb_split_batch(sv_args, 2);
        if (ranges.empty())
        {
            output->SetError("ERR TB CREATE_ACCOUNT(S): no account data provided");
            return brpc::REDIS_CMD_HANDLED;
        }

        std::vector<TbAccount> accounts;
        accounts.reserve(ranges.size());
        for (auto [s, e] : ranges)
        {
            TbAccount acc{};
            std::string err;
            if (!tb_parse_account_text(sv_args, s, e, acc, err))
            {
                output->SetError(err);
                return brpc::REDIS_CMD_HANDLED;
            }
            accounts.push_back(acc);
        }
        tb_exec_create_accounts(redis_impl_, ctx, accounts, output);
    }
    else if (subcmd == "create_transfer" || subcmd == "create_transfers")
    {
        auto ranges = tb_split_batch(sv_args, 2);
        if (ranges.empty())
        {
            output->SetError(
                "ERR TB CREATE_TRANSFER(S): no transfer data provided");
            return brpc::REDIS_CMD_HANDLED;
        }

        std::vector<TbTransfer> transfers;
        transfers.reserve(ranges.size());
        for (auto [s, e] : ranges)
        {
            TbTransfer txfr{};
            std::string err;
            if (!tb_parse_transfer_text(sv_args, s, e, txfr, err))
            {
                output->SetError(err);
                return brpc::REDIS_CMD_HANDLED;
            }
            transfers.push_back(txfr);
        }
        tb_exec_create_transfers(redis_impl_, ctx, transfers, output);
    }
    else if (subcmd == "lookup_account" || subcmd == "lookup_accounts")
    {
        // Each arg from index 2 is either "id=<val>" or just "<val>"
        std::vector<TbU128> ids;
        for (size_t i = 2; i < sv_args.size(); i++)
        {
            // Support both "id=1" and bare "1"
            std::string_view val = sv_args[i];
            if (val.size() > 3 && val.substr(0, 3) == "id=")
                val = val.substr(3);
            TbU128 id{};
            if (!tb_parse_u128(val, id))
            {
                output->SetError("ERR TB LOOKUP_ACCOUNT: invalid id: " +
                                 std::string(val));
                return brpc::REDIS_CMD_HANDLED;
            }
            ids.push_back(id);
        }
        if (ids.empty())
        {
            output->SetError("ERR TB LOOKUP_ACCOUNT(S): no ids provided");
            return brpc::REDIS_CMD_HANDLED;
        }
        tb_exec_lookup_accounts(redis_impl_, ctx, ids, output);
    }
    else if (subcmd == "lookup_transfer" || subcmd == "lookup_transfers")
    {
        std::vector<TbU128> ids;
        for (size_t i = 2; i < sv_args.size(); i++)
        {
            std::string_view val = sv_args[i];
            if (val.size() > 3 && val.substr(0, 3) == "id=")
                val = val.substr(3);
            TbU128 id{};
            if (!tb_parse_u128(val, id))
            {
                output->SetError("ERR TB LOOKUP_TRANSFER: invalid id: " +
                                 std::string(val));
                return brpc::REDIS_CMD_HANDLED;
            }
            ids.push_back(id);
        }
        if (ids.empty())
        {
            output->SetError("ERR TB LOOKUP_TRANSFER(S): no ids provided");
            return brpc::REDIS_CMD_HANDLED;
        }
        tb_exec_lookup_transfers(redis_impl_, ctx, ids, output);
    }
    else if (subcmd == "get_account_transfers")
    {
        TbAccountFilter filter{};
        std::string err;
        if (!tb_parse_account_filter_text(sv_args, 2, sv_args.size(), filter, err))
        {
            output->SetError(err);
            return brpc::REDIS_CMD_HANDLED;
        }
        if (filter.account_id.is_zero())
        {
            output->SetError("ERR TB GET_ACCOUNT_TRANSFERS: account_id required");
            return brpc::REDIS_CMD_HANDLED;
        }
        tb_exec_get_account_transfers(redis_impl_, ctx, filter, output);
    }
    else if (subcmd == "get_account_balances")
    {
        TbAccountFilter filter{};
        std::string err;
        if (!tb_parse_account_filter_text(sv_args, 2, sv_args.size(), filter, err))
        {
            output->SetError(err);
            return brpc::REDIS_CMD_HANDLED;
        }
        if (filter.account_id.is_zero())
        {
            output->SetError("ERR TB GET_ACCOUNT_BALANCES: account_id required");
            return brpc::REDIS_CMD_HANDLED;
        }
        tb_exec_get_account_balances(redis_impl_, ctx, filter, output);
    }
    else
    {
        output->SetError("ERR TB unknown subcommand: " + subcmd);
    }

    return brpc::REDIS_CMD_HANDLED;
}

// ---------------------------------------------------------------------------
// TbBinCommandHandler::Run — binary format
// Each argument after the subcommand is a raw binary blob:
//   CREATE_ACCOUNT(S)  : N × 128-byte Account blobs
//   CREATE_TRANSFER(S) : N × 128-byte Transfer blobs
//   LOOKUP_ACCOUNT(S)  : N × 16-byte ID blobs
//   LOOKUP_TRANSFER(S) : N × 16-byte ID blobs
//   GET_ACCOUNT_TRANSFERS / GET_ACCOUNT_BALANCES : 1 × 128-byte AccountFilter
// ---------------------------------------------------------------------------

brpc::RedisCommandHandlerResult TbBinCommandHandler::Run(
    RedisConnectionContext *ctx,
    const std::vector<butil::StringPiece> &args,
    brpc::RedisReply *output,
    bool /*flush_batched*/)
{
    assert(!args.empty() && (args[0] == "tb_bin" || args[0] == "TB_BIN"));

    if (args.size() < 2)
    {
        output->SetError("ERR TB_BIN requires a subcommand");
        return brpc::REDIS_CMD_HANDLED;
    }

    std::string subcmd = to_lower(std::string_view(args[1].data(), args[1].size()));

    if (subcmd == "create_account" || subcmd == "create_accounts")
    {
        std::vector<TbAccount> accounts;
        for (size_t i = 2; i < args.size(); i++)
        {
            std::string_view blob(args[i].data(), args[i].size());
            TbAccount acc{};
            if (!tb_decode_account(blob, acc))
            {
                output->SetError(
                    "ERR TB_BIN CREATE_ACCOUNT: each argument must be a 128-byte "
                    "account blob");
                return brpc::REDIS_CMD_HANDLED;
            }
            accounts.push_back(acc);
        }
        if (accounts.empty())
        {
            output->SetError("ERR TB_BIN CREATE_ACCOUNT(S): no account blobs provided");
            return brpc::REDIS_CMD_HANDLED;
        }
        tb_exec_create_accounts(redis_impl_, ctx, accounts, output);
    }
    else if (subcmd == "create_transfer" || subcmd == "create_transfers")
    {
        std::vector<TbTransfer> transfers;
        for (size_t i = 2; i < args.size(); i++)
        {
            std::string_view blob(args[i].data(), args[i].size());
            TbTransfer txfr{};
            if (!tb_decode_transfer(blob, txfr))
            {
                output->SetError(
                    "ERR TB_BIN CREATE_TRANSFER: each argument must be a 128-byte "
                    "transfer blob");
                return brpc::REDIS_CMD_HANDLED;
            }
            transfers.push_back(txfr);
        }
        if (transfers.empty())
        {
            output->SetError(
                "ERR TB_BIN CREATE_TRANSFER(S): no transfer blobs provided");
            return brpc::REDIS_CMD_HANDLED;
        }
        tb_exec_create_transfers(redis_impl_, ctx, transfers, output);
    }
    else if (subcmd == "lookup_account" || subcmd == "lookup_accounts")
    {
        std::vector<TbU128> ids;
        for (size_t i = 2; i < args.size(); i++)
        {
            std::string_view blob(args[i].data(), args[i].size());
            if (blob.size() != 16)
            {
                output->SetError(
                    "ERR TB_BIN LOOKUP_ACCOUNT: each id must be a 16-byte blob");
                return brpc::REDIS_CMD_HANDLED;
            }
            TbU128 id = tb_decode_u128(
                reinterpret_cast<const uint8_t *>(blob.data()));
            ids.push_back(id);
        }
        if (ids.empty())
        {
            output->SetError("ERR TB_BIN LOOKUP_ACCOUNT(S): no ids provided");
            return brpc::REDIS_CMD_HANDLED;
        }
        tb_exec_lookup_accounts(redis_impl_, ctx, ids, output);
    }
    else if (subcmd == "lookup_transfer" || subcmd == "lookup_transfers")
    {
        std::vector<TbU128> ids;
        for (size_t i = 2; i < args.size(); i++)
        {
            std::string_view blob(args[i].data(), args[i].size());
            if (blob.size() != 16)
            {
                output->SetError(
                    "ERR TB_BIN LOOKUP_TRANSFER: each id must be a 16-byte blob");
                return brpc::REDIS_CMD_HANDLED;
            }
            TbU128 id = tb_decode_u128(
                reinterpret_cast<const uint8_t *>(blob.data()));
            ids.push_back(id);
        }
        if (ids.empty())
        {
            output->SetError("ERR TB_BIN LOOKUP_TRANSFER(S): no ids provided");
            return brpc::REDIS_CMD_HANDLED;
        }
        tb_exec_lookup_transfers(redis_impl_, ctx, ids, output);
    }
    else if (subcmd == "get_account_transfers")
    {
        if (args.size() != 3 || args[2].size() != 128)
        {
            output->SetError(
                "ERR TB_BIN GET_ACCOUNT_TRANSFERS requires exactly one 128-byte "
                "AccountFilter blob");
            return brpc::REDIS_CMD_HANDLED;
        }
        TbAccountFilter filter{};
        std::string_view blob(args[2].data(), args[2].size());
        if (!tb_decode_account_filter(blob, filter))
        {
            output->SetError(
                "ERR TB_BIN GET_ACCOUNT_TRANSFERS: invalid AccountFilter blob");
            return brpc::REDIS_CMD_HANDLED;
        }
        tb_exec_get_account_transfers(redis_impl_, ctx, filter, output);
    }
    else if (subcmd == "get_account_balances")
    {
        if (args.size() != 3 || args[2].size() != 128)
        {
            output->SetError(
                "ERR TB_BIN GET_ACCOUNT_BALANCES requires exactly one 128-byte "
                "AccountFilter blob");
            return brpc::REDIS_CMD_HANDLED;
        }
        TbAccountFilter filter{};
        std::string_view blob(args[2].data(), args[2].size());
        if (!tb_decode_account_filter(blob, filter))
        {
            output->SetError(
                "ERR TB_BIN GET_ACCOUNT_BALANCES: invalid AccountFilter blob");
            return brpc::REDIS_CMD_HANDLED;
        }
        tb_exec_get_account_balances(redis_impl_, ctx, filter, output);
    }
    else
    {
        output->SetError("ERR TB_BIN unknown subcommand: " + subcmd);
    }

    return brpc::REDIS_CMD_HANDLED;
}

}  // namespace EloqKV

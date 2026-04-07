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

#include <brpc/redis.h>

#include <string>
#include <vector>

#include "output_handler.h"
#include "redis_handler.h"
#include "tb_types.h"

namespace EloqKV
{

class RedisServiceImpl;
class RedisConnectionContext;

// ---------------------------------------------------------------------------
// CaptureOutputHandler: captures a single string result from a GET call
// ---------------------------------------------------------------------------
class CaptureOutputHandler : public OutputHandler
{
public:
    enum class Type
    {
        NONE,
        NIL,
        STRING,
        INT,
        ERROR,
        ARRAY
    };

    Type type{Type::NONE};
    std::string str_val;
    int64_t int_val{0};
    bool has_error{false};
    std::string error_msg;
    std::vector<std::string> array_vals;  // for ZRANGEBYSCORE / LRANGE results

    void OnBool(bool b) override
    {
        type = Type::INT;
        int_val = b ? 1 : 0;
    }
    void OnString(std::string_view s) override
    {
        type = Type::STRING;
        str_val.assign(s.data(), s.size());
    }
    void OnInt(int64_t v) override
    {
        type = Type::INT;
        int_val = v;
    }
    void OnArrayStart(unsigned /*len*/) override
    {
        type = Type::ARRAY;
        array_vals.clear();
    }
    void OnArrayEnd() override
    {
    }
    void OnNil() override
    {
        type = Type::NIL;
    }
    void OnStatus(std::string_view s) override
    {
        type = Type::STRING;
        str_val.assign(s.data(), s.size());
    }
    void OnError(std::string_view s) override
    {
        has_error = true;
        error_msg.assign(s.data(), s.size());
        type = Type::ERROR;
    }
    void OnFormatError(const char *fmt, ...) override;

    bool is_nil() const
    {
        return type == Type::NIL;
    }
    bool is_string() const
    {
        return type == Type::STRING;
    }
    bool is_ok_status() const
    {
        return type == Type::STRING && str_val == "OK";
    }

    void reset()
    {
        type = Type::NONE;
        str_val.clear();
        int_val = 0;
        has_error = false;
        error_msg.clear();
        array_vals.clear();
    }
};

// ---------------------------------------------------------------------------
// ArrayCaptureOutputHandler: captures an array of strings (for LRANGE /
// ZRANGEBYSCORE results)
// ---------------------------------------------------------------------------
class ArrayCaptureOutputHandler : public OutputHandler
{
public:
    std::vector<std::string> values;
    bool has_error{false};
    std::string error_msg;

    void OnBool(bool /*b*/) override
    {
    }
    void OnString(std::string_view s) override
    {
        values.emplace_back(s.data(), s.size());
    }
    void OnInt(int64_t /*v*/) override
    {
    }
    void OnArrayStart(unsigned /*len*/) override
    {
        values.clear();
    }
    void OnArrayEnd() override
    {
    }
    void OnNil() override
    {
        // nil element within array — skip
    }
    void OnStatus(std::string_view s) override
    {
        values.emplace_back(s.data(), s.size());
    }
    void OnError(std::string_view s) override
    {
        has_error = true;
        error_msg.assign(s.data(), s.size());
    }
    void OnFormatError(const char *fmt, ...) override;

    void reset()
    {
        values.clear();
        has_error = false;
        error_msg.clear();
    }
};

// ---------------------------------------------------------------------------
// TbCommandHandler: handles "TB <subcommand> [args...]" (text format)
// ---------------------------------------------------------------------------
class TbCommandHandler : public RedisCommandHandler
{
public:
    explicit TbCommandHandler(RedisServiceImpl *redis_impl)
        : redis_impl_(redis_impl)
    {
    }

    brpc::RedisCommandHandlerResult Run(
        RedisConnectionContext *ctx,
        const std::vector<butil::StringPiece> &args,
        brpc::RedisReply *output,
        bool flush_batched) override;

private:
    RedisServiceImpl *redis_impl_;
};

// ---------------------------------------------------------------------------
// TbBinCommandHandler: handles "TB_BIN <subcommand> [128-byte-binary-args...]"
// ---------------------------------------------------------------------------
class TbBinCommandHandler : public RedisCommandHandler
{
public:
    explicit TbBinCommandHandler(RedisServiceImpl *redis_impl)
        : redis_impl_(redis_impl)
    {
    }

    brpc::RedisCommandHandlerResult Run(
        RedisConnectionContext *ctx,
        const std::vector<butil::StringPiece> &args,
        brpc::RedisReply *output,
        bool flush_batched) override;

private:
    RedisServiceImpl *redis_impl_;
};

}  // namespace EloqKV

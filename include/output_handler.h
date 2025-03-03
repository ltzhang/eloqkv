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
#include <string_view>

namespace EloqKV
{
/**
 * The handler class to handle command results. For now, only RESP2 protocol is
 * supported.
 */
class OutputHandler
{
public:
    virtual ~OutputHandler() = default;

    virtual void OnBool(bool b) = 0;
    virtual void OnString(std::string_view str) = 0;
    virtual void OnInt(int64_t val) = 0;
    virtual void OnArrayStart(unsigned len) = 0;
    virtual void OnArrayEnd() = 0;
    virtual void OnNil() = 0;
    virtual void OnStatus(std::string_view str) = 0;
    virtual void OnError(std::string_view str) = 0;
    virtual void OnFormatError(const char *fmt, ...) = 0;
};
}  // namespace EloqKV

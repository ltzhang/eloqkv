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

#include <vector>

#include "output_handler.h"

extern "C"
{
#include "lua/src/lua.h"
}

namespace EloqKV
{
/**
 * Handler of Redis command result.
 */
class LuaOutputHandler : public OutputHandler
{
public:
    explicit LuaOutputHandler(lua_State *lua) : lua_(lua)
    {
    }
    void OnBool(bool b) override;
    void OnString(std::string_view str) override;
    void OnInt(int64_t val) override;
    void OnArrayStart(unsigned len) override;
    void OnArrayEnd() override;
    void OnNil() override;
    void OnStatus(std::string_view str) override;
    void OnError(std::string_view str) override;
    void OnFormatError(const char *fmt, ...) override;

    bool HasError();

private:
    void UpdateArray()
    {
        if (!array_index_.empty())
        {
            lua_rawseti(
                lua_, -2, array_index_.back()++); /* set table at key `i' */
        }
    }

    std::vector<unsigned> array_index_;
    lua_State *lua_;
    bool has_error_{false};
};

}  // namespace EloqKV

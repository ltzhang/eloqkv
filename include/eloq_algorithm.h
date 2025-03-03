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

#include <cstddef>
#include <cstdint>
#include <map>

namespace EloqKV
{
// Randomly select `count `or `size` numbers from range [0-size). If repeat is
// true, numbers can be repeated, and outputs `count` numbers. If repeat is
// false, numbers are distinct, and outpus min(count, size) numbers.
//
// map.first is the selected random number. map.second is the index where the
// random number should be placed, some commands hope that the order of output
// is random, too.
void GenRandMap(int32_t size,
                int64_t count,
                bool repeat,
                std::multimap<int32_t, int64_t> &map);
}  // namespace EloqKV

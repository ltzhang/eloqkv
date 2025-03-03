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
#include "eloq_algorithm.h"

#include <butil/logging.h>

#include <cassert>
#include <cstdlib>

namespace EloqKV
{
// std::multimap<set_index, result_index>, set_index is the index of redis
// set/sorted_set (range from 0 to set.size()-1). result_index is the index of
// result string list (range from 0 to count-1). The multimap is sorted by
// set_index and there could be duplicate set_index.
void GenRandMap(int32_t size,
                int64_t count,
                bool repeat,
                std::multimap<int32_t, int64_t> &map)
{
    for (int64_t i = 0; i < count; i++)
    {
        int32_t ran = std::rand() % size;
        if (repeat)
        {
            map.emplace(ran, i);
            continue;
        }

        auto iter = map.find(ran);
        if (iter == map.end())
        {
            map.emplace(ran, i);
            continue;
        }

        while (true)
        {
            iter++;
            ran++;
            if (iter == map.end())
            {
                iter = map.begin();
            }

            if (ran == size)
            {
                ran = 0;
            }

            if (ran != iter->first)
            {
                map.emplace(ran, i);
                break;
            }
        }
    }
}
}  // namespace EloqKV

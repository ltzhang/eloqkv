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

namespace EloqKV
{
class RedisStats
{
public:
    static void ExposeBVar();
    static void HideBVar();

    static void IncrConnReceived();
    static void IncrConnRejected();
    static void IncrConnClosed();
    static void IncrBlockClient();
    static void DecrBlockClient();

    static void IncrReadCommand();
    static void IncrWriteCommand();
    static void IncrMultiObjectCommand();
    static void IncrCmdPerSec();

    static int64_t GetConnReceivedCount();
    static int64_t GetConnRejectedCount();
    static int64_t GetConnectingCount();
    static int64_t GetBlockedClientsCount();

    static int64_t GetTotalCommandsCount();
    static int64_t GetReadCommandsCount();
    static int64_t GetWriteCommandsCount();
    static int64_t GetMultiObjectCommandsCount();
    static int64_t GetCommandsPerSecond();

    static uint64_t GetStartNs()
    {
        return ns_start_;
    }

protected:
    static uint64_t ns_start_;
    static bool is_exposed_;
};
}  // namespace EloqKV

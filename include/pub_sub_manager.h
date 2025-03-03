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

#include <bthread/mutex.h>

#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "redis_connection_context.h"

namespace EloqKV
{

class PubSubManager
{
public:
    void Subscribe(const std::vector<std::string_view> &chans,
                   RedisConnectionContext *client);

    void Unsubscribe(const std::vector<std::string_view> &chans,
                     RedisConnectionContext *client);

    void UnsubscribeAll(RedisConnectionContext *client);

    void PSubscribe(const std::vector<std::string_view> &patterns,
                    RedisConnectionContext *client);

    void PUnsubscribe(const std::vector<std::string_view> &patterns,
                      RedisConnectionContext *client);

    int Publish(std::string_view chan, std::string_view msg);

private:
    bool SubscribeChannel(std::string_view chan,
                          RedisConnectionContext *client);

    bool UnsubscribeChannel(std::string_view chan,
                            RedisConnectionContext *client);

    bool SubscribePattern(std::string_view pattern,
                          RedisConnectionContext *client);

    bool UnsubscribePattern(std::string_view pattern,
                            RedisConnectionContext *client);

    // channels and the subscribed clients
    bthread::Mutex pub_sub_mu_;
    absl::flat_hash_map<std::string,
                        absl::flat_hash_set<RedisConnectionContext *>>
        pub_sub_channels_;

    // patterns and the subscribed clients
    absl::flat_hash_map<std::string,
                        absl::flat_hash_set<RedisConnectionContext *>>
        pattern_subs_;
};

}  // namespace EloqKV

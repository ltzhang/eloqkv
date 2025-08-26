#ifndef KVT_MANAGER_H
#define KVT_MANAGER_H

#include <string>
#include <memory>
#include "redis_connection_context.h"

namespace EloqKV {

class KVTManager {
public:
    static void handleCommand(RedisConnectionContext* ctx, 
                            const std::vector<butil::StringPiece>& args,
                            brpc::RedisReply* output) {}
    
private:
    static void handleCreateTable(RedisConnectionContext* ctx,
                                const std::vector<butil::StringPiece>& args,
                                brpc::RedisReply* output) {}
    static void handleStartTx(RedisConnectionContext* ctx,
                             const std::vector<butil::StringPiece>& args,
                             brpc::RedisReply* output) {}
    static void handleGet(RedisConnectionContext* ctx,
                         const std::vector<butil::StringPiece>& args,
                         brpc::RedisReply* output) {}
    static void handleSet(RedisConnectionContext* ctx,
                         const std::vector<butil::StringPiece>& args,
                         brpc::RedisReply* output) {}
    static void handleScan(RedisConnectionContext* ctx,
                          const std::vector<butil::StringPiece>& args,
                          brpc::RedisReply* output) {}
    static void handleReadCursor(RedisConnectionContext* ctx,
                                const std::vector<butil::StringPiece>& args,
                                brpc::RedisReply* output) {}
    static void handleCommitTx(RedisConnectionContext* ctx,
                              const std::vector<butil::StringPiece>& args,
                              brpc::RedisReply* output) {}
    static void handleAbortTx(RedisConnectionContext* ctx,
                             const std::vector<butil::StringPiece>& args,
                             brpc::RedisReply* output) {}
};

} // namespace EloqKV

#endif // KVT_MANAGER_H
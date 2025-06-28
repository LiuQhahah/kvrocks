#include "redis_cuckoo_filter_chain.h"

namespace redis {
rocksdb::Status CuckooFilterChain::Reserve(engine::Context &ctx, const Slice &user_key, uint32_t capacity,
                                           uint32_t bucket_size, uint16_t expansion, uint16_t max_iterations) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  capacity = 0;
  bucket_size = 0;
  expansion = 0;
  max_iterations = 0;
  ctx.txn_context_enabled = false;
}
}  // namespace redis

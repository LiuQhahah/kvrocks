#include "redis_cuckoo_filter_chain.h"

#include "cuckoo_filter.h"

namespace redis {
rocksdb::Status CuckooFilterChain::Reserve(engine::Context &ctx, const Slice &user_key, uint32_t capacity,
                                           uint32_t bucket_size, uint16_t expansion, uint16_t max_iterations) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  capacity = 0;
  bucket_size = 0;
  expansion = 0;
  max_iterations = 0;
  ctx.txn_context_enabled = false;
  CuckooFilterChainMetadata cuckoo_filter_chain_metadata;

  return createCuckooFilterChain(ctx, ns_key, capacity, bucket_size, expansion, max_iterations,
                                 &cuckoo_filter_chain_metadata);
}

rocksdb::Status CuckooFilterChain::createCuckooFilterChain(engine::Context &ctx, const Slice &ns_key, uint32_t capacity,
                                                           uint32_t bucket_size, uint16_t expansion,
                                                           uint16_t max_iterations,
                                                           CuckooFilterChainMetadata *metadata) {
  metadata->n_filters = 1;
  metadata->expansion = expansion;
  metadata->size = 0;
  metadata->bucket_size = bucket_size;
  metadata->capacity = capacity;
  metadata->max_iterations = max_iterations;

  auto [block_split_cuckoo_filter, _] = CreateBlockSplitCuckooFilter(metadata);

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter, {"createCuckooFilterChain"});
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::string cuckoo_filter_meta_bytes;
  metadata->Encode(&cuckoo_filter_meta_bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, cuckoo_filter_meta_bytes);
  if (!s.ok()) return s;

  std::string cf_key = getCFKey(ns_key, *metadata, metadata->n_filters - 1);
  s = batch->Put(cf_key, block_split_cuckoo_filter.GetData());
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

std::string CuckooFilterChain::getCFKey(const Slice &ns_key, const CuckooFilterChainMetadata &metadata,
                                        uint16_t filters_index) {
  std::string sub_key;
  PutFixed16(&sub_key, filters_index);
  std::string cf_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return cf_key;
}
}  // namespace redis

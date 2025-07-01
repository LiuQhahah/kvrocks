#include "redis_cuckoo_filter_chain.h"

#include "cuckoo_filter.h"

namespace redis {

rocksdb::Status CuckooFilterChain::MAdd(engine::Context &ctx, const Slice &user_key,
                                        const std::vector<std::string> &items,
                                        std::vector<CuckooFilterAddResult> *rets) {
  CuckooFilterInsertOptions insert_options;
  return InsertCommon(ctx, user_key, items, insert_options, rets);
}

rocksdb::Status CuckooFilterChain::getCuckooFilterChainMetadata(engine::Context &ctx, const Slice &ns_key,
                                                                CuckooFilterChainMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisCuckooFilter}, ns_key, metadata);
}

rocksdb::Status CuckooFilterChain::InsertCommon(engine::Context &ctx, const Slice &user_key,
                                                const std::vector<std::string> &items,
                                                const CuckooFilterInsertOptions &insert_options,
                                                std::vector<CuckooFilterAddResult> *results) {
  if (items.empty()) {
    return rocksdb::Status::OK();
  }

  std::string ns_key = AppendNamespacePrefix(user_key);
  ctx.txn_context_enabled = false;

  CuckooFilterChainMetadata metadata;
  rocksdb::Status s = getCuckooFilterChainMetadata(ctx, ns_key, &metadata);

  // if no metadata found, create a new cuckoo filter chain
  if (s.IsNotFound() && insert_options.auto_create) {
    s = createCuckooFilterChain(ctx, ns_key, insert_options.capacity, insert_options.bucket_size,
                                insert_options.expansion, insert_options.max_iterations, &metadata);
  }

  if (!s.ok()) {
    return s;
  }

  std::vector<std::string> cf_key_lists;
  getCFKeyList(ns_key, metadata, &cf_key_lists);

  std::vector < rocksdb::PinnableSlice cf_data_list;
  s = getCFDataList(ctx, bf_key_list, &bf_data_list);
  if (!s.ok()) {
    return s;
  }

  uint64_t origin_size = metadata.size;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter, {"insert"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) {
    return s;
  }

  // for-loop items
  for (const auto &item : items) {
    if (item.empty()) {
      results->push_back(CuckooFilterAddResult::kOk);
      continue;
    }

    // Calculate the hash of the item
    uint64_t hash = CUCKOO_GEN_HASH(item.data(), item.size());
    LookupParams params;
    getLookupParams(hash, &params);
    
    bool inserted = false;
  }
}

void getLookupParams(CuckooHash hash, LookupParams *params) {
   params->fp = hash % 255 + 1; 
   params->h1 = hash;
   parmas->h2 = getAltHash(params->fp,params->h1);
}

CuckooHash getAltHash(CuckooFingerprint fp,CuckooHash index){
  return index^(fp*0x5bd1e995);
}

void CuckooFilterChain::getCFKeyList(const Slice &ns_key, const CuckooFilterChainMetadata &metadata,
                                     std::vector<std::string> *cf_key_list) {
  cf_key_list->reserve(metadata.n_filters);
  for (uint16_t i = 0; i < metadata.n_filters; ++i) {
    std::string cf_key = getCFKey(ns_key, metadata, i);
    cf_key_list->push_back(std::move(cf_key));
  }
}
rocksdb::Status CuckooFilterChain::Add(engine::Context &ctx, const Slice &user_key, const std::string &item,
                                       CuckooFilterAddResult *ret) {
  std::vector < CuckooFilterAddResult tmp{CuckooFilterAddResult::kOk};
  rocksdb::Status s = MAdd(ctx, user_key, {item}, &tmp);
  *ret = tmp[0];
  return s;
}
rocksdb::Status CuckooFilterChain::Reserve(engine::Context &ctx, const Slice &user_key, uint32_t capacity,
                                           uint32_t bucket_size, uint16_t expansion, uint16_t max_iterations) {
  std::string ns_key = AppendNamespacePrefix(user_key);
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

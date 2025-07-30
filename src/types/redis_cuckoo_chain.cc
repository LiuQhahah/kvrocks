/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "redis_cuckoo_chain.h"

#include <glog/logging.h>

#include <cmath>

#include "cuckoo_filter.h"

namespace redis {

rocksdb::Status CuckooChain::getCuckooChainMetadata(engine::Context &ctx, const Slice &ns_key,
                                                    CuckooChainMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisCuckooFilter}, ns_key, metadata);
}

std::string CuckooChain::getCFKey(const Slice &ns_key, const CuckooChainMetadata &metadata, uint16_t filter_index) {
  std::string sub_key;
  PutFixed16(&sub_key, filter_index);
  std::string cf_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return cf_key;
}

std::vector<std::string> CuckooChain::getCFKeys(const Slice &user_key, const CuckooChainMetadata &metadata) {
  std::vector<std::string> keys;
  std::string ns_key = AppendNamespacePrefix(user_key);
  for (uint16_t i = 0; i < metadata.n_filters; ++i) {
    keys.push_back(getCFKey(ns_key, metadata, i));
  }
  return keys;
}

rocksdb::Status CuckooChain::Reserve(engine::Context &ctx, const Slice &user_key, uint64_t capacity,
                                     uint8_t bucket_size, uint16_t max_iterations, uint8_t expansion) {
  if (capacity == 0) {
    return rocksdb::Status::InvalidArgument("capacity should be larger than 0");
  }

  std::string ns_key = AppendNamespacePrefix(user_key);

  CuckooChainMetadata metadata;
  rocksdb::Status s = getCuckooChainMetadata(ctx, ns_key, &metadata);
  if (s.ok()) {
    return rocksdb::Status::InvalidArgument("the key already exists");
  }

  metadata.size = 0;
  metadata.base_capacity = capacity;
  metadata.bucket_size = bucket_size;
  metadata.max_iterations = max_iterations;
  metadata.expansion = expansion;
  metadata.n_filters = 1;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter, {"RESERVE"});
  batch->PutLogData(log_data.Encode());

  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);

  size_t table_size = CuckooFilter::OptimalTableSize(capacity, bucket_size);
  std::string cf_key = getCFKey(ns_key, metadata, 0);
  batch->Put(cf_key, CuckooFilter::Create(table_size));

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CuckooChain::expand(engine::Context &ctx, const Slice &user_key, CuckooChainMetadata &metadata) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter, {"EXPAND"});
  batch->PutLogData(log_data.Encode());

  uint64_t new_shard_capacity = metadata.base_capacity * pow(metadata.expansion, metadata.n_filters);
  size_t new_shard_table_size = CuckooFilter::OptimalTableSize(new_shard_capacity, metadata.bucket_size);

  metadata.n_filters++;

  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);

  std::string cf_key = getCFKey(ns_key, metadata, metadata.n_filters - 1);
  batch->Put(cf_key, CuckooFilter::Create(new_shard_table_size));

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CuckooChain::Add(engine::Context &ctx, const Slice &user_key, const std::string &item,
                                 CuckooFilterAddResult *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  CuckooChainMetadata metadata;
  rocksdb::Status s = getCuckooChainMetadata(ctx, ns_key, &metadata);
  if (s.IsNotFound()) {
    *ret = CuckooFilterAddResult::kFull;
    return rocksdb::Status::NotFound("key not found");
  }
  if (!s.ok()) return s;

  uint16_t last_filter_idx = metadata.n_filters - 1;
  std::string last_cf_key = getCFKey(ns_key, metadata, last_filter_idx);
  std::string cf_data;
  s = storage_->Get(ctx, ctx.GetReadOptions(), last_cf_key, &cf_data);
  if (!s.ok()) return s;

  CuckooFilter cuckoo_filter(cf_data, metadata.bucket_size, metadata.max_iterations);

  if (cuckoo_filter.Add(item)) {
    metadata.size++;
    auto batch = storage_->GetWriteBatchBase();
    WriteBatchLogData log_data(kRedisCuckooFilter, {"ADD"});
    batch->PutLogData(log_data.Encode());
    std::string metadata_bytes;
    metadata.Encode(&metadata_bytes);
    batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
    batch->Put(last_cf_key, cf_data);

    *ret = CuckooFilterAddResult::kOk;
    return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }

  if (!metadata.IsScaling()) {
    *ret = CuckooFilterAddResult::kFull;
    return rocksdb::Status::OK();
  }

  s = expand(ctx, user_key, metadata);
  if (!s.ok()) return s;

  s = getCuckooChainMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  return Add(ctx, user_key, item, ret);
}

rocksdb::Status CuckooChain::AddNX(engine::Context &ctx, const Slice &user_key, const std::string &item, int *added) {
  *added = 0;
  int exists = 0;
  rocksdb::Status s = Exists(ctx, user_key, item, &exists);
  if (!s.ok()) return s;
  if (exists) return rocksdb::Status::OK();

  CuckooFilterAddResult ret;
  s = Add(ctx, user_key, item, &ret);
  if (!s.ok()) return s;

  if (ret == CuckooFilterAddResult::kOk) {
    *added = 1;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status CuckooChain::Exists(engine::Context &ctx, const Slice &user_key, const std::string &item, int *exists) {
  *exists = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  CuckooChainMetadata metadata;
  rocksdb::Status s = getCuckooChainMetadata(ctx, ns_key, &metadata);
  if (s.IsNotFound()) return rocksdb::Status::OK();
  if (!s.ok()) return s;

  std::vector<std::string> cf_keys = getCFKeys(user_key, metadata);
  for (const auto &cf_key : cf_keys) {
    std::string cf_data;
    s = storage_->Get(ctx, ctx.GetReadOptions(), cf_key, &cf_data);
    if (s.ok()) {
      CuckooFilter cuckoo_filter(cf_data, metadata.bucket_size, metadata.max_iterations);
      if (cuckoo_filter.Contains(item)) {
        *exists = 1;
        return rocksdb::Status::OK();
      }
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status CuckooChain::Delete(engine::Context &ctx, const Slice &user_key, const std::string &item, int *deleted) {
  *deleted = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  CuckooChainMetadata metadata;
  rocksdb::Status s = getCuckooChainMetadata(ctx, ns_key, &metadata);
  if (s.IsNotFound()) return rocksdb::Status::OK();
  if (!s.ok()) return s;

  std::vector<std::string> cf_keys = getCFKeys(user_key, metadata);
  for (int i = cf_keys.size() - 1; i >= 0; --i) {
    std::string cf_data;
    s = storage_->Get(ctx, ctx.GetReadOptions(), cf_keys[i], &cf_data);
    if (!s.ok()) continue;

    CuckooFilter cuckoo_filter(cf_data, metadata.bucket_size, metadata.max_iterations);
    if (cuckoo_filter.Delete(item)) {
      metadata.size--;
      metadata.num_deleted_items++;
      auto batch = storage_->GetWriteBatchBase();
      WriteBatchLogData log_data(kRedisCuckooFilter, {"DEL"});
      batch->PutLogData(log_data.Encode());
      std::string metadata_bytes;
      metadata.Encode(&metadata_bytes);
      batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
      batch->Put(cf_keys[i], cf_data);

      *deleted = 1;
      return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status CuckooChain::Count(engine::Context &ctx, const Slice &user_key, const std::string &item, int *count) {
  *count = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  CuckooChainMetadata metadata;
  rocksdb::Status s = getCuckooChainMetadata(ctx, ns_key, &metadata);
  if (s.IsNotFound()) return rocksdb::Status::OK();
  if (!s.ok()) return s;

  std::vector<std::string> cf_keys = getCFKeys(user_key, metadata);
  for (const auto &cf_key : cf_keys) {
    std::string cf_data;
    s = storage_->Get(ctx, ctx.GetReadOptions(), cf_key, &cf_data);
    if (s.ok()) {
      CuckooFilter cuckoo_filter(cf_data, metadata.bucket_size, metadata.max_iterations);
      *count += cuckoo_filter.Count(item);
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status CuckooChain::Info(engine::Context &ctx, const Slice &user_key, CuckooChainMetadata *metadata) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  return getCuckooChainMetadata(ctx, ns_key, metadata);
}

}  // namespace redis
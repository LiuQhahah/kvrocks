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

rocksdb::Status CuckooChain::Reserve(engine::Context &ctx, const Slice &user_key, uint32_t capacity,
                                     uint8_t bucket_size, uint16_t max_iterations) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  CuckooChainMetadata metadata;
  rocksdb::Status s = getCuckooChainMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (!s.IsNotFound()) {
    return rocksdb::Status::InvalidArgument("the key already exists");
  }

  metadata.size = 0;
  metadata.capacity = capacity;
  metadata.bucket_size = bucket_size;
  metadata.max_iterations = max_iterations;
  metadata.table_size = CuckooFilter::OptimalTableSize(capacity, bucket_size);
  metadata.n_filters = 1; // Initial filter
  metadata.expansion = 0; // Cuckoo filters are typically non-scaling

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter, {"RESERVE"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
  if (!s.ok()) return s;

  std::string cf_key = getCFKey(ns_key, metadata, 0); // First filter at index 0
  s = batch->Put(cf_key, CuckooFilter::Create(metadata.table_size));
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CuckooChain::Add(engine::Context &ctx, const Slice &user_key, const std::string &item,
                                 CuckooFilterAddResult *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  CuckooChainMetadata metadata;
  rocksdb::Status s = getCuckooChainMetadata(ctx, ns_key, &metadata);
  if (s.IsNotFound()) {
    *ret = CuckooFilterAddResult::kFull; // Or kNotFound, depending on desired behavior
    return rocksdb::Status::NotFound("key not found");
  }
  if (!s.ok()) return s;

  // Get the Cuckoo Filter data (the actual bit array)
  std::string cf_key = getCFKey(ns_key, metadata, 0); // Assuming single filter for now
  rocksdb::PinnableSlice cf_data_slice;
  s = storage_->Get(ctx, ctx.GetReadOptions(), cf_key, &cf_data_slice);
  if (!s.ok()) return s;

  std::string cf_data = cf_data_slice.ToString(); // Copy data to a mutable string

  // Check if item already exists (optional, but good for performance)
  CuckooFilter cuckoo_filter_check(cf_data, metadata.bucket_size, metadata.max_iterations);
  if (cuckoo_filter_check.Contains(item)) {
    *ret = CuckooFilterAddResult::kExist;
    return rocksdb::Status::OK();
  }

  // Try to add the item
  CuckooFilter cuckoo_filter_add(cf_data, metadata.bucket_size, metadata.max_iterations);
  if (cuckoo_filter_add.Add(item)) {
    // Update metadata and write back
    metadata.size++;

    auto batch = storage_->GetWriteBatchBase();
    WriteBatchLogData log_data(kRedisCuckooFilter, {"ADD"});
    s = batch->PutLogData(log_data.Encode());
    if (!s.ok()) return s;

    std::string metadata_bytes;
    metadata.Encode(&metadata_bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
    if (!s.ok()) return s;

    s = batch->Put(cf_key, cf_data);
    if (!s.ok()) return s;

    *ret = CuckooFilterAddResult::kOk;
    return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  } else {
    // Filter is full
    *ret = CuckooFilterAddResult::kFull;
    return rocksdb::Status::OK(); // Not a RocksDB error, but a filter-specific status
  }
}

}  // namespace redis
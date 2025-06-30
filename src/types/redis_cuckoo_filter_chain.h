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

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

struct CuckooFilterInsertOptions {
  uint32_t capacity = 1000;       // default capacity
  uint32_t bucket_size = 4;       // default bucket size
  uint16_t expansion = 2;         // default expansion factor
  uint16_t max_iterations = 500;  // default max iterations
  bool auto_create = true;        // default auto create
};

enum class CuckooFilterAddResult {
  kOk,
  kExist,
  kFull,
};

class CuckooFilterChain : public Database {
 public:
  // constructor
  CuckooFilterChain(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  // Reserve a cuckoo filter chain with the given parameters
  rocksdb::Status Reserve(engine::Context &ctx, const Slice &user_key, uint32_t capacity, uint32_t bucket_size,
                          uint16_t expansion, uint16_t max_iterations);
  rocksdb::Status Add(engine::Context &ctx, const Slice &user_key, const std::string &item, CuckooFilterAddResult *ret);
  rocksdb::Status MAdd(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &items,
                       std::vector < CuckooFilterAddResult);
  rocksdb::Status InsertCommon(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &items,
                               const CuckooFilterInsertOptions &insert_options,
                               std::vector<CuckooFilterAddResult> *results);

 private:
  rocksdb::Status createCuckooFilterChain(engine::Context &ctx, const Slice &ns_key, uint32_t capacity,
                                          uint32_t bucket_size, uint16_t expansion, uint16_t max_iterations,
                                          CuckooFilterChainMetadata *metadata);
  std::string getCFKey(const Slice &ns_key, const CuckooFilterChainMetadata &metadata, uint16_t filters_index);
  rocksdb::Status getCuckooFilterChainMetadata(engine::Context &ctx, const Slice &ns_key,
                                               CuckooFilterChainMetadata *metadata);
  void getCFKeyList(const Slice &ns_key, const CuckooFilterChainMetadata &metadata, std::vector<std::string> *cf_keys);
  rocksdb::Status getCFDataList(engine::Context &ctx, const std::vector<std::string> &cf_key_list,
                                std::vector<rocksdb::PinnableSlice> *cf_data_list);

  rocksdb::Status CuckooFilterChain::getCFDataList(engine::Context &ctx, const std::vector<std::string> &cf_key_list,
                                                   std::vector<rocksdb::PinnableSlice> *cf_data_list) {
    cf_data_list->reserve(cf_key_list.size());
    for (const auto &cf_key : cf_key_list) {
      rocksdb::PinnableSlice pin_value;

      rocksdb::Status s = storage_->Get(ctx, ctx.GetReadOptions(), cf_key, &pin_value);

      if (!s.ok()) return s;
      cf_data_list->push_back(std::move(pin_value));
    }

    return rocksdb::Status::OK();
  }
};

}  // namespace redis
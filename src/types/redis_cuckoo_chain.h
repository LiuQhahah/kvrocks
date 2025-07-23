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

#pragma once

#include "cuckoo_filter.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

// Default values for Cuckoo Filter
const uint32_t kCFDefaultCapacity = 1024;
const uint8_t kCFDefaultBucketSize = 2;
const uint16_t kCFDefaultMaxIterations = 500;
const uint8_t kCFDefaultExpansion = 1;

enum class CuckooFilterAddResult {
  kOk,
  kExist,
  kFull,
};

enum class CuckooFilterDelResult {
  kOk,
  kNotFound,
};

class CuckooChain : public Database {
 public:
  CuckooChain(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Reserve(engine::Context &ctx, const Slice &user_key, uint32_t capacity, uint8_t bucket_size,
                          uint16_t max_iterations, uint8_t expansion);
  rocksdb::Status Add(engine::Context &ctx, const Slice &user_key, const std::string &item, CuckooFilterAddResult *ret);
  rocksdb::Status AddNX(engine::Context &ctx, const Slice &user_key, const std::string &item, int *added);
  rocksdb::Status Exists(engine::Context &ctx, const Slice &user_key, const std::string &item, int *exists);
  rocksdb::Status MExists(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &items,
                          std::vector<bool> *exists);
  rocksdb::Status Insert(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &items,
                         uint32_t capacity, bool no_create, std::vector<int> *results);
  rocksdb::Status InsertNX(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &items,
                           uint32_t capacity, bool no_create, std::vector<int> *results);
  rocksdb::Status ScanDump(engine::Context &ctx, const Slice &user_key, uint64_t iter, uint64_t *next_iter, std::string *data);
  rocksdb::Status LoadChunk(engine::Context &ctx, const Slice &user_key, uint64_t iter, const std::string &data);
  rocksdb::Status Delete(engine::Context &ctx, const Slice &user_key, const std::string &item, int *deleted);
  rocksdb::Status Count(engine::Context &ctx, const Slice &user_key, const std::string &item, int *count);
  rocksdb::Status Info(engine::Context &ctx, const Slice &user_key, CuckooChainMetadata *metadata);

 private:
  rocksdb::Status getCuckooChainMetadata(engine::Context &ctx, const Slice &ns_key, CuckooChainMetadata *metadata);
  std::string getCFKey(const Slice &ns_key, const CuckooChainMetadata &metadata, uint16_t filter_index);
  std::vector<std::string> getCFKeys(const Slice &user_key, const CuckooChainMetadata &metadata);
  rocksdb::Status expand(engine::Context &ctx, const Slice &user_key, CuckooChainMetadata &metadata);
};

}  // namespace redis
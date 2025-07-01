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

// define MurmurHash64A_Bloom function
#include "murmur2/murmurhash2.h"
#define CUCKOO_GEN_HASH(s, n) MurmurHash64A_Bloom(s, n, 0)

namespace redis {

struct CuckooFilterInsertOptions {
  uint32_t capacity = 1000;       // default capacity
  uint32_t bucket_size = 4;       // default bucket size
  uint16_t expansion = 2;         // default expansion factor
  uint16_t max_iterations = 500;  // default max iterations
  bool auto_create = true;        // default auto create
};

uint8_t CuckooFingerprint;
uint64_t CuckooHash;
uint8_t CuckooBucket[1];
uint8_t MyCuckooBucket;
#define CF_MAX_NUM_BUCKETS (0x00FFFFFFFFFFFFFFULL) // 56 bits, see struct SubCF

struct SubCF{
  uint64_t num_buckets:56;
  uint64_t bucket_size:8; 
  MyCuckooBucket *my_cuckoo_bucket; 
};


struct LookupParams{
  CuckooHash h1;
  Cuckoohash h2;
  CuckooFingerprint fp;
}
struct CuckooFilter{
  uint64_t num_buckets;  // number of buckets
  uint64_t num_items;   // number of items in the filter
  uint64_t num_deletes; // number of deleted items
  uint16_t num_filters; // number of filters in the chain
  uint16_t bucket_size; // size of each bucket
  uint16_t expansion;   // expansion factor
  SubCF *sub_cf; // pointer to the sub cuckoo filters
};



enum class CuckooFilterAddResult {
  kOk,
  kExist,
  kFull,
};
enum CuckooFilterInsertStatus{
  CuckooInsert_Inserted = 1,
  CuckooInsert_Exists = 0,
  CuckooInsert_NoSpace = -1,
  CuckooInsert_MemAllocFailed = -2
};

enum CuckooRc{
  CUCKOO_OK = 0,
  CUCKOO_ERR = -1,
  CUCKOO_OOM = -2,
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
  void getLookupParams(CuckooHash hash,LookupParams *params);
  CuckooHash getAltHash(CuckooFingerprint fp,CuckooHash index);
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
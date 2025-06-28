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

namespace redis {

class CuckooFilterChain : public Database {
 public:
  // constructor
  CuckooFilterChain(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  // Reserve a cuckoo filter chain with the given parameters
  rocksdb::Status Reserve(engine::Context &ctx, const Slice &user_key, uint32_t capacity, uint32_t bucket_size,
                          uint16_t expansion, uint16_t max_iterations);
};

}  // namespace redis
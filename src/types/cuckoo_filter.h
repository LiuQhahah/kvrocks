// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cmath>
#include <cstdint>
#include <memory>
#include <nonstd/span.hpp>
#include <string>
#include "storage/redis_metadata.h"

#include "status.h"

class BlockSplitCuckooFilter;
using OwnedBlockSplitCuckooFilter = std::tuple<BlockSplitCuckooFilter, std::string>;
OwnedBlockSplitCuckooFilter CreateBlockSplitCuckooFilter(CuckooFilterChainMetadata *metadata);

class BlockSplitCuckooFilter {
 public:
  explicit BlockSplitCuckooFilter(nonstd::span<char> data) : data_(data) {};
  std::string_view GetData() const { return {data_.data(), data_.size()}; }

 private:
  nonstd::span<char> data_;
};
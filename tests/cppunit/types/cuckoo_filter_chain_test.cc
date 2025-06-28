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

#include <gtest/gtest.h>

#include "test_base.h"
#include "types/redis_cuckoo_filter_chain.h"

class RedisCuckooFilterChainTest : public TestBase {
 protected:
  explicit RedisCuckooFilterChainTest() = default;
  ~RedisCuckooFilterChainTest() override = default;

  void SetUp() override {
    key_ = "test_sb_chain_key";
    sb_chain_ = std::make_unique<redis::CuckooFilterChain>(storage_.get(), "sb_chain_ns");
  }
  void TearDown() override {}
  std::unique_ptr<redis::CuckooFilterChain> sb_chain_;
};

TEST_F(RedisCuckooFilterChainTest, Reserve) {
  uint32_t capacity = 1000;
  uint16_t expansion = 0;
  uint32_t bucket_size = 2;
  uint16_t max_iterations = 500;
  auto s = sb_chain_->Reserve(*ctx_, key_, capacity, bucket_size, expansion, max_iterations);
  EXPECT_TRUE(s.ok());
}
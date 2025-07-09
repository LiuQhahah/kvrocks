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
#include "types/redis_cuckoo_chain.h"

class RedisCuckooFilterChainTest : public TestBase {
 protected:
  explicit RedisCuckooFilterChainTest() = default;
  ~RedisCuckooFilterChainTest() override = default;

  void SetUp() override {
    key_ = "test_sb_chain_key";
    sb_chain_ = std::make_unique<redis::CuckooChain>(storage_.get(), "sb_chain_ns");
  }
  void TearDown() override {}
  std::unique_ptr<redis::CuckooChain> sb_chain_;
};

TEST_F(RedisCuckooFilterChainTest, Reserve) {
  uint32_t capacity = 1000;
  // uint16_t expansion = 0; // Cuckoo filters are typically non-scaling
  uint8_t bucket_size = 2;
  uint16_t max_iterations = 500;
  auto s = sb_chain_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, 1);
  EXPECT_TRUE(s.ok());
}

/*
class CuckooFilterCommandsTest : public TestBase {};

TEST_F(CuckooFilterCommandsTest, Reserve) {
  // Test basic CF.RESERVE
  std::vector<std::string> args = {"cf.reserve", "mycf", "1000"};
  std::string output;
  Status s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  ASSERT_EQ(output, "+OK\r\n");

  // Test CF.RESERVE with BUCKETSIZE and MAXITERATIONS
  args = {"cf.reserve", "mycf2", "2000", "BUCKETSIZE", "4", "MAXITERATIONS", "100"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  ASSERT_EQ(output, "+OK\r\n");

  // Test CF.RESERVE with capacity 0
  args = {"cf.reserve", "mycf3", "0"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_FALSE(s.IsOK());
  ASSERT_EQ(output, "-ERR capacity should be larger than 0\r\n");

  // Test CF.RESERVE with existing key
  args = {"cf.reserve", "mycf", "1000"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_FALSE(s.IsOK());
  ASSERT_EQ(output, "-ERR the key already exists\r\n");

  // Test CF.RESERVE with invalid BUCKETSIZE
  args = {"cf.reserve", "mycf4", "1000", "BUCKETSIZE", "abc"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_FALSE(s.IsOK());
  ASSERT_EQ(output, "-ERR invalid integer\r\n");

  // Test CF.RESERVE with invalid MAXITERATIONS
  args = {"cf.reserve", "mycf5", "1000", "MAXITERATIONS", "xyz"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_FALSE(s.IsOK());
  ASSERT_EQ(output, "-ERR invalid integer\r\n");
}

TEST_F(CuckooFilterCommandsTest, Add) {
  // Reserve a cuckoo filter first
  std::vector<std::string> args = {"cf.reserve", "mycf_add", "10"};
  std::string output;
  Status s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  ASSERT_EQ(output, "+OK\r\n");

  // Add a new item
  args = {"cf.add", "mycf_add", "item1"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  ASSERT_EQ(output, ":1\r\n");

  // Add an existing item
  args = {"cf.add", "mycf_add", "item1"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  ASSERT_EQ(output, ":0\r\n");

  // Add another new item
  args = {"cf.add", "mycf_add", "item2"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  ASSERT_EQ(output, ":1\r\n");

  // Add to a non-existent key
  args = {"cf.add", "nonexistent_cf", "item1"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_FALSE(s.IsOK());
  ASSERT_EQ(output, "-ERR key not found\r\n");

  // Test adding until full (this might take many adds depending on capacity and bucket size)
  // For a capacity of 10, bucket size 2, it should fill up quickly
  args = {"cf.reserve", "mycf_full", "2", "BUCKETSIZE", "2"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  ASSERT_EQ(output, "+OK\r\n");

  args = {"cf.add", "mycf_full", "a"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  ASSERT_EQ(output, ":1\r\n");

  args = {"cf.add", "mycf_full", "b"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  ASSERT_EQ(output, ":1\r\n");

  args = {"cf.add", "mycf_full", "c"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  ASSERT_EQ(output, ":1\r\n");

  args = {"cf.add", "mycf_full", "d"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  ASSERT_EQ(output, ":1\r\n");

  // This one should fail as the filter is likely full
  args = {"cf.add", "mycf_full", "e"};
  s = redis_connection->ExecuteCommand(args, &output);
  ASSERT_FALSE(s.IsOK());
  ASSERT_EQ(output, "-ERR Cuckoo filter is full\r\n");
}
*/

TEST_F(RedisCuckooFilterChainTest, AddNxExistsCountDelete) {
  uint32_t capacity = 1000;
  uint8_t bucket_size = 2;
  uint16_t max_iterations = 500;
  auto s = sb_chain_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, 1);
  EXPECT_TRUE(s.ok());

  // Test AddNX
  int added = 0;
  s = sb_chain_->AddNX(*ctx_, key_, "item1", &added);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(added, 1);

  s = sb_chain_->AddNX(*ctx_, key_, "item1", &added);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(added, 0);

  // Test Exists
  int exists = 0;
  s = sb_chain_->Exists(*ctx_, key_, "item1", &exists);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(exists, 1);

  s = sb_chain_->Exists(*ctx_, key_, "item_non_exist", &exists);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(exists, 0);

  // Test Count
  int count = 0;
  s = sb_chain_->Count(*ctx_, key_, "item1", &count);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(count, 1);

  s = sb_chain_->Count(*ctx_, key_, "item_non_exist", &count);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(count, 0);

  // Test Delete
  int deleted = 0;
  s = sb_chain_->Delete(*ctx_, key_, "item_non_exist", &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(deleted, 0);

  s = sb_chain_->Delete(*ctx_, key_, "item1", &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(deleted, 1);

  s = sb_chain_->Exists(*ctx_, key_, "item1", &exists);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(exists, 0);
}

TEST_F(RedisCuckooFilterChainTest, Info) {
  uint32_t capacity = 2048;
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 100;
  auto s = sb_chain_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, 1);
  EXPECT_TRUE(s.ok());

  int added = 0;
  s = sb_chain_->AddNX(*ctx_, key_, "item1", &added);
  EXPECT_TRUE(s.ok());
  s = sb_chain_->AddNX(*ctx_, key_, "item2", &added);
  EXPECT_TRUE(s.ok());

  CuckooChainMetadata metadata;
  s = sb_chain_->Info(*ctx_, key_, &metadata);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(metadata.capacity, capacity);
  EXPECT_EQ(metadata.bucket_size, bucket_size);
  EXPECT_EQ(metadata.max_iterations, max_iterations);
  EXPECT_EQ(metadata.size, 2);
}

TEST_F(RedisCuckooFilterChainTest, ExpansionTest) {
  // 1. Reserve a small filter with expansion enabled
  uint32_t initial_capacity = 4;
  uint8_t bucket_size = 2;
  uint16_t max_iterations = 10;
  uint8_t expansion = 2;
  auto s = sb_chain_->Reserve(*ctx_, key_, initial_capacity, bucket_size, max_iterations, expansion);
  EXPECT_TRUE(s.ok());

  CuckooChainMetadata metadata;
  s = sb_chain_->Info(*ctx_, key_, &metadata);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(metadata.n_filters, 1);
  EXPECT_EQ(metadata.size, 0);

  // 2. Fill the first filter to trigger expansion.
  // Due to the probabilistic nature, we add items until we observe expansion.
  int added = 0;
  std::vector<std::string> items;
  for (int i = 0; i < 100; ++i) {
    std::string item = "item" + std::to_string(i);
    items.push_back(item);
    s = sb_chain_->AddNX(*ctx_, key_, item, &added);
    EXPECT_TRUE(s.ok());

    s = sb_chain_->Info(*ctx_, key_, &metadata);
    EXPECT_TRUE(s.ok());
    if (metadata.n_filters > 1) {
      break; // Expansion happened
    }
  }

  // 3. Verify expansion details
  s = sb_chain_->Info(*ctx_, key_, &metadata);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(metadata.n_filters, 2);
  EXPECT_EQ(metadata.capacity, initial_capacity * expansion);
  EXPECT_EQ(metadata.size, items.size());

  // 4. Verify operations on the chained filter
  int exists = 0;
  // Check an item that should be in the first filter
  s = sb_chain_->Exists(*ctx_, key_, "item0", &exists);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(exists, 1);

  // Check an item that should be in the second filter
  s = sb_chain_->Exists(*ctx_, key_, items.back(), &exists);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(exists, 1);

  // Delete an item from the first filter
  int deleted = 0;
  s = sb_chain_->Delete(*ctx_, key_, "item0", &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(deleted, 1);
  s = sb_chain_->Exists(*ctx_, key_, "item0", &exists);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(exists, 0);

  // Delete an item from the second filter
  s = sb_chain_->Delete(*ctx_, key_, items.back(), &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(deleted, 1);
  s = sb_chain_->Exists(*ctx_, key_, items.back(), &exists);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(exists, 0);

  s = sb_chain_->Info(*ctx_, key_, &metadata);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(metadata.size, items.size() - 2);
}

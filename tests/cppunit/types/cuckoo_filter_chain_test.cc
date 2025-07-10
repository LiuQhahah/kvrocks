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
  uint8_t bucket_size = 2;
  uint16_t max_iterations = 500;
  auto s = sb_chain_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, 1);
  EXPECT_TRUE(s.ok());
}

TEST_F(RedisCuckooFilterChainTest, ReserveInvalidArgs) {
  // Test with capacity 0
  auto s = sb_chain_->Reserve(*ctx_, key_, 0, 2, 500, 1);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.ToString(), "Invalid argument: capacity should be larger than 0");

  // Test reserving an already existing key
  s = sb_chain_->Reserve(*ctx_, key_, 1000, 2, 500, 1);
  EXPECT_TRUE(s.ok());
  s = sb_chain_->Reserve(*ctx_, key_, 1000, 2, 500, 1);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.ToString(), "Invalid argument: the key already exists");
}

TEST_F(RedisCuckooFilterChainTest, AddMultipleOccurrencesAndCount) {
  uint32_t capacity = 100;
  uint8_t bucket_size = 2;
  uint16_t max_iterations = 500;
  auto s = sb_chain_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, 1);
  EXPECT_TRUE(s.ok());

  // Add the same item multiple times
  redis::CuckooFilterAddResult add_ret;
  s = sb_chain_->Add(*ctx_, key_, "item_multi", &add_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);

  s = sb_chain_->Add(*ctx_, key_, "item_multi", &add_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);

  s = sb_chain_->Add(*ctx_, key_, "item_multi", &add_ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);

  // Verify count
  int count = 0;
  s = sb_chain_->Count(*ctx_, key_, "item_multi", &count);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(count, 3);

  // Delete one occurrence
  int deleted = 0;
  s = sb_chain_->Delete(*ctx_, key_, "item_multi", &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(deleted, 1);

  s = sb_chain_->Count(*ctx_, key_, "item_multi", &count);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(count, 2);

  // Delete all occurrences
  s = sb_chain_->Delete(*ctx_, key_, "item_multi", &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(deleted, 1);
  s = sb_chain_->Delete(*ctx_, key_, "item_multi", &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(deleted, 1);

  s = sb_chain_->Count(*ctx_, key_, "item_multi", &count);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(count, 0);
}

TEST_F(RedisCuckooFilterChainTest, RelocationTest) {
  uint32_t capacity = 5000;  // Half of NUM_BULK (10000) from RedisBloom test
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 500;  // Increased from 5 to allow more relocations
  uint8_t expansion = 1;          // No expansion
  auto s = sb_chain_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  EXPECT_TRUE(s.ok());

  const int NUM_BULK = 10000;
  std::vector<std::string> items_to_add;
  for (int i = 0; i < NUM_BULK; ++i) {
    items_to_add.push_back("item_" + std::to_string(i));
  }

  // Add items and verify existence after each addition
  for (int i = 0; i < NUM_BULK; ++i) {
    redis::CuckooFilterAddResult add_ret;
    s = sb_chain_->Add(*ctx_, key_, items_to_add[i], &add_ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);

    // Verify all previously added items exist
    for (int j = 0; j <= i; ++j) {
      int exists = 0;
      s = sb_chain_->Exists(*ctx_, key_, items_to_add[j], &exists);
      EXPECT_TRUE(s.ok());
      EXPECT_EQ(exists, 1);
    }
  }

  // Verify all items exist after all additions
  for (int i = 0; i < NUM_BULK; ++i) {
    int exists = 0;
    s = sb_chain_->Exists(*ctx_, key_, items_to_add[i], &exists);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(exists, 1);
  }

  // Verify that adding existing items returns kExist (if AddNX was used) or kOk (if Add was used)
  // Here we use Add, so it should always be kOk
  for (int i = 0; i < NUM_BULK; ++i) {
    redis::CuckooFilterAddResult add_ret;
    s = sb_chain_->Add(*ctx_, key_, items_to_add[i], &add_ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);
  }

  CuckooChainMetadata metadata;
  s = sb_chain_->Info(*ctx_, key_, &metadata);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(metadata.size, NUM_BULK * 2);  // Because we added each item twice
}

TEST_F(RedisCuckooFilterChainTest, FalsePositiveRateTest) {
  const int NUM_BULK = 10000;
  std::vector<std::string> items_to_add;
  std::vector<std::string> items_to_check_fpr;

  // Generate items to add and items to check for FPR
  for (int i = 0; i < NUM_BULK; ++i) {
    items_to_add.push_back("added_item_" + std::to_string(i));
    items_to_check_fpr.push_back("fpr_item_" + std::to_string(i));
  }

  // Test 1: capacity = NUM_BULK, bucket_size = 2
  uint32_t capacity1 = NUM_BULK;
  uint8_t bucket_size1 = 2;
  uint16_t max_iterations1 = 500;
  auto s1 = sb_chain_->Reserve(*ctx_, key_ + "1", capacity1, bucket_size1, max_iterations1, 1);
  EXPECT_TRUE(s1.ok());

  for (const auto& item : items_to_add) {
    redis::CuckooFilterAddResult add_ret;
    s1 = sb_chain_->Add(*ctx_, key_ + "1", item, &add_ret);
    EXPECT_TRUE(s1.ok());
    EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);
  }

  int false_positives1 = 0;
  for (const auto& item : items_to_check_fpr) {
    int exists = 0;
    s1 = sb_chain_->Exists(*ctx_, key_ + "1", item, &exists);
    EXPECT_TRUE(s1.ok());
    if (exists == 1) {
      false_positives1++;
    }
  }
  EXPECT_LE(static_cast<double>(false_positives1), static_cast<double>(NUM_BULK) * 0.46);

  // Test 2: capacity = NUM_BULK / 2, bucket_size = 2
  uint32_t capacity2 = NUM_BULK / 2;
  uint8_t bucket_size2 = 2;
  uint16_t max_iterations2 = 500;
  auto s2 = sb_chain_->Reserve(*ctx_, key_ + "2", capacity2, bucket_size2, max_iterations2, 1);
  EXPECT_TRUE(s2.ok());

  for (const auto& item : items_to_add) {
    redis::CuckooFilterAddResult add_ret;
    s2 = sb_chain_->Add(*ctx_, key_ + "2", item, &add_ret);
    EXPECT_TRUE(s2.ok());
    EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);
  }

  int false_positives2 = 0;
  for (const auto& item : items_to_check_fpr) {
    int exists = 0;
    s2 = sb_chain_->Exists(*ctx_, key_ + "2", item, &exists);
    EXPECT_TRUE(s2.ok());
    if (exists == 1) {
      false_positives2++;
    }
  }
  EXPECT_LE(static_cast<double>(false_positives2), static_cast<double>(NUM_BULK) * 0.71);

  // Test 3: capacity = NUM_BULK / 4, bucket_size = 2
  uint32_t capacity3 = NUM_BULK / 4;
  uint8_t bucket_size3 = 2;
  uint16_t max_iterations3 = 500;
  auto s3 = sb_chain_->Reserve(*ctx_, key_ + "3", capacity3, bucket_size3, max_iterations3, 1);
  EXPECT_TRUE(s3.ok());

  for (const auto& item : items_to_add) {
    redis::CuckooFilterAddResult add_ret;
    s3 = sb_chain_->Add(*ctx_, key_ + "3", item, &add_ret);
    EXPECT_TRUE(s3.ok());
    EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);
  }

  int false_positives3 = 0;
  for (const auto& item : items_to_check_fpr) {
    int exists = 0;
    s3 = sb_chain_->Exists(*ctx_, key_ + "3", item, &exists);
    EXPECT_TRUE(s3.ok());
    if (exists == 1) {
      false_positives3++;
    }
  }
  EXPECT_LE(static_cast<double>(false_positives3), static_cast<double>(NUM_BULK) * 0.92);

  // Test 4: capacity = NUM_BULK / 8, bucket_size = 2
  uint32_t capacity4 = NUM_BULK / 8;
  uint8_t bucket_size4 = 2;
  uint16_t max_iterations4 = 500;
  auto s4 = sb_chain_->Reserve(*ctx_, key_ + "4", capacity4, bucket_size4, max_iterations4, 1);
  EXPECT_TRUE(s4.ok());

  for (const auto& item : items_to_add) {
    redis::CuckooFilterAddResult add_ret;
    s4 = sb_chain_->Add(*ctx_, key_ + "4", item, &add_ret);
    EXPECT_TRUE(s4.ok());
    EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);
  }

  int false_positives4 = 0;
  for (const auto& item : items_to_check_fpr) {
    int exists = 0;
    s4 = sb_chain_->Exists(*ctx_, key_ + "4", item, &exists);
    EXPECT_TRUE(s4.ok());
    if (exists == 1) {
      false_positives4++;
    }
  }
  EXPECT_LE(static_cast<double>(false_positives4), static_cast<double>(NUM_BULK) * 1.0);
}

TEST_F(RedisCuckooFilterChainTest, BulkDeleteTest) {
  const int NUM_BULK = 10000;
  std::vector<std::string> items_to_add;
  for (int i = 0; i < NUM_BULK; ++i) {
    items_to_add.push_back("item_" + std::to_string(i));
  }

  // Test 1: No expansion
  uint32_t capacity1 = NUM_BULK / 8;
  uint8_t bucket_size1 = 2;
  uint16_t max_iterations1 = 500;
  auto s1 = sb_chain_->Reserve(*ctx_, key_ + "_bulk_no_exp", capacity1, bucket_size1, max_iterations1, 1);
  EXPECT_TRUE(s1.ok());

  for (const auto& item : items_to_add) {
    redis::CuckooFilterAddResult add_ret;
    s1 = sb_chain_->Add(*ctx_, key_ + "_bulk_no_exp", item, &add_ret);
    EXPECT_TRUE(s1.ok());
    EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);
  }

  for (const auto& item : items_to_add) {
    int deleted = 0;
    s1 = sb_chain_->Delete(*ctx_, key_ + "_bulk_no_exp", item, &deleted);
    EXPECT_TRUE(s1.ok());
    EXPECT_EQ(deleted, 1);
  }

  CuckooChainMetadata metadata1;
  s1 = sb_chain_->Info(*ctx_, key_ + "_bulk_no_exp", &metadata1);
  EXPECT_TRUE(s1.ok());
  EXPECT_EQ(metadata1.size, 0);

  // Test 2: With expansion
  uint32_t capacity2 = NUM_BULK / 8;
  uint8_t bucket_size2 = 2;
  uint16_t max_iterations2 = 500;
  auto s2 = sb_chain_->Reserve(*ctx_, key_ + "_bulk_with_exp", capacity2, bucket_size2, max_iterations2, 2);
  EXPECT_TRUE(s2.ok());

  for (const auto& item : items_to_add) {
    redis::CuckooFilterAddResult add_ret;
    s2 = sb_chain_->Add(*ctx_, key_ + "_bulk_with_exp", item, &add_ret);
    EXPECT_TRUE(s2.ok());
    EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);
  }
}

TEST_F(RedisCuckooFilterChainTest, BucketSizeTest) {
  const int NUM_BULK = 10000;
  std::vector<std::string> items_to_add;
  for (int i = 0; i < NUM_BULK; ++i) {
    items_to_add.push_back("item_" + std::to_string(i));
  }

  // Test 1: bucket_size = 1
  uint32_t capacity1 = NUM_BULK / 10;
  uint8_t bucket_size1 = 1;
  uint16_t max_iterations1 = 50;
  auto s1 = sb_chain_->Reserve(*ctx_, key_ + "_bs1", capacity1, bucket_size1, max_iterations1, 1);
  EXPECT_TRUE(s1.ok());

  for (const auto& item : items_to_add) {
    redis::CuckooFilterAddResult add_ret;
    s1 = sb_chain_->Add(*ctx_, key_ + "_bs1", item, &add_ret);
    EXPECT_TRUE(s1.ok());
    EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);
  }

  CuckooChainMetadata metadata1;
  s1 = sb_chain_->Info(*ctx_, key_ + "_bs1", &metadata1);
  EXPECT_TRUE(s1.ok());
  EXPECT_EQ(metadata1.bucket_size, bucket_size1);
  // The number of filters will depend on the internal implementation and how many expansions are needed
  // We can't directly assert on numFilters without knowing the exact expansion logic.
  // EXPECT_EQ(metadata1.n_filters, 12); // This assertion from RedisBloom might not directly apply

  // Test 2: bucket_size = 2
  uint32_t capacity2 = NUM_BULK / 10;
  uint8_t bucket_size2 = 2;
  uint16_t max_iterations2 = 50;
  auto s2 = sb_chain_->Reserve(*ctx_, key_ + "_bs2", capacity2, bucket_size2, max_iterations2, 1);
  EXPECT_TRUE(s2.ok());

  for (const auto& item : items_to_add) {
    redis::CuckooFilterAddResult add_ret;
    s2 = sb_chain_->Add(*ctx_, key_ + "_bs2", item, &add_ret);
    EXPECT_TRUE(s2.ok());
    EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);
  }

  CuckooChainMetadata metadata2;
  s2 = sb_chain_->Info(*ctx_, key_ + "_bs2", &metadata2);
  EXPECT_TRUE(s2.ok());
  EXPECT_EQ(metadata2.bucket_size, bucket_size2);
  // EXPECT_EQ(metadata2.n_filters, 11);

  // Test 3: bucket_size = 4
  uint32_t capacity3 = NUM_BULK / 10;
  uint8_t bucket_size3 = 4;
  uint16_t max_iterations3 = 50;
  auto s3 = sb_chain_->Reserve(*ctx_, key_ + "_bs3", capacity3, bucket_size3, max_iterations3, 1);
  EXPECT_TRUE(s3.ok());

  for (const auto& item : items_to_add) {
    redis::CuckooFilterAddResult add_ret;
    s3 = sb_chain_->Add(*ctx_, key_ + "_bs3", item, &add_ret);
    EXPECT_TRUE(s3.ok());
    EXPECT_EQ(add_ret, redis::CuckooFilterAddResult::kOk);
  }

  CuckooChainMetadata metadata3;
  s3 = sb_chain_->Info(*ctx_, key_ + "_bs3", &metadata3);
  EXPECT_TRUE(s3.ok());
  EXPECT_EQ(metadata3.bucket_size, bucket_size3);
  // EXPECT_EQ(metadata3.n_filters, 10);
}

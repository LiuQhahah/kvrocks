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

#include "cuckoo_filter.h"

#include <algorithm>
#include <chrono>

namespace redis {

// Use XXH64 for hashing
uint64_t CuckooFilter::Hash(const std::string &item) const {
  return XXH64(item.data(), item.size(), 0); // Seed 0
}

// Generate a fingerprint from a hash
uint8_t CuckooFilter::GenerateFingerprint(uint64_t hash) const {
  // Use lower 8 bits for fingerprint, ensure it's not 0
  uint8_t fp = hash & 0xFF;
  return fp == 0 ? 1 : fp;
}

// Get the primary bucket index
uint32_t CuckooFilter::GetBucketIndex(uint64_t hash) const {
  return hash % num_buckets_;
}

// Get the alternate bucket index
uint32_t CuckooFilter::GetAltBucketIndex(uint32_t bucket_index, uint8_t fingerprint) const {
  // This is the core Cuckoo hashing formula: i2 = i1 XOR hash(fingerprint)
  return (bucket_index ^ Hash(std::string(1, fingerprint))) % num_buckets_;
}

// Insert a fingerprint into a bucket
bool CuckooFilter::InsertFingerprint(uint8_t fingerprint, uint32_t bucket_index) {
  uint32_t offset = bucket_index * bucket_size_;
  for (uint8_t i = 0; i < bucket_size_; ++i) {
    if (data_[offset + i] == 0) {
      data_[offset + i] = fingerprint;
      return true;
    }
  }
  return false;
}

// Delete a fingerprint from a bucket
bool CuckooFilter::DeleteFingerprint(uint8_t fingerprint, uint32_t bucket_index) {
  uint32_t offset = bucket_index * bucket_size_;
  for (uint8_t i = 0; i < bucket_size_; ++i) {
    if (data_[offset + i] == fingerprint) {
      data_[offset + i] = 0;
      return true;
    }
  }
  return false;
}

// Find a fingerprint in a bucket
bool CuckooFilter::FindFingerprint(uint8_t fingerprint, uint32_t bucket_index) const {
  uint32_t offset = bucket_index * bucket_size_;
  for (uint8_t i = 0; i < bucket_size_; ++i) {
    if (data_[offset + i] == fingerprint) {
      return true;
    }
  }
  return false;
}

bool CuckooFilter::Add(const std::string &item) {
  uint64_t item_hash = Hash(item);
  uint8_t fingerprint = GenerateFingerprint(item_hash);
  uint32_t bucket_index1 = GetBucketIndex(item_hash);
  uint32_t bucket_index2 = GetAltBucketIndex(bucket_index1, fingerprint);

  // Try to insert into either bucket1 or bucket2
  if (InsertFingerprint(fingerprint, bucket_index1)) {
    return true;
  }
  if (InsertFingerprint(fingerprint, bucket_index2)) {
    return true;
  }

  // Both buckets are full, start kicking
  uint32_t current_bucket_index = bucket_index1; // Start kicking from bucket1
  uint8_t current_fingerprint = fingerprint;

  // Initialize random number generator with a time-based seed
  rng_.seed(std::chrono::high_resolution_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<int> dist(0, bucket_size_ - 1);

  for (uint16_t i = 0; i < max_iterations_; ++i) {
    // Randomly choose an entry to kick out from the current bucket
    uint32_t kick_pos = dist(rng_);
    uint32_t offset = current_bucket_index * bucket_size_;

    // Swap the fingerprint to be inserted with the kicked one
    char temp = data_[offset + kick_pos];
    data_[offset + kick_pos] = current_fingerprint;
    current_fingerprint = temp;

    // Calculate the alternate bucket for the kicked fingerprint
    current_bucket_index = GetAltBucketIndex(current_bucket_index, current_fingerprint);

    // Try to insert the kicked fingerprint into its alternate bucket
    if (InsertFingerprint(current_fingerprint, current_bucket_index)) {
      return true; // Successfully inserted after kicking
    }
  }

  return false; // Failed to insert after max_iterations
}

bool CuckooFilter::Contains(const std::string &item) const {
  uint64_t item_hash = Hash(item);
  uint8_t fingerprint = GenerateFingerprint(item_hash);
  uint32_t bucket_index1 = GetBucketIndex(item_hash);
  uint32_t bucket_index2 = GetAltBucketIndex(bucket_index1, fingerprint);

  // Check both possible buckets
  if (FindFingerprint(fingerprint, bucket_index1)) {
    return true;
  }
  if (FindFingerprint(fingerprint, bucket_index2)) {
    return true;
  }

  return false;
}

bool CuckooFilter::Delete(const std::string &item) {
  uint64_t item_hash = Hash(item);
  uint8_t fingerprint = GenerateFingerprint(item_hash);
  uint32_t bucket_index1 = GetBucketIndex(item_hash);
  uint32_t bucket_index2 = GetAltBucketIndex(bucket_index1, fingerprint);

  if (DeleteFingerprint(fingerprint, bucket_index1)) {
    return true;
  }
  if (DeleteFingerprint(fingerprint, bucket_index2)) {
    return true;
  }

  return false;
}

size_t CuckooFilter::Count(const std::string &item) const {
  uint64_t item_hash = Hash(item);
  uint8_t fingerprint = GenerateFingerprint(item_hash);
  uint32_t bucket_index1 = GetBucketIndex(item_hash);
  uint32_t bucket_index2 = GetAltBucketIndex(bucket_index1, fingerprint);

  size_t count = 0;
  uint32_t offset1 = bucket_index1 * bucket_size_;
  for (uint8_t i = 0; i < bucket_size_; ++i) {
    if (data_[offset1 + i] == fingerprint) {
      count++;
    }
  }

  uint32_t offset2 = bucket_index2 * bucket_size_;
  for (uint8_t i = 0; i < bucket_size_; ++i) {
    if (data_[offset2 + i] == fingerprint) {
      count++;
    }
  }

  return count;
}

}  // namespace redis
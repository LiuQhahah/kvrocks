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

#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <random> // For std::mt19937 and std::uniform_int_distribution

#include "common/encoding.h"
#include "common/string_util.h"
#include "xxhash.h" // For XXH64

namespace redis {

// Cuckoo filter implementation from the paper:
// "Cuckoo Filter: Practically Better Than Bloom" by Fan et al.
class CuckooFilter {
 public:
  // Calculate the optimal number of bytes for the filter data
  static size_t OptimalTableSize(uint32_t capacity, uint8_t bucket_size) {
    // A load factor of 95.5% is chosen for the cuckoo filter
    uint32_t num_buckets = util::NextPowerOf2(capacity / bucket_size / 0.955);
    if (num_buckets == 0) num_buckets = 1;
    // For now, we use a fixed fingerprint size of 1 byte.
    // This can be made configurable later.
    const uint8_t fingerprint_size = 1;
    return num_buckets * bucket_size * fingerprint_size;
  }

  // Creates an empty Cuckoo Filter data string
  static std::string Create(size_t table_size) { return std::string(table_size, 0); }

  // Constructor to initialize CuckooFilter with existing data
  CuckooFilter(std::string &data, uint8_t bucket_size, uint16_t max_iterations)
      : data_(data), bucket_size_(bucket_size), max_iterations_(max_iterations),
        num_buckets_(data.size() / bucket_size) {}

  // Add an item to the filter
  bool Add(const std::string &item);

  // Check if an item exists in the filter
  bool Contains(const std::string &item) const;

 private:
  // Hashing functions
  uint64_t Hash(const std::string &item) const;
  uint8_t GenerateFingerprint(uint64_t hash) const;
  uint32_t GetBucketIndex(uint64_t hash) const;
  uint32_t GetAltBucketIndex(uint32_t bucket_index, uint8_t fingerprint) const;

  // Helper functions for bucket operations
  bool InsertFingerprint(uint8_t fingerprint, uint32_t bucket_index);
  bool DeleteFingerprint(uint8_t fingerprint, uint32_t bucket_index);
  bool FindFingerprint(uint8_t fingerprint, uint32_t bucket_index) const;

  std::string &data_; // Reference to the filter data (byte array)
  uint8_t bucket_size_;
  uint16_t max_iterations_;
  uint32_t num_buckets_;

  mutable std::mt19937 rng_; // Random number generator for kicks
};

}  // namespace redis
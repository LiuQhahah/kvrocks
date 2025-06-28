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

#include "command_parser.h"
#include "commander.h"
#include "types/redis_cuckoo_filter_chain.h"
#include "server/server.h"
namespace {

constexpr const char *errBadCapacity = "Bad capacity";
constexpr const char *errBadBucketSize = "Bad bucket size";
constexpr const char *errBadMaxIterations = "Bad max iterations";
constexpr const char *errBadExpansion = "Bad expansion";
constexpr const char *errInvalidBucketSize = "Bucket size should be greater than 0";
constexpr const char *errInvalidMaxIterations = "Max iterations should be greater than 0";
constexpr const char *errInvalidExpansion = "Expansion should be greater than 0";
constexpr const char *errInvalidSyntax = "Invalid syntax";

}  // namespace
namespace redis {

class CoomandCFReserve : public Commander {
 public:
  // CF.RESERVE cf 1000 or CF.RESERVE cf_params 1000 BUCKETSIZE 8 MAXITERATIONS 20 EXPANSION 2
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() != 3 || args.size() != 6) {
      return {Status::RedisParseErr, "Wrong number of arguments for 'cf.reserve' command"};
    }
    if (args.size() == 3) {
      //  parse capacity with default bucket size 8, max iterations 20, expansion 2
      auto parse_capacity = ParseInt<uint32_t>(args[2], 10);
      if (!parse_capacity) {
        return {Status::RedisParseErr, errBadCapacity};
      }
      capacity_ = *parse_capacity;
      if (capacity_ <= 0) {
        return {Status::RedisParseErr, errInvalidExpansion};
      }
      bucket_size_ = 8;
      max_iterations_ = 20;
      expansion_ = 2;
    } else if (args.size() == 6) {
      // parse capacity with custom parameters
      auto parse_capacity = ParseInt<uint32_t>(args[2], 10);
      if (!parse_capacity) {
        return {Status::RedisParseErr, errBadCapacity};
      }
      capacity_ = *parse_capacity;
      if (capacity_ <= 0) {
        return {Status::RedisParseErr, "Capacity should be larger than 0"};
      }
      CommandParser parser(args, 3);
      while (parser.Good()) {
        if (parser.EatEqICase("bucketsize")) {
          auto parse_bucket_size = parser.TakeInt<uint16_t>();
          if (!parse_bucket_size.IsOK()) {
            return {Status::RedisParseErr, "Bad bucket size"};
          }
          bucket_size_ = parse_bucket_size.GetValue();
          if (bucket_size_ < 1) {
            return {Status::RedisParseErr, "Bucket size should be greater than 0"};
          }

        } else if (parser.EatEqICase("maxiterations")) {
          auto parse_max_iterations = parser.TakeInt<uint16_t>();
          if (!parse_max_iterations.IsOK()) {
            return {Status::RedisParseErr, "Bad max iterations"};
          }
          max_iterations_ = parse_max_iterations.GetValue();
          if (max_iterations_ < 1) {
            return {Status::RedisParseErr, "Max iterations should be greater than 0"};
          }

        } else if (parser.EatEqICase("expansion")) {
          auto parse_expansion = parser.TakeInt<uint16_t>();
          if (!parse_expansion.IsOK()) {
            return {Status::RedisParseErr, "Bad expansion"};
          }
          expansion_ = parse_expansion.GetValue();
          if (expansion_ < 1) {
            return {Status::RedisParseErr, "Expansion should be greater than 0"};
          }
        } else {
          return {Status::RedisParseErr, "Invalid syntax"};
        }
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooFilterChain cuckoo_db(srv->storage, conn->GetNamespace());
    cuckoo_db.Reserve(ctx, args_[1], capacity_, bucket_size_, max_iterations_, expansion_);

    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  uint32_t capacity_ = 1000;      // capacity of the cuckoo filter
  uint16_t bucket_size_ = 8;      // size of each bucket
  uint16_t max_iterations_ = 20;  // max iterations for cuckoo filter operations
  uint16_t expansion_ = 2;        // expansion factor for cuckoo filter
};

REDIS_REGISTER_COMMANDS(CuckooFilter, MakeCmdAttr<CoomandCFReserve>("cf.reserver", 3, "write", 1, 1, 1))
}  // namespace redis
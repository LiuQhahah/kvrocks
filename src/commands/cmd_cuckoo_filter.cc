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
#include "error_constants.h"
#include "server/server.h"
#include "types/redis_cuckoo_chain.h"

namespace redis {

class CommandCFReserve : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_capacity = ParseInt<uint32_t>(args[2], 10);
    if (!parse_capacity) {
      return {Status::RedisParseErr, "invalid capacity"};
    }
    capacity_ = *parse_capacity;
    if (capacity_ <= 0) {
      return {Status::RedisParseErr, "capacity should be larger than 0"};
    }

    CommandParser parser(args, 3);
    while (parser.Good()) {
      if (parser.EatEqICase("BUCKETSIZE")) {
        auto parse_bucket_size = parser.TakeInt<uint8_t>();
        if (!parse_bucket_size.IsOK()) return parse_bucket_size.ToStatus();
        bucket_size_ = parse_bucket_size.GetValue();
      } else if (parser.EatEqICase("MAXITERATIONS")) {
        auto parse_max_iterations = parser.TakeInt<uint16_t>();
        if (!parse_max_iterations.IsOK()) return parse_max_iterations.ToStatus();
        max_iterations_ = parse_max_iterations.GetValue();
      } else if (parser.EatEqICase("EXPANSION")) {
        auto parse_expansion = parser.TakeInt<uint8_t>();
        if (!parse_expansion.IsOK()) return parse_expansion.ToStatus();
        expansion_ = parse_expansion.GetValue();
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooChain cuckoo_db(srv->storage, conn->GetNamespace());
    auto s = cuckoo_db.Reserve(ctx, args_[1], capacity_, bucket_size_, max_iterations_, expansion_);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  uint32_t capacity_ = kCFDefaultCapacity;
  uint8_t bucket_size_ = kCFDefaultBucketSize;
  uint16_t max_iterations_ = kCFDefaultMaxIterations;
  uint8_t expansion_ = kCFDefaultExpansion;
};

class CommandCFAdd : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooChain cuckoo_db(srv->storage, conn->GetNamespace());
    CuckooFilterAddResult ret = CuckooFilterAddResult::kOk;

    auto s = cuckoo_db.Add(ctx, args_[1], args_[2], &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    switch (ret) {
      case CuckooFilterAddResult::kOk:
        *output = redis::Integer(1);
        break;
      case CuckooFilterAddResult::kExist:
        *output = redis::Integer(0);
        break;
      case CuckooFilterAddResult::kFull:
        *output = redis::Error({Status::NotOK, "Cuckoo filter is full"});
        break;
    }
    return Status::OK();
  }
};

class CommandCFAddNX : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooChain cuckoo_db(srv->storage, conn->GetNamespace());
    int added = 0;
    auto s = cuckoo_db.AddNX(ctx, args_[1], args_[2], &added);
    if (s.IsAborted()) return {Status::RedisExecErr, s.ToString()};
    if (!s.ok() && !s.IsNotFound()) return {Status::RedisExecErr, s.ToString()};
    if (s.IsNotFound()) {
      *output = redis::Error({Status::NotFound, errKeyNotFound});
      return Status::OK();
    }

    *output = redis::Integer(added);
    return Status::OK();
  }
};

class CommandCFExists : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooChain cuckoo_db(srv->storage, conn->GetNamespace());
    int exists = 0;
    auto s = cuckoo_db.Exists(ctx, args_[1], args_[2], &exists);
    if (!s.ok() && !s.IsNotFound()) return {Status::RedisExecErr, s.ToString()};
    if (s.IsNotFound()) {
      *output = redis::Integer(0);
      return Status::OK();
    }

    *output = redis::Integer(exists);
    return Status::OK();
  }
};

class CommandCFMExists : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    items_.reserve(args_.size() - 2);
    for (size_t i = 2; i < args_.size(); ++i) {
      items_.emplace_back(args_[i]);
    }
    return Commander::Parse(args);
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooChain cuckoo_db(srv->storage, conn->GetNamespace());
    std::vector<bool> exists(items_.size(), false);

    auto s = cuckoo_db.MExists(ctx, args_[1], items_, &exists);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiLen(items_.size());
    for (size_t i = 0; i < items_.size(); ++i) {
      *output += Integer(exists[i] ? 1 : 0);
    }

    return Status::OK();
  }

 private:
  std::vector<std::string> items_;
};

class CommandCFInsert : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2); // Start parsing from the third argument (after key)
    while (parser.Good()) {
      if (parser.EatEqICase("CAPACITY")) {
        auto parse_capacity = parser.TakeInt<unsigned int>();
        if (!parse_capacity.IsOK()) return parse_capacity.ToStatus();
        capacity_ = *parse_capacity;
        if (capacity_ <= 0) {
          return {Status::RedisParseErr, "capacity should be larger than 0"};
        }
      } else if (parser.EatEqICase("NOCREATE")) {
        no_create_ = true;
      } else if (parser.EatEqICase("ITEMS")) {
        while (parser.Good()) {
          items_.emplace_back(GET_OR_RET(parser.TakeStr()));
        }
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    if (items_.empty()) {
      return {Status::RedisParseErr, "at least one item must be specified"};
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooChain cuckoo_db(srv->storage, conn->GetNamespace());
    std::vector<int> results(items_.size());
    auto s = cuckoo_db.Insert(ctx, args_[1], items_, capacity_, no_create_, &results);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiLen(results.size());
    for (int res : results) {
      *output += redis::Integer(res);
    }
    return Status::OK();
  }

 private:
  uint32_t capacity_ = kCFDefaultCapacity;
  bool no_create_ = false;
  std::vector<std::string> items_;
};

class CommandCFInsertNX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2); // Start parsing from the third argument (after key)
    while (parser.Good()) {
      if (parser.EatEqICase("CAPACITY")) {
        auto parse_capacity = parser.TakeInt<unsigned int>();
        if (!parse_capacity.IsOK()) return parse_capacity.ToStatus();
        capacity_ = *parse_capacity;
        if (capacity_ <= 0) {
          return {Status::RedisParseErr, "capacity should be larger than 0"};
        }
      } else if (parser.EatEqICase("NOCREATE")) {
        no_create_ = true;
      } else if (parser.EatEqICase("ITEMS")) {
        while (parser.Good()) {
          items_.emplace_back(GET_OR_RET(parser.TakeStr()));
        }
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    if (items_.empty()) {
      return {Status::RedisParseErr, "at least one item must be specified"};
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooChain cuckoo_db(srv->storage, conn->GetNamespace());
    std::vector<int> results(items_.size());
    auto s = cuckoo_db.InsertNX(ctx, args_[1], items_, capacity_, no_create_, &results);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiLen(results.size());
    for (int res : results) {
      *output += redis::Integer(res);
    }
    return Status::OK();
  }

 private:
  uint32_t capacity_ = kCFDefaultCapacity;
  bool no_create_ = false;
  std::vector<std::string> items_;
};

class CommandCFDel : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooChain cuckoo_db(srv->storage, conn->GetNamespace());
    int deleted = 0;
    auto s = cuckoo_db.Delete(ctx, args_[1], args_[2], &deleted);
    if (!s.ok() && !s.IsNotFound()) return {Status::RedisExecErr, s.ToString()};
    if (s.IsNotFound()) {
      *output = redis::Integer(0);
      return Status::OK();
    }

    *output = redis::Integer(deleted);
    return Status::OK();
  }
};

class CommandCFCount : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooChain cuckoo_db(srv->storage, conn->GetNamespace());
    int count = 0;
    auto s = cuckoo_db.Count(ctx, args_[1], args_[2], &count);
    if (!s.ok() && !s.IsNotFound()) return {Status::RedisExecErr, s.ToString()};
    if (s.IsNotFound()) {
      *output = redis::Integer(0);
      return Status::OK();
    }

    *output = redis::Integer(count);
    return Status::OK();
  }
};

class CommandCFInfo : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::CuckooChain cuckoo_db(srv->storage, conn->GetNamespace());
    CuckooChainMetadata metadata;
    auto s = cuckoo_db.Info(ctx, args_[1], &metadata);
    if (!s.ok() && !s.IsNotFound()) return {Status::RedisExecErr, s.ToString()};
    if (s.IsNotFound()) {
      *output = redis::Error({Status::NotFound, errKeyNotFound});
      return Status::OK();
    }

    std::vector<std::string> infos;
    infos.push_back("Size");
    infos.push_back(std::to_string(metadata.size));
    infos.push_back("Number of buckets");
    infos.push_back(std::to_string(metadata.table_size));
    infos.push_back("Number of filters");
    infos.push_back(std::to_string(metadata.n_filters));
    infos.push_back("Number of items inserted");
    infos.push_back(std::to_string(metadata.size));
    infos.push_back("Number of items deleted");
    infos.push_back(std::to_string(metadata.num_deleted_items));
    infos.push_back("Bucket size");
    infos.push_back(std::to_string(metadata.bucket_size));
    infos.push_back("Expansion rate");
    infos.push_back(std::to_string(metadata.expansion));
    infos.push_back("Max iterations");
    infos.push_back(std::to_string(metadata.max_iterations));

    *output = redis::ArrayOfBulkStrings(infos);
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(CuckooFilter, MakeCmdAttr<CommandCFReserve>("cf.reserve", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandCFAdd>("cf.add", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandCFAddNX>("cf.addnx", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandCFExists>("cf.exists", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandCFDel>("cf.del", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandCFCount>("cf.count", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandCFMExists>("cf.mexists", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandCFInsert>("cf.insert", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandCFInsertNX>("cf.insertnx", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandCFInfo>("cf.info", 2, "read-only", 1, 1, 1), )

}  // namespace redis
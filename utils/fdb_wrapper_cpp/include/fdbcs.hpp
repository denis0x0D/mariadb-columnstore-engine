/* Copyright (C) 2024 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#pragma once

#include <string>
#include <iostream>
#include <thread>
#include <memory>
#include <vector>
#include <unordered_map>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/lexical_cast.hpp>
#
// https://apple.github.io/foundationdb/api-c.html
// We have to define `FDB_API_VERSION` before include `fdb_c.h` header.
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

namespace FDBCS
{
// TODO: How about uint8_t.
using ByteArray = std::string;

// Represensts a `transaction`.
class Transaction
{
 public:
  Transaction() = delete;
  Transaction(const Transaction&) = delete;
  Transaction(Transaction&&) = delete;
  Transaction& operator=(const Transaction&) = delete;
  Transaction& operator=(Transaction&&) = delete;
  explicit Transaction(FDBTransaction* tnx);
  ~Transaction();

  // Sets a given `key` and given `value`.
  void set(const ByteArray& key, const ByteArray& value) const;
  // Gets a `value` by the given `key`.
  std::pair<bool, ByteArray> get(const ByteArray& key) const;
  // Removes a given `key` from database.
  void remove(const ByteArray& key) const;
  // Removes all keys in the given range, starting from `beginKey` until `endKey`, but not including `endKey`.
  void removeRange(const ByteArray& beginKey, const ByteArray& endKey) const;
  // Commits transaction.
  bool commit() const;

 private:
  FDBTransaction* tnx_{nullptr};
};

// Represents network class.
class FDBNetwork
{
 public:
  FDBNetwork() = default;
  FDBNetwork(const FDBNetwork&) = delete;
  FDBNetwork(FDBNetwork&&) = delete;
  FDBNetwork& operator=(const FDBNetwork&) = delete;
  FDBNetwork& operator=(FDBNetwork&&) = delete;
  ~FDBNetwork();

  bool setUpAndRunNetwork();

 private:
  std::thread netThread;
};

// Represents database class.
class FDBDataBase
{
 public:
  FDBDataBase() = delete;
  FDBDataBase(const FDBDataBase&) = delete;
  FDBDataBase& operator=(FDBDataBase&) = delete;
  FDBDataBase(FDBDataBase&&) = delete;
  FDBDataBase& operator=(FDBDataBase&&) = delete;
  explicit FDBDataBase(FDBDatabase* database);
  ~FDBDataBase();

  std::unique_ptr<Transaction> createTransaction() const;
  bool isDataBaseReady() const;

 private:
  FDBDatabase* database_;
  const uint32_t secondsToWait_ = 3;
};

// Represents a creator class for the `FDBDatabase`.
class DataBaseCreator
{
 public:
  // Creates a `FDBDataBase` from the given `clusterFilePath` (path to the cluster file).
  static std::shared_ptr<FDBDataBase> createDataBase(const std::string clusterFilePath);
};

using Block = std::pair<uint32_t, std::string>;
using Key = std::string;
using Keys = std::vector<Key>;

class KeyGenerator
{
 public:
  virtual ~KeyGenerator()
  {
  }
  virtual Key generateKey() = 0;
  virtual uint32_t getKeySize() = 0;
};

class BoostUIDKeyGenerator : public KeyGenerator
{
 public:
  Key generateKey() override;
  uint32_t getKeySize() override;
};

class BlobHandler
{
 public:
  BlobHandler(std::shared_ptr<KeyGenerator> keyGen, uint32_t blockSizeInBytes = 100000)
   : keyGen_(keyGen), blockSizeInBytes_(blockSizeInBytes)
  {
    // Block size in 100KB shows the best performance.
    assert(keySizeInBytes_);
    assert(blockSizeInBytes_);
    keySizeInBytes_ = keyGen_->getKeySize();
    assert((keySizeInBytes_ + keyBlockIdentifier.size()) <= blockSizeInBytes_);
    numKeysInBlock_ = (blockSizeInBytes_ - keyBlockIdentifier.size()) / keySizeInBytes_;
  }

  bool writeBlob(std::shared_ptr<FDBCS::FDBDataBase> database, const ByteArray& key, const ByteArray& blob);
  std::pair<bool, std::string> readBlob(std::shared_ptr<FDBCS::FDBDataBase> database, ByteArray& key);
  bool removeBlob(std::shared_ptr<FDBCS::FDBDataBase> database, ByteArray& key);

 private:
  void insertData(Block& block, const std::string& blob, const uint32_t offset);
  void insertKey(Block& block, const std::string& value);
  std::pair<bool, Keys> getKeysFromBlock(const Block& block);
  Keys generateKeys(const uint32_t num);
  uint32_t getNextLevelKeysNums(const uint32_t nextLevel, const uint32_t numBlocks, const uint32_t treeLen);
  bool isDataBlock(const Block& block);
  bool commitKeys(std::shared_ptr<FDBCS::FDBDataBase> database, std::unordered_map<Key, Block>& map,
                  const Keys& keys);
  bool commitKey(std::shared_ptr<FDBCS::FDBDataBase> database, const Key& key, const ByteArray& value);
  bool removeKeys(std::shared_ptr<FDBCS::FDBDataBase> database, Keys& keys);
  std::unordered_map<uint32_t, uint32_t> computeNumKeysForEachTreeLevel(int32_t treeLen, uint32_t numBlocks);
  inline float log(uint32_t base, uint32_t value);

  std::shared_ptr<KeyGenerator> keyGen_;
  uint32_t blockSizeInBytes_;
  uint32_t keySizeInBytes_;
  uint32_t numKeysInBlock_;
  // FIXME: Doc says that 10MB is limit, currently taking in account `key` size and `value` size, but 10MB
  // limit returns error on transaction.
  const uint32_t maxTnxSize_{8192000};
  const std::string keyBlockIdentifier{"K"};
};

bool setAPIVersion();
}  // namespace FDBCS

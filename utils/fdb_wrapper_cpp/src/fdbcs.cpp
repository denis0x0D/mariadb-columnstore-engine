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

#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/lexical_cast.hpp>
#include "fdbcs.hpp"

namespace FDBCS
{
Transaction::Transaction(FDBTransaction* tnx) : tnx_(tnx)
{
}

Transaction::~Transaction()
{
  if (tnx_)
  {
    fdb_transaction_destroy(tnx_);
    tnx_ = nullptr;
  }
}

void Transaction::set(const ByteArray& key, const ByteArray& value) const
{
  if (tnx_)
  {
    fdb_transaction_set(tnx_, (uint8_t*)key.c_str(), key.length(), (uint8_t*)value.c_str(), value.length());
  }
}

std::pair<bool, ByteArray> Transaction::get(const ByteArray& key) const
{
  if (tnx_)
  {
    FDBFuture* future = fdb_transaction_get(tnx_, (uint8_t*)key.c_str(), key.length(), 0);
    auto err = fdb_future_block_until_ready(future);
    if (err)
    {
      fdb_future_destroy(future);
      std::cerr << "fdb_future_block_until_read error, code: " << (int)err << std::endl;
      return {false, {}};
    }

    const uint8_t* outValue;
    int outValueLength;
    fdb_bool_t present;
    err = fdb_future_get_value(future, &present, &outValue, &outValueLength);
    if (err)
    {
      fdb_future_destroy(future);

      std::cerr << "fdb_future_get_value error, code: " << (int)err << std::endl;
      return {false, {}};
    }

    fdb_future_destroy(future);
    if (present)
    {
      return {true, ByteArray(outValue, outValue + outValueLength)};
    }
    else
    {
      return {false, {}};
    }
  }
  return {false, {}};
}

void Transaction::remove(const ByteArray& key) const
{
  if (tnx_)
  {
    fdb_transaction_clear(tnx_, (uint8_t*)key.c_str(), key.length());
  }
}

void Transaction::removeRange(const ByteArray& beginKey, const ByteArray& endKey) const
{
  if (tnx_)
  {
    fdb_transaction_clear_range(tnx_, (uint8_t*)beginKey.c_str(), beginKey.length(), (uint8_t*)endKey.c_str(),
                                endKey.length());
  }
}

bool Transaction::commit() const
{
  if (tnx_)
  {
    FDBFuture* future = fdb_transaction_commit(tnx_);
    auto err = fdb_future_block_until_ready(future);
    if (err)
    {
      fdb_future_destroy(future);
      std::cerr << "fdb_future_block_until_ready error, code: " << (int)err << std::endl;
      return false;
    }
    fdb_future_destroy(future);
  }
  return true;
}

FDBNetwork::~FDBNetwork()
{
  auto err = fdb_stop_network();
  if (err)
    std::cerr << "fdb_stop_network error, code: " << err << std::endl;
  if (netThread.joinable())
    netThread.join();
}

bool FDBNetwork::setUpAndRunNetwork()
{
  auto err = fdb_setup_network();
  if (err)
  {
    std::cerr << "fdb_setup_network error, code: " << (int)err << std::endl;
    return false;
  }

  netThread = std::thread(
      []
      {
        // TODO: Try more than on time.
        auto err = fdb_run_network();
        if (err)
        {
          std::cerr << "fdb_run_network error, code: " << (int)err << std::endl;
          abort();
        }
      });
  return true;
}

FDBDataBase::FDBDataBase(FDBDatabase* database) : database_(database)
{
}

FDBDataBase::~FDBDataBase()
{
  if (database_)
    fdb_database_destroy(database_);
}

std::unique_ptr<Transaction> FDBDataBase::createTransaction() const
{
  FDBTransaction* tnx;
  auto err = fdb_database_create_transaction(database_, &tnx);
  if (err)
  {
    std::cerr << "fdb_database_create_transaction error, code: " << (int)err << std::endl;
    return nullptr;
  }
  return std::make_unique<Transaction>(tnx);
}

bool FDBDataBase::isDataBaseReady() const
{
  FDBTransaction* tnx;
  auto err = fdb_database_create_transaction(database_, &tnx);
  if (err)
  {
    std::cerr << "fdb_database_create_transaction error, code: " << (int)err << std::endl;
    return false;
  }
  ByteArray emptyKey{""};
  FDBFuture* future = fdb_transaction_get(tnx, (uint8_t*)emptyKey.c_str(), emptyKey.length(), 0);

  uint32_t count = 0;
  while (!fdb_future_is_ready(future) && count < secondsToWait_)
  {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ++count;
  }
  bool ready = fdb_future_is_ready(future);
  fdb_future_destroy(future);
  fdb_transaction_destroy(tnx);
  return ready;
}

std::shared_ptr<FDBDataBase> DataBaseCreator::createDataBase(const std::string clusterFilePath)
{
  FDBDatabase* database;
  auto err = fdb_create_database(clusterFilePath.c_str(), &database);
  if (err)
  {
    std::cerr << "fdb_create_database error, code: " << (int)err << std::endl;
    return nullptr;
  }
  return std::make_shared<FDBDataBase>(database);
}

std::vector<std::string> BlobHandler::generateKeys(const uint32_t num)
{
  std::vector<std::string> keys;
  keys.reserve(num);
  for (uint32_t i = 0; i < num; ++i)
    keys.push_back(boost::lexical_cast<std::string>(boost::uuids::random_generator()()));

  return keys;
}

float BlobHandler::log(const uint32_t base, const uint32_t value)
{
  return std::log(value) / std::log(base);
}

void BlobHandler::insertKey(std::pair<uint32_t, std::string>& block, const std::string& value)
{
  if (!block.first)
  {
    block.second.reserve(blockSizeInBytes_);
    block.second.push_back('K');
    block.first += 1;
  }

  block.second.insert(block.second.begin() + block.first, value.begin(), value.end());
  block.first += value.size();
}

void BlobHandler::insertData(std::pair<uint32_t, std::string>& block, const std::string& blob,
                             const uint32_t offset)
{
  const uint32_t endOfBlob = std::min(offset + blockSizeInBytes_, (uint32_t)blob.size());
  auto& dataBlock = block.second;
  dataBlock.reserve(blockSizeInBytes_);
  dataBlock.push_back('D');
  dataBlock.insert(dataBlock.begin() + block.first + 1, blob.begin() + offset, blob.begin() + endOfBlob);
}

bool BlobHandler::writeBlob(std::shared_ptr<FDBCS::FDBDataBase> dataBase,
                            std::unordered_map<std::string, std::pair<uint32_t, std::string>>& map,
                            const ByteArray& key, const ByteArray& blob)
{
  const uint32_t blobSizeInBytes = blob.size();
  // FIXME: Make the size `constexpr`.
  const uint32_t keySizeInBytes =
      (boost::lexical_cast<std::string>(boost::uuids::random_generator()())).size();
  const uint32_t numKeysInBlock = blockSizeInBytes_ / keySizeInBytes;
  uint32_t numBlocks = blobSizeInBytes / blockSizeInBytes_;
  if (blobSizeInBytes % blockSizeInBytes_)
    ++numBlocks;

  const uint32_t treeLen = std::ceil(log(numKeysInBlock, numBlocks));
  std::cout << "Tree len " << treeLen << std::endl;
  std::vector<std::string> currentKeys{key};

  // std::unordered_map<std::string, std::pair<uint32_t, std::string>> map;
  // How about to use block class?
  map[key] = {0, std::string()};

  for (uint32_t currentLevel = 0; currentLevel < treeLen; ++currentLevel)
  {
    std::cout << "current level " << currentLevel << std::endl;
    const auto nextLevel = currentLevel + 1;
    const uint32_t nextLevelKeyNum = std::min((uint32_t)std::pow(numKeysInBlock, nextLevel), numBlocks);
    std::cout << "next level key num " << nextLevelKeyNum << std::endl;
    std::vector<std::string> nextLevelKeys = generateKeys(nextLevelKeyNum);
    std::cout << "keys generated " << nextLevelKeys.size() << std::endl;
    uint32_t nextKeysIt = 0;
    for (uint32_t i = 0, size = currentKeys.size(); i < size && nextKeysIt < nextLevelKeyNum; ++i)
    {
      auto& block = map[currentKeys[i]];
      for (uint32_t j = 0; j < numKeysInBlock && nextKeysIt < nextLevelKeyNum; ++j, ++nextKeysIt)
      {
        const auto& nextKey = nextLevelKeys[nextKeysIt];
        insertKey(block, nextKey);
        map[nextKey] = {0, std::string()};
      }
      std::cout << "set " << currentKeys[i] << std::endl;
      auto tnx = dataBase->createTransaction();
      tnx->set(currentKeys[i], block.second);
      tnx->commit();
      // insert [currentKey, block] into kv storage
    }
    std::cout << "next key it " << nextKeysIt << std::endl;
    // Clear old keys from map.
    currentKeys = std::move(nextLevelKeys);
  }

  std::cout << "num blocks " << numBlocks << std::endl;
  std::cout << "key size " << currentKeys.size() << std::endl;

  uint32_t offset = 0;

  for (uint32_t i = 0; i < numBlocks; ++i)  // numBlocks; ++i)
  {
    auto tnx = dataBase->createTransaction();
    auto& block = map[currentKeys[i]];
    insertData(block, blob, offset);
    std::cout << "set " << currentKeys[i] << std::endl;
    tnx->set(currentKeys[i], block.second);
    offset += blockSizeInBytes_;
    tnx->commit();
  }

  std::cout << "offset " << offset << std::endl;
  return true;
}

std::pair<bool, std::vector<std::string>> BlobHandler::getKeysFromBlock(
    const std::pair<uint32_t, std::string>& block, const uint32_t keySize)
{
  std::cout << "key from block :" << block.second << std::endl;
  std::vector<std::string> keys;
  const auto& blockData = block.second;
  const uint32_t numKeysInBlock = blockSizeInBytes_ / keySize;
  if (blockData.size() > blockSizeInBytes_)
    return {false, {""}};

  uint32_t offset = 1;
  for (uint32_t i = 0; i < numKeysInBlock && offset + keySize <= blockData.size(); ++i)
  {
    std::string key(blockData.begin() + offset, blockData.begin() + offset + keySize);
    keys.push_back(std::move(key));
    offset += keySize;
  }

  return {true, keys};
}

std::pair<bool, std::string> BlobHandler::readBlob(
    std::shared_ptr<FDBCS::FDBDataBase> database,
    std::unordered_map<std::string, std::pair<uint32_t, std::string>>& map, ByteArray& key)
{
  const uint32_t keySizeInBytes =
      (boost::lexical_cast<std::string>(boost::uuids::random_generator()())).size();

  uint32_t level = 0;
  std::vector<std::string> currentKeys{key};
  while (currentKeys.size())
  {
    std::cout << "level " << level << std::endl;
    std::cout << "key size " << currentKeys.size() << std::endl;
    std::vector<std::pair<uint32_t, std::string>> blocks;
    for (const auto& key : currentKeys)
    {
      auto tnx = database->createTransaction();
      auto p = tnx->get(key);
      if (!p.first)
      {
        std::cout << "key not found " << key << std::endl;
        // return {false, ""};
      }
      std::cout << "key found " << key << std::endl;
      std::cout << "value :" << p.second << std::endl;
      blocks.push_back({0, p.second});
    }
    // is block data?
    if (blocks.front().second[0] == 'D')
      break;

    std::vector<std::string> nextKeys;
    for (const auto& block : blocks)
    {
      auto p = getKeysFromBlock(block, keySizeInBytes);
      if (!p.first)
      {
        return {false, ""};
      }
      auto& keys = p.second;
      nextKeys.insert(nextKeys.end(), keys.begin(), keys.end());
    }
    ++level;
    currentKeys = std::move(nextKeys);
  }

  std::cout << "keys size " << currentKeys.size() << std::endl;

  std::string blob;
  for (const auto& key : currentKeys)
  {
    auto& dataBlock = map[key].second;
    if (!dataBlock.size())
      break;
    blob.insert(blob.end(), dataBlock.begin() + 1, dataBlock.end());
  }

  return {true, blob};
}

bool setAPIVersion()
{
  auto err = fdb_select_api_version(FDB_API_VERSION);
  return err ? false : true;
}
}  // namespace FDBCS

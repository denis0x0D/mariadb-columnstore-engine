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

#include <vector>
#include "fdbcs.hpp"

using namespace std;
using namespace FDBCS;
using std::chrono::duration;
using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::milliseconds;

template <typename T>
static void assert_internal(const T& value, const std::string& errMessage)
{
  if (!value)
  {
    std::cerr << errMessage << std::endl;
    abort();
  }
}

static std::string generateBlob(const uint32_t len)
{
  std::string blob;
  blob.reserve(len);
  for (uint32_t i = 0; i < len; ++i)
  {
    blob.push_back('a' + (i % 26));
  }
  return blob;
}

static void testBlobHandler(std::shared_ptr<FDBCS::FDBDataBase> db)
{
  const uint32_t size = 10090801;
  std::string rootKey = "root";
  auto blobA = generateBlob(size);
  BlobHandler handler;
  auto t1 = high_resolution_clock::now();
  handler.writeBlob(db, rootKey, blobA);
  auto t2 = high_resolution_clock::now();
  auto ms_int = duration_cast<milliseconds>(t2 - t1);
 // std::cout << "Write blob time: " << ms_int.count() << std::endl;
  t1 = high_resolution_clock::now();
  auto p = handler.readBlob(db, rootKey);
  t1 = high_resolution_clock::now();
  ms_int = duration_cast<milliseconds>(t2 - t1);
  //std::cout << "Read blob time: " << ms_int.count() << std::endl;
  assert_internal(blobA == p.second, "Blobs not equal");
}

int main()
{
  std::string path = "/etc/foundationdb/fdb.cluster";
  assert_internal(setAPIVersion(), "Set api version failed.");
  FDBNetwork netWork;
  // Create and run network.
  assert_internal(netWork.setUpAndRunNetwork(), "Set up network failed.");

  // Create database.
  auto db = DataBaseCreator::createDataBase(path);
  assert_internal(db, "Cannot create FDB.");
  assert_internal(db->isDataBaseReady(), "FDB is down.");
  const std::vector<std::pair<std::string, std::string>> kvArray{
      {"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}};
  const std::string endKey = "key4";

  // Set a key/value.
  {
    auto tnx = db->createTransaction();
    tnx->set(kvArray[0].first, kvArray[0].second);
    assert_internal(tnx->commit(), "Cannot commit set().");
  }
  // Get a value by a key.
  {
    auto tnx = db->createTransaction();
    auto p = tnx->get(kvArray[0].first);
    assert_internal(p.first, "get() failed.");
    assert_internal(p.second == kvArray[0].second, "get(): keys are not matched.");
  }
  // Remove a key.
  {
    auto tnx = db->createTransaction();
    tnx->remove(kvArray[0].first);
    assert_internal(tnx->commit(), "Cannot commit remove().");
  }
  // Check that key is not presetnt anymore.
  {
    auto tnx = db->createTransaction();
    auto p = tnx->get(kvArray[0].first);
    assert_internal(!p.first, "Key exists after remove().");
  }

  // Remove range.
  for (const auto& [key, value] : kvArray)
  {
    {
      auto tnx = db->createTransaction();
      tnx->set(key, value);
      assert_internal(tnx->commit(), "Cannot commit set() for range.");
    }
  }

  {
    auto tnx = db->createTransaction();
    tnx->removeRange(kvArray.front().first, endKey);
    assert_internal(tnx->commit(), "Cannot commit removeRange().");
  }

  for (const auto& [key, value] : kvArray)
  {
    {
      auto tnx = db->createTransaction();
      auto rPair = tnx->get(key);
      assert_internal(!rPair.first, "Key exists after remove range.");
    }
  }

  testBlobHandler(db);
  return 0;
}

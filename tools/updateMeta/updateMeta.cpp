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

#include <iostream>
#include <string>
#include <filesystem>
#include <vector>
#include "configcpp.h"
#include "fdbcs.hpp"
#include <fstream>
#include <sstream>

using namespace std;

static std::shared_ptr<FDBCS::FDBDataBase> fdbDataBaseInstance;
static std::unique_ptr<FDBCS::FDBNetwork> fdbNetworkInstance;
static std::mutex kvStorageLock;

static std::shared_ptr<FDBCS::FDBDataBase> getStorageInstance()
{
  if (!FDBCS::setAPIVersion())
  {
    cerr << "Update meta: FDB setAPIVersion failed." << endl;
    return nullptr;
  }

  fdbNetworkInstance = std::make_unique<FDBCS::FDBNetwork>();
  if (!fdbNetworkInstance->setUpAndRunNetwork())
  {
    cerr << "Update meta: FDB setUpAndRunNetwork failed." << endl;
    return nullptr;
  }

  std::string clusterFilePath = "/etc/foundationdb/fdb.cluster";
  fdbDataBaseInstance = FDBCS::DataBaseCreator::createDataBase(clusterFilePath);
  if (!fdbDataBaseInstance) {
    cerr <<  "Update meta: FDB createDataBase failed." << endl;
    return nullptr;
  }

  return fdbDataBaseInstance;
}

class MetaCollector
{
 public:
  MetaCollector(const std::string& metaPath) : metaPath(metaPath)
  {
  }

  void collect(const std::string& dbRoot)
  {
    auto path = metaPath + dbRoot;
    for (auto const& file : std::filesystem::recursive_directory_iterator{path})
    {
      if (std::filesystem::is_regular_file(file))
      {
        files.push_back(file.path());
      }
    }
  }

  bool commit()
  {
    for (const auto& file : files)
    {
      auto fileName = file.string();
      ifstream iFile(fileName);
      if (!iFile.is_open())
        return false;

      std::stringstream stream;
      stream << iFile.rdbuf();
      auto kvStorage = getStorageInstance();
      auto keyGen = std::make_shared<FDBCS::BoostUIDKeyGenerator>();
      FDBCS::BlobHandler blobWriter(keyGen);
      fileName = "SM_M" + fileName;
      if (!blobWriter.writeBlob(kvStorage, fileName, stream.str())) {
        return false;
      }
    }
    return true;
  }

 private:
  std::vector<filesystem::path> files;
  string metaPath;
  string dbRoot;
};

int main(int argc, char** argv)
{
  auto* config = config::Config::makeConfig();
  std::string count = config->getConfig("SystemConfig", "DBRootCount");
  // Read the number of DBRoots.
  uint32_t dbRootCount = config->uFromText(count);
  std::string metaPath = "/var/lib/columnstore/storagemanager/metadata";
  MetaCollector collector(metaPath);

  // Iterate over DBRoots starting from the first one.
  for (uint32_t dbRootNumber = 1; dbRootNumber <= dbRootCount; ++dbRootNumber)
  {
    std::string dbRootName = "DBRoot" + std::to_string(dbRootNumber);
    auto dbRootPath = config->getConfig("SystemConfig", dbRootName);
    collector.collect(dbRootPath);
  }
  return 0;
}

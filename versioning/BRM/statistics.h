/* Copyright (C) 2021 MariaDB Corporation

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

#ifndef STATISTICS_H
#define STATISTICS_H

#include "rowgroup.h"
#include "logger.h"
#include "hasher.h"
#include "IDBPolicy.h"

#include <unordered_map>
#include <unordered_set>
#include <mutex>

using namespace idbdatafile;

namespace statistics
{

struct StatisticsFileHeader
{
    uint64_t version;
    uint64_t epoch;
    uint64_t fileHash;
    uint64_t dataSize;
    uint8_t offset[1024];
};

enum class KeyType
{
    PK,
    FK
};

enum class StatisticsType
{
    PK_FK
};

class StatisticsManager
{
  public:
    static StatisticsManager* instance();
    void analyzeColumnKeyTypes(const rowgroup::RowGroup& rowGroup, bool trace);
    void toString(StatisticsType statisticsType = StatisticsType::PK_FK);
    void saveToFile();
    void loadFromFile();
    void incEpoch() { ++epoch; }
    void serialize(messageqcpp::ByteStream& bs);
    void unserialize(messageqcpp::ByteStream& bs);

  private:
    std::unordered_map<uint32_t, KeyType> keyTypes;
    StatisticsManager() : epoch(0), version(1) { IDBPolicy::init(true, false, "", 0); }
    std::mutex mut;
    uint32_t epoch;
    uint32_t version;
    // FIXME:
    // Should we put it to the config file?
    std::string statsFile = "/var/lib/columnstore/local/statistics";
};

class StatisticsDistributor
{
  public:
    static StatisticsDistributor* instance()
    {
        static StatisticsDistributor* sd = new StatisticsDistributor();
        return sd;
    }

    void countClients();
    void distributeStatistics();

  private:
    StatisticsDistributor() : clientsCount(0) {}
    uint32_t clientsCount;
};

} // namespace statistics
#endif

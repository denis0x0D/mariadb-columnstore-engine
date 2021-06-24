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

#include <iostream>
#include <boost/filesystem.hpp>

#include "statistics.h"
#include "IDBPolicy.h"
#include "brmtypes.h"
#include "hasher.h"

using namespace idbdatafile;
using namespace logging;

namespace statistics
{

StatisticsManager* StatisticsManager::instance()
{
    static StatisticsManager* sm = new StatisticsManager();
    return sm;
}

void StatisticsManager::analyzeColumnKeyTypes(const rowgroup::RowGroup& rowGroup, bool trace)
{
    std::lock_guard<std::mutex> lock(mut);
    auto rowCount = rowGroup.getRowCount();
    const auto columnCount = rowGroup.getColumnCount();
    if (!rowCount || !columnCount)
        return;

    auto& oids = rowGroup.getOIDs();

    rowgroup::Row r;
    rowGroup.initRow(&r);
    rowGroup.getRow(0, &r);

    std::vector<std::unordered_set<uint32_t>> columns(columnCount, std::unordered_set<uint32_t>());
    // Init key types.
    for (uint32_t index = 0; index < columnCount; ++index)
        keyTypes[oids[index]] = KeyType::PK;

    const uint32_t maxRowCount = 4096;
    // TODO: We should read just couple of blocks from columns, not all data, but this requires
    // more deep refactoring of column commands.
    rowCount = std::min(rowCount, maxRowCount);
    // This is strange, it's a CS but I'm processing data as row by row, how to fix it?
    for (uint32_t i = 0; i < rowCount; ++i)
    {
        for (uint32_t j = 0; j < columnCount; ++j)
        {
            if (r.isNullValue(j) || columns[j].count(r.getIntField(j)))
                keyTypes[oids[j]] = KeyType::FK;
            else
                columns[j].insert(r.getIntField(j));
        }
        r.nextRow();
    }

    if (trace)
        toString(StatisticsType::PK_FK);
}

void StatisticsManager::toString(StatisticsType statisticsType)
{
    if (statisticsType == StatisticsType::PK_FK)
    {
        std::cout << "Columns count: " << keyTypes.size() << std::endl;
        for (const auto& p : keyTypes)
            std::cout << p.first << " " << (int) p.second << std::endl;
    }
}

void StatisticsManager::saveToFile()
{
    std::lock_guard<std::mutex> lock(mut);

    const char* fileName = statsFile.c_str();
    std::unique_ptr<IDBDataFile> out(
        IDBDataFile::open(IDBPolicy::getType(fileName, IDBPolicy::WRITEENG), fileName, "wb", 1));

    if (!out)
    {
        BRM::log_errno("StatisticsManager::saveToFile(): open");
        throw ios_base::failure(
            "StatisticsManager::saveToFile(): open failed. Check the error log.");
    }

    // Number of pairs.
    uint64_t count = keyTypes.size();
    // count, [[uid, keyType], ... ]
    const uint64_t dataStreamSize = sizeof(uint64_t) + count * (sizeof(uint32_t) + sizeof(KeyType));

    // Allocate memory for data stream.
    char* dataStream = new char[dataStreamSize];
    std::unique_ptr<char[]> dataStreamSmartPtr;
    dataStreamSmartPtr.reset(dataStream);

    // Initialize the data stream.
    uint64_t offset = 0;
    std::memcpy(dataStream, reinterpret_cast<char*>(&count), sizeof(uint64_t));
    offset += sizeof(uint64_t);

    // For each pair [oid, key type].
    for (const auto& p : keyTypes)
    {
        uint32_t oid = p.first;
        std::memcpy(&dataStream[offset], reinterpret_cast<char*>(&oid), sizeof(uint32_t));
        offset += sizeof(uint32_t);

        KeyType keyType = p.second;
        std::memcpy(&dataStream[offset], reinterpret_cast<char*>(&keyType), sizeof(KeyType));
        offset += sizeof(KeyType);
    }

    utils::Hasher128 hasher;
    // Prepare a statistics file header.
    const uint32_t headerSize = sizeof(StatisticsFileHeader);
    StatisticsFileHeader fileHeader;
    std::memset(&fileHeader, 0, headerSize);
    fileHeader.version = version;
    fileHeader.epoch = epoch;
    fileHeader.dataSize = dataStreamSize;
    // Compute hash from the data.
    fileHeader.fileHash = hasher(dataStream, dataStreamSize);

    // Write statistics file header.
    int64_t size = out->write(reinterpret_cast<char*>(&fileHeader), headerSize);
    if (size != headerSize)
    {
        auto rc = IDBPolicy::remove(fileName);
        if (rc == -1)
            std::cerr << "Cannot remove file " << fileName << std::endl;

        throw ios_base::failure("StatisticsManager::saveToFile(): write failed. ");
    }

    // Write data.
    size = out->write(dataStream, dataStreamSize);
    if (size != dataStreamSize)
    {
        auto rc = IDBPolicy::remove(fileName);
        if (rc == -1)
            std::cerr << "Cannot remove file " << fileName << std::endl;

        throw ios_base::failure("StatisticsManager::saveToFile(): write failed. ");
    }
}

void StatisticsManager::loadFromFile()
{
    std::lock_guard<std::mutex> lock(mut);
    // Check that stats file does exist.
    if (!boost::filesystem::exists(statsFile))
        return;

    const char* fileName = statsFile.c_str();
    std::unique_ptr<IDBDataFile> in(
        IDBDataFile::open(IDBPolicy::getType(fileName, IDBPolicy::WRITEENG), fileName, "rb", 1));

    if (!in)
    {
        BRM::log_errno("StatisticsManager::loadFromFile(): open");
        throw ios_base::failure(
            "StatisticsManager::loadFromFile(): open failed. Check the error log.");
    }

    // Read the file header.
    StatisticsFileHeader fileHeader;
    const uint32_t headerSize = sizeof(StatisticsFileHeader);
    int64_t size = in->read(reinterpret_cast<char*>(&fileHeader), headerSize);
    if (size != headerSize)
        throw ios_base::failure("StatisticsManager::loadFromFile(): read failed. ");

    // Initialize fields from the file header.
    version = fileHeader.version;
    epoch = fileHeader.epoch;
    const auto fileHash = fileHeader.fileHash;
    const auto dataStreamSize = fileHeader.dataSize;

    // Allocate the memory for the file data.
    char* dataStream = new char[dataStreamSize];
    std::unique_ptr<char[]> dataStreamSmartPtr;
    dataStreamSmartPtr.reset(dataStream);

    // Read the data.
    uint64_t dataOffset = 0;
    auto sizeToRead = dataStreamSize;
    size = in->read(dataStream, sizeToRead);
    sizeToRead -= size;
    dataOffset += size;

    while (sizeToRead > 0)
    {
        size = in->read(dataStream + dataOffset, sizeToRead);
        if (size < 0)
            throw ios_base::failure("StatisticsManager::loadFromFile(): read failed. ");

        sizeToRead -= size;
        dataOffset += size;
    }

    utils::Hasher128 hasher;
    auto computedFileHash = hasher(dataStream, dataStreamSize);
    if (fileHash != computedFileHash)
        throw ios_base::failure("StatisticsManager::loadFromFile(): invalid file hash. ");

    uint64_t count = 0;
    std::memcpy(reinterpret_cast<char*>(&count), dataStream, sizeof(uint64_t));
    uint64_t offset = sizeof(uint64_t);

    // For each pair.
    for (uint64_t i = 0; i < count; ++i)
    {
        uint32_t oid;
        KeyType keyType;
        std::memcpy(reinterpret_cast<char*>(&oid), &dataStream[offset], sizeof(uint32_t));
        offset += sizeof(uint32_t);
        std::memcpy(reinterpret_cast<char*>(&keyType), &dataStream[offset], sizeof(KeyType));
        offset += sizeof(KeyType);
        // Insert pair.
        keyTypes[oid] = keyType;
    }
}

void StatisticsManager::serialize(messageqcpp::ByteStream& bs)
{
    uint64_t count = keyTypes.size();
    bs << version;
    bs << epoch;
    bs << count;

    for (const auto& keyType : keyTypes)
    {
        bs << keyType.first;
        bs << (uint32_t) keyType.second;
    }
}

void StatisticsManager::unserialize(messageqcpp::ByteStream& bs)
{
    uint64_t count;
    bs >> version;
    bs >> epoch;
    bs >> count;

    for (uint32_t i = 0; i < count; ++i)
    {
        uint32_t oid, keyType;
        bs >> oid;
        bs >> keyType;
        keyTypes[oid] = static_cast<KeyType>(keyType);
    }
}

} // namespace statistics

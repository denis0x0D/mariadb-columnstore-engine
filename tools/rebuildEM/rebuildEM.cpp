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
#include <stdint.h>

#include "rebuildEM.h"
#include "calpontsystemcatalog.h"
#include "idbcompress.h"
#include "we_convertor.h"
#include "we_fileop.h"
#include "we_blockop.h"
#include "IDBPolicy.h"
#include "IDBFileSystem.h"
#include "we_chunkmanager.h"
#include "BRM_saves_em_system_tables_blob.h"

using namespace idbdatafile;

namespace RebuildExtentMap
{

int32_t EMReBuilder::collectExtents(const string& dbRootPath)
{
    if (doVerbose())
    {
        std::cout << "Collect extents for the DBRoot " << dbRootPath
                  << std::endl;
    }
    for (const auto& dir :
         boost::filesystem::recursive_directory_iterator(dbRootPath))
    {
        (void) collectExtent(dir.path().string());
    }

    return 0;
}

int32_t EMReBuilder::collectExtent(const std::string& fullFileName)
{
    WriteEngine::FileOp fileOp;
    uint32_t oid;
    uint32_t partition;
    uint32_t segment;
    // Initialize oid, partition and segment from the given `fullFileName`.
    auto rc = WriteEngine::Convertor::fileName2Oid(fullFileName, oid,
                                                   partition, segment);
    if (rc != 0)
    {
        return rc;
    }

    // Open the given file.
    std::unique_ptr<IDBDataFile> dbFile(IDBDataFile::open(
        IDBPolicy::getType(fullFileName, IDBPolicy::WRITEENG),
        fullFileName.c_str(), "rb", 1));

    if (!dbFile)
    {
        std::cerr << "Cannot open file " << fullFileName << std::endl;
        return -1;
    }

    rc = dbFile->seek(0, 0);
    if (rc != 0)
    {
        std::cerr << "IDBDataFile::seek failed for the file " << fullFileName
                  << std::endl;
        return rc;
    }

    // Read and verify header.
    char fileHeader[compress::IDBCompressInterface::HDR_BUF_LEN * 2];
    rc = fileOp.readHeaders(dbFile.get(), fileHeader);
    if (rc != 0)
    {
        // FIXME: If the file was created without a compression, it does not
        // have a header block, so header verification fails in this case,
        // currently we skip it, because we cannot deduce needed data to create
        // a column extent from the blob file.
        if (doVerbose())
        {
            std::cerr
                << "Cannot read file header from the file " << fullFileName
                << ", probably this file was created without compression. "
                << std::endl;
        }
        return rc;
    }

    if (doVerbose())
    {
        std::cout << "Processing file: " << fullFileName << std::endl;
        std::cout << "fileName2Oid:  [OID: " << oid
                  << ", partition: " << partition << ", segment: " << segment
                  << "] " << std::endl;
    }

    // Read the `colDataType` and `colWidth` from the given header.
    compress::IDBCompressInterface compressor;
    const auto versionNumber = compressor.getVersionNumber(fileHeader);
    // Verify header number.
    if (versionNumber < 3)
    {
        if (doVerbose())
        {
            std::cerr << "File version " << versionNumber
                      << " is not supported. " << std::endl;
        }
        return -1;
    }

    auto colDataType = compressor.getColDataType(fileHeader);
    auto colWidth = compressor.getColumnWidth(fileHeader);
    auto lbid = compressor.getLBID(fileHeader);
    auto blockCount = compressor.getBlockCount(fileHeader);

    if (colDataType == execplan::CalpontSystemCatalog::UNDEFINED)
    {
        if (doVerbose())
        {
            std::cout << "File header has invalid data. " << std::endl;
        }
        return -1;
    }

    auto isDict = isDictFile(colDataType, colWidth);
    uint64_t hwm = 0;
    // We don't need to calculate HWM for dictionary files, because dictionary
    // file stores char data and has associated segment file with tokens which
    // holds pointers to that data.
    if (!isDict)
    {
        if (doVerbose())
        {
            std::cout << "Searching for HWM... " << std::endl;
            std::cout << "Block count: " << blockCount << std::endl;
        }

        rc = searchHWMInSegmentFile(oid, getDBRoot(), partition, segment,
                                    colDataType, colWidth, blockCount, hwm);
        if (rc != 0)
        {
            return rc;
        }

        if (doVerbose())
        {
            std::cout << "HWM is: " << hwm << std::endl;
        }
    }

    FileId fileId(oid, partition, segment, colWidth, colDataType, lbid, hwm,
                  isDict);
    extentMap.push_back(fileId);

    if (doVerbose())
    {
        std::cout << "FileId is collected " << fileId << std::endl;
    }
    return 0;
}

int32_t EMReBuilder::rebuildExtentMap()
{
    if (doVerbose())
    {
        std::cout << "Build extent map with size " << extentMap.size()
                  << std::endl;
    }

    // We have to restore extent map by restoring individual extent in order
    // they were created. This is important part, otherwise we will get invalid
    // extents for dictionary segment files and will not be able to access
    // columns through dictionay segment files.
    sort(extentMap.begin(), extentMap.end(),
         [](const FileId& lhs, const FileId& rhs) {
             return lhs.lbid < rhs.lbid;
         });

    for (const auto& fileId : extentMap)
    {
        uint32_t startBlockOffset;
        int32_t allocdSize;
        BRM::LBID_t lbid;

        if (!doDisplay())
        {
            // Check the extent map first.
            bool found;
            int32_t status;
            getEM().getExtentState(fileId.oid, fileId.partition,
                                   fileId.segment, found, status);
            if (found)
            {
                if (doVerbose())
                {
                    std::cout << "The extent for " << fileId
                              << " already exists." << std::endl;
                }
                return -1;
            }

            try
            {
                if (fileId.isDict)
                {
                    // Create a dictionary extent for the given oid, partition,
                    // segment, dbroot.
                    getEM().createDictStoreExtent(
                        fileId.oid, getDBRoot(), fileId.partition,
                        fileId.segment, lbid, allocdSize);
                }
                else
                {
                    // Create a column extent for the given oid, partition,
                    // segment, dbroot and column width.
                    getEM().createColumnExtentExactFile(
                        fileId.oid, fileId.colWidth, getDBRoot(),
                        fileId.partition, fileId.segment, fileId.colDataType,
                        lbid, allocdSize, startBlockOffset);
                }
            }
            // Could throw an logic_error exception.
            catch (std::exception& e)
            {
                getEM().undoChanges();
                std::cerr << "Cannot create column extent: " << e.what()
                          << std::endl;
                return -1;
            }

            getEM().confirmChanges();
            if (doVerbose())
            {
                std::cout << "Extent is created, allocated size " << allocdSize
                          << " actual LBID " << lbid << std::endl;
                std::cout << "For " << fileId << std::endl;
            }

            if (!fileId.isDict)
            {
                // This is important part, it sets a status for specific extent
                // as 'available' that means we can use it.
                if (doVerbose())
                {
                    std::cout << "Setting a HWM for " << fileId << std::endl;
                }
                getEM().setLocalHWM(fileId.oid, fileId.partition,
                                    fileId.segment, fileId.hwm, false, true);
                getEM().confirmChanges();
            }
        }
    }
    return 0;
}

int32_t EMReBuilder::searchHWMInSegmentFile(
    uint32_t oid, uint32_t dbRoot, uint32_t partition, uint32_t segment,
    execplan::CalpontSystemCatalog::ColDataType colDataType, uint32_t colWidth,
    uint64_t blockCount, uint64_t& hwm)
{
    WriteEngine::ChunkManager chunkManager;
    WriteEngine::FileOp fileOp;
    // Spent one night to debug, if not set will get a strange segfault in
    // m_typeHandler which is not null but points to memory which is not
    // accessible by current process. Is it related to initialization order
    // fiasko?
    chunkManager.fileOp(&fileOp);
    std::string fileName;
    uint8_t blockData[WriteEngine::BYTE_PER_BLOCK];
    const uint8_t* emptyValue = fileOp.getEmptyRowValue(colDataType, colWidth);
    int32_t size = colWidth;
    hwm = 0;

    // Open compressed segment file. We will read block by block from
    // the compressed chunks.
    // Note: We cannot use `unique_ptr` here or close it directly, because
    // `ChunkManager` closes this file for us, otherwise we will get double
    // free error.
    auto* pFile = chunkManager.getColumnFilePtr(oid, dbRoot, partition,
                                                segment, colDataType, colWidth,
                                                fileName, "rb", size, false);
    if (!pFile)
    {
        return -1;
    }

    // Starting from the end.
    // Note: This solves problem related to `bulk` insertion.
    // Currently it could start to insert values from any block into empty
    // column.
    for (int32_t currentBlock = blockCount - 1; currentBlock >= 0;
         --currentBlock)
    {
        // Read the block associated to HWM.
        // The uncompressed chunk size is 512 * 1024 * 8, so for `abbreviated`
        // extent one chunk will hold all blocks, therefore we need to
        // decompress chunk only once, the cached chunk will be used later.
        auto rc = chunkManager.readBlock(pFile, blockData, currentBlock);
        if (rc != 0)
        {
            return rc;
        }
        // Check the first row for not empty value.
        if (!isEmptyValue(blockData, emptyValue, colWidth))
        {
            hwm = currentBlock;
            break;
        }
    }

    return 0;
}

void EMReBuilder::showExtentMap()
{
    std::cout << "range.start|range.size|fileId|blockOffset|HWM|partition|"
                 "segment|dbroot|width|status|hiVal|loVal|seqNum|isValid|"
              << std::endl;
    getEM().dumpTo(std::cout);
}

// Check for hard-coded values which define dictionary file.
bool EMReBuilder::isDictFile(
    execplan::CalpontSystemCatalog::ColDataType colDataType, uint64_t width)
{
    // Dictionary store extent has a width == 0. See more detais in
    // `createDictStoreExtent` function.
    return (colDataType == execplan::CalpontSystemCatalog::VARCHAR) &&
           (width == 0);
}

int32_t EMReBuilder::initializeSystemExtents()
{

    if (!doDisplay())
    {
        if (doVerbose())
        {
            std::cout << "Initialize system extents from the initial state"
                      << std::endl;
        }
        try
        {
            getEM().loadFromBinaryBlob(reinterpret_cast<const char*>(
                BRM_saves_em_system_tables_blob));
        }
        catch (...)
        {
            std::cerr << "Cannot initialize system extents from binary blob "
                      << std::endl;
            return -1;
        }
    }
    return 0;
}

// This function is copy pasted from `ColumnOp` file, unfortunately it's not
// possible to reuse it directly form `ColumnOp` without a creating a some kind
// of object which inherits from `ColumnOp`, probably this should be moved to
// utils and make a static as well, to be able to use it without creating an
// object.
bool EMReBuilder::isEmptyValue(uint8_t* value, const uint8_t* emptyValue,
                               uint32_t colWidth)
{
    switch (colWidth)
    {
    case 1:
        return *(uint8_t*) value == *(uint8_t*) emptyValue;

    case 2:
        return *(uint16_t*) value == *(uint16_t*) emptyValue;

    case 4:
        return *(uint32_t*) value == *(uint32_t*) emptyValue;

    case 8:
        return *(uint64_t*) value == *(uint64_t*) emptyValue;

    case 16:
        return *(uint128_t*) value == *(uint128_t*) emptyValue;
    }

    return false;
}

std::ostream& operator<<(std::ostream& os, const FileId& fileID)
{
    os << "[OID: " << fileID.oid << ", partition: " << fileID.partition
       << ", segment: " << fileID.segment << ", col width: " << fileID.colWidth
       << ", lbid:" << fileID.lbid << ", hwm: " << fileID.hwm
       << ", isDict: " << fileID.isDict << "]";
    return os;
}
} // namespace RebuildExtentMap

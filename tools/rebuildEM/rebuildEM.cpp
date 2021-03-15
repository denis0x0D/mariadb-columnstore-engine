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

#include "rebuildEM.h"
#include "calpontsystemcatalog.h"
#include "idbcompress.h"
#include "we_convertor.h"
#include "we_fileop.h"
#include "IDBPolicy.h"
#include "IDBFileSystem.h"

using namespace idbdatafile;

namespace RebuildExtentMap
{
using RM = RebuildEMManager;

// Check for hard-coded values which define dictionary file.
static bool isDictFile(execplan::CalpontSystemCatalog::ColDataType colDataType,
                       uint64_t width)
{
    return (colDataType == execplan::CalpontSystemCatalog::VARCHAR) &&
           (width == 65000);
}

int32_t rebuildEM(const std::string& fullFileName)
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

    bool found = false;
    int32_t status = 0;
    // Check the extent map first.
    RM::instance()->getEM().getExtentState(oid, partition, segment, found,
                                           status);
    if (found)
    {
        if (RM::instance()->verbose())
        {
            std::cout << "The extent for oid " << oid << ", partition "
                      << partition << ", segment " << segment
                      << " already exists." << std::endl;
        }
        return -1;
    }

    // Open the given file.
    auto* dbFile = IDBDataFile::open(
        IDBPolicy::getType(fullFileName, IDBPolicy::WRITEENG),
        fullFileName.c_str(), "rb", 1);
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
        fileOp.closeFile(dbFile);
        return rc;
    }

    // Read and verify header.
    char fileHeader[compress::IDBCompressInterface::HDR_BUF_LEN * 2];
    rc = fileOp.readHeaders(dbFile, fileHeader);
    if (rc != 0)
    {
        fileOp.closeFile(dbFile);
        // FIXME: If the file was created without a compression, it does not
        // have a header block, so header verification fails in this case,
        // currently we skip it, because we cannot deduce needed data to create
        // a column extent from the blob file.
        if (RM::instance()->verbose())
        {
            std::cerr
                << "Cannot read file header from the file " << fullFileName
                << ", probably this file was created without compression. "
                << std::endl;
        }
        return rc;
    }

    if (RM::instance()->verbose())
    {
        std::cout << "Processing file: " << fullFileName << "  [OID: " << oid
                  << ", partition: " << partition << ", segment: " << segment
                  << "] " << std::endl;
    }


    // Read the `colDataType` and `colWidth` from the given header.
    compress::IDBCompressInterface compressor;
    auto versionNumber = compressor.getVersionNumber(fileHeader);
    // Verify header number.
    if (versionNumber < 3)
    {
        fileOp.closeFile(dbFile);
        if (RM::instance()->verbose())
        {
            std::cerr << "File version " << versionNumber
                      << " is not supported. " << std::endl;
        }
        return -1;
    }

    auto colDataType = compressor.getColDataType(fileHeader);
    auto colWidth = compressor.getColumnWidth(fileHeader);
    if (colDataType == execplan::CalpontSystemCatalog::UNDEFINED || !colWidth)
    {
        fileOp.closeFile(dbFile);
        if (RM::instance()->verbose())
        {
            std::cout << "File header has invalid data. " << std::endl;
        }
        return -1;
    }

    uint32_t startBlockOffset;
    int32_t allocdSize;
    BRM::LBID_t lbid;
    bool isDictionaryFile = isDictFile(colDataType, colWidth);

    if (!RM::instance()->display())
    {
        try
        {
            if (isDictionaryFile)
            {
                // Create a dictionary extent for the given oid, partition,
                // segment, dbroot.
                RM::instance()->getEM().createDictStoreExtent(
                    oid, RM::instance()->getDBRoot(), partition, segment, lbid,
                    allocdSize);
            }
            else
            {
                // Create a column extent for the given oid, partition,
                // segment, dbroot and column width.
                RM::instance()->getEM().createColumnExtentExactFile(
                    oid, colWidth, RM::instance()->getDBRoot(), partition,
                    segment, colDataType, lbid, allocdSize, startBlockOffset);
            }
        }
        // Could throw an logic_error exception.
        catch (std::exception& e)
        {
            RM::instance()->getEM().undoChanges();
            fileOp.closeFile(dbFile);
            std::cerr << "Cannot create column extent: " << e.what()
                      << std::endl;
            return -1;
        }
        RM::instance()->getEM().confirmChanges();
        // This is important part, it sets a status for specific extent as
        // 'available' that means we can use it.
        RM::instance()->getEM().setLocalHWM(oid, partition, segment, 0,
                                            /*firstNode=*/false,
                                            /*uselock=*/true);
        RM::instance()->getEM().confirmChanges();
    }

    if (RM::instance()->verbose())
    {
        if (isDictionaryFile)
            std::cout << "Dictionary ";
        else
            std::cout << "Column ";
        std::cout << "extent is created, allocated size " << allocdSize
                  << " starting LBID " << lbid << " for OID " << oid
                  << std::endl;
    }
    fileOp.closeFile(dbFile);
    return 0;
}
} // namespace RebuildExtentMap

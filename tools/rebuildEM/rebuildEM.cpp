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
#include "IDBPolicy.h"
#include "IDBFileSystem.h"
#include "BRM_saves_em_system_tables_blob.h"

using namespace idbdatafile;

namespace RebuildExtentMap
{

// Check for hard-coded values which define dictionary file.
bool EMReBuilder::isDictFile(
    execplan::CalpontSystemCatalog::ColDataType colDataType, uint64_t width)
{
    return (colDataType == execplan::CalpontSystemCatalog::VARCHAR) &&
           (width == 65000);
}

void EMReBuilder::showExtentMap()
{
    std::cout << "range.start|range.size|fileId|blockOffset|HWM|partition|"
                 "segment|dbroot|width|status|hiVal|loVal|seqNum|isValid|"
              << std::endl;
    getEM().dumpTo(std::cout);
}

void EMReBuilder::initializeSystemTables()
{
    getEM().loadFromBinaryBlob(
        reinterpret_cast<const char*>(BRM_saves_em_system_tables_blob));
}

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

int32_t EMReBuilder::rebuildExtentMap()
{
    if (doVerbose())
    {
        std::cout << "Build extent map with size " << extentMap.size()
                  << std::endl;
    }

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
                    std::cout << "The extent for oid " << fileId.oid
                              << ", partition " << fileId.partition
                              << ", segment " << fileId.segment
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
                          << " starting LBID " << lbid << " for OID "
                          << fileId.oid << std::endl;
            }

            // This is important part, it sets a status for specific extent as
            // 'available' that means we can use it.
            if (doVerbose())
            {
                std::cout << "Setting a HWM for oid " << fileId.oid
                          << ", partition " << fileId.partition << ", segment "
                          << fileId.segment << std::endl;
            }
            getEM().setLocalHWM(fileId.oid, fileId.partition, fileId.segment,
                                0, false, true);
            getEM().confirmChanges();
        }
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
        if (doVerbose())
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
        if (doVerbose())
        {
            std::cout << "File header has invalid data. " << std::endl;
        }
        return -1;
    }

    extentMap.insert(FileId(oid, partition, segment, colWidth, colDataType,
                            isDictFile(colDataType, colWidth)));
    if (doVerbose())
    {
        std::cout << "FileId is collected for [OID: " << oid
                  << ", partition: " << partition << ", segment: " << segment
                  << "] " << std::endl;
    }
    return 0;
}
} // namespace RebuildExtentMap

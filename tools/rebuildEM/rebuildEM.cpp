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

    if (RM::instance()->doVerbose())
    {
        std::cout << "Processing file: " << fullFileName << "  [OID: " << oid
                  << ", partition: " << partition << ", segment: " << segment
                  << "] " << std::endl;
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
        if (RM::instance()->doVerbose())
        {
            std::cerr
                << "Cannot read file header from the file " << fullFileName
                << ", probably this file was created without compression. "
                << std::endl;
        }
        return rc;
    }

    // Read the `colDataType` and `colWidth` from the given header.
    compress::IDBCompressInterface compressor;
    auto colDataType = compressor.getColDataType(fileHeader);
    auto colWidth = compressor.getColumnWidth(fileHeader);
    uint32_t startBlockOffset;

    BRM::LBID_t lbid;
    int32_t allocdSize = 0;
    if (!RM::instance()->doDisplay())
    {
        try
        {
            // Create a column extent for the given oid, partition, segment,
            // dbroot and column width.
            RM::instance()->getEM().createColumnExtentExactFile(
                oid, colWidth, RM::instance()->getDBRoot(), partition, segment,
                colDataType, lbid, allocdSize, startBlockOffset);
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
        // Release write lock.
        RM::instance()->getEM().confirmChanges();
    }

    if (RM::instance()->doVerbose())
    {
        std::cout << "Column extent is created, allocated size " << allocdSize
                  << " starting LBID " << lbid << " for OID " << oid
                  << std::endl;
    }

    fileOp.closeFile(dbFile);
    return 0;
}
} // namespace RebuildExtentMap

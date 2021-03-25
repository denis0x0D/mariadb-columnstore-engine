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

#ifndef REBUILD_EM_H
#define REBUILD_EM_H

#include <string>
#include <map>
#include <ftw.h>

#include "../writeengine/dictionary/we_dctnry.h"
#include "calpontsystemcatalog.h"
#include "extentmap.h"
#include "IDBPolicy.h"
#include "IDBFileSystem.h"
#include "idbcompress.h"
#include "blocksize.h"
#include "we_convertor.h"
#include "we_fileop.h"
#include "we_blockop.h"
#include "IDBPolicy.h"
#include "we_chunkmanager.h"

using namespace idbdatafile;

namespace RebuildExtentMap
{
// This struct represents a FileId. For internal purpose only.
struct FileId
{
    FileId(uint32_t oid, uint32_t partition, uint32_t segment,
           uint32_t colWidth,
           execplan::CalpontSystemCatalog::ColDataType colDataType,
           int64_t lbid, uint64_t hwm, bool isDict)
        : oid(oid), partition(partition), segment(segment), colWidth(colWidth),
          colDataType(colDataType), lbid(lbid), hwm(hwm), isDict(isDict)
    {
    }

    uint32_t oid;
    uint32_t partition;
    uint32_t segment;
    uint32_t colWidth;
    execplan::CalpontSystemCatalog::ColDataType colDataType;
    int64_t lbid;
    uint64_t hwm;
    bool isDict;
};
std::ostream& operator<<(std::ostream& os, const FileId& fileID);

// This class represents extent map rebuilder.
class EMReBuilder
{
  public:
    EMReBuilder(bool verbose, bool display)
        : verbose(verbose), display(display)
    {
        // Initalize plugins.
        IDBPolicy::init(true, false, "", 0);
    }
    ~EMReBuilder() = default;

    // Collects extents from the given DBRoot path.
    int32_t collectExtents(const std::string& dbRootPath);

    // Clears collected extents.
    void clear() { extentMap.clear(); }

    // Specifies whether we need verbose to output.
    bool doVerbose() const { return verbose; }

    // Specifies whether we need just display a pipeline, but not actually run
    // it.
    bool doDisplay() const { return display; }

    // Returns the number of current DBRoot.
    uint32_t getDBRoot() const { return dbRoot; }

    // Retunrs a reference to `ExtentMap` object.
    BRM::ExtentMap& getEM() { return em; }

    // Checks if the given data specifies a dictionary file.
    static bool
    isDictFile(execplan::CalpontSystemCatalog::ColDataType colDataType,
               uint64_t width);

    // Checks if the given `value` is equal `emptyVaue` by specified width.
    static bool isEmptyValue(uint8_t* value, const uint8_t* emptyValue,
                             uint32_t width);

    // Check if the given dict block is empty.
    bool isEmptyDict(uint8_t* block);

    // Initializes system extents from the binary blob.
    // This function solves the problem related to system segment files.
    // Currently those files do not have file header, so we cannot
    // get the data (like width, colType, lbid) to restore an extent for this
    // particular segment file. The current approach is to keep a binary blob
    // of initial state of the system extents.
    // Returns -1 on error.
    int32_t initializeSystemExtents();

    // Rebuilds extent map from the collected map.
    int32_t rebuildExtentMap();

    // Search HWM in the given segment file.
    int32_t searchHWMInSegmentFile(
        uint32_t oid, uint32_t dbRoot, uint32_t partition, uint32_t segment,
        execplan::CalpontSystemCatalog::ColDataType colDataType,
        uint32_t width, uint64_t blocksCount, bool isDict, uint64_t& hwm);

    // Sets the dbroot to the given `number`.
    void setDBRoot(uint32_t number) { dbRoot = number; }

    // Shows the extent map.
    void showExtentMap();

  private:
    EMReBuilder(const EMReBuilder&) = delete;
    EMReBuilder(EMReBuilder&&) = delete;
    EMReBuilder& operator=(const EMReBuilder&) = delete;
    EMReBuilder& operator=(EMReBuilder&&) = delete;

    // Collects the information for extent from the given file and stores
    // it in `extentMap` set.
    int32_t collectExtent(const std::string& fullFileName);
    bool display;
    bool verbose;
    uint32_t dbRoot;
    BRM::ExtentMap em;
    std::vector<FileId> extentMap;
};

class ChunkManagerWrapper
{
  public:
    ChunkManagerWrapper(
        uint32_t oid, uint32_t dbRoot, uint32_t partition, uint32_t segment,
        execplan::CalpontSystemCatalog::ColDataType colDataType,
        uint32_t colWidth)
        : oid(oid), dbRoot(dbRoot), partition(partition), segment(segment),
          colDataType(colDataType), colWidth(colWidth), size(colWidth)
    {
    }

    virtual ~ChunkManagerWrapper()
    {
        if (pFileOp)
            delete pFileOp;
    }

    int32_t readBlock(uint32_t blockNumber)
    {
        auto rc = chunkManager.readBlock(pFile, blockData, blockNumber);
        if (rc != 0)
            return rc;
        return 0;
    }

    virtual bool isEmptyBlock() = 0;

  protected:
    uint32_t oid;
    uint32_t dbRoot;
    uint32_t partition;
    uint32_t segment;
    execplan::CalpontSystemCatalog::ColDataType colDataType;
    uint32_t colWidth;
    int32_t size;
    std::string fileName;
    WriteEngine::FileOp* pFileOp;
    // Note: We cannot use clear this pointer directly,  because
    // `ChunkManager` closes this file for us, otherwise we will get double
    // free error.
    IDBDataFile* pFile;
    WriteEngine::ChunkManager chunkManager;
    uint8_t blockData[WriteEngine::BYTE_PER_BLOCK];
};

class ChunkManagerWrapperColumn : public ChunkManagerWrapper
{
  public:
    ChunkManagerWrapperColumn(
        uint32_t oid, uint32_t dbRoot, uint32_t partition, uint32_t segment,
        execplan::CalpontSystemCatalog::ColDataType colDataType,
        uint32_t colWidth)
        : ChunkManagerWrapper(oid, dbRoot, partition, segment, colDataType,
                              colWidth)
    {
        pFileOp = new WriteEngine::FileOp();
        chunkManager.fileOp(pFileOp);
        // Open compressed column segment file. We will read block by block
        // from the compressed chunks.
        pFile = chunkManager.getColumnFilePtr(oid, dbRoot, partition, segment,
                                              colDataType, colWidth, fileName,
                                              "rb", size, false, false);
        if (!pFile)
        {
            throw std::bad_alloc();
        }

        emptyValue = pFileOp->getEmptyRowValue(colDataType, colWidth);
    }

    bool isEmptyBlock()
    {
        uint8_t* value = blockData;
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

  private:
    const uint8_t* emptyValue;
};

class ChunkManagerWrapperDict : public ChunkManagerWrapper
{
  public:
    ChunkManagerWrapperDict(
        uint32_t oid, uint32_t dbRoot, uint32_t partition, uint32_t segment,
        execplan::CalpontSystemCatalog::ColDataType colDataType,
        uint32_t colWidth)
        : ChunkManagerWrapper(oid, dbRoot, partition, segment, colDataType,
                              colWidth)
    {
        pFileOp = new WriteEngine::Dctnry();
        chunkManager.fileOp(pFileOp);
        // Open compressed dict segment file.
        pFile = chunkManager.getColumnFilePtr(oid, dbRoot, partition, segment,
                                              colDataType, colWidth, fileName,
                                              "rb", size, false, true);
        if (!pFile)
        {
            throw std::bad_alloc();
        }

        auto dictBlockHeaderSize =
            WriteEngine::HDR_UNIT_SIZE + WriteEngine::NEXT_PTR_BYTES +
            WriteEngine::HDR_UNIT_SIZE + WriteEngine::HDR_UNIT_SIZE;

        emptyBlock =
            WriteEngine::BYTE_PER_BLOCK - dictBlockHeaderSize;
    }

    bool isEmptyBlock() { return (*(uint16_t*) blockData) == emptyBlock; }

  private:
    uint32_t emptyBlock;
};

} // namespace RebuildExtentMap
#endif

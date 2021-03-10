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

#include "calpontsystemcatalog.h"
#include "extentmap.h"
#include "IDBPolicy.h"
#include "IDBFileSystem.h"

using namespace idbdatafile;

namespace RebuildExtentMap
{
struct FileId
{
    FileId(uint32_t oid, uint32_t partition, uint32_t segment,
           uint32_t colWidth,
           execplan::CalpontSystemCatalog::ColDataType colDataType,
           bool isDict)
        : oid(oid), partition(partition), segment(segment), colWidth(colWidth),
          colDataType(colDataType), isDict(isDict)
    {
    }

    uint32_t oid;
    uint32_t partition;
    uint32_t segment;
    uint32_t colWidth;
    execplan::CalpontSystemCatalog::ColDataType colDataType;
    bool isDict;
};

struct FileIdComparator
{
    bool operator()(const FileId& lhs, const FileId& rhs)
    {
        return lhs.oid < rhs.oid;
    }
};

// This class represents extent map rebuilder.
class EMReBuilder
{
  public:
    EMReBuilder(bool verbose, bool display)
        : verbose(verbose), display(display)
    {
        IDBPolicy::init(true, false, "", 0);
    }

    EMReBuilder(const EMReBuilder&) = delete;
    EMReBuilder(EMReBuilder&&) = delete;
    EMReBuilder& operator=(const EMReBuilder&) = delete;
    EMReBuilder& operator=(EMReBuilder&&) = delete;
    ~EMReBuilder() = default;

    // Sets the dbroot to the given `number`.
    void setDBRoot(uint32_t number) { dbRoot = number; }

    // Collects extents from the given DBRoot path.
    int32_t collectExtents(const std::string& dbRootPath);

    // Rebuilds extent map from the collected map.
    int32_t rebuildExtentMap();

    // Checks if the given data specifies a dictionary file.
    bool isDictFile(execplan::CalpontSystemCatalog::ColDataType colDataType,
                    uint64_t width);

    // Initializes system tables from the initial state.
    void initializeSystemTables();

    // Shows the extent map.
    void showExtentMap();

    void clear() { extentMap.clear(); }
    uint32_t getDBRoot() const { return dbRoot; }
    bool doVerbose() const { return verbose; }
    bool doDisplay() const { return display; }
    BRM::ExtentMap& getEM() { return em; }

  private:
    // Collect extent information from the given file.
    int32_t collectExtent(const std::string& fullFileName);
    BRM::ExtentMap em;
    bool verbose;
    bool display;
    uint32_t dbRoot;
    std::set<FileId, FileIdComparator> extentMap;
};

} // namespace RebuildExtentMap
#endif

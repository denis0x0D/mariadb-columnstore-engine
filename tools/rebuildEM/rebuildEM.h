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

namespace RebuildExtentMap {
// TODO:
struct FileId {
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

struct FileIdComparator {
    bool operator()(const FileId& lhs, const FileId& rhs)
    {
        return lhs.oid < rhs.oid;
    }
};

// TODO.
class EMBuilder
{
  public:
    EMBuilder(bool verbose, bool display) : verbose(verbose), display(display)
    {
        IDBPolicy::init(true, false, "", 0);
    }

    EMBuilder(const EMBuilder&) = delete;
    EMBuilder(EMBuilder&&) = delete;
    EMBuilder& operator=(const EMBuilder&) = delete;
    EMBuilder& operator=(EMBuilder&&) = delete;
    ~EMBuilder() = default;

    void setDBRoot(uint32_t number) { dbRoot = number; }
    int32_t collectExtent(const std::string& fullFileName);
    int32_t collectExtents(const std::string& fullFileName);
    int32_t rebuildEM();
    bool isDictFile(execplan::CalpontSystemCatalog::ColDataType colDataType,
                    uint64_t width);
    void initializeSystemTables();
    void showExtentMap();

    void clear() { extentMap.clear(); }
    uint32_t getDBRoot() const { return dbRoot; }
    bool doVerbose() const { return verbose; }
    bool doDisplay() const { return display; }
    BRM::ExtentMap& getEM() { return em; }

  private:
    BRM::ExtentMap em;
    bool verbose;
    bool display;
    uint32_t dbRoot;
    std::set<FileId, FileIdComparator> extentMap;
};

} // namespace RebuildExtentMap
#endif

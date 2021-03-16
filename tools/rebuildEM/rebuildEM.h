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

  public:
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
struct EMBuilder
{
    EMBuilder(bool verbose, bool display)
        : verbose_(verbose), display_(display)
    {
        IDBPolicy::init(true, false, "", 0);
    }

    EMBuilder(const EMBuilder&) = delete;
    EMBuilder(EMBuilder&&) = delete;
    EMBuilder& operator=(const EMBuilder&) = delete;
    EMBuilder& operator=(EMBuilder&&) = delete;
    ~EMBuilder() = default;
    /*
        static EMBuilder* instance()
        {
            // Initialize `EMBuilder` only once when call `instance` for the
            // first time.
            static EMBuilder* instance = new EMBuilder();
            return instance;
        }
        */

    void setVerbose(bool verbose) { verbose_ = verbose; }
    void setDisplay(bool display) { display_ = display; }
    void setDBRoot(uint32_t number) { dbRoot_ = number; }
    int32_t collectExtent(const std::string& fullFileName);
    int32_t collect(const std::string& fullFileName);
    int32_t rebuildEM();
    void initializeSystemTables()
    {
        extentMap_.load("/home/denis/task/BRM_saves_em", 0);
    }

    void showExtentMap()
    {
        std::cout << "range.start|range.size|fileId|blockOffset|HWM|partition|"
                     "segment|dbroot|width|status|hiVal|loVal|seqNum|isValid|"
                  << std::endl;
        extentMap_.dumpTo(std::cout);
    }

    int32_t walkDB(const char* fp, const struct stat* sb, int typeflag,
                   struct FTW* ftwbuf);
    void clear() { extentMap.clear(); }

    uint32_t getDBRoot() const { return dbRoot_; }
    bool verbose() const { return verbose_; }
    bool display() const { return display_; }
    BRM::ExtentMap& getEM() { return extentMap_; }

  private:

    BRM::ExtentMap extentMap_;
    bool verbose_{false};
    bool display_{false};
    uint32_t dbRoot_{1};
    std::set<FileId, FileIdComparator> extentMap;
};
} // namespace RebuildExtentMap
#endif

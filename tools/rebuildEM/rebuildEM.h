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

#include "calpontsystemcatalog.h"
#include "extentmap.h"

namespace RebuildExtentMap {
// This struct manages the global data.
// Actually it is a much safe to use singleton to manage global data,
// instead of defining it directly e.g. "When destructors are trivial, their
// execution is not subject to ordering at all (they are effectively not
// "run"); otherwise we are exposed to the risk of accessing objects after the
// end of their lifetime."
struct RebuildEMManager
{
    RebuildEMManager(const RebuildEMManager&) = delete;
    RebuildEMManager(RebuildEMManager&&) = delete;
    RebuildEMManager& operator=(const RebuildEMManager&) = delete;
    RebuildEMManager& operator=(RebuildEMManager&&) = delete;
    ~RebuildEMManager() = delete;

    static RebuildEMManager* instance()
    {
        // Initialize `RebuildEMManager` only once when call `instance` for the
        // first time.
        static RebuildEMManager* instance = new RebuildEMManager();
        return instance;
    }

    void setVerbose(bool verbose) { verbose_ = verbose; }
    void setDisplay(bool display) { display_ = display; }
    void setDBRoot(uint32_t number) { dbRoot_ = number; }

    uint32_t getDBRoot() const { return dbRoot_; }
    bool verbose() const { return verbose_; }
    bool display() const { return display_; }
    BRM::ExtentMap& getEM() { return extentMap_; }

  private:
    RebuildEMManager() = default;

    // FIXME: Should we call destructor for the ExtentMap?
    BRM::ExtentMap extentMap_;
    bool verbose_{false};
    bool display_{false};
    uint32_t dbRoot_{1};
};

// Rebuild extention map by the given `fullFileName`
// `fullFileName` should include full path to the segment file
// e.g. syscat + DBRoot + filename.
int32_t rebuildEM(const std::string& fullFileName);

} // namespace RebuildExtentMap
#endif

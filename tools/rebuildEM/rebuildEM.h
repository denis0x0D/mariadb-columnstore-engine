#ifndef REBUILD_EM_H
#define REBUILD_EM_H

#include <iostream>
#include "calpontsystemcatalog.h"
#include "configcpp.h"
#include "extentmap.h"
#include "we_convertor.h"
#include "idbcompress.h"
#include "we_fileop.h"
#include <ftw.h>
#include "IDBDataFile.h"
#include "BufferedFile.h"
#include "IDBPolicy.h"
#include "IDBFileSystem.h"

// This struct represents a manager class, which manages global data.
// Actually it is a much safe to use singleton to manage global data,
// instead of defining it directly. "When destructors are trivial, their
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
        static RebuildEMManager* instance = new RebuildEMManager();
        return instance;
    }

    void setDBRoot(const std::string& dbRoot) { dbRootName_ = dbRoot; }
    void setDBroot(uint32_t dbRootNumber) { dbRootNumber_ = dbRootNumber; }
    void setVerbose(bool verbose) { verbose_ = verbose; }
    void setDisplay(bool display) { display_ = display; }
    void setDBRootNumber(uint32_t number) { dbRootNumber_ = number; }

    const std::string& getDBRoot() const { return dbRootName_; }
    uint32_t getDBRootNumber() const { return dbRootNumber_; }
    bool doVerbose() const { return verbose_; }
    bool doDisplay() const { return display_; }
    BRM::ExtentMap& getEM() { return extentMap_; }

  private:
    RebuildEMManager() = default;

    BRM::ExtentMap extentMap_;
    bool verbose_{false};
    bool display_{false};
    std::string dbRootName_;
    uint32_t dbRootNumber_;
};

int32_t rebuildEM(const std::string& fullFileName);
#endif

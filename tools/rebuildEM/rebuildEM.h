#ifndef REBUILD_EM_H
#define REBUILD_EM_H

#include <string>
#include "extentmap.h"

namespace RebuildExtentMap {
// This struct represents a manager class, which manages global data.
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
        static RebuildEMManager* instance = new RebuildEMManager();
        return instance;
    }

    void setVerbose(bool verbose) { verbose_ = verbose; }
    void setDisplay(bool display) { display_ = display; }
    void setDBRoot(uint32_t number) { dbRoot_ = number; }

    uint32_t getDBRoot() const { return dbRoot_; }
    bool doVerbose() const { return verbose_; }
    bool doDisplay() const { return display_; }
    BRM::ExtentMap& getEM() { return extentMap_; }

  private:
    RebuildEMManager() = default;

    BRM::ExtentMap extentMap_;
    bool verbose_{false};
    bool display_{false};
    uint32_t dbRoot_;
};

int32_t rebuildEM(const std::string& fullFileName);

} // namespace RebuildExtentMap
#endif

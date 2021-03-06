/*****************************************************************************
* $Id: main.cpp 1739 2012-03-22 12:57:59Z pleblanc $
*
*****************************************************************************/
#include <iostream>
#include <unistd.h>
#include <ftw.h>
#include <cassert>

#include "calpontsystemcatalog.h"
#include "configcpp.h"
#include "extentmap.h"
#include "we_convertor.h"
#include "idbcompress.h"
#include "we_fileop.h"

#include "IDBDataFile.h"
#include "BufferedFile.h"
#include "IDBPolicy.h"
#include "IDBFileSystem.h"

using namespace idbdatafile;

namespace
{
struct RebuildEMManager;
using RM = RebuildEMManager;

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

void usage(const string& pname)
{
    std::cout << "usage: " << pname << " [-vdh]" << std::endl;
    std::cout
        << "rebuilds the extent map from the contents of the database file "
           "system."
        << std::endl;
    std::cout
        << "   -v display verbose progress information. More v's, more debug"
        << std::endl;
    std::cout << "   -d display what would be done--don't do it" << std::endl;
    std::cout << "   -h display this help text" << std::endl;
}

int walkDB(const char* fp, const struct stat* sb, int typeflag, struct FTW* ftwbuf)
{
    if (typeflag != FTW_F)
    {
        return FTW_CONTINUE;
    }
    std::string fullFileName = fp;

    uint32_t oid;
    uint32_t partition;
    uint32_t segment;
    // Initialize oid, partition and segment from the given `fullFileName`.
    auto rc = WriteEngine::Convertor::fileName2Oid(fullFileName, oid,
                                                   partition, segment);
    if (rc != 0)
    {
        return FTW_CONTINUE;
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
        if (RM::instance()->doVerbose())
        {
            std::cout << "Cannot open file " << fullFileName << std::endl;
        }
        return FTW_CONTINUE;
    }

    rc = dbFile->seek(0, 0);
    if (rc != 0)
    {
        return FTW_CONTINUE;
    }

    // Read and verify header.
    WriteEngine::FileOp fileOp;
    char fileHeader[compress::IDBCompressInterface::HDR_BUF_LEN * 2];
    rc = fileOp.readHeaders(dbFile, fileHeader);
    if (rc != 0)
    {
        if (RM::instance()->doVerbose())
        {
            std::cout << "Cannot read file header from the file " << fullFileName << std::endl;
        }
        return FTW_CONTINUE;
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
        // Create a column extent for the given oid, partition, segment, dbroot
        // and column width.
        RM::instance()->getEM().createColumnExtentExactFile(
            oid, colWidth, RM::instance()->getDBRootNumber(), partition,
            segment, colDataType, lbid, allocdSize, startBlockOffset);

        RM::instance()->getEM().confirmChanges();
    }

    if (RM::instance()->doVerbose())
    {
        std::cout << "Created, allocated size " << allocdSize
                  << " starting LBID " << lbid << " for OID " << oid
                  << std::endl;
    }

    return FTW_CONTINUE;
}
} // namespace


int main(int argc, char** argv)
{
    int32_t c;
    std::string pname(argv[0]);

    while ((c = getopt(argc, argv, "vdh")) != EOF)
    {
        switch (c)
        {
            case 'v':
                RM::instance()->setVerbose(true);
                break;

            case 'd':
                RM::instance()->setDisplay(true);
                break;

            case 'h':
            case '?':
            default:
                usage(pname);
                return (c == 'h' ? 0 : 1);
                break;
        }
    }

    // Make config from default path.
    // FIXME: Should we allow user to specify a path to config?
    auto* config = config::Config::makeConfig();
    std::string dbRootCount = config->getConfig("SystemConfig", "DBRootCount");

    // Read the number of DBRoots.
    uint32_t count = config->uFromText(dbRootCount);
    idbdatafile::IDBPolicy::init(true, false, "", 0);

    // Iterate over DBRoots starting from the first one.
    for (uint32_t dbRootNumber = 1; dbRootNumber <= count; ++dbRootNumber)
    {
        std::string dbRootName = "DBRoot" + std::to_string(dbRootNumber);
        RM::instance()->setDBRoot(
            config->getConfig("SystemConfig", dbRootName));
        RM::instance()->setDBRootNumber(dbRootNumber);

        if (RM::instance()->doVerbose())
        {
            std::cout << "Using DBRoot " << RM::instance()->getDBRoot()
                      << std::endl;
        }

        if (access(RM::instance()->getDBRoot().c_str(), X_OK) != 0)
        {
            std::cerr << "Could not scan DBRoot "
                      << RM::instance()->getDBRoot() << '!' << std::endl;
            return 1;
        }

        if (nftw(RM::instance()->getDBRoot().c_str(), walkDB, 64,
                 FTW_PHYS | FTW_ACTIONRETVAL) != 0)
        {
            std::cerr << "Error processing files in DBRoot "
                      << RM::instance()->getDBRoot() << '!' << std::endl;
            return 1;
        }
    }

    return 0;
}

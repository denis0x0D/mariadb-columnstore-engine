/*****************************************************************************
* $Id: main.cpp 1739 2012-03-22 12:57:59Z pleblanc $
*
*****************************************************************************/
#include <iostream>
#include <string>
#include <ftw.h>

#include "configcpp.h"
#include "rebuildEM.h"
#include "IDBPolicy.h"
#include "IDBFileSystem.h"

using namespace idbdatafile;
using namespace RebuildExtentMap;
using RM = RebuildEMManager;

int32_t walkDB(const char* fp, const struct stat* sb, int typeflag,
               struct FTW* ftwbuf)
{
    if (typeflag != FTW_F)
    {
        return FTW_CONTINUE;
    }
    (void) sb;
    (void) ftwbuf;

    (void) rebuildEM(fp);

    return FTW_CONTINUE;
}

static void usage(const string& pname)
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
    std::string count = config->getConfig("SystemConfig", "DBRootCount");

    // Read the number of DBRoots.
    uint32_t dbRootCount = config->uFromText(count);
    IDBPolicy::init(true, false, "", 0);

    // Iterate over DBRoots starting from the first one.
    for (uint32_t dbRootNumber = 1; dbRootNumber <= dbRootCount;
         ++dbRootNumber)
    {
        std::string dbRootName = "DBRoot" + std::to_string(dbRootNumber);
        auto dbRootPath = config->getConfig("SystemConfig", dbRootName);
        RM::instance()->setDBRoot(dbRootNumber);

        if (RM::instance()->doVerbose())
        {
            std::cout << "Using DBRoot " << dbRootPath << std::endl;
        }

        if (access(dbRootPath.c_str(), X_OK) != 0)
        {
            std::cerr << "Could not scan DBRoot " << dbRootPath << std::endl;
            return 1;
        }

        if (nftw(dbRootPath.c_str(), walkDB, 64,
                 FTW_PHYS | FTW_ACTIONRETVAL) != 0)
        {
            std::cerr << "Error processing files in DBRoot " << dbRootPath
                      << std::endl;
            return 1;
        }
    }

    return 0;
}

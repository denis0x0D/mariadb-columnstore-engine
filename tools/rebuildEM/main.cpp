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

static void usage(const string& pname)
{
    std::cout << "usage: " << pname << " [-vdhsi]" << std::endl;
    std::cout
        << "rebuilds the extent map from the contents of the database file "
           "system."
        << std::endl;
    std::cout
        << "   -v display verbose progress information. More v's, more debug"
        << std::endl;
    std::cout << "   -d display what would be done--don't do it" << std::endl;
    std::cout << "   -h display this help text" << std::endl;
    std::cout << "   -s show extent map and quit" << std::endl;
}

int main(int argc, char** argv)
{
    int32_t c;
    std::string pname(argv[0]);
    bool showExtentMap = false;
    bool verbose = false;
    bool display = false;

    while ((c = getopt(argc, argv, "vdhsi")) != EOF)
    {
        switch (c)
        {
            case 'v':
                verbose = true;
                break;

            case 'd':
                display = true;
                break;

            case 's':
                showExtentMap = true;
                break;

            case 'h':
            case '?':
            default:
                usage(pname);
                return (c == 'h' ? 0 : 1);
                break;
        }
    }

    EMReBuilder emReBuilder(verbose, display);
    // Just show EM and quit.
    if (showExtentMap)
    {
        emReBuilder.showExtentMap();
        return 0;
    }

    emReBuilder.initializeSystemTables();
    // Make config from default path.
    // FIXME: Should we allow user to specify a path to config?
    auto* config = config::Config::makeConfig();
    std::string count = config->getConfig("SystemConfig", "DBRootCount");

    // Read the number of DBRoots.
    uint32_t dbRootCount = config->uFromText(count);

    // Iterate over DBRoots starting from the first one.
    for (uint32_t dbRootNumber = 1; dbRootNumber <= dbRootCount;
         ++dbRootNumber)
    {
        std::string dbRootName = "DBRoot" + std::to_string(dbRootNumber);
        auto dbRootPath = config->getConfig("SystemConfig", dbRootName);
        emReBuilder.setDBRoot(dbRootNumber);

        if (verbose)
        {
            std::cout << "Using DBRoot " << dbRootPath << std::endl;
        }

        emReBuilder.collectExtents(dbRootPath.c_str());
        emReBuilder.rebuildExtentMap();
        emReBuilder.clear();
    }

    return 0;
}

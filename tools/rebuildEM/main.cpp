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
using RM = EMBuilder;
/*
class StreamReader
{
  public:
    StreamReader(const vectror<uint8_t>& data) : data(data) {}
    uint32_t read(uint8_t* ptr, uint32_t size) {

    }

  private:
    const vector<uint8_t>& data;
};
*/

int32_t walkDB(const char* fp, const struct stat* sb, int typeflag,
               struct FTW* ftwbuf)
{
    if (typeflag != FTW_F)
    {
        return FTW_CONTINUE;
    }
    (void) sb;
    (void) ftwbuf;

    (void) RM::instance()->collect(fp);

    return FTW_CONTINUE;
}

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

static void header() {
    std::cout << "range.start|range.size|fileId|blockOffset|HWM|partition|"
                 "segment|dbroot|width|status|hiVal|loVal|seqNum|isValid|"
              << std::endl;
}

int main(int argc, char** argv)
{
    int32_t c;
    std::string pname(argv[0]);
    bool showExtentMap = false;
    bool initSysCat = false;

    while ((c = getopt(argc, argv, "vdhsi")) != EOF)
    {
        switch (c)
        {
            case 'v':
                RM::instance()->setVerbose(true);
                break;

            case 'd':
                RM::instance()->setDisplay(true);
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

    // Just show EM and quit.
    if (showExtentMap)
    {
        header();
        RM::instance()->getEM().dumpTo(std::cout);
        return 0;
    }

    IDBPolicy::init(true, false, "", 0);
    RM::instance()->getEM().load("/home/denis/task/BRM_saves_em", 0);

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
        RM::instance()->setDBRoot(dbRootNumber);

        if (RM::instance()->verbose())
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
        RM::instance()->rebuildEM();
    }

    return 0;
}

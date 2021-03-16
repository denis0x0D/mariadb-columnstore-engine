/* Copyright (C) 2020 MariaDB Corporation

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

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "we_convertor.h"
#include "we_fileop.h"
#include "rebuildEM.h"

using namespace idbdatafile;
using namespace WriteEngine;
using namespace RebuildExtentMap;
using RM = RebuildEMManager;

class RebuildEMTest : public ::testing::Test
{
protected:
  struct FileId
  {
      FileId() : oid(0), partition(0), segment(0) {}
      FileId(uint32_t oid, uint32_t partition, uint32_t segment)
          : oid(oid), partition(partition), segment(segment)
      {
      }
      uint32_t oid;
      uint32_t partition;
      uint32_t segment;
  };

  static uint32_t getOid(uint32_t a, uint32_t b, uint32_t c, uint32_t d) {
    uint32_t oid = 0;
    oid |= a << 24;
    oid |= b << 16;
    oid |= c << 8;
    oid |= d;
    return oid;
  }
};

TEST_F(RebuildEMTest, File2OidCheckFileFormatTest)
{
    // Valid file names.
    auto aFileName = "001.dir/002.dir/003.dir/004.dir/005.dir/FILE006.cdf";
    FileId aExpected(getOid(1, 2, 3, 4), 5, 6);

    auto bFileName = "/011.dir/022.dir/033.dir/044.dir/055.dir/FILE066.cdf";
    FileId bExpected(getOid(11, 22, 33, 44), 55, 66);

    auto cFileName =
        "data1/255.dir/255.dir/255.dir/255.dir/255.dir/FILE255.cdf";
    FileId cExpected(getOid(255, 255, 255, 255), 255, 255);

    auto dFileName =
        "/data1/000.dir/000.dir/000.dir/001.dir/001.dir/FILE001.cdf";
    FileId dExpected(getOid(0, 0, 0, 1), 1, 1);

    auto eFileName = "/data0/data1/data2/data3/data4/data5/data6/../../../../"
                     "../data1/000.dir/000.dir/000.dir/000.dir/"
                     "000.dir/FILE000.cdf";
    FileId eExpected(getOid(0, 0, 0, 0), 0, 0);

    auto fFileName = "data1/data2/data3/data4/data5/data6/data7/000.dir/"
                     "000.dir/008.dir/028.dir/000.dir/FILE079.cdf";
    FileId fExpected(getOid(0, 0, 8, 28), 00, 79);

    std::vector<std::pair<std::string, FileId>> expectedFileIds = {
        make_pair(aFileName, aExpected), make_pair(bFileName, bExpected),
        make_pair(cFileName, cExpected), make_pair(dFileName, dExpected),
        make_pair(eFileName, eExpected), make_pair(fFileName, fExpected)};

    for (const auto& expectedPair : expectedFileIds)
    {
        FileId calculated;
        auto rc = WriteEngine::Convertor::fileName2Oid(
            expectedPair.first, calculated.oid, calculated.partition,
            calculated.segment);

        ASSERT_EQ(rc, 0);
        EXPECT_EQ(expectedPair.second.oid, calculated.oid);
        EXPECT_EQ(expectedPair.second.partition, calculated.partition);
        EXPECT_EQ(expectedPair.second.segment, calculated.segment);
    }

    // Invalid file names.
    // Dir exceed 255.
    auto aInvalidFileName =
        "256.dir/000.dir/001.dir/002.dir/003.dir/FILE004.cdf";
    // Segment exceed 255.
    auto bInvalidFileName =
        "000.dir/000.dir/001.dir/002.dir/003.dir/FILE256.cdf";
    // Just a random path.
    auto cInvalidFileName = "/usr////bin//lib///";
    // Empty string.
    auto dInvalidFileName = "";
    // Does not have A dir.
    auto eInvalidFileName =
        "/data1/000.dir/001.dir/002.dir/003.dir/FILE000.cdf";
    // Invalid partition name dir.
    auto fInvalidFileName =
        "/000.dir/000.dir/001.dir/002.dir/003.ir/FILE000.cdf";
    // Invalid segment name.
    auto gInvalidFileName =
        "/000.dir/000.dir/001.dir/002.dir/003.ir/FILE00.cdf";
    // Invalid dir name.
    auto hInvalidFileName = "/00.dir/00.dir/001.dir/002.dir/003.ir/FIE000.cdf";
    // Invalid amount of dirs.
    auto iInvalidFileName = "/002.dir/003.ir/FIE000.cdf";

    std::vector<std::string> invalidFileNames = {
        aInvalidFileName, bInvalidFileName, cInvalidFileName,
        dInvalidFileName, eInvalidFileName, fInvalidFileName,
        gInvalidFileName, hInvalidFileName, iInvalidFileName};

    for (const auto& invalidFileName : invalidFileNames)
    {
        FileId calculated;
        auto rc = WriteEngine::Convertor::fileName2Oid(
            invalidFileName, calculated.oid, calculated.partition,
            calculated.segment);
        ASSERT_NE(rc, 0);
    }
}

TEST_F(RebuildEMTest, File2OidCalculationTest)
{
    char dbDirName[20][20];
    char fileName[64];

    for (uint32_t i = 3; i < 256; ++i)
    {
        FileId expectedFileId(getOid(i, i - 1, i - 2, i - 3), i, i);
        memset(fileName, 0, 64);
        // Generate the filename by the oid, partition and segment.
        auto rc = WriteEngine::Convertor::oid2FileName(
            expectedFileId.oid, fileName, dbDirName, expectedFileId.partition,
            expectedFileId.segment);
        ASSERT_EQ(rc, 0);

        FileId calculatedFileId;
        // Generate an oid, partition and segment from the given file name.
        rc = WriteEngine::Convertor::fileName2Oid(
            fileName, calculatedFileId.oid, calculatedFileId.partition,
            calculatedFileId.segment);
        ASSERT_EQ(rc, 0);
        EXPECT_EQ(expectedFileId.oid, calculatedFileId.oid);
        EXPECT_EQ(expectedFileId.partition, calculatedFileId.partition);
        EXPECT_EQ(expectedFileId.segment, calculatedFileId.segment);
    }
}

// Currently we cannot delete on oid created for the file outside the systemcat
// written in config file, for exampe following pipeline will not work for us:
// filename - valid `ColumnStore` file name.
// `fileOp.createDir("/tmp/dbroot/*);`
// `fileOp.createFile("/tmp/dbroot" + filename)`
// `oid = file2oid("000.dir/*")`
// `deleteOID(OID)` -> throws an exception.
// So currently this test should be run by user with a `write access` to
// systemcat.
#define REBUILD_EM_UT_USER_CAN_WRITE_TO_SYSCAT
#define DEBUG_REBUILD_EM_UT
#ifdef REBUILD_EM_UT_USER_CAN_WRITE_TO_SYSCAT
/*
TEST_F(RebuildEMTest, rebuildExtentMap)
{
    WriteEngine::FileOp fileOp;
    WriteEngine::BlockOp blockOp;
    IDBPolicy::init(true, false, "", 0);
    fileOp.compressionType(1);

    const uint8_t* emptyVal =
        blockOp.getEmptyRowValue(execplan::CalpontSystemCatalog::BIGINT, 8);
    int32_t width =
        blockOp.getCorrectRowWidth(execplan::CalpontSystemCatalog::BIGINT, 8);
    int32_t nBlocks = INITIAL_EXTENT_ROWS_TO_DISK / BYTE_PER_BLOCK * width;

    uint32_t dbRoot = 1;
    // FIXME: How to choose right oid and make sure the system does not have
    // the same in use.
    uint32_t oid = getOid(0, 255, 255, 255);
    uint32_t partition = 0;
    uint32_t segment = 0;
    int32_t allocSize;

    bool fileExists = fileOp.exists(oid, dbRoot, partition, segment);
    ASSERT_EQ(fileExists, false);

    auto rc = fileOp.createFile(oid, allocSize, dbRoot, partition,
                                execplan::CalpontSystemCatalog::BIGINT,
                                emptyVal, width);
    ASSERT_EQ(rc, 0);

#ifdef DEBUG_REBUILD_EM_UT
    RM::instance()->getEM().dumpTo(std::cout);
    std::cout << std::endl;
#endif
    // Delete Extent by OID.
    try
    {
        RM::instance()->getEM().deleteOID(oid);
    }
    catch (std::exception& e)
    {
        // This is really nightmare to debug, if we fogret to release `WRITE`
        // lock.
        RM::instance()->getEM().undoChanges();
        fileOp.deleteFile(oid);
        std::cerr << e.what() << std::endl;
        ASSERT_EQ(1, 0);
    }
    RM::instance()->getEM().confirmChanges();

#ifdef DEBUG_REBUILD_EM_UT
    RM::instance()->getEM().dumpTo(std::cout);
    std::cout << std::endl;
#endif

    // Get the filename.
    char fileName[64];
    char dbDirName[20][20];
    rc = WriteEngine::Convertor::oid2FileName(oid, fileName, dbDirName,
                                              partition, segment);
    ASSERT_EQ(rc, 0);

    // Initialize.
    auto* config = config::Config::makeConfig();
    std::string filePath =
        config->getConfig("SystemConfig", "DBRoot" + std::to_string(dbRoot));

    RM::instance()->setDBRoot(dbRoot);
    RM::instance()->setVerbose(true);
    std::string fullFileName = filePath + "/" + fileName;

    // Rebuild extent map.
    rc = RebuildExtentMap::rebuildEM(fullFileName);
    ASSERT_EQ(rc, 0);
#ifdef DEBUG_REBUILD_EM_UT
    RM::instance()->getEM().dumpTo(std::cout);
    std::cout << endl;
#endif
    rc = fileOp.deleteFile(fullFileName.c_str());
    EXPECT_EQ(rc, 0);
}
*/
#endif

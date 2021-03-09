#include "rebuildEM.h"

using namespace idbdatafile;

struct RebuildEMManager;
using RM = RebuildEMManager;

int32_t rebuildEM(const std::string& fullFileName)
{
    uint32_t oid;
    uint32_t partition;
    uint32_t segment;
    // Initialize oid, partition and segment from the given `fullFileName`.
    auto rc = WriteEngine::Convertor::fileName2Oid(fullFileName, oid,
                                                   partition, segment);
    if (rc != 0)
    {
        return rc;
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
        return -1;
    }

    rc = dbFile->seek(0, 0);
    if (rc != 0)
    {
        return rc;
    }

    // Read and verify header.
    WriteEngine::FileOp fileOp;
    char fileHeader[compress::IDBCompressInterface::HDR_BUF_LEN * 2];
    rc = fileOp.readHeaders(dbFile, fileHeader);
    if (rc != 0)
    {
        if (RM::instance()->doVerbose())
        {
            // FIXME: Some preinstalled files have invalid MAGIC number? How is
            // it possible?
            std::cout << "Cannot read file header from the file " << fullFileName << std::endl;
        }
        return rc;
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

    return 0;
}

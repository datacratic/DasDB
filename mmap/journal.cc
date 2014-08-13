/* journal.cc                                                      -*- C++ -*-
   Rémi Attab, 12 April 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Handles writes to the mmap file in a safe and consistent manner.
*/

#include "journal.h"
#include "jml/utils/exc_check.h"
#include "jml/utils/guard.h"

#include <sys/stat.h>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <array>
#include <algorithm>
#include <iterator>
#include <iostream>

using namespace std;
using namespace ML;

namespace Datacratic {
namespace MMap {


static const uint64_t JOURNAL_HEADER = 0x4C4E524A50414D4DULL;
static const uint64_t COMMIT_MARKER = 0xFFEEDDCCCCDDEEFFULL;


/*****************************************************************************/
/* JOURNAL                                                                   */
/*****************************************************************************/

Journal::
Journal(int targetFd, const string& filename) :
    filename(filename),
    journalStream(filename, ios::out | ios::binary | ios::trunc),
    targetFd(targetFd)
{
    ExcCheckErrno(!journalStream.fail(), "Failed to open the journal stream");

    journalStream.write(
            reinterpret_cast<const char*>(&JOURNAL_HEADER),
            sizeof(JOURNAL_HEADER));
}

void
Journal::
addEntry(uint64_t offset, uint64_t size, const void* newData)
{
    // \todo Should probably make sure the offset is correctly aligned
    //       to make good use of the cache.
    enum { CHUNK = cache_line };

    // Diff the data to split into smaller chunks (saves on space).
    array<char, CHUNK> fileBuffer;
    vector<char> oldData;
    int64_t startChunk = -1; // used for coalescing chunks.

    for (uint64_t i = 0; i < size; i += CHUNK) {
        // make sure we don't read pass the end.
        uint64_t readSize = CHUNK;
        if (i + CHUNK > size) readSize = size - i;

        ssize_t res = pread(targetFd, fileBuffer.data(), readSize, offset + i);
        ExcCheckErrno(res != -1, "Failed to read the target file.");
        ExcCheckEqual(res, readSize, "Failed to read enough bytes.");

        const char* memStart = ((const char*) newData) + i;
        bool match = equal(memStart, memStart + readSize, fileBuffer.data());

        if (!match) {
            oldData.insert(
                    oldData.end(),
                    fileBuffer.begin(), fileBuffer.begin() + readSize);
        }

        // Coalesce adjacent modified chunks.
        if (startChunk < 0 && !match) {
            startChunk = i;
        }
        if (startChunk >= 0 && (match || i + CHUNK >= size)) {
            const char* memOffset = ((const char*)newData) + startChunk;
            uint64_t fileOffset = offset + startChunk;

            // cerr << "CHUNK: "
            //     << "start=" << fileOffset
            //     << ", size=" << oldData.size()
            //     << ", mem=" << ((void*) memOffset)
            //     << endl;

            // Will be used when applying the modifications.
            Entry entry = { fileOffset, oldData.size(),  memOffset };
            journal.push_back(entry);

            journalStream.write(
                    reinterpret_cast<char*>(&entry.offset),
                    sizeof(entry.offset));
            journalStream.write(
                    reinterpret_cast<char*>(&entry.size),
                    sizeof(entry.size));

            journalStream.write(&oldData[0], oldData.size());

            // Reset the state.
            startChunk = -1;
            oldData.clear();
        }
    }
}

uint64_t
Journal::
applyToTarget()
{
    if (journal.empty()) {
        unlink(filename.c_str());
        return 0;
    }

    // Finalize the journal.
    {
        journalStream.flush();

        // we need an fd to fsync and the stream doesn't have one visible...
        int journalFd = open(filename.c_str(), O_RDONLY);
        ExcCheckErrno(journalFd != -1, "Failed to open journal for fsync");
        Call_Guard closeGuard([&]{ close(journalFd); });

        int res = fdatasync(journalFd);
        ExcCheckErrno(res != -1, "Failed to fsync the journal");

        // Indicates that the journal is complete.
        journalStream.write(
                reinterpret_cast<const char*>(&COMMIT_MARKER),
                sizeof(COMMIT_MARKER));
        journalStream.flush();

        res = fdatasync(journalFd);
        ExcCheckErrno(res != -1, "Failed to fsync the commit marker");
    }

    uint64_t writeCount = 0;

    for (auto it = journal.begin(), end = journal.end(); it != end; ++it) {
        ssize_t res = pwrite(targetFd, it->newData, it->size, it->offset);
        ExcCheckErrno(res != -1, "Failed to write to the target");
        ExcCheckEqual(res, it->size, "Failed to write all bytes to the target");

        writeCount += it->size;
    }

    int res = fdatasync(targetFd);
    ExcCheckErrno(res != -1, "Failed to fsync the target");

    journalStream.close();

    // Delete the journal so that we don't try to undo it.
    res = unlink(filename.c_str());
    ExcCheckErrno(res != -1, "Failed to unlink the journal");

    return writeCount;
}

uint64_t
Journal::
undo(int targetFd, const string& journalFile)
{
    // If the journal doesn't exist, don't do anything.
    {
        struct stat s;
        if (stat(journalFile.c_str(), &s)) return 0;
    }

    struct UndoEntry : public Entry {
        vector<char> oldData;
    };
    vector<UndoEntry> journal;

    fstream journalStream (journalFile, ios::in | ios::binary);
    bool foundMarker = false;

    uint64_t header = 0;
    journalStream.read(reinterpret_cast<char*>(&header), sizeof(header));

    // \todo Should we throw in this case?
    // It could be a corrupted journal just like it could be a random file.
    if (header != JOURNAL_HEADER) {
        // cerr << "BAD HEADER: " << hex << header << dec << endl;
        return 0;
    }

    // Read the journal and look for the commit marker.
    while (journalStream && journalStream.good()) {
        uint64_t value = 0;
        journalStream.read(reinterpret_cast<char*>(&value), sizeof(value));

        if (value == COMMIT_MARKER) {
            foundMarker = true;
            break;
        }

        UndoEntry entry;
        entry.offset = value;
        entry.size = 0;

        journalStream.read(
                reinterpret_cast<char*>(&entry.size),
                sizeof(entry.size));

        entry.oldData.resize(entry.size);
        journalStream.read(&entry.oldData[0], entry.size);

        journal.push_back(entry);
    };

    uint64_t writeCount = 0;
    if (foundMarker) {

        // If we found our commit marker then it's safe to write the journal
        // to the file.
        for (auto it = journal.begin(), end = journal.end(); it != end; ++it) {
            ssize_t res =
                pwrite(targetFd, &it->oldData[0], it->size, it->offset);

            ExcCheckErrno(res != -1, "Failed to write to the target");
            ExcCheckEqual(res, it->size,
                    "Failed to write all the bytes to the target");

            writeCount += it->size;
        }
    }

    int res = fdatasync(targetFd);
    ExcCheckErrno(res != -1, "Failed to fsync the target");

    // Delete the journal so that we don't try to undo it.
    res = unlink(journalFile.c_str());
    ExcCheckErrno(res != -1, "Failed to unlink the journal");

    return writeCount;
}


} // namespace MMap
} // namespace Datacratic

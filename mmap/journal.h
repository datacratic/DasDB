/* journal.h                                                      -*- C++ -*-
   Rémi Attab, 12 April 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Handles writes to a target file from a memory range in a safe and consistent
   manner.
*/


#ifndef __storage__journal_h__
#define __storage__journal_h__

#include "mmap_const.h"

#include <string>
#include <vector>
#include <fstream>

namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* JOURNAL                                                                   */
/*****************************************************************************/

/** Write ahead journal used to safefly write a target file from a memory
    region.

    Journaling is done bye a series of calls to addEntry() which keeps track of
    all the items that needs to be written to the disk. A write operation can
    then be comitted to disk by calling applyToTarget(). A partial write to
    the target file can be undoned by calling undo().

    Note that this class is not thread-safe. This means that the memory region
    and the target file should not be modified in any way while journalling.

    \todo Could add a crc counter to each entry to make sure that the journal
          data isn't corrupted.
*/
struct Journal {

    /** Creates a new journal for the target fd and using the given filename. */
    Journal(int targetFd, const std::string& filename);

    /** Logs the data at the given offset with the given size in the journal.
        The data pointed to by newData of the given size will eventually be
        written to the target file.
    */
    void addEntry(uint64_t offset, uint64_t size, const void* newData);

    /** Executes all the logged write operations.
        In the event that this call is interrupted, calling undo() will undo 
        any partial writes.

        Note that a Journal object is no longer usable after calling this 
        method. To log more writes, create a new Journal object.

        Returns the number of bytes written in the target file.
    */
    uint64_t applyToTarget();

    /** Undoes any partial writes made to a target file using a journal.
        Note that this can be safely called even if the journal is in a 
        incomplete or corrupted state. In this case, the target file is assumed
        to be consistent and no writes are made to it.

        Returns the number of bytes written in the target file.
    */
    static uint64_t undo(int targetFd, const std::string& journalFile);

private:

    struct Entry {
        uint64_t offset;
        uint64_t size;
        const char* newData;
    };
    std::vector<Entry> journal;

    std::string filename;
    std::fstream journalStream;
    int targetFd;
};


} // namespace MMap
} // namespace Datacratic

#endif /* __storage__journal_h__ */

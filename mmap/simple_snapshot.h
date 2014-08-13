/** simple_snapshot.h                                 -*- C++ -*-
    Rémi Attab, 31 Aug 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Simpler experimental snapshotting that uses the dirty page table to avoid
    all the mprotect kernel bugs.

*/

#ifndef __mmap__simple_snapshot_h__
#define __mmap__simple_snapshot_h__

#include "dirty_page_table.h"

#include <string>

namespace Datacratic {
namespace MMap {

/******************************************************************************/
/* SIMPLE SNAPSHOT                                                            */
/******************************************************************************/

/** Alternative to the Snapshot class that uses userland page tracking instead
    of (buggy) hardware-assisted kernel trickery.
 */
struct SimpleSnapshot
{
    /** Forks the current process in order to have a consistent CoW view of the
        memory region to be written to disk.
     */
    SimpleSnapshot(DirtyPageTable& dirtyPages, const std::string& journalFile);

    /** Terminate the snapshot process. */
    ~SimpleSnapshot();

    /** Syncs the content of the mmap-ed file with the conent of the memory
        region.
    */
    size_t sync(int fd, size_t foffset, void* start, size_t len);

private:

    int runChild(int controlFd);
    size_t doSync(int fd, size_t foffset, char* start, size_t len);

    DirtyPageTable& dirtyPages;
    std::string journalFile;

    pid_t snapshotPid;
    int snapshotFd;

};


} // MMap
} // Datacratic

#endif // __mmap__simple_snapshot_h__

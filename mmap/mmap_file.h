/* mmap_file.h                                                     -*- C++ -*-
   Jeremy Barnes, 16 December 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Overall file used at the root of an actual mmap file use case.
*/

#ifndef __mmap__mmap_file_h__
#define __mmap__mmap_file_h__


#include "mmap_const.h"
#include "memory_allocator.h"

#include <memory>

namespace Datacratic {
namespace MMap {

struct PageTableAllocator;
struct MemoryRegion;
struct Trie;


/*****************************************************************************/
/* COW REGION                                                                */
/*****************************************************************************/

/** A copy-on-write region within a memory mapped file.  There is one of
    these for each independent data structure within the file.
*/

struct COWRegion {
    uint64_t version;          ///<
    uint64_t magic;
    GcLock::Data gcData;       ///< GC data for structure for CS detection
    struct {
        uint64_t type;         ///< Type of structure at root
        uint64_t root;         ///< Root of structure; opaque (depends on type)
    } JML_ALIGNED(16);
    char name[16];         ///< Symbolic name of structure
} JML_ALIGNED(64);


/*****************************************************************************/
/* THREAD REGION                                                             */
/*****************************************************************************/

/** A block of data for each thread that is active within the region.  Allows
    each thread to see what the others are up to and for a stuck or crashed
    thread to have its operations backed out.
*/

struct ThreadRegion {
};

/*****************************************************************************/
/* FILE METADATA                                                             */
/*****************************************************************************/

/** Metadata for a memory mapped file.   This is what is mapped into the
    first page of a MMapFile.
*/

struct FileMetadata {
    COWRegion regions[32];   ///< Max of 32 data structures available

};


/*****************************************************************************/
/* MMAP FILE                                                                 */
/*****************************************************************************/

/** A memory allocator class that ties together a shared anonymous
    memory region, a page table allocator, and a memory allocator into
    a single class.

    The memory region can be shared amongst multiple processes, but
    cannot be accessed from outside processes.
*/

struct MMapFileBase {
    MMapFileBase(ResCreateOpen,
            const std::string & mapFile, Permissions perm, size_t initialAlloc);
    MMapFileBase(ResCreate,
            const std::string & mapFile, Permissions perm, size_t initialAlloc);
    MMapFileBase(ResOpen,
            const std::string & mapFile, Permissions perm);

    /** Permanently deletes all the resources associated with this mmap file */
    void unlink();

    /** Create a snapshot at the current point in time.
        This guarantees that, once the function returns, the backing file
        will be completely consistent and that any writes made by this process
        will be visible to other processes in a consistent manner.
    */
    uint64_t snapshot();

    /** The size of the datastore in bytes. */
    size_t fileSize() { return mmapRegion_->length(); }

protected:

    std::shared_ptr<MemoryRegion> mmapRegion_;
    bool createdMmap; // ugly hack for ResCreateOpen

private:

    static MemoryRegion * getRegion(
            const std::string & mapFile, Permissions perm, size_t size);
    static MemoryRegion * getRegion(
            const std::string & mapFile, Permissions perm);

};


/** Memory allocator that has an internal page table allocator. */

struct MMapFile
    : public MMapFileBase,
      public MemoryAllocator
{
    MMapFile(ResCreateOpen,
            const std::string & mapFile = "PRIVATE",
            Permissions perm = PERM_READ_WRITE,
            size_t initialToAlloc = page_size * 64);

    MMapFile(ResCreate,
            const std::string & mapFile = "PRIVATE",
            Permissions perm = PERM_READ_WRITE,
            size_t initialToAlloc = page_size * 64);

    MMapFile(ResOpen,
            const std::string & mapFile = "PRIVATE",
            Permissions perm = PERM_READ_WRITE);

    /** Permanently deletes any ressources associated with file. */
    void unlink();
};


/*****************************************************************************/
/* UTILITIES                                                                 */
/*****************************************************************************/

/** Applies the journal and delete any auxiliary files associated with a given
    mmap file. To delete the mmap file itself, use the unlink() method of
    MMapFile. It should be called after a crash, before opening an mmap file.

    IMPORTANT: Don't call this function if there's an active process using the
               mmap file. Doing so could lead to a corrupted mmap file that can
               only be recovered using hopes and dreams.
*/
void cleanupMMapFile(const std::string& mapFile);

} // namespace MMap
} // namespace Datacratic


#endif /* __mmap__mmap_file_h__ */

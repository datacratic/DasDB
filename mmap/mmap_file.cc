/* mmap_file.cc                                                    -*- C++ -*-
   Jeremy Barnes, 16 December 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

*/

#include "mmap_file.h"
#include "memory_region.h"
#include "mmap_trie.h"
#include "journal.h"
#include "jml/utils/guard.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <thread>

namespace Datacratic {
namespace MMap {

using namespace std;

/*****************************************************************************/
/* MMAP FILE BASE                                                            */
/*****************************************************************************/

MMapFileBase::
MMapFileBase(ResCreate,
        const std::string & mapFile, Permissions perm, size_t initialAlloc) :
    mmapRegion_(getRegion(mapFile, perm, initialAlloc)),
    createdMmap(true)
{
}

MMapFileBase::
MMapFileBase(ResOpen, const std::string & mapFile, Permissions perm) :
    mmapRegion_(getRegion(mapFile, perm)),
    createdMmap(false)
{
}

MMapFileBase::
MMapFileBase(ResCreateOpen,
        const std::string & mapFile, Permissions perm, size_t initialAlloc)
{
    struct stat s;
    if (::stat(mapFile.c_str(), &s)) {
        mmapRegion_.reset(getRegion(mapFile, perm, initialAlloc));
        createdMmap = true;
    }
    else {
        mmapRegion_.reset(getRegion(mapFile, perm));
        createdMmap = false;
    }
}


MemoryRegion *
MMapFileBase::
getRegion(const std::string & mapFile, Permissions perm, size_t size)
{
    if (mapFile == "PRIVATE")
        return new MallocRegion(perm, size);
    return new MMapRegion(RES_CREATE, mapFile, perm, size);
}

MemoryRegion *
MMapFileBase::
getRegion(const std::string & mapFile, Permissions perm)
{
    if (mapFile == "PRIVATE")
        return new MallocRegion(perm);
    return new MMapRegion(RES_OPEN, mapFile, perm);
}


void
MMapFileBase::
unlink()
{
    mmapRegion_->unlink();
}

uint64_t
MMapFileBase::
snapshot()
{
    return mmapRegion_->snapshot();
}


/*****************************************************************************/
/* MMAP FILE                                                                 */
/*****************************************************************************/

MMapFile::
MMapFile(ResCreate,
        const std::string & mapFile, Permissions perm, size_t initialToAlloc)
    : MMapFileBase(RES_CREATE, mapFile, perm, initialToAlloc),
      MemoryAllocator(*mmapRegion_, true)
{
    // Writeback the initialization of the mmap.
    snapshot();
}

MMapFile::
MMapFile(ResOpen, const std::string & mapFile, Permissions perm)
    : MMapFileBase(RES_OPEN, mapFile, perm),
      MemoryAllocator(*mmapRegion_, false)
{
}

MMapFile::
MMapFile(ResCreateOpen,
        const std::string & mapFile, Permissions perm, size_t initialToAlloc)
    : MMapFileBase(RES_CREATE_OPEN, mapFile, perm, initialToAlloc),
      MemoryAllocator(*mmapRegion_, createdMmap)
{
    // Writeback the initialization of the mmap.
    snapshot();
}

void
MMapFile::
unlink()
{
    MemoryAllocator::unlink();
    MMapFileBase::unlink();
}


/*****************************************************************************/
/* UTILITIES                                                                 */
/*****************************************************************************/

static void
unlinkShmFiles(const std::string& mapFile)
{
    // gc lock file.
    auto gcName = [] (const std::string& name, unsigned id) -> std::string {
        std::stringstream ss;
        ss << "gc." << name << "_" << id;
        return ss.str();
    };

    // boost ipc mutex file.
    auto semName = [] (const std::string& name) -> std::string {
        return "sem." + name;
    };

    auto trieName = [] (const std::string& name, unsigned id) -> std::string {
        stringstream ss;
        ss << "trie." << name << "_" << id;
        return ss.str();
    };

    // Delete all the shm files
    shm_unlink(semName(mapFile).c_str());
    shm_unlink(semName("resize." + mapFile).c_str());
    shm_unlink(semName("snapshot." + mapFile).c_str());

    for (int id = 0; id < 64; ++id) {
        shm_unlink(gcName(mapFile, id).c_str());
        shm_unlink(semName(gcName(mapFile, id)).c_str());
        shm_unlink(semName(trieName(mapFile, id)).c_str());
    }
}


void
cleanupMMapFile(const std::string& mapFile)
{
    // Recover the journal if necessary.
    int fd = open(mapFile.c_str(), O_RDWR);
    if (fd != -1) {
        ML::Call_Guard close_guard([&] { close(fd); });
        Journal::undo(fd, mapFile + ".log");
    }

    unlinkShmFiles(mapFile);

    // Malloc regions will use external locks for the trie merge algo.
    stringstream ss;
    ss << "mmap_anon_" << getuid();
    unlinkShmFiles(ss.str());
}

} // namespace MMap
} // namespace Datacratic

/* memory_region.cc
   Jeremy Barnes, 26 September 2011
   Copyright (c) 2011 Datacratic Inc.  All rights reserved.

   Implementation of memory region.
*/

#include "memory_region.h"
#include "trie_allocator.h"
#include "snapshot.h"
#include "simple_snapshot.h"
#include "sigsegv.h"
#include "sync_stream.h"
#include "jml/utils/exc_check.h"
#include "jml/utils/guard.h"
#include "jml/arch/vm.h"
#include "jml/arch/backtrace.h"

#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <sys/mman.h>
#include <iostream>
#include <mutex>
#include <thread>

using namespace std;
using namespace ML;
namespace ipc = boost::interprocess;

namespace Datacratic {
namespace MMap {



/******************************************************************************/
/* REGION PTR                                                                 */
/******************************************************************************/

namespace {

uint64_t test_nextTrigger = 1;
uint64_t test_testCounter = 0;
string test_lastBacktrace = "";

} // namespace anonymous


void doRegionExceptionTest()
{
    test_testCounter++;
    if (test_testCounter == test_nextTrigger) {
        test_nextTrigger++;
        test_testCounter = 0;

        stringstream ss;
        ML::backtrace(ss);
        test_lastBacktrace = ss.str();

        // Won't trigger a resize.
        throw RegionResizeException(0, false);
    }
}

void resetRegionExceptionTest()
{
    test_nextTrigger = 1;
    test_testCounter = 0;
}

string regionExceptionTestLastBacktrace()
{
    return test_lastBacktrace;
}


/*****************************************************************************/
/* UTILITIES                                                                 */
/*****************************************************************************/

static int
mmapProtFlags(Permissions perm)
{
    int flags = 0;

    if (perm & PERM_READ) flags |= PROT_READ;
    if (perm & PERM_WRITE) flags |= PROT_WRITE;

    ExcCheck(flags, "Invalid region permissions.");

    return flags;
}

/*****************************************************************************/
/* MEMORY REGION                                                             */
/*****************************************************************************/

#if !DASDB_SW_PAGE_TRACKING

MemoryRegion::
MemoryRegion()
{
}

#else

MemoryRegion::
MemoryRegion() :
    dirtyPageTable(new DirtyPageTable())
{
}

#endif

MemoryRegion::
~MemoryRegion()
{
}

/** \todo This is bordering on a very ugly hack. Could be improved. */
void
MemoryRegion::
initGcLock(bool allocGc)
{
    if (allocGc)
        gc = allocateGcLock(0);
    else gc = gcLock(0);
}

void
MemoryRegion::
dumpPages(std::ostream& stream) const
{
    Data current = data;
    dump_page_info(current.start, current.start + current.length, stream);
}


/*****************************************************************************/
/* MALLOC REGION                                                             */
/*****************************************************************************/

MallocRegion::
MallocRegion(Permissions perm, uint64_t size) :
    perm(perm),
    resizeLock(new mutex())
{
    // We always create a new gcLock because it's not possible to only open a 
    // private regon
    MemoryRegion::initGcLock(true);

    GcLock::ExclusiveGuard guard(*gc);
    resize(size);
}

MallocRegion::
~MallocRegion()
{
    gc->forceUnlock();
    gc->deferBarrier();

    GcLock::ExclusiveGuard guard(*gc);
    resize(0);
}

void
MallocRegion::
resize(uint64_t newLength)
{
    doResize(newLength, true);
}

void
MallocRegion::
grow(uint64_t minimumLength)
{
    doResize(minimumLength, false);
}

void
MallocRegion::
doResize(uint64_t newLength, bool canShrink)
{
    ExcAssert(isPinned());
    ExcAssertLessEqual(newLength, 1ULL << 40);

    // Round to page size
    newLength = (newLength + page_size - 1) & (~(page_size - 1));

    lock_guard<mutex> guard(*resizeLock);

    Data current = data;

    if (current.length == newLength) return;
    if (current.length >= newLength && !canShrink) return;

    // Initial mapping
    if (current.length == 0) {
        void * res = mmap(
                0, newLength, mmapProtFlags(perm),
                MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

        if (res == MAP_FAILED)
            throw ML::Exception("Initial MMap failed: %s", strerror(errno));

        // sync_cerr() << "resize-init("
        //     << "newLength=" << hex << newLength << dec
        //     << ", cur.start=" << ((void*) current.start)
        //     << ", cur.length=" << hex << current.length << dec
        //     << ", new.start=" << res
        //     << ")" << endl << sync_dump;

        current.start = reinterpret_cast <char *>(res);
        current.length = newLength;
        data = current;
        return;
    }

    // We can't handle shrinking without grabbing an exclusive lock.
    if (current.length < newLength) {

        // Try to resize without moving the memory region.
        void * newAddr = mremap(current.start, current.length, newLength, 0);

        if (newAddr != MAP_FAILED) {
            // sync_cerr() << "resize-fix ("
            //     << "newLength=" << hex << newLength << dec
            //     << ", cur.start=" << ((void*) current.start)
            //     << ", cur.length=" << hex << current.length << dec
            //     << ", new.start=" << newAddr
            //     << ")" << endl << sync_dump;

            current.start = reinterpret_cast<char *>(newAddr);
            current.length = newLength;
            data = current;
            return;
        }

        ExcCheckErrno(errno == ENOMEM, "mremap expand");
    }

    // We're about to invalidate all our pointers to the memory region
    // so in order to safely move the memory region, we have to be the
    // only one referencing it.
    // Throwing this exception will defer the work until we have an
    // exclusive lock.
    if (!gc->isLockedExclusive())
        throw RegionResizeException(newLength, canShrink);

    // mremap fails with a size of zero so just munmap it manually.
    if (newLength == 0) {
        int res = munmap(current.start, current.length);
        ExcCheckErrno(res == 0, "Failed to munmap");

        // sync_cerr() << "resize-unmp("
        //     << "newLength=" << hex << newLength << dec
        //     << ", cur.start=" << ((void*) current.start)
        //     << ", cur.length=" << hex << current.length << dec
        //     << ", new.start=" << 0
        //     << ")" << endl << sync_dump;

        current.start = 0;
        current.length = 0;
        data = current;
        return;
    }

    // May shrink the region.
    void * newAddr = mremap(
            current.start, current.length, newLength, MREMAP_MAYMOVE);

    if (newAddr == MAP_FAILED)
        throw ML::Exception("unable to remap mmap area from %p length %llx "
                            "to length %llx: %s",
                            current.start, current.length,
                            newLength, strerror(errno));

    // sync_cerr() << "resize-move("
    //     << "newLength=" << hex << newLength << dec
    //     << ", cur.start=" << ((void*) current.start)
    //     << ", cur.length=" << hex << current.length << dec
    //     << ", new.start=" << newAddr
    //     << ")" << endl << sync_dump;

    current.start = reinterpret_cast<char *>(newAddr);
    current.length = newLength;
    data = current;
}

void
MallocRegion::
unlink()
{
    // Malloc region isn't backed by a file.
}

uint64_t
MallocRegion::
snapshot()
{
    // Malloc region doesn't do snapshots
    return 0ULL;
}

shared_ptr<GcLockBase> 
MallocRegion::
gcLock(unsigned id)
{
    return gcLocks[id];
}

shared_ptr<GcLockBase> 
MallocRegion::
allocateGcLock(unsigned id)
{
    auto lock = make_shared<GcLock>();
    gcLocks[id] = lock;
    return lock;
}

void
MallocRegion::
unlinkGcLock(unsigned id)
{
    gcLocks[id].reset();
}

std::string
MallocRegion::
name(unsigned id)
{
    stringstream ss;
    ss << "mmap_anon_" << getuid() << "_" << id;
    return ss.str();
}


/*****************************************************************************/
/* MMAP REGION                                                               */
/*****************************************************************************/

uint64_t
MMapRegion::
getFileSize() const
{
    struct stat stats;
    int res = fstat(fd, &stats);
    ExcCheckErrno(!res, "can't stat file");
    
    uint64_t length = stats.st_size;

    return length;
}

void 
MMapRegion::
doOpen(const std::string& filename, bool create)
{
    ExcCheckNotEqual(filename, "ANONYMOUS",
            "can't do anonymous memory mapped regions");

    int flags = O_RDWR;
    if (create) flags |= O_CREAT | O_EXCL;

    initMutex = make_shared<ipc::named_mutex>(
            ipc::open_or_create, filename.c_str());
    ipc::scoped_lock<ipc::named_mutex> lock(*initMutex);

    resizeMutex =  make_shared<ipc::named_mutex>(
            ipc::open_or_create, ("resize." + filename).c_str());

    snapshotMutex =  make_shared<ipc::named_mutex>(
            ipc::open_or_create, ("snapshot." + filename).c_str());

    fd = open(filename.c_str(), flags, 0644);
    ExcCheckErrno(fd != -1, "can't open file");
}


MMapRegion::
MMapRegion(ResCreate, 
        const std::string & filename, Permissions perm, int64_t sizeToCreate) :
    filename(filename),
    perm(perm),
    gcLocksMutex(new mutex())
{
    MemoryRegion::initGcLock(true);

    doOpen(filename, true);

    if (sizeToCreate > 0) {
        // Extra page is used when resizing the region.
        int res = ftruncate(fd, sizeToCreate + page_size);
        ExcCheckErrno(!res, "file resize failed");
    }

    doInitialMmap();

}

MMapRegion::
MMapRegion(ResOpen, const std::string & filename, Permissions perm) :
    filename(filename),
    perm(perm),
    gcLocksMutex(new mutex())
{
    MemoryRegion::initGcLock(false);

    doOpen(filename, false);
    doInitialMmap();
}

    
MMapRegion::
~MMapRegion()
{
    gc->forceUnlock();

    gc->deferBarrier();

    // Extra page is used when resizing the region.
    int res = munmap(data.start, data.length + page_size);
    ExcCheckErrno(!res, "error munmapping");

    res = close(fd);
    ExcCheckErrno(!res, "error closing the fd");
}

void
MMapRegion::
doInitialMmap()
{
    uint64_t length = getFileSize();

    // Remove the extra page used when resizing the region.
    if (length > 0) length -= page_size;

    if (length % page_size != 0)
        throw ML::Exception("can't mmap non page aligned file");

    void * addr = 0;

    if (length != 0) {
        // Extra page is used when resizing the region.
        addr = mmap(0, length + page_size,
                    mmapProtFlags(perm),
                    MAP_PRIVATE,
                    fd, 0);
        if (addr == MAP_FAILED)
            throw ML::Exception("MMap failed: %s", strerror(errno));
    }

    Data current;
    current.start = reinterpret_cast<char *>(addr);
    current.length = length;
        
    data = current;
}

void
MMapRegion::
doResize(uint64_t newLength, bool canShrink)
{
    ExcAssert(isPinned());
    ExcAssertLessEqual(newLength, 1ULL << 40);

    // Round to page size
    newLength = (newLength + page_size - 1) & (~(page_size - 1));
    
    ipc::scoped_lock<ipc::named_mutex> resizeLock(*resizeMutex);
    // gc->deferBarrier();

    Data current = data;
    
    // cerr << "RESIZE: length = " << current.length
    //      << " file size = " << getFileSize()
    //      << " newLength = " << newLength << endl;

    // Check the size of the file
    uint64_t fileLength = getFileSize() - page_size;

    if (fileLength != newLength) {
        if (canShrink) {
            ExcCheckLessEqual(fileLength, newLength,
                    "shrinking of MMapRegion not done yet");
        }

        if (canShrink || newLength > fileLength) {
            // Change the size of the underlying file
            // Extra page is used when resizing the region.

            int res = ftruncate(fd, newLength + page_size);
            ExcCheckErrno(!res, "doResize::ftruncate");
        }
    }

    // Check again after we have the lock to avoid races
    if (current.length == newLength) return;
    if (current.length > newLength && !canShrink) return;

    if (canShrink) {
        ExcCheckLessEqual(current.length, newLength,
                "shrinking of MMapRegion not done yet");
    }

    // First mapping?  We need a mmap, not a mremap
    if (current.start == 0) {
        doInitialMmap();
        return;
    }

    // Now we do the remap.

    /** First we try to simply expand the region at the current address to avoid
        throwing a resize exception and blocking all read/write ops.

        We use a bit of a hack to get around any currently ongoing snapshots who
        may be fragmenting our VMA. The idea is instead of specifying the entire
        region to mremap, we only specify a hidden page at the end that only we
        are aware off. The kernel will take care of merging the VMAs into one
        solid area.
     */

    uint64_t adjNewLength = newLength - current.length + page_size;
    void * newAddr = mremap(
            current.start + current.length, page_size, adjNewLength, 0);

    // Nothing should have changed
    ExcAssertEqual(current.start, data.start);
    ExcAssertEqual(current.length, data.length);
    
    if (newAddr != MAP_FAILED) {
        ExcAssertEqual(newAddr, current.start + current.length);
        current.length = newLength;
        data = current;
        
        return;
    }

    ExcCheckErrno(errno == ENOMEM, "mremap expand");
    
    // We failed to remap because there wasn't a virtual memory hole where
    // we wanted it. If we don't have an exclusive lock then we can't resize
    // right away (throwing the exception will defer the work).
    if (!gc->isLockedExclusive())
        throw RegionResizeException(newLength, canShrink);

    /** Currently moving the region will completely screw up the rebacking
        snapshoting rebacking process.

        \todo Need to find a way to avoid blocking here.
    */
    ipc::scoped_lock<ipc::named_mutex> snapshotLock(*snapshotMutex);
    
    // We allocate an extra secret page that will be used when expanding the
    // region.
    newAddr = mremap(
            current.start, current.length, newLength + page_size,
            MREMAP_MAYMOVE);

    if (newAddr == MAP_FAILED)
        throw ML::Exception("unable to remap mmap area from %p length %llx "
                            "to length %llx: %s",
                            current.start, current.length,
                            newLength, strerror(errno));
    
    current.start = reinterpret_cast<char *>(newAddr);
    current.length = newLength;

    data = current;
}

void
MMapRegion::
resize(uint64_t newLength)
{
    doResize(newLength, true);
}

void
MMapRegion::
grow(uint64_t minimumLength)
{
    doResize(minimumLength, false);
}

void
MMapRegion::
unlink()
{
    ::unlink(filename.c_str());
    gc->unlink();
    ipc::named_mutex::remove(filename.c_str());
    ipc::named_mutex::remove(("resize." + filename).c_str());
    ipc::named_mutex::remove(("snapshot." + filename).c_str());
}


#if !DASDB_SW_PAGE_TRACKING

uint64_t
MMapRegion::
snapshot()
{
    /* Pretend that this lock wasn't here and that after we read the bounds of
       the region our thread decided to take a nice long nap. During sleepy time
       another decided to expand the region and finish a write op all the way to
       the root of the trie. Now when the first thread wakes up and starts
       snapshotting it would completely miss the expanded area of the region and
       would miss nodes that are part of the trie version that the second thread
       just wrote.

       This lock prevents this race while still preventing a full block until
       all the region has been snapshotted. Still not optimal but as good as its
       likely to get.
     */
    ipc::scoped_lock<ipc::named_mutex> resizeLock(*resizeMutex);

    // make sure that there's no ongoing resizing or snapshotting
    ipc::scoped_lock<ipc::named_mutex> snapshotLock(*snapshotMutex);

    Data current = data;
    Snapshot snapshot(filename + ".log");

    resizeLock.unlock();

    // If this isn't true you need to change how the snapshot is made.
    ExcAssertEqual(TrieAllocator::StartOffset, 0);

    size_t written, rebacked, reclaimed;
    boost::tie(written, rebacked, reclaimed) = snapshot.sync_and_reback(
            fd, 0, current.start, current.length, Snapshot::NO_RECLAIM);

    // \todo Could gather some stats using the return values.

    snapshot.terminate();
    return written;
}

#else // DASDB_SW_PAGE_TRACKING

uint64_t
MMapRegion::
snapshot()
{
    unique_ptr<DirtyPageTable> oldPageTable(new DirtyPageTable(length()));

    // Must be destroyed before oldPageTable because it has a reference to it.
    unique_ptr<SimpleSnapshot> snapshot;

    {
        // Stops all writes to the mmap.
        // Also happens to stop all resizes (good) and reads (bad).
        GcLockBase::ExclusiveGuard guard(*gc);

        oldPageTable.swap(dirtyPageTable);
        snapshot.reset(new SimpleSnapshot(*oldPageTable, filename + ".log"));
    }

    Data current = data;
    return snapshot->sync(fd, 0, current.start, current.length);
}

#endif

std::string
MMapRegion::
name(unsigned id)
{
    stringstream ss;
    ss << filename << "_" << id;
    return ss.str();
}

shared_ptr<GcLockBase> 
MMapRegion::
gcLock(unsigned id)
{
    lock_guard<mutex> guard(*gcLocksMutex);

    if (!gcLocks[id])
        gcLocks[id] = make_shared<SharedGcLock>(GC_OPEN, name(id));

    return gcLocks[id];
}

shared_ptr<GcLockBase>
MMapRegion::
allocateGcLock(unsigned id)
{
    lock_guard<mutex> guard(*gcLocksMutex);

    gcLocks[id] = make_shared<SharedGcLock>(GC_CREATE, name(id));

    return gcLocks[id];
}

void
MMapRegion::
unlinkGcLock(unsigned id)
{
    shared_ptr<GcLockBase> gcLock;

    {
        lock_guard<mutex> guard(*gcLocksMutex);

        gcLock = gcLocks[id];
        gcLocks[id].reset();
    }

    gcLock->unlink();
}

  
} // namespace MMap
} // namespace Datacratic

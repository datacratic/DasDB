/* memory_region.h                                                 -*- C++ -*-
   Jeremy Barnes, 26 September 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Definition of basic memory regions.
*/

#ifndef __mmap__memory_region_h__
#define __mmap__memory_region_h__


#include "mmap_const.h"
#include "soa/gc/gc_lock.h"
#include "dirty_page_table.h"

#include <cstdlib>
#include <cstddef>
#include <string>
#include <iostream>
#include <memory>
#include <array>
#include <algorithm>

namespace boost {
namespace interprocess {

struct named_mutex;

}; // namespace interprocess
}; // namespace boost

namespace std {

struct mutex;

};

namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* REGION RESIZE EXCEPTION                                                   */
/*****************************************************************************/

/** Signals that we need to resize the region while holding an exclusive lock.

    Note that this should only occur in cases where we want to grow the region
    because we should always be able to shrink a region without requiring
    an exclusive lock.
*/
struct RegionResizeException : public ML::SilentException
{
    RegionResizeException(size_t size, bool canShrink) :
        size(size), canShrink(canShrink)
    {}

    /** The size that we want to resize to. */
    size_t size;
    bool canShrink;
};

/** Macro that marks the beginning of an operation that could on a mmap region.
    This is used in conjunction with MMAP_REGION_EN_OP to gracefully handle
    resizing exceptions.

    Use of this macro can be nested, in which case only the outer macro will
    have an effect. Note that the inner macro calls are not free so nesting
    should be avoided where possible.

    Usage example (braces are optional):

        MMAP_PIN_REGION(region)
        {
            // do whatever.
        }
        MMAP_UNPIN_REGION;

   Note that the block of code within the two macros may be called multiple
   times. Also the region parameter should be passed as a reference to the
   macro.

   The MMAP_PIN_REGION_READ should be used when you don't want any defer
   to be executed, thus keeping a good latency on read.
*/
#define MMAP_PIN_REGION_IMPL(region, guard, doDefer)                   \
    while(true) {                                                      \
        Datacratic::MMap::MemoryRegion& region__ = region;             \
        try {                                                          \
            using Datacratic::GcLockBase;                              \
            GcLockBase::guard guard__(*region__.gc, doDefer);          \
            do {

#define MMAP_UNPIN_REGION                                              \
            } while(false);                                            \
            break;                                                     \
        } catch (Datacratic::MMap::RegionResizeException& ex) {        \
            region__.handleException(ex);                              \
        }                                                              \
    } ((void)0)

#define MMAP_PIN_REGION(region)                                     \
    MMAP_PIN_REGION_IMPL(region, SharedGuard, GcLock::RD_YES)
#define MMAP_PIN_REGION_READ(region)                                \
    MMAP_PIN_REGION_IMPL(region, SpeculativeGuard, GcLock::RD_NO)

/** Inline version of the MMAP_[UN]PIN_REGION macros
    Note: The ({ ... }) syntax is a gcc extension (statement expressions).
*/
#define MMAP_PIN_REGION_RET(region, op)         \
    ({                                          \
        decltype(op) r__;                       \
        MMAP_PIN_REGION(region)                 \
        {                                       \
            r__ = op;                           \
        }                                       \
        MMAP_UNPIN_REGION;                      \
        r__;                                    \
    })

#define MMAP_PIN_REGION_INL(region, op)                 \
    MMAP_PIN_REGION(region) { op; } MMAP_UNPIN_REGION;

/** Specialized pinning macro to call one of the resizing functions.
    Note that this should only be used in extremely rare cases (tests mostly).
*/
#define MMAP_PIN_REGION_RESIZE(region, op)                              \
    do {                                                                \
        try {                                                           \
            Datacratic::GcLockBase::SharedGuard guard__(*region.gc);       \
            do { op; } while(false);                                    \
        } catch (Datacratic::MMap::RegionResizeException& ex) {            \
            region.handleException(ex);                                 \
        }                                                               \
    } while(false);



/*****************************************************************************/
/* MEMORY REGION                                                             */
/*****************************************************************************/

/** Models a contiguous memory region. */

struct MemoryRegion {
    
    MemoryRegion();
    virtual ~MemoryRegion();

    /** Initializes the memory region. */
    void initGcLock(bool alloc);
    
    /** Resize the memory region to the given size.  Calling from multiple
        threads with different sizes may cause the mapping to expand and
        then shrink.
    */
    virtual void resize(uint64_t newSize) = 0;

    /** Grow the memory region to be at least the given size.  This call may
        safely be called from multiple threads with a guaranteed behaviour:
        it will never cause memory that is mapped to be unmapped and can
        safely be called in multiple threads with a different minimum size.

        The implementation is allowed to grow past this minimum in order to
        amortize the cost of growing.
    */
    virtual void grow(uint64_t minimumSize) = 0;

    /** Permanently deletes all the resources associated with this region. */
    virtual void unlink() = 0;

    /** Create a snapshot at the current point in time.
        This guarantees that, once the function returns, the backing file
        will be completely consistent and that any writes made by this process
        will be visible to other processes in a consistent manner.
    */
    virtual uint64_t snapshot() = 0;

    /** Returns the gc lock associated with the id. Ids can be between 0 and
        64 but note that id 0 is reserved for internal use.

        Multiple concurrent call to this function are supported.
    */
    virtual std::shared_ptr<GcLockBase> gcLock(unsigned id) = 0;

    /** Creates a new gc lock for this region and associate with the given id.
        This function is not guaranteed to be thread-safe for a given id.
    */
    virtual std::shared_ptr<GcLockBase> allocateGcLock(unsigned id) = 0;

    /** Permanently deletes all ressources associated with the gc lock.
        This function is not guaranteed to tbe thread-safe for a given id.
    */
    virtual void unlinkGcLock(unsigned id) = 0;

    /** Returns a name that uniquely represents the current memory region.
        \todo It actually a fairly unique but not unique enough name.
    */
    virtual std::string name(unsigned id) = 0;

    /** Dumps the page table associated with this memory region.
        Should only be used as a debugging aide.
    */
    void dumpPages(std::ostream& stream = std::cerr) const;

    /** Does the handling for the RegionResizeException.

        Note that this should never be called directly. Use the
        MMAP_PIN_REGION, MMAP_UNPIN_REGION and MMAP_REGION_OP
        macros instead.
    */
    void handleException(const RegionResizeException& ex)
    {
        // We might still be speculatively locked, force unlock before
        gc->forceUnlock();

        // Ops can be nested in which case grabbing the exclusive lock will lead
        // to a deadlock. Just defer the execution to the outer op.
        if (gc->isLockedShared()) throw;

        GcLock::ExclusiveGuard guard(*gc);

        if (ex.canShrink)
            resize(ex.size);
        else
            grow(ex.size * 2);
    }

    template<typename T>
    struct RangeT {

#if !DASDB_SW_PAGE_TRACKING

        RangeT(T * start, uint64_t numObjects)
            : start(start), numObjects(numObjects)
        {
        }
#else
        RangeT( DirtyPageTable* pageTable,
                uint64_t offset,
                T * start,
                uint64_t numObjects) :
            pageTable(pageTable),
            offset(offset),
            start(start),
            numObjects(numObjects)
        {}

        ~RangeT()
        {
            pageTable->markPages(offset, sizeof(T) * numObjects);
        }

        DirtyPageTable* pageTable;
#endif

        uint64_t offset;
        T * start;
        uint64_t numObjects;

        operator T * () const
        {
            return start;
        }

        T * operator -> () const
        {
            return start;
        }

        T & operator * () const
        {
            return *start;
        }

        T & operator [] (uint64_t index)
        {
            ExcCheckLess(index, numObjects, "accessing out of range element");
            return start[index];
        }

        T * get() const
        {
            return start;
        }

        bool operator! () const
        {
            return !start;
        }
    };

    template<typename T>
    RangeT<T> range(uint64_t startOffset, uint64_t numObjects = 1)
    {
        // Expensive to do this check on every access.
        // ExcCheck(isPinned(), "Attempt to access unpinned memory");

        uint64_t length = numObjects * sizeof(T /* todo: alignment */);

        if (startOffset + length > this->length())
            grow(startOffset + length);

#if !DASDB_SW_PAGE_TRACKING
        return RangeT<T>(
                reinterpret_cast<T *>(start() + startOffset), numObjects);
#else
        return RangeT<T>(
                dirtyPageTable.get(),
                startOffset,
                reinterpret_cast<T *>(start() + startOffset),
                numObjects);
#endif
    }

    /** Check that the region really is pinned.  Return the number of
        times that it is pinned.
    */
    int isPinned(GcLock::ThreadGcInfo * info = 0)
    {
        return gc->isLockedShared(info);
    }

    /** Return the start.  Not guaranteed to be atomic with respect to
        a call to length().
    */
    char * start() { return data.start; }

    /** Return the length.  Not guaranteed to be atomic with respect to
        a call to start().
    */
    uint64_t length() { return data.length; }

    typedef uint64_t q2 __attribute__((__vector_size__(16)));

    struct Data {
        Data()
            : start(0), length(0)
        {
        }

        Data(const Data & other)
            : q(other.q)
        {
        }

        Data & operator = (const Data & other)
        {
            this->q = other.q;
            return *this;
        }
        
        union {
            struct {
                char * start;     ///< Start of memory
                uint64_t length;  ///< Length of region
            };
            struct {
                q2 q;
            };
        } JML_ALIGNED(16);
    };
    
    Data data;

    std::shared_ptr<GcLockBase> gc;

#if DASDB_SW_PAGE_TRACKING
    std::unique_ptr<DirtyPageTable> dirtyPageTable;
#endif

};

namespace details {

    template <typename T>
    struct lambda_traits : lambda_traits<decltype(&T::operator())>
    {
    };

    template <typename R, typename C>
    struct lambda_traits<R (C::*)()> 
    {
      typedef R result_type;
    };

    template <typename R, typename C>
    struct lambda_traits<R (C::*)() const> 
    {
      typedef R result_type;
    };

}

template<typename TrieVersion, typename Func, typename Ret> 
struct RegionPinGuardImpl {

    RegionPinGuardImpl(TrieVersion *trie, Func func) 
     {
        MMAP_PIN_REGION(trie->trie->area().region())
        {
            ret_ = func();
        }
        MMAP_UNPIN_REGION;
    }

    operator Ret() noexcept {
        return ret_;
    }
    
private:
    Ret ret_;
};

/* Specialization for void lambda */
template<typename TrieVersion, typename Func>
struct RegionPinGuardImpl<TrieVersion, Func, void> {

    RegionPinGuardImpl(TrieVersion *trie, Func func) 
     {
        MMAP_PIN_REGION(trie->trie->area().region())
        {
            func();
        }
        MMAP_UNPIN_REGION;
    }
};

template<typename TrieVersion, typename Func> 
struct RegionPinGuard : 
public RegionPinGuardImpl<TrieVersion, Func,
                        typename details::lambda_traits<Func>::result_type>
{
    typedef typename details::lambda_traits<Func>::result_type Ret;
    typedef RegionPinGuardImpl<TrieVersion, Func, Ret> Base;
    RegionPinGuard(TrieVersion *trie, Func func) : Base(trie, func) {
    }
 
};

/* Constructs and returns a RegionPinGuard.
*/

template<typename TrieVersion, typename Func> 
RegionPinGuard<TrieVersion, Func>
make_pin_guard(TrieVersion *trie, Func func) {
    return RegionPinGuard<TrieVersion, Func>(trie, func); 
} 

/******************************************************************************/
/* REGION PTR                                                                 */
/******************************************************************************/

/** For internal use only. */
void doRegionExceptionTest();

/** Resets the region exception tests */
void resetRegionExceptionTest();

/** Returns the backtrace of the last thrown exception */
std::string regionExceptionTestLastBacktrace();



/** Utility wrapper for the MemoryRegion::range() function and the
    MemoryRegion::RangeT struct.
*/
template<typename Repr>
struct RegionPtr : public MemoryRegion::RangeT<Repr>
{
    typedef MemoryRegion::RangeT<Repr> RangeT;

    RegionPtr(MemoryRegion& region, uint64_t offset, uint64_t numObjects = 1) :
        RangeT::RangeT(region.range<Repr>(offset, numObjects))
    {

#if 0
        if (regionExceptionTest)
            doRegionExceptionTest();
#endif

    }
};

/*****************************************************************************/
/* MALLOC REGION                                                             */
/*****************************************************************************/

/** Models a malloc'd memory region.  Note that growing requires a write lock
    as the memory contents have to be copied and this must be finished before
    any updates.
*/

struct MallocRegion : public MemoryRegion {
    MallocRegion(Permissions perm = PERM_READ_WRITE, uint64_t size = 0);

    virtual ~MallocRegion();

    virtual void resize(uint64_t newSize);

    virtual void grow(uint64_t minimumSize);

    virtual void unlink();

    virtual uint64_t snapshot();

    virtual std::shared_ptr<GcLockBase> gcLock(unsigned id);

    virtual std::shared_ptr<GcLockBase> allocateGcLock(unsigned id);

    virtual void unlinkGcLock(unsigned id);

    virtual std::string name(unsigned id);

    /** Perform the actual resize operation. */
    void doResize(uint64_t newLength, bool canShrink);

private:

    Permissions perm;
    std::array<std::shared_ptr<GcLockBase>, 64> gcLocks;
    std::unique_ptr<std::mutex> resizeLock;

};

/*****************************************************************************/
/* MMAP REGION                                                               */
/*****************************************************************************/

/** Models a memory mapped memory region backed by a file. */

struct MMapRegion : public MemoryRegion {
    MMapRegion(ResCreate,
            const std::string & file = "ANONYMOUS",
            Permissions perm = PERM_READ_WRITE,
            int64_t sizeToCreate = -1);

    MMapRegion(ResOpen,
            const std::string & file = "ANONYMOUS",
            Permissions perm = PERM_READ_WRITE);
    
    virtual ~MMapRegion();

    virtual void resize(uint64_t newSize);

    virtual void grow(uint64_t minimumSize);

    virtual void unlink();

    virtual uint64_t snapshot();

    virtual std::shared_ptr<GcLockBase> gcLock(unsigned id);

    virtual std::shared_ptr<GcLockBase> allocateGcLock(unsigned id);

    virtual void unlinkGcLock(unsigned id);

    virtual std::string name(unsigned id);

    std::string filename;
    int fd;

    /** Implementation of the common logic to construct a MMapRegion object. */
    void doOpen(const std::string& filename, bool create);

    /** Implementation of the common logic to grow or resize the memory
        region.
    */
    void doResize(uint64_t newLength, bool canShrink);

    /** Perform the initial mmap() call on the current state of the fd. */
    void doInitialMmap();

    /** Ensures that the mmap can only be initialized by one process at a time.
     */
    std::shared_ptr<boost::interprocess::named_mutex> initMutex;

    /** Ensures that there's only one ongoing resize at a time. */
    std::shared_ptr<boost::interprocess::named_mutex> resizeMutex;
    
    /** Ensures that there's only one ongoing snapshot at a time and that we
        don't attempt to move the region while we're snapshotting.
    */
    std::shared_ptr<boost::interprocess::named_mutex> snapshotMutex;

    /** Return the current size of the underlying file. */
    uint64_t getFileSize() const;

private:

    Permissions perm;
    std::unique_ptr<std::mutex> gcLocksMutex;
    std::array<std::shared_ptr<GcLockBase>, 64> gcLocks;

};


} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__memory_region_h__ */



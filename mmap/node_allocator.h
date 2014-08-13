/* node_allocator.h                                              -*- C++ -*-
   Rémi Attab, 26 March 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Lock-free allocator for fixed size nodes.
*/

#ifndef __mmap__node_allocator_h__
#define __mmap__node_allocator_h__

#include "memory_region.h"
#include "soa/gc/gc_lock.h"

#include <iostream>
#include <memory>

namespace Datacratic {
namespace MMap {

struct PageTableAllocator;

#if 0
// Flags for the memory allocator
enum {
    HINT_TRANSIENT = 1,   ///< Allocation is transient (not for long)
    HINT_PERMANENT = 2,   ///< Allocation is permanent

    HINT_ARENA,
    HINT_
};
#endif


/*****************************************************************************/
/* NODE ALLOCATOR                                                            */
/*****************************************************************************/

static const uint64_t NO_HINT = (uint64_t)-1;


/** Lock-free allocator for aligned fixed size nodes. */

struct NodeAllocator {

    enum { MaxNodeSize = !NodeAllocSentinels ? 256 : 64 };

    /** Initialize to allocate pages from the given page table allocator. */
    NodeAllocator(PageTableAllocator & pageAlloc);

    /** Allocate a node (ie, a regularly sized chunk).  The size should be
        a power of two with 8 <= size <= 256; the returned memory will be
        aligned to the given chunk boundary.

        The optional hint parameter specifies a hint about where the
        memory should be for optimal locality of reference.

        If hint == NO_HINT, then no effort is made to put the memory in any
        particular place.

        If hint < 4096, then the hint specifies a "locality zone" such that
        an attempt will be made to group allocations from within the same
        zone together.

        If hint >= 4096, then the hint specifies an offset of an existing
        allocation, and an attempt will be made to allocate the memory
        as close as possible to that allocation.

        This function is thread safe and lock free in most cases.
    */
    uint64_t allocate(uint64_t size, int64_t hint = NO_HINT);

    /** Deallocate the given node.  Its size must be given in order for the
        deallocation to work properly and efficiently.

        This function is thread safe and lock free in most cases.
    */
    void deallocate(uint64_t offset, uint64_t size);

    template<typename P, typename... Args>
    uint64_t allocateT(GcLock::ThreadGcInfo * info,
                           Args... args)
    {
        size_t sz = sizeof(P);
        size_t offset = allocate(sz);

        auto result = RegionPtr<P>(region_, offset);
        try {
            new (result) P(args...);
        } catch (...) {
            deallocate(offset, sz);
            throw;
        }
        return offset;
    }

    void checkSentinels(uint64_t offset, uint64_t size);

    uint64_t bytesAllocated() const
    {
        return bytesAllocated_;
    }

    uint64_t bytesDeallocated() const
    {
        return bytesDeallocated_;
    }

    uint64_t bytesOutstanding() const
    {
        return bytesAllocated_ - bytesDeallocated_;
    }

    uint64_t usedSize();

private:

    uint64_t bytesAllocated_, bytesDeallocated_;
    PageTableAllocator & pageAlloc_;
    MemoryRegion & region_;

};


} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__node_allocator_h__ */

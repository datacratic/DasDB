/* string_allocator.h                                              -*- C++ -*-
   Rémi Attab, 26 March 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   String allocator that uses a trie for it's free list.

   \todo Needs a generous dose of optimization.
*/


#ifndef __mmap__string_allocator_h__
#define __mmap__string_allocator_h__

#include <iostream>
#include <memory>

namespace Datacratic {
namespace MMap {

struct PageTableAllocator;
struct NodeAllocator;
struct TrieAllocator;
struct MemoryRegion;
struct Trie;


/*****************************************************************************/
/* STRING ALLOCATOR                                                          */
/*****************************************************************************/


/** Allocator for chunks of arbitrary size. */

struct StringAllocator {

    static const uint64_t NO_HINT = (uint64_t)-1;

    StringAllocator(
            PageTableAllocator & pageAlloc,
            NodeAllocator & nodeAlloc,
            Trie freeList);

    /**
    Allocates a chunk of memory with the given size. The size can be anything
    but there is no garanties on the alignment of the chunk.

    Note that sentinel bytes may be added at the end of the string to detect
    write overflows during deallocation.

    The hint parameter is currently ignored.
    */
    uint64_t allocate(uint64_t size, int64_t hint = NO_HINT);

    /**
    Deallocates a given string. The expectedSize is only used as a sanity check
    and nothing else.

    May throw if a write overflow is detected.
    */
    void deallocate(uint64_t offset, uint64_t expectedSize = 0);

    /** Returns the size of a string at the given offset. */
    uint64_t stringSize(uint64_t offset);

    /* Debug helper */
    void dumpFreeList(std::ostream& stream = std::cerr);

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

    /** Number of bytes allocated for private use for the allocator. */
    int64_t bytesPrivate() const
    {
        return bytesPrivate_;
    }

private:

    uint64_t bytesAllocated_, bytesDeallocated_, bytesPrivate_;

    PageTableAllocator & pageAlloc_;
    NodeAllocator & nodeAlloc_;
    MemoryRegion & region_;

    std::shared_ptr<Trie> freeList_;
};


} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__string_allocator_h__ */

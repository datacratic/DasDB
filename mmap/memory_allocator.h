/* memory_allocator.h                                              -*- C++ -*-
   Jeremy Barnes, 23 October 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   A memory mapped memory allocator class.

   Essentially lock-free.
*/

#ifndef __mmap__memory_allocator_h__
#define __mmap__memory_allocator_h__

#include "node_allocator.h"
#include "trie_allocator.h"
#include "string_allocator.h"

namespace Datacratic {
namespace MMap {

struct MemoryRegion;
struct PageTableAllocator;
struct Trie;

/*****************************************************************************/
/* MEMORY ALLOCATOR                                                          */
/*****************************************************************************/

/** Contains all the allocators necessary to manipulate the mmap. */

struct MemoryAllocator
{
    /** Initialize to allocate pages from the given page table allocator. */
    MemoryAllocator(MemoryRegion & region, bool init);

    /** Aggregated stats of all the allocators. */
    uint64_t bytesAllocated() const;
    uint64_t bytesDeallocated() const;
    uint64_t bytesPrivate() const;
    uint64_t bytesOutstanding() const
    {
        return bytesAllocated() - bytesDeallocated();
    }

    /** The region associated with this memory allocator. */
    MemoryRegion& region() const { return region_; }

    /** Returns the trie object associated with the id. */
    Trie trie(unsigned id);

    /** Returns the free list to be used by the string allocator. */
    Trie freeList(bool init);

    /** Permanently deletes any ressources associated with this allocator. */
    void unlink();

private:

    MemoryRegion & region_;     //< Region where everything is stored.
    std::shared_ptr<PageTableAllocator> pageAlloc_;

public:

    /** All the allocators available */
    PageTableAllocator& pageAlloc;
    NodeAllocator nodeAlloc;
    TrieAllocator trieAlloc;
    StringAllocator stringAlloc;

};

} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__memory_allocator_h__ */

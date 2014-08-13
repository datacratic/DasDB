/* memory_allocator.cc
   Jeremy Barnes, 1 November 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Memory allocator class.
*/

#include "memory_allocator.h"
#include "page_table_allocator.h"
#include "mmap_trie.h"

using namespace std;
using namespace ML;


namespace Datacratic {
namespace MMap {



/*****************************************************************************/
/* MEMORY ALLOCATOR                                                          */
/*****************************************************************************/

static PageTableAllocator*
newPageAlloc(MemoryRegion& region, size_t offset, bool init)
{
    MMAP_PIN_REGION(region)
    {
        return new PageTableAllocator(region, offset, init);
    }
    MMAP_UNPIN_REGION;
}

MemoryAllocator::
MemoryAllocator(MemoryRegion& region, bool init) :
    region_(region),
    pageAlloc_(newPageAlloc(region_, 8, init)),
    pageAlloc(*pageAlloc_),
    nodeAlloc(*pageAlloc_),
    trieAlloc(region_, init),
    stringAlloc(*pageAlloc_, nodeAlloc, freeList(init))
{
}

// Trie id reserved for our string allocator.
enum { StringAllocTrieId = TrieAllocator::MaxTrieId };

Trie
MemoryAllocator::
trie(unsigned id)
{
    if (!trieAlloc.isAllocated(id)) {
        MMAP_PIN_REGION(region_)
        {
            trieAlloc.allocate(id);
        }
        MMAP_UNPIN_REGION;
    }
    return Trie(this, id);
}

Trie
MemoryAllocator::
freeList(bool init)
{
    if (init)
        trieAlloc.allocate(StringAllocTrieId);

    return trie(StringAllocTrieId);
}

void
MemoryAllocator::
unlink()
{
    trieAlloc.deallocate(StringAllocTrieId);
}

uint64_t
MemoryAllocator::
bytesAllocated() const
{
    return nodeAlloc.bytesAllocated()
        + stringAlloc.bytesAllocated()
        + trieAlloc.bytesAllocated();
}

uint64_t
MemoryAllocator::
bytesDeallocated() const
{
    return nodeAlloc.bytesDeallocated()
        + stringAlloc.bytesDeallocated()
        + trieAlloc.bytesDeallocated();
}

uint64_t
MemoryAllocator::
bytesPrivate() const
{
    return stringAlloc.bytesPrivate();
}



} // namespace MMap
} // namespace Datacratic

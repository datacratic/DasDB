/* trie_allocator.h                                              -*- C++ -*-
   Rémi Attab, 26 March 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Lock-free allocator for trie roots.
*/

#ifndef __mmap__trie_allocator_h__
#define __mmap__trie_allocator_h__

#include "mmap_const.h"

#include <iostream>
#include <memory>

namespace Datacratic {
namespace MMap {

struct MemoryAllocator;
struct MemoryRegion;
struct Trie;


/*****************************************************************************/
/* TRIE ALLOCATOR                                                            */
/*****************************************************************************/

/** Lightweight and simple allocator for trie roots.

    This allocator is entirely contained within a single page so that it can
    be easily and quickly snapshotted.

    \todo Should probably migrate to string based ids at some point.
*/
struct TrieAllocator {

    enum {
        // For the moment, we fix the trie allocator at the begning and restrict
        // it to a single page. Note changing these values will require changes
        // to the page allocator.
        StartOffset = 0,
        NumPages = 1,

        // Blocks taken up by GenericNodePage.
        HeadBlocks = 1,

        // Number of reserved blocks located after the header block.
        // Currently unused but will come in handy later.
        ReservedBlocks = 6,

        // Range of accepted trie ids.
        MinTrieId = 1,
        MaxTrieId = ((NumPages * page_size) / 64) - (HeadBlocks + ReservedBlocks)
    };

    /** Places the trie allocator in the given region at the given offset. */
    TrieAllocator(MemoryRegion& region, bool init);

    /** Returns the offset of the trie associated with the id.
        Note that a convenience function is provided in MMapFile which will
        return a fully constructed Trie object.

        Throws if no trie was previously allocated for the given id.

        \todo Return a trie object instead (need to know about MemoryAllocator).
    */
    uint64_t trieOffset(unsigned id);

    /** Returns true if a trie was previously allocated for the given id. */
    bool isAllocated(unsigned id);

    /** Allocates a new trie and associates it with the given id.
        The returned trie is guaranteed to be empty. A gc lock will also have
        been allocated and initialized in the main region.

        Throws if a trie already exists with the same id.
    */
    void allocate(unsigned id);

    /** Deallocates the trie associated with the given id.
        This will also unlink the gc lock associated with the trie.

        Note that this does not modify the trie in anyway. This means that
        if the trie isn't empty prior to being deallocated then all its
        nodes will be leaked.

        Throws if no trie was previously allocated for the given id.
    */
    void deallocate(unsigned id);

    /** debug helper */
    void dumpAllocatedTries(std::ostream& stream = std::cerr) const;

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

private:

    uint64_t bytesAllocated_, bytesDeallocated_;
    MemoryRegion& region_;
    uint64_t offset_;
};


} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__trie_allocator_h__ */

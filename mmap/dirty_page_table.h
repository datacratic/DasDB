/** dirty_page_table.h                                 -*- C++ -*-
    Rémi Attab, 29 Aug 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Structure to keep track of dirty pages that potentially need to be written
    back to disk.

*/

#ifndef __mmap__dirty_page_table_h__
#define __mmap__dirty_page_table_h__

#include "mmap_const.h"

#include <cstdint>
#include <string>
#include <array>
#include <memory>


namespace Datacratic {
namespace MMap {

struct DirtyPageGroup;

/******************************************************************************/
/* DIRTY PAGE TABLE                                                           */
/******************************************************************************/

/** Keeps track of pages within the mmap that have been accessed.

    It's used in conjunction with SimpleSnapshot to find what needs to be
    written to disk. It's also a alternative to the original Snapshot class
    which used hardware assisted-kernel trickery to do the same job.
    Unfortunately that part of the linux kernel is fairly buggy.

    This class is entirely lock free except during memory allocations. To make
    up for that, the number of possible calls to malloc is bounded to a constant
    number which is a function of the mmap size. Roughly speaking, we're talking
    20 allocation tops which, if managed correctly, means 20 allocation for the
    entire life-time of the mmap.

 */
struct DirtyPageTable
{
    /** initialSize is used to preallocate enough groups to hold the given size.
     */
    DirtyPageTable(size_t initialSize = 0ULL);
    ~DirtyPageTable();

    void markPage(uint64_t addr);
    void markPages(uint64_t start, uint64_t length);

    /** Returns the offset of the next marked page starting at the given offset
        inclusively. Returns -1 if no other pages have been marked.
    */
    int64_t nextPage(uint64_t start);

    /** Removes the mark on the page at given offset. */
    bool clearPage(uint64_t addr);

    /** Debug helper. Returned string is optimized for small page tables. */
    std::string print() const;

private:

    DirtyPageGroup* getGroup(int group);
    std::pair<int, uint64_t> addrToIndex(uint64_t addr) const;
    uint64_t indexToAddr(int group, uint64_t index) const;

    enum {
        // 2^MinGroupBits: number of pages that the smallest group can track.
        MinGroupBits = cache_shift + 3,

        // page_shift represents the offset within a page (useless)
        // MinGroupBits represents are smallest group resolution.
        GroupCount = 64 - page_shift - MinGroupBits
    };

    std::array<DirtyPageGroup*, GroupCount> groups;
};

} // MMap
} // Datacratic

#endif // __mmap__dirty_page_table_h__

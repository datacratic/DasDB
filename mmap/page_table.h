/** page_table.h                                                   -*- C++ -*-
    Jeremy Barnes, 2 November 2011
    Copyright (c) 2011 Datacratic.  All rights reserved.

    Page table class.
*/

#ifndef __mmap__page_table_h__
#define __mmap__page_table_h__

#include "full_bitmap.h"
#include <cstring>
#include <boost/static_assert.hpp>
#include "jml/arch/atomic_ops.h"
#include "jml/arch/bitops.h"
#include "jml/arch/format.h"
#include "jml/utils/exc_assert.h"
#include "jml/compiler/compiler.h"
#include <boost/tuple/tuple.hpp>
#include <iostream>

namespace Datacratic {
namespace MMap {

enum PageType {
    PT_EMPTY = 0,
    PT_METADATA = 1,
    PT_PAGE_ALLOCATOR = 2,

    PT_L4_PTE = 8,   ///< PTE containing information about 1024 L4 pages
    PT_L3_PTE = 9,   ///< PTE containing information about 1024 L3 pages
    PT_L2_PTE = 10,   ///< PTE containing information about 1024 L2 pages
    PT_L1_PTE = 11,   ///< PTE containing information about 1024 L1 pages

    PT_4P_PAGE = 16,
    PT_4T_PAGE = 17,
    PT_4G_PAGE = 18,
    PT_4M_PAGE = 19,
    PT_4K_PAGE = 20,

    PT_4P_SPLIT = 24,
    PT_4T_SPLIT = 25,
    PT_4G_SPLIT = 26,
    PT_4M_SPLIT = 27,

    PT_ARENA_8B = 32,
    PT_ARENA_12B = 33,
    PT_ARENA_16B = 34,
    PT_ARENA_24B = 35,
    PT_ARENA_32B = 36,
    PT_ARENA_48B = 37,
    PT_ARENA_64B = 38, // Used to be index 32
    PT_ARENA_96B = 39,
    PT_ARENA_128B = 40,
    PT_ARENA_192B = 41,
    PT_ARENA_256B = 42
};

std::string print(PageType type);
std::ostream & operator << (std::ostream & stream, PageType type);


/*****************************************************************************/
/* PAGE TABLE                                                                */
/*****************************************************************************/

// Information about 1024 (2^10) pages

struct PageTable {
    uint64_t magic;
    uint32_t order;
    uint32_t unused;

    // Bitmap saying which of our pages are full
    FullBitmap<1024> allocated;

    // One character for the type of each page
    char types[1024]; 

    // Bitmap saying which of the pages contains (eventually) free pages
    // of the given size (L4, L3, L2, L1).
    FullBitmap<1024> fullLevels[4];

    // 8b, 12b, 16b, 24b, 32b, 48b, 64b, 96b, 128b, 192b, 256b
    // 8b, 16b, 32b, 64b, 128b, 256b
    FullBitmap<1024> fullNodes[11];

    void init(int order);

    bool valid() const
    {
        return magic == 0x1293847673827334;
    }

    void setType(unsigned pageNum, PageType type)
    {
        if (pageNum >= 1024)
            throw ML::Exception("setType: invalid page number");
        //if (type != PT_EMPTY)
        //    ExcAssertEqual(PageType(types[pageNum]), PT_EMPTY);
        types[pageNum] = type;
    }

    PageType getType(unsigned pageNum) const
    {
        ExcAssertLess(pageNum, 1024);
        return PageType(types[pageNum]);
    }

    /** Allocate a free page and mark it as allocated.
        Returns a pair: the first element is the page number allocated, and
        the second element is true if and only if the page was not full
        before but now it is full.
        
        The allocation can fail, in which case (-1, false) will be
        returned.

        Lock free and thread safe.
    */
    std::pair<int, bool> allocate(int startAt)
    {
        return allocated.allocate(startAt);
    }

    void reserve(unsigned pageNum)
    {
        bool result = allocated.markAllocated(pageNum);
        if (result)
            throw ML::Exception("reserving a page filled it up");
    }

    /** Mark that the given page number contains one or more deallocated
        pages of the given order.
      
        If it returns true, it means that the page transitioned from full
        to not full and so the page table at the next higher level up
        also needs to be modified.
    */
    bool markDeallocated(unsigned pageNum, unsigned order);

    /** Mark that the given page number contains one or more deallocated
        pages of the given order.
      
        If it returns true, it means that the page transitioned from full
        to not full and so the page table at the next higher level up
        also needs to be modified.
    */
    bool markAllocated(unsigned pageNum, unsigned order);

    bool subpageIsSplit(int subPage)
    {
        // TODO: more accurate...
        return types[subPage] >= PT_4P_SPLIT
            && types[subPage] <= PT_4M_SPLIT;
    }

    void dump();
};

} // namespace MMap
} // namespace Datacratic


#endif /* __mmap__page_table_h__ */

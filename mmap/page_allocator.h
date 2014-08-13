/* page_allocator.h                                                -*- C++ -*-
   Jeremy Barnes, 26 September 2011
   Copyright (c) 2011 Datacratic Inc.  All rights reserved.

   Basic allocator on which the others are built.
*/

#ifndef __mmap__page_allocator_h__
#define __mmap__page_allocator_h__

#include "mmap_const.h"
#include "memory_region.h"
#include "jml/utils/unnamed_bool.h"
#include "jml/arch/exception.h"
#include "jml/utils/compact_vector.h"
#include "jml/utils/exc_assert.h"
#include <boost/static_assert.hpp>

namespace Datacratic {
namespace MMap {


/** Return the page size for the given order. */
inline uint64_t
getPageSizeForOrder(int order)
{
    if (order < 0 || order > 5)
        throw ML::Exception("getPageSizeForOrder: invalid order");
    
    return (unsigned long long)(order != 0) << (2 + 10 * order);
}



/*****************************************************************************/
/* PAGE                                                                      */
/*****************************************************************************/

/** Encapsulates a page of memory, including its length. */

struct Page {
    Page(uint64_t offset = 0, int order = 0)
        : offset(offset), order(order)
    {
    }

    /** Return the page of the given order that contains the given address
        in memory.
    */
    static Page containing(uint64_t offset, int order)
    {
        ExcAssertGreaterEqual(order, 1);
        ExcAssertLessEqual(order, 5);
        int shift = 10 * order + 2;
        offset >>= shift;
        offset <<= shift;

        return Page(offset, order);
    }

    uint64_t offset;
    int order;

    uint64_t length() const
    {
        return getPageSizeForOrder(order);
    }

    uint64_t endOffset() const
    {
        return offset + length();
    }
    
    bool valid() const
    {
        return order > 0;
    }

    // Return the parent page that this is part of.  Order of -1 means
    // the next one up
    Page superpage(int order = -1) const
    {
        if (order == -1) order = this->order + 1;
        if (order <= this->order)
            throw ML::Exception("invalid order for superpage: %d isn't > %d",
                                order, this->order);
        if (order > 5)
            throw ML::Exception("no higher page");
        uint64_t mask = (1ULL << (2 + 10 * (order))) - 1;
        return Page(offset & ~mask, order);
    }

    Page subpage(unsigned n) const
    {
        return Page(offset + n * getPageSizeForOrder(order - 1), order - 1);
    }

    typedef ML::compact_vector<uint16_t, 5, uint16_t> Coords;

    /** Provides the coordinates for the page.  The entries are:
        coords[0]: page number (0 to 1023) saying which 4T page we're in
        coords[1]: (if order < 4) which 4G page we're in
        coords[2]: (if order < 3) which 4M page we're in
        coords[3]: (if order < 2) which 4K page we are

        These correspond to the following bits within the offset:

         zero   zero     [0]      [1]      [2]       [3]     zero
        +----+--------+--------+--------+--------+--------+--------+
        |    |        | L4 pg  | L3 pg  | L2 pg  | L1 pg  |        |
        |    |        |  num   |  num   |  num   |  num   |        |
        +----+--------+--------+--------+--------+--------+--------+
        64   62       52       42       32       22       12       0  

        The last 12 bits will be empty for an offset.
    */
    Coords coords();
    
    unsigned l1PageNumber() const { return (offset >> 12) & 1023; }
    unsigned l2PageNumber() const { return (offset >> 22) & 1023; }
    unsigned l3PageNumber() const { return (offset >> 32) & 1023; }
    unsigned l4PageNumber() const { return (offset >> 42) & 1023; }



    JML_IMPLEMENT_OPERATOR_BOOL(valid());

    std::string print() const;
};

std::ostream & operator << (std::ostream & stream, const Page & page);


/*****************************************************************************/
/* PAGE ALLOCATOR                                                            */
/*****************************************************************************/

/** Allows pages of various sizes to be allocated within a memory region.
    
    Page orders:
    1 = 2^12 = 4k bytes (one system page)
    2 = 2^22 = 4M bytes
    3 = 2^32 = 4G bytes
    4 = 2^42 = 4T bytes
    5 = 2^52 = 4P bytes (only one page of this order exists; referred to by
                         the metadata page... it always exists)

    Each page type is always aligned to its page.

*/

struct PageAllocator {

    PageAllocator();

    virtual ~PageAllocator();

    /** Return the underlying memory region. */
    virtual MemoryRegion & memoryRegion() const = 0;

    /** Allocate a page of the given order. */
    virtual Page allocatePage(int order) = 0;

    /** Deallocate a page of the given order. */
    virtual void deallocatePage(Page page) = 0;

    template<typename MapAs>
    RegionPtr<MapAs> mapPage(Page page)
    {
        ExcAssertEqual(page.order, 1);
        BOOST_STATIC_ASSERT(sizeof(MapAs) <= page_size);

        return RegionPtr<MapAs>(memoryRegion(), page.offset);
    }
};


/*****************************************************************************/
/* PAGE ALLOCATOR DESCRIPTOR                                                 */
/*****************************************************************************/

/** A metadata entry used to help describe where a page allocator's data
    goes and to make sure that we can know which one to use.
*/

struct PageAllocatorDescriptor {
    uint32_t type;
    uint32_t unused;
    uint64_t offset;
};

} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__page_allocator_h__ */

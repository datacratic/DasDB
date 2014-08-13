/* page_table.cc
   Jeremy Barnes, 2 November 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

*/

#include "page_table.h"
#include "page_allocator.h"
#include <boost/static_assert.hpp>

namespace Datacratic {
namespace MMap {

BOOST_STATIC_ASSERT(sizeof(PageTable) <= page_size);

std::string print(PageType type)
{
    switch (type) {
    case PT_EMPTY:             return "PT_EMPTY";
    case PT_METADATA:          return "PT_METADATA";
    case PT_PAGE_ALLOCATOR:    return "PT_PAGE_ALLOCATOR";

    case PT_L4_PTE:            return "PT_L4_PTE";
    case PT_L3_PTE:            return "PT_L3_PTE";
    case PT_L2_PTE:            return "PT_L2_PTE";
    case PT_L1_PTE:            return "PT_L1_PTE";

    case PT_4P_PAGE:           return "PT_4P_PAGE";
    case PT_4T_PAGE:           return "PT_4T_PAGE";
    case PT_4G_PAGE:           return "PT_4G_PAGE";
    case PT_4M_PAGE:           return "PT_4M_PAGE";
    case PT_4K_PAGE:           return "PT_4K_PAGE";

    case PT_4P_SPLIT:          return "PT_4P_SPLIT";
    case PT_4T_SPLIT:          return "PT_4T_SPLIT";
    case PT_4G_SPLIT:          return "PT_4G_SPLIT";
    case PT_4M_SPLIT:          return "PT_4M_SPLIT";

    default:
        return ML::format("PageType(%d)", type);
    }
}

std::ostream & operator << (std::ostream & stream, PageType type)
{
    return stream << print(type);
}


/*****************************************************************************/
/* PAGE TABLE                                                                */
/*****************************************************************************/

void
PageTable::
init(int order)
{
    // Empty the whole page
    memset(this, 0, page_size);
    this->order = order;
    this->magic = 0x1293847673827334;

    // Nothing is allocated
    allocated.init(false);

    // For the moment, everything underneath is full
    for (unsigned i = 0;  i < 4;  ++i)
        fullLevels[i].init(true);

    for (unsigned i = 0;  i < 11;  ++i)
        fullNodes[i].init(true);
}

bool
PageTable::
markDeallocated(unsigned pageNum, unsigned order)
{
    using namespace std;
    //cerr << "markDeallocated: pageNum " << pageNum
    //     << " order " << order << " my order " << this->order
    //     << endl;

    if (order == this->order) {
        // We have the bitmap for that page order here, so we simply
        // deal with it directly...
        setType(pageNum, PT_EMPTY);
        ExcAssert(allocated.isFull(pageNum));
        return allocated.markDeallocated(pageNum);
    }

    // Otherwise, we need to find the right full bitmap
    FullBitmap<1024> & bitmap = fullLevels[order - 1];
    return bitmap.markDeallocated(pageNum);
        
}

bool
PageTable::
markAllocated(unsigned pageNum, unsigned order)
{
    using namespace std;
    //cerr << "markAllocated: pageNum " << pageNum
    //     << " order " << order << " my order " << this->order
    //     << endl;

    if (order == this->order)
        throw ML::Exception("should use allocate() not markAllocated");

    ExcAssertGreaterEqual(order, 1);
    ExcAssertLess(order, this->order);

    // Otherwise, we need to find the right full bitmap
    FullBitmap<1024> & bitmap = fullLevels[order - 1];
    return bitmap.markAllocated(pageNum) == 1;
        
}

void
PageTable::
dump()
{
    using namespace std;
    cerr << "------------------------------------------" << endl;
    cerr << "PageTable order " << order << " valid " << valid() << endl;
    cerr << "allocated pages: (" << allocated.numFull() << ") - "
        << allocated.print() << endl;

    for (unsigned i = 0;  i < 1023;  ++i) {
        if (allocated.isFull(i)) {
            cerr << ML::format("%4d", i)
                 << ": " << print(PageType(types[i]))
                 << endl;
        }
    }

    for (unsigned i = 0;  i < 4;  ++i) {
        if (fullLevels[i].numFull() < 1024) {
            cerr << "contains free pages at L" << (i + 1) << " ("
                 << (1024 - fullLevels[i].numFull()) << "): ";
            for (unsigned j = 0;  j < 1024;  ++j)
                if (!fullLevels[i].isFull(j))
                    cerr << j << " ";
            cerr << "- " << fullLevels[i].print();
            cerr << endl;
        }
    }

    for (unsigned i = 0; i < 11; ++i) {
        if (fullNodes[i].numFull() < 1024) {
            cerr << "contains free node of size ordinal " << (i + 1) << " ("
                 << (1024 - fullNodes[i].numFull()) << "): ";
            for (unsigned j = 0;  j < 1024;  ++j)
                if (!fullNodes[i].isFull(j))
                    cerr << j << " ";
            cerr << "- " << fullNodes[i].print();
            cerr << endl;

        }
    }
}



} // namespace MMap
} // namespace Datacratic

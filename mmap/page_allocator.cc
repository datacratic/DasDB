/* page_allocator.cc
   Jeremy Barnes, 26 September 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Implementation of the page allocator class.
*/


#include "page_allocator.h"
#include "jml/arch/exception.h"
#include "jml/arch/format.h"


using namespace std;
using namespace ML;


namespace Datacratic {
namespace MMap {


/*****************************************************************************/
/* PAGE                                                                      */
/*****************************************************************************/



std::string
Page::
print() const
{
    const char * lengths[6] = { "NULL", "4k", "4M", "4G", "4T", "4P" };

    if (order == 0 && offset == 0)
        return "Page(NULL)";
    else if (order < 0 || order > 5)
        return ML::format("Page(order=INVALID(%d),offset=%llx)",
                          order, (long long)offset);
    return ML::format("Page(order=%d,offset=%llx,length=%s)",
                      order, (long long)offset, lengths[order]);
}

std::ostream & operator << (std::ostream & stream, const Page & page)
{
    return stream << page.print();
}


/*****************************************************************************/
/* PAGE ALLOCATOR                                                            */
/*****************************************************************************/

PageAllocator::
PageAllocator()
{
}

PageAllocator::
~PageAllocator()
{
}


} // namespace MMap
} // namespace Datacratic

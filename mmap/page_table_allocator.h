/* page_table_allocator.h                                          -*- C++ -*-
   Jeremy Barnes, 28 September 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Allocator using a page table scheme.
*/

#ifndef __mmap__page_table_allocator_h__
#define __mmap__page_table_allocator_h__

#include "page_allocator.h"
#include "jml/arch/exception.h"

namespace Datacratic {
namespace MMap {

struct PageTable;


/*****************************************************************************/
/* PAGE TABLE ALLOCATOR                                                      */
/*****************************************************************************/

/** A page allocator that takes a memory region and uses page tables to
    allocate pages.
*/

struct PageTableAllocator : public PageAllocator {

    /** Initialize a page table allocator from the given memory
        region.  The offset says where within the region there is
        a PageAllocatorDescriptor field that the allocator can use to
        initialize its data structures.
    */
    PageTableAllocator(MemoryRegion & region, size_t offset, bool init);

    virtual ~PageTableAllocator();

    /** Return the underlying memory region. */
    virtual MemoryRegion & memoryRegion() const
    {
        return *region_;
    }

    /** Allocate a page of the given order. */
    virtual Page allocatePage(int order);

    /** Allocate a page of the given order and set its page type. */
    Page allocatePageOfType(int order, uint8_t pageType);
    
    /** Deallocate a page that was previously allocated. */
    virtual void deallocatePage(Page page);

    /** Initialize the memory region to have an empty superblock. */
    void init(size_t offset);

//private:
    struct Metadata;

    // \todo Can this completely replace getMetadata?
    RegionPtr<Metadata> getMetadata();

    /** Find the page table of the given order that contains the given page.
        If order is -1 (default), then order is set to the page's order.

        The page table returned will be such that entry number
        getPageTableIndexForPage(page, order) will refer to the given
        page.

        Note that all page tables are actually order 1 pages (4K), and the
        definition of "order" here is a bit difficult... if I ask for the
        page table for an order 1 page, it's actually another order 1 page
        within the superpage (order 2).  Maybe a picture would help:

        +----------------------------------------------------------------+
        + Order 2 (4MB) page split into 1024 order 1 (4K) pages          +
        +----+----+----+----+----+----+------+---+-----------------------+
        |  0 |  1 |  2 |  3 |  4 |  5 |      | n |              ... 1023 |
        | MD | PT | PT | PT | PT | PT |      |   |                       |
        |    | L5 | L4 | L3 | L2 | L1 | ...  |   |  ... other 4K pages   |
        | ?? | ?? | ?? | ?? | ?? |    |      |   |                       |
        +----+----+----+----+----+----+------+---+-----------------------+
                                   ^           ^
                                   |           |
                                 result       page

        If we were to call getUninitializedPageTableForPage(page, 1), we'd
        get the indicated page as a result.  If we were to call
        getPageTableIndexForPage(page, 1), we'd get n (the index of the
        page within its page table) as a result.

        When the requested order is higher than the order of the current
        page, we do the call recursively.  So for
        getUninitializedPageTable(l1Page, 2), we return the page table
        entry for it's containing (order 2) table instead, and so on
    */
    RegionPtr<PageTable>
    getUninitializedPageTableForPage(Page page, int order = -1);

    /** Return the page table for the given page. */
    RegionPtr<PageTable>
    getPageTableForPage(Page page, int order = -1);

    /** Return the offset of the parent page table of the given page. */
    static uint64_t getPageTableOffset(Page page, int order = -1);
    
    /** Return the index of this page within its parent page table. */
    static int getPageTableIndex(Page page, int order = -1);

    Page doAllocatePage(int order, bool toSplit);

    Page allocatePageAndSplit(int order, bool allocate);

    /** Notify that the given page (of order n) contains (eventually) one or
        more free subpages of order "order" (which must be < n).

        The way that the free page tracking works is as follows:

        - The L5 page table has a bitmap for each page order (1, 2, 3)
          that contains 1024 entries, one for each of the L4 (4TB) pages.
          The bit is set to one if somewhere in that 4TB page there is
          at least one page free of the given order.  The allocation status
          of the L4 pages is tracked directly.

        If we want (say) an L1 page, we can look it up as follows:
        - Look in the L5 page table to find a L4 page that contains at least
          one free L1 page.
        - Go to that L4 (4TB) page's page table, and find a L3 (4GB) page
          that contains a free L1 page.
        - Go to that L3 (4GB) page's page table, and find a L2 (4MB) page
          that contains a free L1 page.
        - Go to that L2 (4MB) page's page table, and allocate the free L1
          page.

       This function recursively sets the correct bits in response to a free
       page for this to happen.  Most often, there is just a single bit that
       will need to be set which limits contention.
    */
    void notifyPageContainsFreeSubpages(Page page, int order,
                                        uint64_t numFree);

    /** Allocate a free page of the specified order.  Implements the
        algorithm given above.
        
        If none is available, it will return a null page and a new series
        of pages of the given order will need to be created.
    */
    Page allocateFreePage(int order);

    /** Split the given page of the given order into 1024 pages of the
        next order down.  Must not already be split.

        If allocate is true, then it returns a single page of that memory
        as already allocated.
        
        The rest will be marked as free.
    */
    Page splitPage(Page page, bool allocate);

    /** Dump the page table containing this page. */
    void dumpPageTableInfo(Page page);
    
    MemoryRegion * region_;  ///< Where our memory lives
};


} // namespace MMap
} // namespace Datacratic

#endif /* __mmap__page_table_allocator_h__ */


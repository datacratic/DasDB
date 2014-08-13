/* page_table_allocator.cc
   Jeremy Barnes, 28 September 2011
   Copyright (c) 2011 Datacratic Inc.  All rights reserved.

*/

#include "page_table_allocator.h"
#include "page_table_allocator_itl.h"
#include "jml/arch/exception.h"
#include "jml/utils/exc_assert.h"
#include <iostream>
#include <boost/static_assert.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/thread.hpp>


using namespace std;
using namespace ML;


namespace Datacratic {
namespace MMap {


/* Both of these need to be able to fit within one page. */
BOOST_STATIC_ASSERT(sizeof(PageTableAllocator::Metadata) <= page_size);


/*****************************************************************************/
/* PAGE TABLE ALLOCATOR                                                      */
/*****************************************************************************/

PageTableAllocator::
PageTableAllocator(MemoryRegion & region, size_t offset, bool initTable)
    : region_(&region)
{
    if (initTable)
        init(offset);
}

PageTableAllocator::
~PageTableAllocator()
{
}

/** Set up the first mapping. */
void
PageTableAllocator::
init(size_t offset)
{
    // We need at least 6 pages to start off with...
    //
    // +---------------+----------------+----+----+----+----+
    // | file metadata | alloc metadata | l4 | l3 | l2 | l1 |
    // +---------------+----------------+----+----+----+----+

    if (region_->length() < 6 * page_size)
        region_->grow(6 * page_size);

    ExcAssert(region_->isPinned());

    // Page 1 is the metadata... make sure it's valid
    auto md = getMetadata();
    md->init();
    ExcAssert(md->valid());

    // Allocate a level 5 page
    allocatePageAndSplit(5, false /* allocate */);
}

//static boost::recursive_mutex mutex;

Page
PageTableAllocator::
splitPage(Page page, bool allocate)
{
    //cerr << "splitPage(" << page << ", " << allocate << ")" << endl;

    // Impossible to split smaller than 4k with the page allocator
    ExcAssert(page.order > 1);

    // First thing: make sure that the first 6 pages of this page are
    // within the region so that we know we can write to the page
    // tables.

    uint64_t minOffset = page.offset + 6 * page_size;

    if (region_->length() < minOffset)
        region_->grow(minOffset);

    // Find the page table, which will be within the page we're splitting
    auto pt = getUninitializedPageTableForPage(page.subpage(0));

    // Initialize the page table
    pt->init(page.order - 1);

    int numFreePages;
    Page result;

    if (page.order == 2) {
        // We're in a 4M page that we want to split up into 4k subpages.

        // The first 6 subpages may be reserved for the different levels of
        // page tables depending upon whether or not we're the first page
        // of a higher order.

        bool hasMetadata = page.offset == 0;
        bool hasL4PT = page.l4PageNumber() == 0;
        bool hasL3PT = page.l3PageNumber() == 0;
        bool hasL2PT = page.l2PageNumber() == 0;
        bool hasL1PT = page.l1PageNumber() == 0;

#if 0
        cerr << "split: page " << page << " hasL4 = " << hasL4PT
             << " hasL3 = " << hasL3PT << " hasL2 = " << hasL2PT
             << " hasL1 = " << hasL1PT << endl;
        cerr << "page.offset = " << ML::format("%llx", page.offset)
             << endl;
        cerr << "(page.offset & ~((1ULL << 22) - 1))"
             << ML::format("%llx", (page.offset & ~((1ULL << 22) - 1)))
             << endl;
        cerr << ML::format("((1ULL << 22) - 1) = %llx", ((1ULL << 22) - 1))
             << endl;
#endif

        if (!hasL1PT) {
            cerr << "page = " << page << endl;
        }

        // Must be true, since we're accessing it above (as the pt variable)
        ExcAssert(hasL1PT);

        int numReserved = 0;

        if (hasMetadata) {   // two pages at start
            pt->reserve(0);
            pt->reserve(1);
            pt->setType(0, PT_METADATA);
            pt->setType(1, PT_PAGE_ALLOCATOR);
            numReserved += 2;
        }
        else {
            if (allocate) {
                result = page.subpage(0);
                pt->reserve(0);
            }
        }

        if (hasL4PT) {      // One every 4PB = 2^52 bytes... so one!
            pt->reserve(2);
            pt->setType(2, PT_L4_PTE);
            ++numReserved;
        } else {
            if (!result && allocate) {
                result = page.subpage(2);
                pt->reserve(2);
            }
        }

        if (hasL3PT) {      // One every 4TB = 2^42 bytes
            pt->reserve(3);
            pt->setType(3, PT_L3_PTE);
            ++numReserved;
        } else {
            if (!result && allocate) {
                result = page.subpage(3);
                pt->reserve(3);
            }
        }

        if (hasL2PT) {      // One every 4GB = 2^32 bytes
            pt->reserve(4);
            pt->setType(4, PT_L2_PTE);
            ++numReserved;
        } else {
            if (!result && allocate) {
                result = page.subpage(4);
                pt->reserve(4);
            }
        }

        if (hasL1PT) {      // One every 4MB = 2^22 bytes
            pt->reserve(5);
            pt->setType(5, PT_L1_PTE);
            ++numReserved;
        } else {
            if (!result && allocate) {
                result = page.subpage(5);
                pt->reserve(5);
            }
        }

        if (!result && allocate) {
            result = page.subpage(6);
            pt->reserve(6);
        }

        numFreePages = 1024 - numReserved - allocate;
    }
    else {
        // The first page has to be split all the way down so that there
        // is room for the page tables
        
        pt->reserve(0);
        splitPage(page.subpage(0), false /* allocate */);
        pt->setType(0, PageType(PT_4M_SPLIT + 3 - page.order));

        // The rest of them are free
        numFreePages = 1023 - allocate;

        if (allocate) {
            pt->reserve(1);
            result = page.subpage(1);
        }
    }

    // Notify that this page contains free pages of the given order.
    notifyPageContainsFreeSubpages(page, page.order - 1, numFreePages);
    
    //cerr << "splitPage returned " << result << endl;

    //cerr << "after split" << endl;
    //dumpPageTableInfo(page.subpage(0));

    return result;
}

Page
PageTableAllocator::
allocatePage(int order)
{
    //boost::lock_guard<boost::recursive_mutex> guard(mutex);
    ExcAssert(region_->isPinned());
    return doAllocatePage(order, false);
}

Page
PageTableAllocator::
allocatePageOfType(int order, uint8_t pageType)
{
    ExcAssert(region_->isPinned());

    // TODO: as part of allocating it
    Page allocated = allocatePage(order);

    // TODO: on exception...

    if (!allocated)
        throw ML::Exception("couldn't allocate page");
    
    auto pt = getPageTableForPage(allocated);
    int index = getPageTableIndex(allocated);

    //cerr << "index = " << index << endl;

    pt->setType(index, (PageType)pageType);

    return allocated;
}

Page
PageTableAllocator::
allocatePageAndSplit(int order, bool allocate)
{
    // TODO: don't allow for more than one 4G page to be allocated at once

    Page bigPage = doAllocatePage(order, true /* to_split */);
    try {
        return splitPage(bigPage, allocate);
    }
    catch(RegionResizeException& ex) {
        // If this exception is raised then the split page wasn't initialized
        // so deallocate it. Note that this should be a fairly rare event.
        deallocatePage(bigPage);
        throw;
    }
}

Page
PageTableAllocator::
doAllocatePage(int order, bool toSplit)
{
    //cerr << "doAllocatePage(" << order << ")" << endl;
    if (order < 0 || order > 5)
        throw ML::Exception("attempt to allocate page of invalid order");

    if (order == 5) {
        ExcAssert(toSplit);

        // There should only be one of these...
        auto md = getMetadata();

        ExcAssert(md->valid());

        if (md->allocatedOrder5)
            throw ML::Exception("Order 5 page already allocated");

        // TODO: make sure it's atomic(?) (shouldn't really be necessary...)
        
        md->allocatedOrder5 = true;

        return Page(0, 5);
    }

    if (order == 1 && toSplit)
        throw ML::Exception("can't split a level 1 page");

    // 1.  Look in the freelist for a page to find a higher level
    //     page that has at least one free entry.

    PageType type;
    if (toSplit) type = PageType(PT_4M_SPLIT + 2 - order);
    else type = PageType(PT_4K_PAGE + 1 - order);

    Page result = allocateFreePage(order);
    bool gotFree = result;
    if (!result) result = allocatePageAndSplit(order + 1, true /* allocate */);
    if (!result)
        throw ML::Exception("couldn't allocate any pages");

    {
        // Update the page's type
        auto pt = getPageTableForPage(result);
        int index = getPageTableIndex(result);

        if (pt->getType(index) != PT_EMPTY) {
            cerr << "gotFree = " << gotFree << endl;
            cerr << "result = " << result << endl;
            cerr << "index = " << index << endl;
            cerr << "pt = " << endl;
            dumpPageTableInfo(result);
        }

        ExcAssertEqual(pt->getType(index), PT_EMPTY);
        pt->setType(index, type);
    }

    // If we're not splitting, make sure that the page is present and
    // backed
    if (!toSplit && result.endOffset() > region_->length()) {
        try {
            region_->grow(result.endOffset());
        }
        catch (RegionResizeException& ex) {
            // If the resize failed, deallocate the page so it doesn't leak.
            deallocatePage(result);
            throw;
        }
    }

    return result;
}

void
PageTableAllocator::
deallocatePage(Page page)
{
    //boost::lock_guard<boost::recursive_mutex> guard2(mutex);

    ExcAssert(region_->isPinned());

    //cerr << "deallocatePage(" << page << ")" << endl;

    if (page.order < 1 || page.order > 5)
        throw ML::Exception("attempt to deallocate page of invalid order");

    if (page.order == 5)
        throw ML::Exception("the level 5 page should never be deallocated");
    
    // Set the page type and allocated flag to false

    bool needUpdate = true;

    for (int order = page.order;  needUpdate && order < 5;  ++order) {
        // The page table was full before but now it's not.  We need to recurse
        // down marking each stage non-full.
        
        //cerr << "marking order " << order << " deallocated" << endl;

        auto pt = getPageTableForPage(page, order);

        //dumpPageTableInfo(Page(page.offset, order));

        needUpdate = pt->markDeallocated(getPageTableIndex(page, order),
                                        page.order);
    }
}

uint64_t
PageTableAllocator::
getPageTableOffset(Page page, int order)
{
    if (order == -1) order = page.order;
    if (order == 5) return page_size;
    Page super = page.superpage(order + 1);
    return super.offset + (6 - order) * page_size;
}

RegionPtr<PageTable>
PageTableAllocator::
getUninitializedPageTableForPage(Page page, int order)
{
    if (order == -1) order = page.order;

    uint64_t offset = getPageTableOffset(page, order);

    // We can't make it grow since that may need a write lock and we
    // already have the read lock... that needs to be handled somewhere
    // else.

    return RegionPtr<PageTable>(*region_, offset);
}

RegionPtr<PageTable>
PageTableAllocator::
getPageTableForPage(Page page, int order)
{
    if (order == -1) order = page.order;
    auto result = getUninitializedPageTableForPage(page, order);
    ExcAssert(result->valid());
    ExcAssertEqual(result->order, order);
    return result;
}

RegionPtr<PageTableAllocator::Metadata>
PageTableAllocator::
getMetadata()
{
    return RegionPtr<Metadata>(*region_, page_size);
}

Page
PageTableAllocator::
allocateFreePage(int order)
{
    ExcAssertGreaterEqual(order, 1);

    // The one L5 page is always split and so can't be allocated
    ExcAssertLessEqual(order, 4);

    // Try up to 3 times
    int NUM_ATTEMPTS = 3;

    Page result;

    //int threadNum = random();

    //cerr << "allocateFreePage(order=" << order << ")" << endl;

    for (int attempt = 0;  !result && attempt < NUM_ATTEMPTS;  ++attempt) {

        Page page(0, 5);
        

        // Go from bigger to smaller pages, narrowing down at each
        // step...
        for (int currentOrder = 5;  currentOrder > order;  --currentOrder) {

            //cerr << "attempt " << attempt << " page " << page
            //     << " currentOrder " << currentOrder
            //     << " order " << order << endl;

            // Find the page table
            auto pt = getPageTableForPage(page.subpage(0));
            
            ExcAssert(pt->valid());
            ExcAssertEqual(pt->order, page.order - 1);

            //dumpPageTableInfo(page.subpage(0));
            
            int startAt = 0;//(currentOrder > 3 ? 0 : random());

            if (currentOrder == order + 1) {
                // Finally, we allocate a page
                int pageNum;
                bool full;

                boost::tie(pageNum, full) = pt->allocate(startAt);

                //cerr << "pageNum = " << pageNum << " full = "
                //     << full << endl;

                if (pageNum == -1)
                    break;  // full; backtrack and try again

                result = page.subpage(pageNum);

                //cerr << "result = " << result << endl;

                while (pageNum != -1 && full && page.order < 5) {
                    // we moved from not full to full... we need to pass
                    // this information down
                    int index = getPageTableIndex(page);
                    //cerr << "page = " << page << " index = " << index
                    //     << endl;

                    auto pt = getPageTableForPage(page);

                    //dumpPageTableInfo(page);

                    try {
                        full = pt->markAllocated(index, result.order);
                    } catch (...) {
                        cerr << "marking allocated: index = " << index
                             << " order = " << order << " page = "
                             << page << " result = " << result << endl;
                        throw;
                    }

                    //cerr << "after markAllocated: full = " << full << endl;
                    
                    //dumpPageTableInfo(page);
                    page = page.superpage();
                }
            }
            else {
                // We're not yet down to the last order.  Look in the
                // bitmap for our order.
                FullBitmap<1024> & bitmap = pt->fullLevels[order - 1];

                int subPage = bitmap.getNonFullEntry(startAt);

                //cerr << "subPage = " << subPage << endl;

                if (subPage == -1)
                    break;  // full; backtrack and try again

                ExcAssert(pt->subpageIsSplit(subPage));

                page = page.subpage(subPage);
            }
        }
    }

    return result;
}

void
PageTableAllocator::
notifyPageContainsFreeSubpages(Page page, int order, uint64_t numFree)
{
    // TODO: contains the same logic as deallocatePage... unify them

    //cerr << "notifyPageContainsFreeSubpages(" << page << ", " << order
    //     << ", " << numFree << ")" << endl;

    // We assume that the l5 page contains free subpages of all kinds
    if (page.order == 5)
        return;

    ExcAssertGreaterEqual(order, 1);
    ExcAssertLessEqual(order, page.order);

    int subpage = getPageTableIndex(page);

    //cerr << "  subpage = " << subpage << endl;

    auto pt = getPageTableForPage(page);
    ExcAssertEqual(pt->order, page.order);

    bool needPropagation;

    if (order == page.order)
        needPropagation = pt->allocated.markDeallocated(subpage);
    else
        needPropagation = pt->fullLevels[order - 1].markDeallocated(subpage);

    if (needPropagation)
        notifyPageContainsFreeSubpages(page.superpage(), order, numFree);
}

int
PageTableAllocator::
getPageTableIndex(Page page, int order)
{
    if (order == -1) order = page.order;
    if (order == 5) return 0;  // only one order 5 page

    //cerr << "getPageTableIndex: page " << page << " order " << order
    //     << endl;

    Page super = page.superpage(order + 1);
    int result = (page.offset - super.offset) / getPageSizeForOrder(order);
    ExcAssertGreaterEqual(result, 0);
    ExcAssertLess(result, 1024);
    return result;
}

void
PageTableAllocator::
dumpPageTableInfo(Page page)
{
    ExcAssert(region_->isPinned());
    // Dump page table info for the given page.
    auto pt = getPageTableForPage(page);

    pt->dump();
}

} // namespace MMap
} // namespace Datacratic

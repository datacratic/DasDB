/* page_table_allocator_test.cc
   Jeremy Barnes, 10 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for the page table allocator.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "jml/utils/smart_ptr_utils.h"
#include "jml/utils/string_functions.h"
#include "jml/arch/atomic_ops.h"
#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/function.hpp>
#include <iostream>
#include "mmap/memory_region.h"
#include "mmap/page_table_allocator.h"
#include "mmap/page_table_allocator_itl.h"
#include <sys/mman.h>
#include <set>


using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;

BOOST_AUTO_TEST_CASE( test_table_indexes )
{
    Page page(63 * page_size, 1);  // 4k page at page 63

    auto getPageTableOffset = PageTableAllocator::getPageTableOffset;
    auto getPageTableIndex = PageTableAllocator::getPageTableIndex;

    // The L1 page table containing this page is at page 5
    BOOST_CHECK_EQUAL(getPageTableOffset(page, 1), 5 * page_size);
    BOOST_CHECK_EQUAL(getPageTableIndex(page, 1), 63);

    // The L2 page table containing this page is at page 4
    BOOST_CHECK_EQUAL(getPageTableOffset(page, 2), 4 * page_size);
    BOOST_CHECK_EQUAL(getPageTableIndex(page, 2), 0);
    
    // etc...
    BOOST_CHECK_EQUAL(getPageTableOffset(page, 3), 3 * page_size);
    BOOST_CHECK_EQUAL(getPageTableIndex(page, 3), 0);

    BOOST_CHECK_EQUAL(getPageTableOffset(page, 4), 2 * page_size);
    BOOST_CHECK_EQUAL(getPageTableIndex(page, 4), 0);

    BOOST_CHECK_EQUAL(getPageTableOffset(page, 5), 1 * page_size);
    BOOST_CHECK_EQUAL(getPageTableIndex(page, 5), 0);
}

BOOST_AUTO_TEST_CASE( test_page_table_allocator_init )
{
    // Get our memory region
    MallocRegion region;

    // Create a page allocator for that region.  That will set up its
    // internal data structures.  The 8 says that we have a 64 bit field
    // at position 8 to use/set for our internal metadata, which is just
    // a pointer to the allocator superblock at page 1.
    unique_ptr<PageTableAllocator> alloc;
    alloc.reset(MMAP_PIN_REGION_RET(
                    region, new PageTableAllocator(region, 8, true)));

    MMAP_PIN_REGION(region) 
    {
        BOOST_CHECK(alloc->getMetadata()->valid());
    }
    MMAP_UNPIN_REGION;

    MMAP_PIN_REGION(region) 
    {
        auto t1 = alloc->getPageTableForPage(Page(0, 1));
        BOOST_CHECK(t1->valid());

        alloc->dumpPageTableInfo(Page(7 * page_size, 1));

        BOOST_CHECK_EQUAL(t1->order, 1);
        BOOST_CHECK_EQUAL(t1->getType(0), PT_METADATA);
        BOOST_CHECK_EQUAL(t1->getType(1), PT_PAGE_ALLOCATOR);
        BOOST_CHECK_EQUAL(t1->getType(2), PT_L4_PTE);
        BOOST_CHECK_EQUAL(t1->getType(3), PT_L3_PTE);
        BOOST_CHECK_EQUAL(t1->getType(4), PT_L2_PTE);
        BOOST_CHECK_EQUAL(t1->getType(5), PT_L1_PTE);
        BOOST_CHECK_EQUAL(t1->getType(6), PT_EMPTY);
    }
    MMAP_UNPIN_REGION;

    MMAP_PIN_REGION_INL(region, alloc->dumpPageTableInfo(Page(0, 2)));
    
    MMAP_PIN_REGION(region)
    {
        auto t2 = alloc->getPageTableForPage(Page(0, 2));
        
        BOOST_CHECK_EQUAL(t2->getType(0), PT_4M_SPLIT);
        BOOST_CHECK_EQUAL(t2->getType(1), PT_EMPTY);
        BOOST_CHECK_EQUAL(t2->fullLevels[0].isFull(0), false);
        BOOST_CHECK_EQUAL(t2->fullLevels[0].isFull(1), true);
    }
    MMAP_UNPIN_REGION;

    MMAP_PIN_REGION_INL(region, alloc->dumpPageTableInfo(Page(0, 3)));

    MMAP_PIN_REGION(region)
    {
        auto t3 = alloc->getPageTableForPage(Page(0, 3));
        
        BOOST_CHECK_EQUAL(t3->getType(0), PT_4G_SPLIT);
        BOOST_CHECK_EQUAL(t3->getType(1), PT_EMPTY);
        BOOST_CHECK_EQUAL(t3->fullLevels[0].isFull(0), false);
        BOOST_CHECK_EQUAL(t3->fullLevels[0].isFull(1), true);
    }
    MMAP_UNPIN_REGION;

    MMAP_PIN_REGION_INL(region, alloc->dumpPageTableInfo(Page(0, 4)));

    MMAP_PIN_REGION(region)
    {
        auto t4 = alloc->getPageTableForPage(Page(0, 4));
        
        BOOST_CHECK_EQUAL(t4->getType(0), PT_4T_SPLIT);
        BOOST_CHECK_EQUAL(t4->getType(1), PT_EMPTY);
        BOOST_CHECK_EQUAL(t4->fullLevels[0].isFull(0), false);
        BOOST_CHECK_EQUAL(t4->fullLevels[0].isFull(1), true);
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE( test_page_table_allocator_alloc )
{
    // Get our memory region
    MallocRegion region;

    // Create a page allocator for that region.  That will set up its
    // internal data structures.  The 8 says that we have a 64 bit field
    // at position 8 to use/set for our internal metadata, which is just
    // a pointer to the allocator superblock at page 1.
    unique_ptr<PageTableAllocator> alloc;
    alloc.reset(MMAP_PIN_REGION_RET(
                    region, new PageTableAllocator(region, 8, true)));

    // Allocate 4096 bytes of memory
    Page page = MMAP_PIN_REGION_RET(region, alloc->allocatePage(1));

    BOOST_CHECK_EQUAL(page.order, 1);
    BOOST_CHECK_GE(page.offset, 6 * page_size);
    BOOST_CHECK_GE(region.length(), page.offset + page.length());

    MMAP_PIN_REGION(region)
    {
        auto t1 = alloc->getPageTableForPage(page);
        BOOST_CHECK(t1->valid());
        alloc->dumpPageTableInfo(page);
    }
    MMAP_UNPIN_REGION;

    // Free that memory
    MMAP_PIN_REGION_INL(region, alloc->deallocatePage(page));
}    

BOOST_AUTO_TEST_CASE( test_sizes )
{
    cerr << "sizeof(FullBitmap<1024>) = "
         << sizeof(FullBitmap<1024>) << endl;
    cerr << "sizeof(PageTableAllocator::PageTable) = "
         << sizeof(PageTable) << endl;
}

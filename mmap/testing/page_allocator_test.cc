/* mmap_trie_test.cc
   Jeremy Barnes, 10 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for the mmap trie class.
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
#include <sys/resource.h>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;

BOOST_AUTO_TEST_CASE( test_malloc_region )
{
    cerr << "sizeof(PageTable) = " << sizeof(PageTable) << endl;

    // Get our memory region
    MallocRegion region;

    // Create a page allocator for that region.  That will set up its
    // internal data structures.  The 8 says that we have a 64 bit field
    // at position 8 to use/set for our internal metadata, which is just
    // a pointer to the allocator superblock at page 1.
    unique_ptr<PageTableAllocator> palloc;
    palloc.reset(MMAP_PIN_REGION_RET(
                    region, new PageTableAllocator(region, 8, true)));
    PageTableAllocator& alloc = *palloc;

    MMAP_PIN_REGION(region)
    {
        BOOST_CHECK(alloc.getMetadata()->valid());
    }
    MMAP_UNPIN_REGION;

    Page page;

    {
        // Allocate 4096 bytes of memory
        page = MMAP_PIN_REGION_RET(region, alloc.allocatePage(1));

        BOOST_CHECK_EQUAL(page.order, 1);
        BOOST_CHECK_EQUAL(page.offset, 6 * page_size);
        BOOST_CHECK_GE(region.length(), page.offset + page.length());

        // Free that memory
        MMAP_PIN_REGION_INL(region, alloc.deallocatePage(page));
    }

    {
        // Check we can allocate it again
        page = MMAP_PIN_REGION_RET(region, alloc.allocatePage(1));

        BOOST_CHECK_EQUAL(page.order, 1);
        BOOST_CHECK_EQUAL(page.offset, 6 * page_size);
        BOOST_CHECK_GE(region.length(), page.offset + page.length());

        // Free that memory
        MMAP_PIN_REGION_INL(region, alloc.deallocatePage(page));
    }

    {
        // Now a 4M page
        page = MMAP_PIN_REGION_RET(region, alloc.allocatePage(2));

        BOOST_CHECK_EQUAL(page.order, 2);
        BOOST_CHECK_EQUAL(page.offset, 1024 * page_size);
        BOOST_CHECK_GE(region.length(), page.offset + page.length());

        MMAP_PIN_REGION_INL(region, alloc.deallocatePage(page));
    }

    // Now 1024 4k pages

    vector<Page> pages;
    for (unsigned i = 0;  i < 1024;  ++i) {
        page = MMAP_PIN_REGION_RET(region, alloc.allocatePage(1));
        // cerr << "i = " << i << " page = " << page << endl;
        BOOST_CHECK_EQUAL(page.order, 1);
        BOOST_CHECK_GE(region.length(), page.offset + page.length());
        pages.push_back(page);
    }

    // And deallocate them all
    for (unsigned i = 0;  i < pages.size();  ++i)
        MMAP_PIN_REGION_INL(region, alloc.deallocatePage(pages[i]));
}

BOOST_AUTO_TEST_CASE( test_lotsa_allocations )
{
    int npages = 4096;
    int nerrors = 0;
    int threadNum = 1;

    // Get our memory region
    MallocRegion region;

    unique_ptr<PageTableAllocator> palloc;
    palloc.reset(MMAP_PIN_REGION_RET(
                    region, new PageTableAllocator(region, 8, true)));
    PageTableAllocator& alloc = *palloc;

    vector<Page> pages;
    for (unsigned i = 0;  i < npages;  ++i) {
        Page page = MMAP_PIN_REGION_RET(region, alloc.allocatePage(1));

        //cerr << "allocating " << i << " got "
        //     << page << endl;


        if (!page) {
            ML::atomic_add(nerrors, 1);
            continue;
        }

        MMAP_PIN_REGION(region)
        {
            auto place = region.range<int>(page.offset);

            if (*place != 0) {
                cerr << "page was already allocated" << endl;
                cerr << page << endl;
                ML::atomic_add(nerrors, 1);
            }

            *place = threadNum;
        }
        MMAP_UNPIN_REGION;
                    
        pages.push_back(page);
    }

    for (unsigned i = 0;  i < pages.size();  ++i) {
        Page page = pages[i];

        MMAP_PIN_REGION(region)
        {
            auto place = region.range<int>(page.offset);
                    
            if (*place != threadNum) {
                cerr << "page had wrong thread number" << endl;
                ML::atomic_add(nerrors, 1);
            }
            *place = 0;

            alloc.deallocatePage(pages[i]);
        }
        MMAP_UNPIN_REGION;
    }

    BOOST_CHECK_EQUAL(nerrors, 0);
}

BOOST_AUTO_TEST_CASE( test_mixed_allocation )
{
    int npages = 4096;
    int nerrors = 0;
    int threadNum = 1;

    // Get our memory region
    MallocRegion region;

    unique_ptr<PageTableAllocator> palloc;
    palloc.reset(MMAP_PIN_REGION_RET(
                    region, new PageTableAllocator(region, 8, true)));
    PageTableAllocator& alloc = *palloc;


    vector<Page> pages;
    for (unsigned i = 0;  i < npages;  ++i) {
        Page page = MMAP_PIN_REGION_RET(
                region, alloc.allocatePage(random() % 20 == 0 ? 1 : 1));

        //cerr << "allocating " << i << " got "
        //     << page << endl;


        if (!page) {
            ML::atomic_add(nerrors, 1);
            continue;
        }

        MMAP_PIN_REGION(region)
        {
            auto place = region.range<int>(page.offset);

            if (*place != 0) {
                cerr << "page was already allocated" << endl;
                cerr << page << endl;
                ML::atomic_add(nerrors, 1);
            }

            *place = threadNum;
        }
        MMAP_UNPIN_REGION;
                    
        pages.push_back(page);
    }

    for (unsigned i = 0;  i < pages.size();  ++i) {
        Page page = pages[i];

        MMAP_PIN_REGION(region)
        {
            auto place = region.range<int>(page.offset);
                    
            if (*place != threadNum) {
                cerr << "page had wrong thread number" << endl;
                ML::atomic_add(nerrors, 1);
            }
            *place = 0;

            alloc.deallocatePage(pages[i]);
        }
        MMAP_UNPIN_REGION;
    }

    BOOST_CHECK_EQUAL(nerrors, 0);
}

BOOST_AUTO_TEST_CASE( test_multithreaded_allocation )
{
    size_t toAlloc = 1024ULL * 1024 * page_size;
    
    struct rlimit limit;
    int res = getrlimit(RLIMIT_AS, &limit);

    cerr << "toAlloc = " << toAlloc << " limit = "
         << limit.rlim_cur << " max = " << limit.rlim_max << endl;

    if (res == -1)
        throw ML::Exception(errno, "getrlimit");
    if (limit.rlim_cur < toAlloc * 2) {
        cerr << "can't run this test as maximum vmem ulimit of "
             << limit.rlim_cur / 1024 / 1024 << "MB is too small "
             << "to allocate " << toAlloc / 1024 / 1024 << "MB" << endl;
        return;  // can't run test
    }

    // Test that this machine can allocate that much memory
    {
        void * mem = malloc(toAlloc);
        if (!mem) {
            cerr << "test malloc of " << toAlloc << " bytes failed" << endl;
            cerr << "not running test" << endl;
            return;
        }
        free(mem);
    }

    // Get our memory region
    MallocRegion region(PERM_READ_WRITE, toAlloc);

    unique_ptr<PageTableAllocator> palloc;
    palloc.reset(MMAP_PIN_REGION_RET(
                    region, new PageTableAllocator(region, 8, true)));
    PageTableAllocator& alloc = *palloc;

    int nthreads = 8;
    int npages = 1024;

    // Total memory: nthreads * npages / 2 * 4M

    uint64_t nerrors = 0;
    uint64_t nallocations = 0;

    volatile bool finished = false;

    // Do lots of allocatin and allocatin...
    auto doAllocThread = [&] (int threadNum)
        {
            for (unsigned i = 0;  !finished;  ++i) {
                vector<Page> pages;
                for (unsigned i = 0;  i < npages;  ++i) {
                    Page page = MMAP_PIN_REGION_RET(
                            region, alloc.allocatePage(1 /*random() % 2 + 1*/));

                    //cerr << "allocating " << i << " got "
                    //     << page << endl;


                    if (!page) {
                        ML::atomic_add(nerrors, 1);
                        continue;
                    }

                    ML::atomic_add(nallocations, 1);

                    MMAP_PIN_REGION(region)
                    {
                        auto place = region.range<int>(page.offset);

                        if (*place != 0) {
                            cerr << "page was already allocated" << endl;
                            cerr << page << endl;
                            ML::atomic_add(nerrors, 1);
                        }

                        *place = threadNum;
                    }
                    MMAP_UNPIN_REGION;
                    
                    pages.push_back(page);
                }

                for (unsigned i = 0;  i < pages.size();  ++i) {
                    Page page = pages[i];

                    MMAP_PIN_REGION(region)
                    {
                        auto place = region.range<int>(page.offset);
                    
                        if (*place != threadNum) {
                            cerr << "page had wrong thread number" << endl;
                            ML::atomic_add(nerrors, 1);
                        }
                        *place = 0;

                        alloc.deallocatePage(pages[i]);
                    }
                    MMAP_UNPIN_REGION;
                }
            }
        };

    cerr << "test with single thread only" << endl;
    // First, test with only one thread to make sure the test passes...
    {
        boost::thread_group tg;
        
        tg.create_thread(boost::bind<void>(doAllocThread, 0));
        
        sleep(1);
        
        finished = true;
        
        tg.join_all();

        BOOST_CHECK_EQUAL(nerrors, 0);
        cerr << "nallocations " << nallocations << endl;
    }

    nerrors = 0;
    nallocations = 0;

    cerr << "test with multiple threads" << endl;
    // Now with multiple threads
    {
        finished = false;
        
        boost::thread_group tg;
        
        for (unsigned i = 0;  i < nthreads;  ++i)
            tg.create_thread(boost::bind<void>(doAllocThread, i));
        
        sleep(1);
        
        finished = true;
        
        tg.join_all();

        BOOST_CHECK_EQUAL(nerrors, 0);
        cerr << "nallocations " << nallocations << endl;
    }
}

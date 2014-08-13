/* mmap_trie_test.cc
   Jeremy Barnes, 10 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for the mmap trie class.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap/memory_region.h"
#include "mmap/page_table_allocator.h"
#include "mmap/memory_allocator.h"
#include "soa/types/date.h"
#include "soa/utils/future_fix.h"
#include "jml/utils/smart_ptr_utils.h"
#include "jml/utils/string_functions.h"
#include "jml/arch/atomic_ops.h"

#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/function.hpp>
#include <boost/test/unit_test.hpp>
#include <iostream>
#include <future>
#include <stack>
#include <sys/mman.h>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;

BOOST_AUTO_TEST_CASE( test_lotsa_allocations )
{
    int nallocs = 1000000;
    int nerrors = 0;
    int threadNum = 1;

    // Get our memory region, page table allocator and memory allocator
    MallocRegion region;
    MemoryAllocator alloc(region, true);

    Date before = Date::now();

    vector<uint64_t> offsets;
    for (unsigned i = 0;  i < nallocs;  ++i) {
        uint64_t offset = MMAP_PIN_REGION_RET(
                region, alloc.nodeAlloc.allocate(64));

        MMAP_PIN_REGION(region)
        {
            auto place = region.range<int>(offset);

            if (*place != 0) {
                cerr << "block was already allocated" << endl;
                cerr << offset << endl;
                ML::atomic_add(nerrors, 1);
            }
        
            *place = threadNum;
        }
        MMAP_UNPIN_REGION;
        
        offsets.push_back(offset);
    }
 
    Date after_alloc = Date::now();
    
    for (unsigned i = 0;  i < offsets.size();  ++i) {
        uint64_t offset = offsets[i];
        
        MMAP_PIN_REGION(region)
        {
            auto place = region.range<int>(offset);
                    
            if (*place != threadNum) {
                cerr << "block had wrong thread number" << endl;
                ML::atomic_add(nerrors, 1);
            }
            *place = 0;
        }
        MMAP_UNPIN_REGION;

        MMAP_PIN_REGION_INL(region, alloc.nodeAlloc.deallocate(offset, 64));
    }

    Date after_dealloc = Date::now();
    double t1 = after_alloc.secondsSince(before);
    double t2 = after_dealloc.secondsSince(after_alloc);


    cerr << nallocs << " allocs in " << t1
         << "s at " << ML::format("%.5f", nallocs / t1 / 1000.0)
         << "kallocs/sec" << endl;

    cerr << offsets.size() << " deallocs in " << t2
         << "s at " << ML::format("%.5f", nallocs / t2 / 1000.0)
         << "kdeallocs/sec" << endl;

    BOOST_CHECK_EQUAL(nerrors, 0);
}

BOOST_AUTO_TEST_CASE( test_multithreaded_allocation )
{
    int nthreads = 4;

    // Get our memory region, page table allocator and memory allocator
    MallocRegion region(PERM_READ_WRITE, 1024 * 1024);
    MemoryAllocator alloc(region, true);

    //Date before = Date::now();

    // Total memory: nthreads * npages / 2 * 4M

    uint64_t nerrors = 0;
    uint64_t nallocations = 0;

    volatile bool finished = false;

    // Do lots of allocatin and allocatin...
    auto doAllocThread = [&] (int threadNum)
        {

            uint64_t nall = 0;

            for (unsigned i = 0;  !finished;  ++i) {

                vector<uint64_t> offsets;
                for (unsigned i = 0;  i < 1000;  ++i) {
                    uint64_t offset = MMAP_PIN_REGION_RET(
                            region, alloc.nodeAlloc.allocate(64));

                    MMAP_PIN_REGION(region)
                    {
                        auto place = region.range<int>(offset);

                        int val = *place;

                        if (val != 0 && val != -1) {
                            cerr << "block was already allocated: offset "
                                << offset << " val " << val << " my threadNum "
                                << threadNum << endl;
                            cerr << offset << endl;
                            ML::atomic_add(nerrors, 1);
                        }
        
                        *place = threadNum;
                    }
                    MMAP_UNPIN_REGION;
        
                    offsets.push_back(offset);
                    ++nall;
                }
 
                //Date after_alloc = Date::now();
    
                for (unsigned i = 0;  i < offsets.size();  ++i) {
                    uint64_t offset = offsets[i];

                    MMAP_PIN_REGION(region)
                    {
                        auto place = region.range<int>(offset);
                    
                        if (*place != threadNum) {
                            cerr << "block had wrong thread number" << endl;
                            ML::atomic_add(nerrors, 1);
                        }
                        *place = 0;
                    }
                    MMAP_UNPIN_REGION;

                    ML::memory_barrier();

                    MMAP_PIN_REGION_INL(
                            region, alloc.nodeAlloc.deallocate(offset, 64));
                }
            }

            ML::atomic_add(nallocations, nall);
        };

    cerr << "test with single thread only" << endl;
    // First, test with only one thread to make sure the test passes...
    {
        boost::thread_group tg;
        
        tg.create_thread(boost::bind<void>(doAllocThread, 0));
        
        double t1 = 1;

        sleep(t1);
        
        finished = true;
        
        tg.join_all();

        BOOST_CHECK_EQUAL(nerrors, 0);
        cerr << "single threaded: " << nallocations << " allocs in " << t1
             << "s at " << ML::format("%.5f", nallocations / t1 / 1000.0)
             << "kallocs/sec" << endl;
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

        double t1 = 1;
        
        sleep(t1);
        
        finished = true;
        
        tg.join_all();

        BOOST_CHECK_EQUAL(nerrors, 0);
        cerr << "with " << nthreads << " threads: "
             << nallocations << " allocs in " << t1
             << "s at " << ML::format("%.5f", nallocations / t1 / 1000.0)
             << "kallocs/sec" << endl;
    }
}


BOOST_AUTO_TEST_CASE(test_multithreaded_chunk_alloc) {
    enum {
        threadPerSize = 2,
        iterationCount = 1000
    };

    const vector<uint32_t> sizes = {8,12,17,32,49,64};
    vector<std::promise<int> > futureList;
    vector<thread> threadList;

    MallocRegion region(PERM_READ_WRITE, 1024 * 1024);
    MemoryAllocator alloc(region, true);


    // Helper alloc function.
    auto allocateAndPush = [&](stack<int64_t>& s, int id, uint32_t size)->bool {
        bool ok = true;

        int offset = MMAP_PIN_REGION_RET(region, alloc.nodeAlloc.allocate(size));
        s.push(offset);

        MMAP_PIN_REGION(region)
        {
            auto place = region.range<int>(offset);

            int val = *place;
            if (val != 0 && val != -1) {
                ok = false;
                stringstream ss;
                ss << "ERROR: Allocating region already in use " 
                    << "(offset=" << offset
                    << ", val=" << val << ")" << endl;
                cerr << ss.str();
            }

            *place = id+1;
        }
        MMAP_UNPIN_REGION;

        return ok;
    };

    // Helper dealloc function.
    auto deallocateHead = [&](stack<int64_t>& s, int id, uint32_t size)->bool {
        bool ok = true;

        int offset = s.top();
        s.pop();

        MMAP_PIN_REGION(region)
        {
            auto place = region.range<int>(offset);

            int val = *place;
            if (val != id+1) {
                ok = false;
                stringstream ss;
                ss << "ERROR: Deallocating region we don't own "
                    << "(offset=" << offset
                    << ", val=" << val << ")" << endl;
                cerr << ss.str();
            }

            *place = 0;
        }
        MMAP_UNPIN_REGION;

        ML::memory_barrier();

        MMAP_PIN_REGION_INL(region, alloc.nodeAlloc.deallocate(offset, size));

        return ok;
    };


    // Actual test function.
    auto threadFct = [&](int id, uint32_t size) {
        int errCount = 0;

        mt19937 engine (id);
        uniform_int_distribution<int> opDist(0, 1);
        uniform_int_distribution<int> numDist(0, 100);
        stack<int64_t> allocatedOffsets;

        for (int k = 0; k < iterationCount; ++k) {

            // if (k % 1000 == 0) {
            //     cerr << "<" << id << "> it=" << k 
            //         << ", size=" << size << endl;
            // }


            if (allocatedOffsets.empty() || opDist(engine)) {

                for (int j = numDist(engine); j > 0; --j) {
                    if (!allocateAndPush(allocatedOffsets, id, size))
                        errCount++;
                }
            }
            else {
                for (int j = numDist(engine); 
                     allocatedOffsets.empty() && j > 0; 
                     --j) 
                {
                    if (!deallocateHead(allocatedOffsets, id, size))
                        errCount++;
                }
            }
        }
        //cerr << "<" << id << "> - Cleaning up" << endl;

        //We're done, cleanup.
        while (!allocatedOffsets.empty()) {
            if (!deallocateHead(allocatedOffsets, id, size))
                errCount++;
        }

        // cerr << "<" << id << "> - Done" << endl;
        futureList[id].set_value(errCount);
    };

    // Start the threads.
    for (int i = 0; i < sizes.size(); ++i) {
        for (int j = 0; j < threadPerSize; ++j) {
            int id = i * threadPerSize + j;
            futureList.push_back(promise<int>());
            threadList.push_back(thread(threadFct, id, sizes[i]));
        }
    }

    // Wait for the test to end.
    int errCount = 0;
    chrono::duration<int, milli> dur (100000); // 10 sec

    for (int i = 0; i < threadList.size(); ++i) {
        std::future<int> future = futureList[i].get_future();

        BOOST_REQUIRE_MESSAGE(wait_for(future, dur), "Thread is MIA.");
        errCount += future.get();

        threadList[i].join();
    }

    BOOST_REQUIRE_EQUAL(errCount, 0);
}

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
#include "mmap/mmap_const.h"
#include "mmap/memory_region.h"
#include <sys/mman.h>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;


BOOST_AUTO_TEST_CASE(test_complex_pinning)
{
    MallocRegion region(PERM_READ_WRITE, page_size);

    BOOST_CHECK_EQUAL(region.isPinned(), 0);

    MMAP_PIN_REGION(region)
    {
        BOOST_CHECK_EQUAL(region.isPinned(), 1);

        MMAP_PIN_REGION(region)
        {
            BOOST_CHECK_GE(region.isPinned(), 1);
        }
        MMAP_UNPIN_REGION;

        BOOST_CHECK_EQUAL(region.isPinned(), 1);
    }
    MMAP_UNPIN_REGION;

    BOOST_CHECK_EQUAL(region.isPinned(), 0);

}

BOOST_AUTO_TEST_CASE( test_malloc_region )
{
    MallocRegion region(PERM_READ_WRITE, 128 * page_size);
    BOOST_CHECK_EQUAL(region.length(), 128 * page_size);

    cerr << "start = " << (void *)region.start() << endl;

    string command = ML::format("cat /proc/%d/maps", getpid());

#if 0
    cerr << "after allocation" << endl;
    int res = system(command.c_str());
    if (res == -1)
        throw ML::Exception("system failed: %s", strerror(errno));
#endif    

    int nthreads = 8;
    volatile bool finished = false;
    
    uint64_t errors = 0;

    auto readMemoryThread = [&] (int threadNum)
        {
            //ThreadGuard threadGuard;
            
            for (unsigned char i = 0;  !finished;  ++i) {
                MMAP_PIN_REGION(region)
                {
                    unsigned char * start = (unsigned char *)region.start();
                
                    if (start[threadNum * page_size + 64] != i)
                        ML::atomic_add(errors, 1);
                    
                    start[threadNum * page_size + 64] += 1;
                }
                MMAP_UNPIN_REGION;
            }

            MMAP_PIN_REGION(region)
            {
                unsigned char * start = (unsigned char *)region.start();
                start[threadNum * page_size + 64] = threadNum;
            }
            MMAP_UNPIN_REGION;
        };
        
    boost::thread_group tg;
        
    for (unsigned i = 0;  i < nthreads;  ++i)
        tg.create_thread(boost::bind<void>(readMemoryThread, i));

    MMAP_PIN_REGION(region)
    {
        char * start = region.start();
        BOOST_CHECK_EQUAL(start[0], 0);
        start[0] = 10;
        start[1] = 50;
        BOOST_CHECK_EQUAL(start[0], 10);
        
        start[64 * page_size] = 20;
        BOOST_CHECK_EQUAL(start[64 * page_size], 20);
    }
    MMAP_UNPIN_REGION;


#if 0
    cerr << endl << endl << endl << "after modification" << endl;
    res = system(command.c_str());
    if (res == -1)
        throw ML::Exception("system failed: %s", strerror(errno));
#endif

    MMAP_PIN_REGION_RESIZE(region, region.resize(64 * page_size));

#if 0
    cerr << endl << endl << endl << "after shrink" << endl;
    res = system(command.c_str());
    if (res == -1)
        throw ML::Exception("system failed: %s", strerror(errno));
#endif

    MMAP_PIN_REGION(region)
    {
        char * start = region.start();

        // Check that it's not in core
        unsigned char c[64];
        errno = 0;
        int res = mincore(start, 64 * page_size, c);
        BOOST_CHECK_EQUAL(res, 0);
        BOOST_CHECK_EQUAL(strerror(errno), string("Success"));

        //for (unsigned i = 0;  i < 64;  ++i)
        //    BOOST_CHECK_EQUAL(c[i] & 1, i == 0);
        
        res = mincore(start + 64 * page_size, 64 * page_size, c);
        BOOST_CHECK_EQUAL(res, -1);
        BOOST_CHECK_EQUAL(errno, ENOMEM);

        BOOST_CHECK_EQUAL(start[0], 10);
        start[0] = 20;
        BOOST_CHECK_EQUAL(start[0], 20);
    }
    MMAP_UNPIN_REGION;

    MMAP_PIN_REGION_RESIZE(region, region.resize(128 * page_size));

#if 0
    cerr << endl << endl << endl << "after grow" << endl;
    res = system(command.c_str());
    if (res == -1)
        throw ML::Exception("system failed: %s", strerror(errno));
#endif
    
    MMAP_PIN_REGION(region)
    {
        char * start = region.start();

        unsigned char c[128];
        errno = 0;
        int res = mincore(start, 128 * page_size, c);
        BOOST_CHECK_EQUAL(res, 0);
        BOOST_CHECK_EQUAL(strerror(errno), string("Success"));
        
        //for (unsigned i = 0;  i < 128;  ++i)
        //    BOOST_CHECK_EQUAL(c[i] & 1, i == 0);

        BOOST_CHECK_EQUAL(start[0], 20);
        BOOST_CHECK_EQUAL(start[1], 50);
        BOOST_CHECK_EQUAL(start[64 * page_size], 0);
    }
    MMAP_UNPIN_REGION;

    finished = true;
    
    tg.join_all();

    BOOST_CHECK_EQUAL(errors, 0);

    MMAP_PIN_REGION(region)
    {
        char * start = region.start();
        for (unsigned i = 0;  i < nthreads;  ++i)
            BOOST_CHECK_EQUAL(start[i * page_size + 64], i);
    }
    MMAP_UNPIN_REGION;
}

BOOST_AUTO_TEST_CASE(test_multi_thread_resize)
{
    cerr << "Multi thread resize" << endl;

    MallocRegion region(PERM_READ_WRITE, 128 * page_size);

    int nthreads = 8;
    volatile bool finished = false;
    
    uint64_t errors = 0;

    // Do lots of reading and writing...
    auto readWriteMemoryThread = [&] (int threadNum)
        {
            //ThreadGuard threadGuard;
            
            for (unsigned char i = 0;  !finished;  ++i) {
                MMAP_PIN_REGION(region)
                {
                    auto start = region.range<unsigned char>(
                            threadNum * page_size + 64, 1);
                
                    if (*start != i)
                        ML::atomic_add(errors, 1);
                
                    *start += 1;
                }
                MMAP_UNPIN_REGION;
            }

            MMAP_PIN_REGION(region)
            {
                unsigned char * start = (unsigned char *)region.start();
                start[threadNum * page_size + 64] = threadNum;
            }
            MMAP_UNPIN_REGION;
        };

    // ... at the same time as resizing up and down from multiple threads
    auto resizeThread = [&] (int threadNum)
        {
            //ThreadGuard threadGuard;
            
            for (unsigned char i = 0;  !finished;  ++i) {
                size_t newSize = (64 + random() % 128) * page_size;
                MMAP_PIN_REGION_RESIZE(region, region.resize(newSize));
            }
        };

    // ... and also mmapping random other blocks to force the resizes
    // to jump around
    auto mmapThread = [&] (int threadNum)
        {
            //ThreadGuard threadGuard;
            
            for (unsigned char i = 0;  !finished;  ++i) {
                size_t length = (random() % 1024 + 1) * page_size;
                void * res = mmap(0, length,
                                  PROT_READ | PROT_WRITE,
                                  MAP_PRIVATE | MAP_ANONYMOUS,
                                  -1, 0);
                if (res == MAP_FAILED)
                    throw ML::Exception("MMap failed: %s length: %zd",
                                        strerror(errno), length);
                
                munmap(res, length);
            }
        };
        
    boost::thread_group tg;

    for (unsigned i = 0;  i < 2;  ++i)
        tg.create_thread(boost::bind<void>(mmapThread, i));
        
    for (unsigned i = 0;  i < nthreads;  ++i)
        tg.create_thread(boost::bind<void>(readWriteMemoryThread, i));

    for (unsigned i = 0;  i < nthreads;  ++i)
        tg.create_thread(boost::bind<void>(resizeThread, i));

    sleep(1);

    finished = true;

    tg.join_all();

    BOOST_CHECK_EQUAL(errors, 0);
}


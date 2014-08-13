/* mmap_trie_test.cc
   Jeremy Barnes, 10 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for the mmap string allocator.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"

#include "mmap/memory_region.h"
#include "mmap/page_table_allocator.h"
#include "mmap/memory_allocator.h"
#include "mmap/mmap_trie.h"

#include <boost/test/unit_test.hpp>
#include <iostream>
#include <future>
#include <thread>
#include <array>
#include <stack>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;


void fillBlock(
        MallocRegion& region,
        uint64_t offset,
        uint64_t size,
        char value = 0xFF)
{
    MMAP_PIN_REGION(region)
    {
        RegionPtr<char> ptr(region, offset);
        memset(ptr.get(), value, size);
    }
    MMAP_UNPIN_REGION;
}

bool checkBlock(
        MallocRegion& region,
        uint64_t offset,
        uint64_t size,
        char value = 0xFF)
{
    MMAP_PIN_REGION(region)
    {
        RegionPtr<char> ptr(region, offset);
        char* arr = ptr;

        for (uint64_t i = 0; i < size; ++i) {
            if (arr[i] != value)
                return false;
        }
        return true;
    }
    MMAP_UNPIN_REGION;

}

/**
\todo This is no longer correct since we changed to a first fit approach.
*/
BOOST_AUTO_TEST_CASE( test_seq_basic )
{

    MallocRegion region;
    MemoryAllocator alloc(region, true);

    vector<uint64_t> allocBlocks;

    auto allocate = [&](uint64_t size) -> uint64_t {
        // cerr << "ALLOC(" << size << ")" << endl;

        uint64_t offset = 
            MMAP_PIN_REGION_RET(region, alloc.stringAlloc.allocate(size));
        allocBlocks.push_back(offset);

        fillBlock(region, offset, size);
        
        return offset;
    };

    auto deallocate = [&](int index = -1) {
        if (index < 0) {
            index = allocBlocks.size()-1;
        }

        // cerr << "DEALLOC(" << index << ")" << endl;

        uint64_t offset = allocBlocks[index];
        allocBlocks.erase(allocBlocks.begin() + index);

        MMAP_PIN_REGION_INL(region, alloc.stringAlloc.deallocate(offset));
    };

    auto dump = [&](const string& msg) {
        if (1) return;

        cerr << endl << "------------------------------------------ " 
            << "[" << msg << "]" << endl;
        cerr << "allocBlocks=";
        for (int i = 0; i < allocBlocks.size(); ++i)
            cerr << i << ":" << allocBlocks[i] << ", ";
        cerr << endl;
        alloc.stringAlloc.dumpFreeList();
        cerr << endl;
    };

    enum {
        allocCount = 8,
        allocSize = ((page_size*1024) / allocCount),
    };

    try {
        // The last alloc should use the space from the first dealloc.
        allocate(allocSize);
        allocate(allocSize);
        deallocate(0);
        allocate(allocSize);
        dump("0XX");

        // fix up the ordering a bit.
        swap(allocBlocks[0], allocBlocks[1]);

        // This shouldn't allocate a new block.
        allocate(allocSize*(allocCount-3));
        deallocate();
        dump("0XX");

        // Fill up the page.
        for (int i = 0; i < allocCount-3; ++i)
            allocate(allocSize);
        dump("0XXXXXXX");

        // Should trigger a new page to allocate.
        allocate(allocSize);
        deallocate(0);
        dump("0XXXXXX0 - 0X");
        deallocate();
        dump("0XXXXXXX");

        // Should leave a single contiguous free block.
        deallocate(allocBlocks.size()-3);
        deallocate(allocBlocks.size()-2);
        deallocate();
        dump("0XXX0");

        // Create some well placed holes.
        deallocate(2);
        dump("0X0X0");

        // Clear up the rest.
        while (!allocBlocks.empty())
            deallocate();
        dump("0"); // preferably nothing left.
    } catch (...) {
        dump("ERROR");
        throw;
    }

    BOOST_REQUIRE_EQUAL(alloc.bytesPrivate(), 0);
}

BOOST_AUTO_TEST_CASE( test_seq_random_allocs )
{
    enum {
        allocCount = 50,
        iterationCount = 50,
    };

    MallocRegion region;
    MemoryAllocator alloc(region, true);

    typedef pair<uint64_t, uint64_t> allocBlock_t;
    stack<allocBlock_t> blocks;

    mt19937 engine;
    uniform_int_distribution<uint64_t> sizeDist(1, 1000000);

    try {
        for (int i = 0; i < iterationCount; ++i) {

            for (int j = 0; j < allocCount; ++j) {
                uint64_t size = sizeDist(engine);
                uint64_t offset = MMAP_PIN_REGION_RET(
                        region, alloc.stringAlloc.allocate(size));

                fillBlock(region, offset, size);
                blocks.push(make_pair(size, offset));
            }


            while (!blocks.empty()) {
                uint64_t size, offset;
                tie(size, offset) = blocks.top();
                blocks.pop();

                BOOST_CHECK(checkBlock(region, offset, size));

                MMAP_PIN_REGION_INL(
                        region, alloc.stringAlloc.deallocate(offset, size));
            }
        }
    } catch (...) {
        cerr << "ERROR -----------------------------------------------" << endl;
        alloc.stringAlloc.dumpFreeList();
        cerr << endl;

        throw;
    }

    BOOST_REQUIRE_EQUAL(alloc.bytesPrivate(), 0);
}

BOOST_AUTO_TEST_CASE( test_multithreaded_random_allocs )
{
    enum {
        threadCount = 8,
        allocCount = 10,
        iterationCount = 10,
    };

    MallocRegion region;
    MemoryAllocator alloc(region, true);

    auto runThread = [&](int id) -> int {
        uint64_t errCount = 0;

        typedef pair<uint64_t, uint64_t> allocBlock_t;
        stack<allocBlock_t> blocks;

        mt19937 engine (id);
        uniform_int_distribution<uint64_t> sizeDist(1, 1000000);

        for (int i = 0; i < iterationCount; ++i) {

            for (int j = 0; j < allocCount; ++j) {
                uint64_t size = sizeDist(engine);
                uint64_t offset = MMAP_PIN_REGION_RET(
                        region, alloc.stringAlloc.allocate(size));
                fillBlock(region, offset, size, id);
                blocks.push(make_pair(size, offset));
            }

            while (!blocks.empty()) {
                uint64_t size, offset;
                tie(size, offset) = blocks.top();
                blocks.pop();

                if (!checkBlock(region, offset, size, id)) {
                    cerr << "Block overwritten\n";
                    errCount++;
                }

                MMAP_PIN_REGION_INL(
                        region, alloc.stringAlloc.deallocate(offset, size));
            }
        }

        return errCount;
    };

    ThreadedTest test;
    test.start(runThread, threadCount);
    int errCount = test.joinAll();

    // In a concurrent test, this can often be not null. It's not a bug.
    cerr << "Private bytes still in use: " 
        << (alloc.bytesPrivate() - sizeof(TriePtr))
        << endl;

    BOOST_REQUIRE_EQUAL(errCount, 0);
}

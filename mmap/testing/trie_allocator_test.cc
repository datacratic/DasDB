/* trie_allocator_test.cc
   Rémi Attab, 27 March 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Test for the mmap trie allocator.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"

#include "mmap/mmap_file.h"
#include "mmap/mmap_trie.h"
#include "jml/utils/guard.h"


#include <boost/test/unit_test.hpp>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;


struct TrieAllocatorFixture : public MMapFileFixture
{
    TrieAllocatorFixture() : MMapFileFixture("trie_allocator") {}
    virtual ~TrieAllocatorFixture() {}
};


BOOST_FIXTURE_TEST_CASE( test_basic, TrieAllocatorFixture )
{
    MMapFile area(RES_CREATE, filename);
    Call_Guard unlink_guard([&] { area.unlink(); });

    MemoryRegion& region = area.region();

    enum {
        TrieId1 = TrieAllocator::MinTrieId,
        TrieId2 = 31,
        TrieId3 = TrieAllocator::MaxTrieId - 1,
    };

    // Screw around with the allocated trie and also make sure that they are
    // distinct tries.

    area.trieAlloc.allocate(TrieId1);
    area.trieAlloc.allocate(TrieId2);

    {
        Trie trie1 = area.trie(TrieId1);
        auto current1 = *trie1;

        BOOST_CHECK_EQUAL(current1.size(), 0);
        BOOST_CHECK(current1.insert(1, 1).second);
    }

    {
        Trie trie2 = area.trie(TrieId2);

        auto current2 = *trie2;
        BOOST_CHECK_EQUAL(current2.size(), 0);
        BOOST_CHECK(current2.insert(1, 1).second);
        BOOST_CHECK_EQUAL(current2.remove(1).second, 1);
        current2.reset();

        area.trieAlloc.deallocate(TrieId2);
    }

    {
        Trie trie1 = area.trie(TrieId1);
        auto current1 = *trie1;

        BOOST_CHECK_EQUAL(current1.size(), 1);
        BOOST_CHECK_EQUAL(current1.remove(1).second, 1);
        current1.reset();

        area.trieAlloc.deallocate(TrieId1);
    }

    {
        area.trieAlloc.allocate(TrieId3);

        Trie trie3 = area.trie(TrieId3);

        auto current3 = *trie3;
        BOOST_CHECK_EQUAL(current3.size(), 0);
        BOOST_CHECK(current3.insert(1, 1).second);
        BOOST_CHECK_EQUAL(current3.remove(1).second, 1);
        current3.reset();

        area.trieAlloc.deallocate(TrieId3);
    }

    // make sure the free list trie still works ok.
    uint64_t stringOffset = MMAP_PIN_REGION_RET(
            region, area.stringAlloc.allocate(1000));
    MMAP_PIN_REGION_INL(region, area.stringAlloc.deallocate(stringOffset));

}

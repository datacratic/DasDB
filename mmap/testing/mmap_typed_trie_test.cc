/* mmap_typed_map_test.cc
   Rémi Attab, 17 April 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Usability test for the typed map interface.
   For correctness tests, see the other half dozen test scattered all over.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"
#include "mmap/sync_stream.h"
#include "mmap/mmap_map.h"
#include "mmap/mmap_file.h"
#include "jml/utils/guard.h"

#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>
#include <algorithm>
#include <iterator>


using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;


struct MMapTypedTrieFixture : public MMapFileFixture
{
    MMapTypedTrieFixture() : MMapFileFixture("mmap_typed_trie") {}
    virtual ~MMapTypedTrieFixture() {}
};



BOOST_FIXTURE_TEST_CASE( test_map_basic, MMapTypedTrieFixture )
{
    enum {
        MapA = 12,
        MapASize = 197,

        MapB = 38,
        MapBSize = 203,

        MapC = 42,
        MapD = 56,
    };

    MMapFile db(RES_CREATE, filename);
    Call_Guard unlinkGuard([&] { db.unlink(); });

    MemoryRegion& region = db.region();

    // Basic standard algorithm checks with strings as keys.
    {
        typedef Map<string, uint64_t>::value_type value_type;

        MMAP_PIN_REGION_INL(region, db.trieAlloc.allocate(MapA));

        Map<string, uint64_t> mapA(&db, MapA);

        auto it = mapA.find("bloo!");
        BOOST_CHECK(it == mapA.end());

        for (size_t i = 1; i <= MapASize; ++i)
            mapA.insert(randomString(i), i);

        // Read algo to check the trie invariant on strings.
        bool r = is_sorted(mapA.begin(), mapA.end(),
                [] (const value_type& lhs, const value_type& rhs) -> bool {
                    return lhs.first < rhs.first;
                });
        BOOST_CHECK(r);

        auto versionA = mapA.current();

        // modifying the original map shouldn't change the snapshot
        for (size_t i = MapASize + 1; i <= MapASize * 2; ++i)
            mapA.insert(randomString(i), i);

        // More read algorithm to check whether everything was added properly.
        uint64_t total = accumulate(versionA.begin(), versionA.end(), (uint64_t)0,
                [] (uint64_t a, const value_type& b) -> uint64_t {
                    return a + b.second;
                });
        BOOST_CHECK_EQUAL(total, (MapASize*(MapASize+1))/2);

    }

    // Multi map standard algorithms with strings as values.
    {
        typedef Map<uint64_t, string>::value_type value_type;

        MMAP_PIN_REGION_INL(region, db.trieAlloc.allocate(MapB));

        map<uint64_t, string> validate;

        // setup.
        Map<uint64_t, string> mapB(&db, MapB);
        for (size_t i = 0; i < MapBSize; ++i) {
            uint64_t key = random();
            string value = randomString(100);

            validate.insert(make_pair(key, value));
            mapB.insert(key, value);

        }

        MMAP_PIN_REGION_INL(region, db.trieAlloc.allocate(MapC));

        // Copy the values from one map to the other.
        // Note that the entire string should get a copy and not just the offset
        Map<uint64_t, string> mapC(&db, MapC);

        Map<uint64_t, string>::iterator it, end;
        for (tie(it, end) = mapB.beginEnd(); it != end; ++it)
            mapC.insert(it.key(), it.value());

        // Verify the copy accross 2 maps
        bool r = equal(mapC.begin(), mapC.end(), mapB.begin(),
                [] (const value_type& lhs, const value_type& rhs) -> bool {
                    return lhs.first == rhs.first && lhs.second == rhs.second;
                });
        BOOST_CHECK(r);

        // Deleting the values in one trie shouldn't affect the other trie.
        {
            MMAP_PIN_REGION_INL(region, db.trieAlloc.allocate(MapD));

            Map<uint64_t, string> mapD(&db, MapD);
            Map<uint64_t, string>::iterator it, end;
            for (tie(it, end) = mapB.beginEnd(); it != end; ++it)
                mapD.insert(it.key(), it.value());

            mapD.clear();

            MMAP_PIN_REGION_INL(region, db.trieAlloc.deallocate(MapD));
        }

        r = equal(validate.begin(), validate.end(), mapB.begin(),
                [] (const value_type& lhs, const value_type& rhs) -> bool {
                    return lhs.first == rhs.first && lhs.second == rhs.second;
                });
        BOOST_CHECK(r);

    }

    // Cleanup.
    {
        Map<string, uint64_t> mapA(&db, MapA);
        BOOST_CHECK_EQUAL(mapA.size(), MapASize * 2);

        mapA.clear();
        BOOST_CHECK(mapA.empty());

        MMAP_PIN_REGION_INL(region, db.trieAlloc.deallocate(MapA));
    }

    {
        Map<uint64_t, string> mapB(&db, MapB);
        BOOST_CHECK_EQUAL(mapB.size(), MapBSize);

        mapB.clear();
        BOOST_CHECK(mapB.empty());

        MMAP_PIN_REGION_INL(region, db.trieAlloc.deallocate(MapB));
    }

    {
        Map<uint64_t, string> mapC(&db, MapC);
        BOOST_CHECK_EQUAL(mapC.size(), MapBSize);

        mapC.clear();
        BOOST_CHECK(mapC.empty());

        MMAP_PIN_REGION_INL(region, db.trieAlloc.deallocate(MapC));
    }

    // There's always going to be 8 bytes left because of the string allocator.
    BOOST_CHECK_EQUAL(db.trieAlloc.bytesOutstanding(), 64);
    BOOST_CHECK_EQUAL(db.bytesOutstanding(), db.trieAlloc.bytesOutstanding());

}


BOOST_FIXTURE_TEST_CASE( test_map_tx, MMapTypedTrieFixture )
{
    enum {
        Size = 197,
        TotalSize = Size*2,
    };

    MMapFile db(RES_CREATE, filename);
    Call_Guard unlinkGuard([&] { db.unlink(); });

    Map<uint64_t, uint64_t> aMap(&db, 42);
    auto mapTx = aMap.transaction();

    typedef Map<uint64_t, uint64_t>::value_type value_type;

    for (size_t i = 1; i <= Size; ++i) {
        mapTx.insert(random(), i);
        aMap.insert(random(), i + Size);
    }

    vector<value_type> duplicates;
    set_intersection(
            mapTx.begin(), mapTx.end(), aMap.begin(), aMap.end(),
            back_inserter(duplicates),
            [] (const value_type& lhs, const value_type& rhs) -> bool {
                return lhs.first < rhs.first;
            });

    if (!duplicates.empty()) {
        cerr << "duplicates(" << duplicates.size() << "):" << endl;
        for (int i = 0; i < duplicates.size(); ++i)
            cerr << "\t" << i << ": " << duplicates[i].first << endl;
    }

    // Only the inserts into he transaction should be visible.
    uint64_t totalTx = accumulate(mapTx.begin(), mapTx.end(), (uint64_t)0,
            [] (uint64_t a, const value_type& b) -> uint64_t {
                return a + b.second;
            });
    uint64_t expTotalTx = (Size*(Size+1))/2;
    BOOST_CHECK_EQUAL(totalTx, expTotalTx);

    // Inserts into the transaction shouldn't be visible int he original trie.
    uint64_t aTotal = accumulate(aMap.begin(), aMap.end(), (uint64_t)0,
            [] (uint64_t a, const value_type& b) -> uint64_t {
                return a + b.second;
            });
    uint64_t expATotal = (TotalSize*(TotalSize+1))/2;
    BOOST_CHECK_EQUAL(aTotal, expATotal - expTotalTx);

    // Merge the transaction back into the original map.
    mapTx.commit();

    // Now that we've comitted, everything should be visible.
    uint64_t totalTotal = accumulate(aMap.begin(), aMap.end(), (uint64_t)0,
            [] (uint64_t a, const value_type& b) -> uint64_t {
                return a + b.second;
            });
    BOOST_CHECK_EQUAL(totalTotal, expATotal);
}

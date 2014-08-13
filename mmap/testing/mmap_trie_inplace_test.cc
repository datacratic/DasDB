/* mmap_trie_inplace_test.cc                                         -*- C++ -*-
   Rémi Attab, 12 April 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Testing for the transactional trie version aka the inplace trie.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"
#include "mmap/mmap_const.h"
#include "mmap/sync_stream.h"
#include "mmap/mmap_trie.h"
#include "mmap/mmap_trie_node.h"
#include "mmap/mmap_file.h"

#include "mmap/mmap_trie_terminal_nodes.h"
#include "mmap/mmap_trie_sparse_nodes.h"
#include "mmap/mmap_trie_binary_nodes.h"
#include "mmap/mmap_trie_compressed_nodes.h"
#include "mmap/mmap_trie_large_key_nodes.h"
#include "mmap/memory_tracker.h"

#include <boost/test/unit_test.hpp>
#include <set>

using namespace ML;
using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;

enum { paranoid = false };

void checkMemory(Trie& trie, ConstTrieVersion& tx, MMapFile& area)
{
    // Byte accounting with string keys are annoying hard...
    // BOOST_CHECK_EQUAL(
    //         area.bytesOutstanding(),
    //         tx.memUsage() + area.trieAlloc.bytesOutstanding());

    BOOST_CHECK_EQUAL(BinaryNodeOps::allocated, BinaryNodeOps::deallocated);
    BOOST_CHECK_EQUAL(LargeKeyNodeOps::allocated, LargeKeyNodeOps::deallocated);
    BOOST_CHECK_EQUAL(SparseNodeOps::allocated, SparseNodeOps::deallocated);
    BOOST_CHECK_EQUAL(
            CompressedNodeOps::allocated,
            CompressedNodeOps::deallocated);
    BOOST_CHECK_EQUAL(
            BasicKeyedTerminalOps::allocated,
            BasicKeyedTerminalOps::deallocated);
}


/*****************************************************************************/
/* SEQUENTIAL TESTS                                                          */
/*****************************************************************************/


template<typename Key>
void checkTrie(
        function<std::pair<Key, uint64_t>(int)> gen,
        int n, const std::string& name, bool withNoise)
{
    trieMemoryCheck = true;

    enum { TrieId = 1 };

    cerr << endl
        << name
        << " =================================================================="
        << endl;

    MMapFile area(RES_CREATE);

    Trie trie = area.trie(TrieId);

    // Fill the trie with some random garbage before we fork.
    size_t noiseSize = 0;
    if (withNoise) {

        cerr << endl
            << "--- noise "
            << "---------------------------------------------------------"
            << endl;

        auto current = *trie;
        for (unsigned i = 0; i < n; ++i) {
            Key key;
            uint64_t value;
            tie(key, value) = gen(i);
            current.insert(key, value);
            noiseSize++;
        }
    }


    auto tx = trie.transaction();

    if(!withNoise) checkMemory(trie, tx, area);

    // Generate all the values.
    std::vector<std::pair<Key, uint64_t> > kvs;
    for (unsigned i = noiseSize; i < noiseSize + n; ++i)
        kvs.push_back(gen(i));

    cerr << endl
        << "--- insert --------------------------------------------------------"
        << endl;

    // Insert all the values
    for (unsigned i = 0; i < n; ++i) {
        Key key;
        uint64_t value;
        tie(key, value) = kvs[i];

        // cerr << endl
        //     << "* " << i << " -> {" << key << ", " << value << "}" << endl;

        TrieIterator it;
        bool inserted;
        boost::tie(it, inserted) = tx.insert(key, value);

        BOOST_CHECK(it.valid());
        BOOST_CHECK_EQUAL(it.value(), value);

        Key res = it.key().cast<Key>();
        BOOST_CHECK_EQUAL(res, key);

        if (paranoid) {
            for (int j = 0; j <= i; ++j) {
                auto it = tx.find(kvs[j].first);
                BOOST_CHECK(it.valid());
                BOOST_CHECK_EQUAL(it.value(), kvs[j].second);
            }
        }
        BOOST_CHECK_EQUAL(tx.size(), i+1 + noiseSize);

        if(!withNoise) checkMemory(trie, tx, area);
    }

    cerr << endl
        << "--- replace -------------------------------------------------------"
        << endl;


    // Replace all the values.
    for (unsigned i = 0; i < n; ++i) {
        Key k;
        uint64_t v;
        tie(k, v) = kvs[i];

        bool keyFound;
        uint64_t oldValue;
        tie(keyFound, oldValue) = tx.replace(k, v+v);

        BOOST_CHECK_EQUAL(keyFound, true);
        BOOST_CHECK_EQUAL(oldValue, v);

        tie(keyFound, oldValue) = tx.replace(k, v);

        BOOST_CHECK_EQUAL(keyFound, true);
        BOOST_CHECK_EQUAL(oldValue, v+v);

        if(!withNoise) checkMemory(trie, tx, area);
    }


    cerr << endl
        << "--- remove --------------------------------------------------------"
        << endl;

    // Remove all the values.
    for (unsigned i = 0; i < n; ++i) {
        Key k;
        uint64_t v;
        tie(k, v) = kvs[i];

        // cerr << endl
        //     << "* " << i << " -> {" << key << ", " << value << "}" << endl;

        bool keyFound;
        uint64_t result;
        tie(keyFound, result) = tx.remove(k);
        BOOST_CHECK_EQUAL(keyFound, true);
        BOOST_CHECK_EQUAL(v, result);

        if (paranoid) {
            for (int j = 0; j < n; ++j) {
                auto it = tx.find(kvs[j].first);

                if (j <= i) BOOST_CHECK(!it.valid());
                else BOOST_CHECK(it.valid());
            }
        }
        BOOST_CHECK_EQUAL(tx.size(), (n - (i+1)) + noiseSize);

        if(!withNoise) checkMemory(trie, tx, area);
    }


    cerr << endl
        << "--- cleanup -------------------------------------------------------"
        << endl;

    if(!withNoise) trieMemoryTracker.dumpLeaks();

    tx.rollback();

    {
        auto current = *trie;
        checkMemory(trie, current, area);
        trie.current().clear();
    }

    trieMemoryTracker.dumpLeaks();

    cerr << endl
        << "--- done ----------------------------------------------------------"
        << endl;
}

template<typename Key>
void checkTrie(
        function<std::pair<Key, uint64_t>(int)> gen,
        int n, const std::string& name)
{
    checkTrie(gen, n, name, false);
    checkTrie(gen, n, name, true);
}


enum {
    intSize = 1000,
    strSize = 1000
};

BOOST_AUTO_TEST_CASE( test_zero_value )
{
    auto kv = [] (int i) { return make_pair(i, 0); };
    checkTrie<uint64_t>(kv, intSize, "zero value");
}

BOOST_AUTO_TEST_CASE( test_sequential_insert )
{
    auto kv = [] (int i) { return std::make_pair(i, i); };
    checkTrie<uint64_t>(kv, intSize, "linear");
}

BOOST_AUTO_TEST_CASE( test_random_int64 )
{
    auto kv = [] (int i) {
        uint64_t k = (uint64_t)random() << 32 | random();
        return std::make_pair(k, k);
    };
    checkTrie<uint64_t>(kv, intSize, "random64");
}

BOOST_AUTO_TEST_CASE( test_small_string_insert )
{
    set<string> keySet;
    auto kv = [&] (int i) {
        string str;
        do {
            str = randomString(random() % 15 + 1);
        } while (!keySet.insert(str).second);
        return make_pair(str, i);
    };

    checkTrie<string>(kv, strSize, "small string");
}

BOOST_AUTO_TEST_CASE( test_large_string_insert )
{
    set<string> keySet;
    auto kv = [&] (int i) {
        string str;
        do {
            str = randomString(random() % 512+1);
        } while (!keySet.insert(str).second);
        return make_pair(str, i);
    };
    checkTrie<string>(kv, strSize, "large string");
}

/*****************************************************************************/
/* MULTI THREADED                                                            */
/*****************************************************************************/


// \todo add read threads once commit is there.
BOOST_AUTO_TEST_CASE( test_multithreaded_test )
{
    trieMemoryCheck = false;
    enum {
        TrieId = 1,
        writeThreads = 8,
        totalThreads = writeThreads,
        dataPerThread = 500,
        repeat = 50
    };


    auto rnd = [] () -> uint64_t {
        return random() << 32 | random();
    };

    MMapFile area(RES_CREATE);
    Trie trie = area.trie(TrieId);

    {
        auto current = *trie;
        for (int i = 0; i < totalThreads * dataPerThread; ++i)
            current.insert(rnd(), rnd());
    }

    boost::barrier barrier(totalThreads + 1);

    cerr << "paranoid=" << paranoid << endl;

    auto doWriteThread = [&](int i) -> int {
        vector<pair<uint64_t, uint64_t> > vec;

        for (int i = 0; i < dataPerThread; ++i)
            vec.push_back(make_pair(rnd(), rnd()));

        barrier.wait();
        // sync_cerr() << i << ": start"  << endl << sync_dump;

        auto tx = trie.transaction();

        for (int loop = 0; loop < repeat; ++loop) {
            for (auto it = vec.begin(), end = vec.end(); it != end; ++it) {

                // sync_cerr() << i << ": insert "
                //     << it->first << " -> " << it->second << endl << sync_dump;

                auto res = tx.insert(it->first, it->second);
                // ExcAssert(res.first.valid());
                ExcAssertEqual(res.second, true);

                if (paranoid)
                    for (auto itCheck = vec.begin(); itCheck != it; ++itCheck)
                        ExcAssert(tx.find(it->first).valid());
            }

            for (auto it = vec.begin(), end = vec.end(); it != end; ++it) {

                // sync_cerr() << i << ": remove "
                //     << it->first << " -> " << it->second << endl << sync_dump;

                auto res = tx.remove(it->first);
                // if (!res.first) tx.dump(4);
                ExcAssert(res.first);
                // ExcAssertEqual(res.second, it->second);

                if (paranoid)
                    for (auto itCheck = vec.begin(); itCheck != it; ++itCheck)
                        ExcAssert(!tx.find(it->first).valid());
            }
        }

        return 0;
    };

    ThreadedTest test;
    test.start(doWriteThread, writeThreads);

    barrier.wait();
    // sync_cerr() << -1 << ": start"  << endl << sync_dump;

    int errSum = test.joinAll(10000);
    BOOST_REQUIRE_EQUAL(errSum, 0);
}

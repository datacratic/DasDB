/* mmap_trie_test.cc
   Jeremy Barnes, 10 August 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for the mmap trie class.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"
#include "mmap/mmap_trie.h"
#include "mmap/mmap_trie_node.h"
#include "mmap/mmap_file.h"
#include "mmap/memory_tracker.h"
#include "mmap/profiler.h"
#include "jml/utils/smart_ptr_utils.h"
#include "jml/utils/string_functions.h"
#include "jml/arch/atomic_ops.h"

#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>
#include <boost/function.hpp>
#include <iostream>
#include <set>


using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;


bool check_memory(
        Trie& trie, 
        MutableTrieVersion& current, 
        MMapFile& area, 
        uint64_t n, 
        const string& name, 
        bool printStats = true) 
{
    current.reset();
    trie.gc().deferBarrier();
    current = *trie;

    if (printStats) {
        cerr << name << ": memory usage for " 
            << n << " values is " << current.memUsage()
            << " at " << (1.0 * current.memUsage() / current.size())
            << " bytes/value" << endl;

        cerr << "allocated " << area.bytesAllocated()
            << " deallocated " << area.bytesDeallocated()
            << " outstanding " << area.bytesOutstanding()
            << " total used bytes " << area.nodeAlloc.usedSize() << endl;
    }


    /** \todo fix the memory counters.
    We can't rely on this when we're testing with large keys.
    The reason is that we end up calling allocateString and that allocator
    can allocate more space then requested (padding and metadata).
    This means that the trie's memUsage() counter won't be in sync with
    the allocator's counter.

    BOOST_CHECK_EQUAL(area.bytesOutstanding(), current.memUsage());
    return area.bytesOutstanding() == current.memUsage();
    */

    return true;
}

uint64_t dump(uint64_t key) { return key; }
uint64_t dump (const std::string& key) {
    uint64_t r = 0;

    for (int i = 0; i < key.size() && i < sizeof(r); ++i)
        r = (r << 8) | key[i];

    return r;
}

template<typename KeyType>
void check_trie(boost::function<std::pair<KeyType, uint64_t> (int)> gen,
                int n,
                const std::string & name,
                bool debug = false)
{
    enum { TrieId = 1 };

    auto debugGuard = enableTrieDebug();
    auto trieTrackerGuard = enableTrieMemoryTracker();
    auto kfTrackerGuard = enableKeyFragmentTracker();

    cerr << endl
        << name << " =========================================================="
        << endl;

    MMapFile area(RES_CREATE);
    area.trieAlloc.allocate(TrieId);

    Trie trie = area.trie(TrieId);
    auto current = *trie;

    std::map<KeyType, uint64_t> m;

    BOOST_CHECK_EQUAL(current.size(), 0);

    std::vector<std::pair<KeyType, uint64_t> > vals;

    for (unsigned i = 0;  i < n;  ++i)
        vals.push_back(gen(i));

    //cerr << current.root << endl;
    //current.dump();

    for (unsigned i = 0;  i < n;  ++i) {
        KeyType k;
        uint64_t v;
        boost::tie(k, v) = vals[i];

        // cerr << endl << "--- INSERT[" 
        //     << dump(k)
        //     << ", " << v << "] "
        //     << "---------------------------------------------------" << endl;
        // cerr << "key={" << TrieKey(k).getFragment() << "}" << endl;

        TrieIterator it;
        bool inserted;
        boost::tie(it, inserted) = current.insert(k, v);

        BOOST_CHECK_EQUAL(it.value(), v);
        KeyType r = it.key().cast<KeyType>();
        BOOST_CHECK_EQUAL(r, k);

        m[k] = v;

        // current.dump();
        
        BOOST_CHECK_EQUAL(current.find(k).value(), v);
        BOOST_CHECK_EQUAL(current.size(), m.size());
    }

    BOOST_CHECK_EQUAL(current.size(), n);
    check_memory(trie, current, area, n, name);

    // cerr << endl << "=================== DONE INSERT ==============" << endl;

    // current.dump();

    for (unsigned i = 0; i < n; ++i) {
        KeyType k;
        uint64_t v;
        boost::tie(k, v) = vals[i];

        // cerr << endl << "--- CAS[" 
        //     << dump(k)
        //     << ", " << v << " -> " << (v+v) << "] "
        //     << "---------------------------------------------------" << endl;
        // cerr << "key={" << k << "}" << endl;

        bool keyFound;
        uint64_t oldValue;
        tie(keyFound, oldValue) = current.compareAndSwap(k, v, v+v);

        BOOST_CHECK_EQUAL(keyFound, true);
        BOOST_CHECK_EQUAL(oldValue, v);

        // cerr << endl << "--- CAS[" 
        //     << dump(k)
        //     << ", " << (v+v) << " -> " << v << "] "
        //     << "---------------------------------------------------" << endl;
        // cerr << "key={" << k << "}" << endl;

        tie(keyFound, oldValue) = current.compareAndSwap(k, v+v, v);

        BOOST_CHECK_EQUAL(keyFound, true);
        BOOST_CHECK_EQUAL(oldValue, v+v);
    }

    BOOST_CHECK_EQUAL(current.size(), n);
    check_memory(trie, current, area, n, name);

    // cerr << endl << "================== DONE CAS ==================" << endl;

    BOOST_CHECK_EQUAL(current.size(), n);
    check_memory(trie, current, area, n, name);

    //current.dump();

    string lastDump, curDump;
    KeyType k;
    uint64_t v;
    try {
        for (unsigned i = 0; i < n; ++i) {
            boost::tie(k, v) = vals[i];

            // cerr << endl << "--- REMOVE[" 
            //     << i << " -> "
            //     << dump(k)
            //     << ", " << v << "] "
            //     << "---------------------------------------------------" << endl;
            // cerr << "key={" << k << "}" << endl;

            bool keyFound;
            uint64_t result;
            tie(keyFound, result) = current.remove(k);
            BOOST_CHECK_EQUAL(keyFound, true);
            BOOST_CHECK_EQUAL(v, result);

            //BOOST_REQUIRE_THROW(current.remove(k), ML::Exception);
            BOOST_CHECK(!current.find(k).valid());

            uint64_t numNodes = m.size() - (i+1);

            BOOST_CHECK_EQUAL(current.size(), numNodes);

            lastDump = curDump;

            stringstream ss;
            current.dump(0, 0, ss);
            curDump = ss.str();

            // if (!check_memory(trie, current, area, numNodes, name, false))
            //     throw ML::Exception("Memory leak!");
        }
    }
    catch (...) {
        cerr << endl << endl;
        cerr << "ERROR - trying to delete (key=" << k 
            << ", value=" << v << ")" << endl;
        cerr << "=========================================== LAST_DUMP" << endl;
        cerr << lastDump << endl << endl;
        cerr << "============================================ CUR_DUMP" << endl;
        cerr << curDump << endl << endl;
        throw;
    }
    
    // cerr << endl << "================== DONE REMOVE ===============" << endl;

    check_memory(trie, current, area, 0, name);

    
    current.clear();
    BOOST_CHECK_EQUAL(current.size(), 0);

    current.reset();

    trie.gc().deferBarrier();

    cerr << endl;
    trieMemoryTracker.dumpLeaks();

    current = *trie;
    BOOST_CHECK_EQUAL(current.memUsage(), 0);
    BOOST_CHECK_EQUAL(area.nodeAlloc.bytesOutstanding(), 0);
    current.reset();

    area.trieAlloc.deallocate(TrieId);

    // There should always be one root left for the string allocator.
    BOOST_CHECK_EQUAL(area.bytesOutstanding(), area.trieAlloc.bytesOutstanding());
}

BOOST_AUTO_TEST_CASE( test_zero_value )
{
    auto kv = [&] (int i) {
        return make_pair(i, 0);
    };

    check_trie<uint64_t>(kv, 1000, "zero value", false);
}

BOOST_AUTO_TEST_CASE( test_sequential_insert )
{
    check_trie<uint64_t>([] (int i) { return std::make_pair(i, i); },
               1000,
               "linear",
               false /* debug */);
}

uint64_t reverse(uint64_t v)
{
    uint64_t s = sizeof(v) * 8; // bit size; must be power of 2 
    uint64_t mask = ~0ULL;         
    while ((s >>= 1) > 0)  {
        mask ^= (mask << s);
        v = ((v >> s) & mask) | ((v << s) & ~mask);
    }
    return v;
}

BOOST_AUTO_TEST_CASE( test_backwards_sequential_insert )
{
    auto kv = [] (int i)
        {
            uint64_t v = i;
            uint64_t k = reverse(i);
            BOOST_CHECK_EQUAL(reverse(k), v);
            return std::make_pair(k, v);
        };

    check_trie<uint64_t>(kv, 1000, "reversed", false /* debug */);
}

BOOST_AUTO_TEST_CASE( test_reverse_sequential_insert )
{
    auto kv = [] (int i)
        {
            return std::make_pair(1000-i, 1000-i);
        };

    check_trie<uint64_t>(kv, 1001, "linear backwards", false /* debug */);
}

BOOST_AUTO_TEST_CASE( test_random_order_insert )
{
    int n = 1000;

    vector<int> toInsert(n);
    for (unsigned i = 0;  i < n;  ++i)
        toInsert[i] = i;

    std::random_shuffle(toInsert.begin(), toInsert.end());

    auto kv = [&] (int i)
        {
            return std::make_pair(toInsert[i], i);
        };

    check_trie<uint64_t>(kv, 1000, "random order", false /* debug */);
}

BOOST_AUTO_TEST_CASE( test_random_number_insert )
{
    auto kv = [] (int i)
        {
            uint64_t
                k = (uint64_t)random() << 32 | random(),
                v = (uint64_t)random() << 32 | random();
            return std::make_pair(k, v);
        };

    check_trie<uint64_t>(kv, 1000, "random64", false /* debug */);
}

BOOST_AUTO_TEST_CASE( test_random_key_set_insert )
{
    auto kv = [] (int i)
        {
            uint64_t
                k = (uint64_t)random() << 32 | random(),
                v = k;
            return std::make_pair(k, v);
        };

    check_trie<uint64_t>(kv, 1000, "random64 set", false /* debug */);
}

BOOST_AUTO_TEST_CASE( test_random32_number_insert )
{
    auto kv = [] (int i)
        {
            return std::make_pair(random(), random());
        };

    check_trie<uint64_t>(kv, 1000, "random32 set", false /* debug */);
}

BOOST_AUTO_TEST_CASE( test_small_string_insert )
{
    set<string> keySet;
    auto kv = [&] (int i) {
        string str;
        do {
            str = randomString(random() % 7 + 1);
        } while (!keySet.insert(str).second);
        return make_pair(str, i);
    };

    check_trie<string>(kv, 1000, "small string", false /* debug */);
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
    check_trie<string>(kv, 200, "large string", false /* debug */);
}

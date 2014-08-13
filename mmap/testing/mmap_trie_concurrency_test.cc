/* mmap_trie_concurrency_test.cc
   Jeremy Barnes, 7 September 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for concurrency with the memory mapped trie.
*/


#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"
#include "mmap/memory_tracker.h"
#include "mmap/mmap_file.h"
#include "mmap/mmap_trie.h"
#include "mmap/mmap_trie_node.h"

#include "mmap/mmap_trie_terminal_nodes.h"
#include "mmap/mmap_trie_sparse_nodes.h"
#include "mmap/mmap_trie_binary_nodes.h"
#include "mmap/mmap_trie_compressed_nodes.h"
#include "mmap/mmap_trie_large_key_nodes.h"

#include "jml/arch/atomic_ops.h"
#include "jml/utils/smart_ptr_utils.h"
#include "jml/utils/string_functions.h"
#include "jml/arch/atomic_ops.h"

#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/function.hpp>
#include <iostream>
#include <limits>
#include <array>
#include <future>
#include <map>
#include <set>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;


void checkTrieMemory(Trie& trie, MMapFile& area, MutableTrieVersion& current) {
    cerr << "memory usage for " << current.size()
        << " values is " << current.memUsage()
        << " at " << (1.0 * current.memUsage() / current.size())
        << " bytes/value" << endl;

    cerr << "allocated " << area.bytesAllocated()
        << " deallocated " << area.bytesDeallocated()
        << " outstanding " << area.bytesOutstanding()
        << " total used bytes " << area.nodeAlloc.usedSize() << endl;

    // Make sure that no memory is leaked
    uint64_t memUsage = current.memUsage();
    if (NodeAllocSentinels) memUsage *= 3;
    BOOST_CHECK_EQUAL(area.nodeAlloc.bytesOutstanding(), memUsage);

    current.clear();
    current.reset();

    trie.gc().deferBarrier();

    // Make sure that all memory is reclaimed

    BOOST_CHECK_EQUAL(BinaryNodeOps::allocated - BinaryNodeOps::deallocated, 0);
    BOOST_CHECK_EQUAL(
            BasicKeyedTerminalOps::allocated
            - BasicKeyedTerminalOps::deallocated,
            0);
    BOOST_CHECK_EQUAL(SparseNodeOps::allocated - SparseNodeOps::deallocated, 0);
    BOOST_CHECK_EQUAL(
            CompressedNodeOps::allocated
            - CompressedNodeOps::deallocated,
            0);
    BOOST_CHECK_EQUAL(
            LargeKeyNodeOps::allocated
            - LargeKeyNodeOps::deallocated,
            0);

    if (trieMemoryCheck)
        trieMemoryTracker.dumpLeaks();
}

void dumpTrieStats() {
    cerr << "setRootSuccesses = " << setRootSuccesses << endl;
    cerr << "setRootFailures = " << setRootFailures << endl;
    cerr << "overheadRatio = "
         << (100.0 * setRootFailures / (setRootSuccesses + setRootFailures))
         << "%" << endl;

    setRootSuccesses = 0;
    setRootFailures = 0;
}

BOOST_AUTO_TEST_CASE( test_concurrent_access )
{
    cerr << endl
        << "ACCESS ============================================================"
        << endl;

    trieDebug = true;

    for (unsigned i = 0;  i < 10; ++i) {
        
        enum { TrieId = 42 };

        MMapFile area(RES_CREATE);
        area.trieAlloc.allocate(TrieId);

        Trie trie = area.trie(TrieId);
        
        int nthreads = 4;
        int n = 1000;
        
        boost::barrier barrier(nthreads);
        
        int num_errors = 0;
        int nFinished = 0;

        auto doWritingThread = [&] (int threadNum) -> int
            {
                for (unsigned i = 0;  i < n;  ++i) {
                    uint64_t key = i * nthreads + threadNum;
                    uint64_t value = key;
                    
                    auto current = *trie;

                    current.insert(key, value);
                    
                    TrieIterator it = current.find(key);

                    if (!it.valid() || it.value() != value) {
                        cerr << "error: value " << value << " value2 "
                             << it.value() << endl;
                        ML::atomic_add(num_errors, 1);
                    }
                }
                ML::atomic_inc(nFinished);
                return 0;
            };

        auto doReadingThread = [&] (int threadNum) -> int
            {
                while (nFinished < nthreads) {

                    auto current = *trie;
                    //auto mem = area.pin();

                    try {
                        TrieIterator it, end;
                        boost::tie(it, end) = current.beginEnd();

                        for (; it != end;  ++it) {
                            uint64_t key = it.key().cast<uint64_t>();
                            uint64_t value = it.value();

                            if (key != value) {
                                ML::atomic_add(num_errors, 1);
                                cerr << "read: key and value not equal: " << key
                                     << " != " << value << endl;
                                cerr << "inEpoch: " << trie.gc().lockedInEpoch()
                                     << endl;
                                trie.gc().dump();
                            }

                            if (key > n * nthreads + nthreads) {
                                ML::atomic_add(num_errors, 1);
                                cerr << "too many values in trie" << endl;
                                cerr << "inEpoch: " << trie.gc().lockedInEpoch()
                                     << endl;
                                trie.gc().dump();
                            }
                        }
                    } catch (...) {
                        cerr << "inEpoch: " << trie.gc().lockedInEpoch()
                             << endl;
                        cerr << "size = " << trie.current().size() << endl;
                        trie.gc().dump();
                        ML::atomic_inc(num_errors);
                    }
                }
                return 0;
            };

        ThreadedTest test;
        test.start(doWritingThread, nthreads, 0);
        test.start(doReadingThread, nthreads, 1);
        test.joinAll();

        trie.gc().deferBarrier();

        auto current = *trie;

        BOOST_CHECK_EQUAL(num_errors, 0);
        BOOST_CHECK_EQUAL(current.size(), n * nthreads);

        for (uint64_t i = 0;  i < n * nthreads;  ++i)
            BOOST_CHECK_EQUAL(current[i], i);

        checkTrieMemory(trie, area, current);
        area.trieAlloc.deallocate(TrieId);
    }

    dumpTrieStats();

}

BOOST_AUTO_TEST_CASE( test_concurrent_remove_test )
{
    cerr << endl
        << "REMOVE ============================================================"
        << endl;

    enum {
        readThreads = 2,
        writeThreads = 2,
        writeCount = 50,
        repeatCount = 50,
    };

    enum { TrieId = 42 };

    MMapFile area(RES_CREATE);
    area.trieAlloc.allocate(TrieId);

    Trie trie = area.trie(TrieId);

    auto wrongValError = [] (
            string msg,
            uint64_t key,
            uint64_t value,
            uint64_t expectedValue)
    {
        stringstream ss;
        ss << msg << " - "
            << "key=" << key
            << ", value=" << value
            << ", expectedValue=" << expectedValue
            << endl;
        cerr << ss.str();
    };

    int writesFinished = 0;

    /**
    The write thread will repeatively insert the values from 0 to writeCount on
    random keys and remove these values in the reverse order they were added.

    This means that from the readers point of view, if the value i is detected
    for a giventhread, then the values from 0 to i should all be visible in the
    trie even if we're in the process of removing them.
    */
    auto doWriteThread = [&] (int id) -> int {
        int errCount = 0;

        mt19937 engine (id);
        uniform_int_distribution<uint64_t> keyDist(
                numeric_limits<uint64_t>::min(),
                numeric_limits<uint64_t>::max());

        for (int i = 0; i < repeatCount; ++i) {
            vector<uint64_t> keyList;

            auto current = *trie;
            for (int j = 0; j < writeCount; ++j) {
                uint64_t key = keyDist(engine);

                // Squeeze the id of the thread and the value in a 64b word.
                uint64_t value = ((uint64_t)j) | (((uint64_t)id) << 32);

                keyList.push_back(key);

                uint64_t oldValue = current.insert(key, value).first.value();
                if (oldValue != value) {
                    wrongValError("Collision", key, oldValue, value);
                    errCount++;
                }
            }

            current = *trie;
            while(!keyList.empty()) {
                uint64_t key = keyList.back();
                
                bool keyFound;
                uint64_t value;
                tie(keyFound, value) = current.remove(key);

                if (!keyFound) {
                    wrongValError("Key not found", key, value, 0);
                    errCount++;
                    continue;
                }


                int valueId = value >> 32;
                int valueIndex = value & 0xFFFFFFFF;
                keyList.pop_back();

                if (valueId != id) {
                    wrongValError("Invalid value id", key, value, valueId);
                    errCount++;
                }
                if (valueIndex >= writeCount) {
                    wrongValError("Invalid value index",
                            key, writeCount, valueIndex);
                    errCount++;
                }
            }

            // stringstream ss; ss << "W " << id << " - i=" << i
            //   << ", err=" << errCount << endl;
            // cerr << ss.str();
        }

        ML::atomic_inc(writesFinished);
        return errCount;
    };

    /**
    Scans the entire trie and gathers all the values added by each threads.
    We then sum these values and check them against the expected sum calculated
      from from the largest value we found for that thread.
    Note that we don't keep track of duplicate values so these won't affect the
      result.
    */
    auto doReadThread = [&](int id) -> int {
        int errCount = 0;

        while (writesFinished < writeThreads) {
            map<int, set<int> > indexMap;

            // Scan the trie and gather all the values in it.

            auto current = *trie;

            TrieIterator it, end;
            boost::tie(it, end) = current.beginEnd();

            for (; it != end;  ++it) {
                uint64_t value = it.value();
                int valueId = value >> 32;
                int valueIndex = value & 0xFFFFFFFF;

                indexMap[valueId].insert(valueIndex);
            }

            current.reset();

            // Detect holes in a sets by summing all the indexes encountered
            for(auto it = indexMap.begin(), end = indexMap.end();
                it != end; ++it)
            {
                int id = it->first;
                auto& indexSet = it->second;

                int sum = accumulate(indexSet.begin(), indexSet.end(), 0);
                int max = *(indexSet.rbegin());
                int expectedSum = (max * (max+1))/2;

                if (expectedSum != sum) {
                    wrongValError("Invalid sum", id, sum, expectedSum);
                    errCount++;
                }
            }

            // stringstream ss; ss << "R " << id
            //   << " - err=" << errCount << endl;
            // cerr << ss.str();
        }
        return errCount;
    };

    // Start the test.
    ThreadedTest test;
    test.start(doReadThread, readThreads, 0);
    test.start(doWriteThread, writeThreads, 1);
    int errSum = test.joinAll(10000);

    // Make sure everything is gc-ed
    trie.gc().deferBarrier();

    // Make some final checks on the trie.
    auto current = *trie;

    BOOST_CHECK_EQUAL(current.size(), 0);
    checkTrieMemory(trie, area, current);
    area.trieAlloc.deallocate(TrieId);

    dumpTrieStats();

    BOOST_REQUIRE_MESSAGE(errSum == 0,
            "Errors were detected durring the test.");
}


BOOST_AUTO_TEST_CASE( test_concurrent_cas_test )
{
    cerr << endl
        << "CAS ==============================================================="
        << endl;

    enum {
        TrieId = 42,
        threadCount = 8,
        incCount = 50,
        fillerNodes = 100
    };

    MMapFile area(RES_CREATE);

    area.trieAlloc.allocate(TrieId);
    Trie trie = area.trie(TrieId);

    uint64_t key = random();
    {
        // add some random noise into the trie to complicate the structure a bit
        auto current = *trie;
        for (uint64_t i = 0; i < fillerNodes; ++i)
            current.insert(random(), random());

        // Init our key to 0.
        current.insert(key, 0);
    }

    auto runThread = [&](int id) -> int {
        auto current = *trie;

        uint64_t oldValue = 0;
        for (int i = 0; i < incCount; ++i) {
            uint64_t newValue;
            do {
                newValue = oldValue+1;

                bool keyFound;
                tie(keyFound, oldValue) =
                    current.compareAndSwap(key, oldValue, newValue);

                ExcAssert(keyFound);
            } while (oldValue+1 != newValue);
        }

        return 0;
    };

    ThreadedTest test;
    test.start(runThread, threadCount);
    test.joinAll();

    auto current = *trie;
    uint64_t value = current[key];

    BOOST_REQUIRE_EQUAL(value, threadCount * incCount);

    current = *trie;

    BOOST_CHECK_EQUAL(current.size(), fillerNodes+1);
    checkTrieMemory(trie, area, current);
    area.trieAlloc.deallocate(TrieId);
}

BOOST_AUTO_TEST_CASE( test_concurrent_large_keys )
{
    cerr << endl
        << "LARGE KEYS ========================================================"
        << endl;

    enum {
        TrieId = 42,

        threadCount = 2,
        opCount = 20,
        repeatCount = 20,
        keySize = 64,
    };

    MMapFile area(RES_CREATE);

    area.trieAlloc.allocate(TrieId);
    Trie trie = area.trie(TrieId);

    array<array<array<string, opCount>, repeatCount>, threadCount> testSet;
    {
        // generate all the keys discarding collisions.
        set<string> keyPool;
        while (keyPool.size() < threadCount * repeatCount * opCount)
            keyPool.insert(randomString(random() % keySize + 1));

        // Split the keys into test sets.
        auto keyIt = keyPool.begin();
        for (int i = 0; i < threadCount; ++i)
            for (int j = 0; j < repeatCount; ++j)
                for (int k = 0; k < opCount; ++k, ++keyIt)
                    testSet[i][j][k] = *keyIt;
    }

    auto runThread = [&](int id) -> int {
        int errCount = 0;

        for (int repeat = 0; repeat < repeatCount; ++repeat) {

            // stringstream ss;
            // ss << "[" << id << "] - ATTEMPT(" << repeat << ")" << endl;
            // cerr << ss.str();

            map<string, uint64_t> valMap;

            auto current = *trie;

            for (int i = 0; i < opCount; ++i) {
                string key = testSet[id][repeat][i];
                uint64_t value = id;

                // stringstream ss;
                // ss << "[" << id << "] - INSERT(" << i << ") - "
                //     << "key=" << key
                //     << endl;
                // cerr << ss.str();


                TrieIterator it;
                bool inserted;
                tie(it, inserted) = current.insert(key, value);

                if (!inserted) {
                    cerr << "key collision\n";
                    errCount++;
                }

                valMap[key] = value;
            }

            current = *trie;

            for (auto it = valMap.begin(), end = valMap.end(); it != end; ++it){

                // stringstream ss;
                // ss << "[" << id << "] - FIND - "
                //     << "key=" << it->first
                //     << endl;
                // cerr << ss.str();


                if (current.find(it->first).value() != it->second) {
                    cerr << "invalid value on find\n";
                    errCount++;
                }
            }

            current = *trie;

            for (auto it = valMap.begin(), end = valMap.end(); it != end; ++it){

                // stringstream ss;
                // ss << "[" << id << "] - REMOVE - "
                //     << "key=" << it->first
                //     << endl;
                // cerr << ss.str();

                bool keyFound;
                uint64_t oldValue;
                tie(keyFound, oldValue) = current.remove(it->first);

                if (!keyFound) {
                    cerr << "key not found\n";
                    errCount++;
                }
                if (oldValue != it->second) {
                    cerr << "invalid value on remove\n";
                    errCount++;
                }
            }
        }

        return errCount;
    };

    ThreadedTest test;
    test.start(runThread, threadCount);
    int errSum = test.joinAll(100000);

    // Make some final checks on the trie.

    auto current = *trie;

    BOOST_CHECK_EQUAL(current.size(), 0);
    checkTrieMemory(trie, area, current);
    area.trieAlloc.deallocate(TrieId);

    dumpTrieStats();

    BOOST_REQUIRE_MESSAGE(errSum == 0,
            "Errors were detected durring the test.");
}

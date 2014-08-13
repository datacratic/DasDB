/* mmap_trie_merge_test.cc
   Rémi Attab, 21 June 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Tests the commit part of the transactional trie.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"

#include "mmap/mmap_trie.h"
#include "mmap/mmap_file.h"
#include "mmap/sync_stream.h"
#include "jml/arch/timers.h"

#include <boost/test/unit_test.hpp>
#include <array>
#include <vector>
#include <set>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;



/******************************************************************************/
/* KEY GEN                                                                    */
/******************************************************************************/

template<typename Key> struct KeyGen {};

template<>
struct KeyGen<string>
{
    enum { Size = 64 };

    static string get()
    {
        return randomString(random() % Size + 1);
    }
};

template<>
struct KeyGen<uint64_t>
{
    static uint64_t get()
    {
        return random() << 32 | random();
    }
};


/******************************************************************************/
/* MERGE TEST                                                                 */
/******************************************************************************/

template<typename Key, unsigned NumTx>
struct MergeTest
{
    enum {
        NumKeys = 1000,
        NumOps  =  100,
    };

    Trie trie;
    array<TransactionalTrieVersion, NumTx> tx;

    typedef pair<Key, uint64_t> KV;
    array<vector<KV>, NumTx> txSet;
    array<vector<KV>, NumTx> insertSet;
    vector<KV> finalSet;

    static bool less_kv(const KV& lhs, const KV& rhs)
    {
        if (lhs.first == rhs.first) return lhs.second < rhs.second;
        return lhs.first < rhs.first;
    }


    uint64_t value(uint64_t id, uint64_t c)
    {
        return (id << 32) | c;
    }

    Key key()
    {
        return KeyGen<Key>::get();
    }

    MergeTest(MemoryAllocator& area, unsigned trieId) :
        trie(area.trie(trieId))
    {
        auto current = *trie;

        set<Key> keys;

        // Fill the trie with noise.
        for (int j = 0; j < NumTx; ++j) {
            for(uint64_t i = 0; i < NumKeys; ++i) {
                KV kv;

                // Duplicate key filtering mechanism.
                bool done = false;
                do {
                    kv = {key(), value(0xFFFFFFFF, ((j+1) << 24) | i)};
                    done = current.insert(kv.first, kv.second).second;
                } while(!done);

                // cerr << "init: " << hex
                //     << kv.first << " -> " << kv.second
                //     << dec << endl;

                if (i < NumOps * 2)
                    txSet[j].push_back(kv);
                else finalSet.push_back(kv);

                keys.insert(kv.first);
            }
        }

        for (int id = 0; id < NumTx; ++id) {
            for (int i = 0; i < NumOps; ++i) {
                Key k;
                while (!keys.insert(k = key()).second);
                insertSet[id].push_back({ k, value(id+1, i + NumOps) });
            }
        }
    }

    ~MergeTest()
    {
        trie.current().clear();
    }

    int fork(unsigned id)
    {
        sync_cerr() << "FORK: " << id << endl << sync_dump;

        int errCount = 0;

        tx[id] = trie.transaction();

        for (unsigned i = 0; i < NumOps; ++i) {
            KV kv = txSet[id].back();
            txSet[id].pop_back();

            auto res = tx[id].remove(kv.first);
            // cerr << "removing: " << hex
            //     << kv.first << " -> " << res.second
            //     << dec << endl;

            if (!res.first || res.second != kv.second) {
                sync_cerr() << hex
                    << id << " - Unable to remove "
                    << "( " << kv.first << ", " << kv.second << " )"
                    << " -> ( " << res.first << ", " << res.second << " )"
                    << endl << sync_dump;
                errCount++;
            }
        }

        for (uint64_t i = 0; i < NumOps; ++i) {
            KV& kv = txSet[id][i];
            uint64_t newValue = value(id+1, i);

            // cerr << "replacing: " << hex
            //     << kv.first << " -> " << newValue
            //     << dec<< endl;

            auto res = tx[id].replace(kv.first, newValue);

            if(!res.first || res.second != kv.second) {
                sync_cerr() << hex
                    << id << " - Unable to replace "
                    << "( " << kv.first << ", " << kv.second << " )"
                    << " -> ( " << res.first << ", " << res.second << " )"
                    << endl << sync_dump;
                errCount++;
            }
            kv.second = newValue;
        }

        for (uint64_t i = 0; i < NumOps; ++i) {
            const KV& kv = insertSet[id][i];
            auto res = tx[id].insert(kv.first, kv.second);

            if (!res.second) {
                sync_cerr() << hex
                    << id << " - Unable to insert "
                    << "( " << kv.first << ", " << kv.second << " )"
                    << " -> ( " << res.first << ", " << res.second << " )"
                    << endl << sync_dump;
                errCount++;

            }

            // cerr << "inserting: " << hex
            //     << kv.first << " -> " << kv.second
            //     << dec << endl;

            txSet[id].push_back(kv);
        }

        return errCount;
    }

    void commit(unsigned id)
    {
        sync_cerr() << "COMMIT: " << id << endl << sync_dump;

        ML::Timer tm;
        tx[id].commit();
        double elapsed = tm.elapsed_wall();
        sync_cerr() << "ELAPSED: " << elapsed << endl << sync_dump;
    }

    bool check()
    {
        for (unsigned id = 0; id < NumTx; ++id)
            copy(txSet[id].begin(), txSet[id].end(), back_inserter(finalSet));

        sort(finalSet.begin(), finalSet.end(), less_kv);

        vector<KV> trieSet;

        auto current = *trie;

        TrieIterator it, end;
        for (tie(it, end) = current.beginEnd(); it != end; ++it)
            trieSet.emplace_back(it.key().cast<Key>(), it.value());


        vector<KV> missingSet;
        set_difference(
                finalSet.begin(), finalSet.end(),
                trieSet.begin(), trieSet.end(),
                back_inserter(missingSet), less_kv);

        if (!missingSet.empty()) {
            cerr << "Values missing from the final trie("
                << missingSet.size() << ")" << endl;

            for (int i = 0; i < missingSet.size(); ++i)
                cerr << "\t" << hex
                    << missingSet[i].second << " -> " << missingSet[i].first
                    << dec << endl;
        }

        vector<KV> extraSet;
        set_difference(
                trieSet.begin(), trieSet.end(),
                finalSet.begin(), finalSet.end(),
                back_inserter(extraSet));


        if (!extraSet.empty()) {
            cerr << "Extra values present in the final trie("
                << missingSet.size() << ")" << endl;
            for (int i = 0; i < extraSet.size(); ++i)
                cerr << hex << "\t"
                    << extraSet[i].second << " -> " << extraSet[i].first
                    << dec << endl;
        }

        return extraSet.empty() && missingSet.empty();
    }


    bool check(const vector<unsigned>& ids)
    {
        vector<KV> set;

        for (auto it = ids.begin(), end = ids.end(); it != end; ++it)
            copy(txSet[*it].begin(), txSet[*it].end(), back_inserter(set));

        copy(finalSet.begin(), finalSet.end(), back_inserter(set));
        sort(set.begin(), set.end(), less_kv);

        vector<KV> trieSet;

        auto current = *trie;

        TrieIterator it, end;
        for (tie(it, end) = current.beginEnd(); it != end; ++it)
            trieSet.emplace_back(it.key().cast<Key>(), it.value());

        sort(trieSet.begin(), trieSet.end(), less_kv);
        ExcAssert(is_sorted(trieSet.begin(), trieSet.end(), less_kv));

        vector<KV> missingSet;
        set_difference(
                set.begin(), set.end(),
                trieSet.begin(), trieSet.end(),
                back_inserter(missingSet), less_kv);

        if (!missingSet.empty()) {
            sync_cerr() << "Values missing from the final trie("
                << missingSet.size() << ")" << endl;

            for (int i = 0; i < missingSet.size(); ++i)
                sync_cerr() << "\tvals: "
                    << hex << missingSet[i].second
                    << dec << " = " << missingSet[i].second
                    << " -> " << missingSet[i].first
                    << endl;

            sync_cerr() << sync_dump;
        }

        return missingSet.empty();
    }
};


/******************************************************************************/
/* 2-WAY                                                                      */
/******************************************************************************/

template<typename Key>
void twoWayMerge(MemoryAllocator& area)
{
    enum { NumAttempts = 2 };

    for (int attempt = 0; attempt < NumAttempts; ++attempt) {
        cerr << "ATTEMPT: " << attempt << endl;

        MergeTest<Key, 1> mt(area, 12);
        BOOST_CHECK_EQUAL(mt.fork(0), 0);
        mt.commit(0);
        BOOST_REQUIRE(mt.check());
    }
}

BOOST_FIXTURE_TEST_CASE( test_2way_merge_string, MMapAnonFixture )
{
    cerr << endl
        << "[ 2-Way string ]==================================================="
        << endl << endl;

    twoWayMerge<string>(area);
}


BOOST_FIXTURE_TEST_CASE( test_2way_merge_int, MMapAnonFixture )
{
    cerr << endl
        << "[ 2-Way int ]======================================================"
        << endl << endl;

    twoWayMerge<uint64_t>(area);
}


/******************************************************************************/
/* 3-WAY                                                                      */
/******************************************************************************/

template<typename Key>
void threeWayMerge(MemoryAllocator& area)
{

    enum { NumTx = 5 };

    for (int attempt = 0; attempt < 3; ++attempt) {
        MergeTest<Key, NumTx> mt(area, 12);
        vector<unsigned> commitSet;

        cerr << endl << "ATTEMPT: " << attempt << endl;

        for (unsigned id = 0; id < NumTx; ++id)
            mt.fork(id);

        for (unsigned id = NumTx; id > 0; --id) {
            mt.commit(id-1);

            commitSet.push_back(id-1);
            mt.check(commitSet);
        }

        BOOST_REQUIRE(mt.check());
    }
}


BOOST_FIXTURE_TEST_CASE( test_3way_merge_string, MMapAnonFixture )
{
    cerr << endl
        << "[ 3-Way string ]==================================================="
        << endl << endl;
    threeWayMerge<string>(area);
}


BOOST_FIXTURE_TEST_CASE( test_3way_merge_int, MMapAnonFixture )
{
    cerr << endl
        << "[ 3-Way int ]======================================================"
        << endl << endl;
    threeWayMerge<uint64_t>(area);
}


/******************************************************************************/
/* RANDOM                                                                     */
/******************************************************************************/

template<typename Key>
void rndMerge(MemoryAllocator& area)
{
    enum { NumTx = 10 };

    vector<unsigned> startSet;
    vector<unsigned> forkSet;
    vector<unsigned> commitSet;

    for (int id = 0; id < NumTx; ++id)
        startSet.push_back(id);

    auto pickId = [](vector<unsigned>& set) -> unsigned {
        int i = random() % set.size();
        unsigned id = set[i];
        set.erase(set.begin() + i);
        return id;
    };

    MergeTest<Key, NumTx> mt(area, 12);

    while (commitSet.size() != NumTx) {
        int r = random() % 2;

        if (!startSet.empty() && (forkSet.empty() || r == 0)) {
            int id = pickId(startSet);
            mt.fork(id);
            forkSet.push_back(id);
        }
        else {
            int id = pickId(forkSet);
            mt.commit(id);
            commitSet.push_back(id);
        }
        mt.check(commitSet);
    }
    BOOST_REQUIRE(mt.check());
}

BOOST_FIXTURE_TEST_CASE( test_rnd_merge_string, MMapAnonFixture )
{
    cerr << endl
        << "[ Random string ]=================================================="
        << endl << endl;
    rndMerge<string>(area);
}

BOOST_FIXTURE_TEST_CASE( test_rnd_merge_int, MMapAnonFixture )
{
    cerr << endl
        << "[ Random int ]====================================================="
        << endl << endl;
    rndMerge<uint64_t>(area);
}


/******************************************************************************/
/* CONCURRENT                                                                 */
/******************************************************************************/

/** Before you get a heart attack on the commit times for this test, just
    remember that there's a commit lock. Meaning that one thread commits while
    all the other threads wait in line. This also explains why the fork-commit
    end up serializing real quick.

    The main point of this test is to check that concurrent reads will work
    properly with concurrent transactions and commits. Could probably throw in
    some rollbacks in there just to spice things up a bit.
 */
template<typename Key>
void concurrentMerge(MemoryAllocator& area)
{
    enum {
        ReadThreads = 2,
        WriteThreads = 2,
        NumTx = 5,

        TotalTx = WriteThreads * NumTx,
    };


    MergeTest<Key, TotalTx> mt(area, 12);

    array<volatile int, WriteThreads> writeProgress;
    for (int i = 0; i < WriteThreads; ++i)
        writeProgress[i] = -1;

    auto getTxId = [](int thread, int tx) -> unsigned {
        return thread * NumTx + tx;
    };

    auto writeThread = [&](int id) -> int {
        for (int tx = 0; tx < NumTx; ++tx) {
            int txId = getTxId(id, tx);

            mt.fork(txId);
            mt.commit(txId);

            writeProgress[id] = tx;
        }

        return 0;
    };

    auto readThread = [&](int id) -> int {
        int errCount = 0;

        bool writesCompleted = false;
        bool done = false;

        do {
            sync_cerr() << id << ": CHECK { ";

            vector<unsigned> commitSet;
            for (int i = 0; i < WriteThreads; ++i) {
                sync_cerr() << writeProgress[i] << ", ";
                for (int j = 0; j <= writeProgress[i]; ++j) {
                    commitSet.push_back(getTxId(i,j));
                }
            }

            sync_cerr() << "} " << endl << sync_dump;

            errCount += !mt.check(commitSet);

            // The 2 bools ensures we do one last pass once everything is done.
            done = writesCompleted;

            int txCompleted = accumulate(
                    writeProgress.begin(), writeProgress.end(), 0);
            writesCompleted = txCompleted == (WriteThreads * (NumTx -1));

        } while (!done);

        return errCount;
    };

    enum { WriteGroup, ReadGroup };

    ThreadedTest test;
    test.start(writeThread, WriteThreads, WriteGroup);
    test.start(readThread, ReadThreads, ReadGroup);
    int errCount = test.joinAll(10000000);

    mt.check();
    BOOST_CHECK_EQUAL(errCount, 0);
}

BOOST_FIXTURE_TEST_CASE( test_concurrent_merge_string, MMapAnonFixture )
{
    cerr << endl
        << "[ Concurrent string ]=============================================="
        << endl << endl;
    concurrentMerge<string>(area);
}

BOOST_FIXTURE_TEST_CASE( test_concurrent_merge_int, MMapAnonFixture )
{
    cerr << endl
        << "[ Concurrent int ]=============================================="
        << endl << endl;
    concurrentMerge<uint64_t>(area);
}

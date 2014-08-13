/* mmap_region_test.cc
   Jeremy Barnes, 8 December 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Test for mmap region class.
*/



#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"
#include "mmap/sync_stream.h"
#include "mmap/mmap_file.h"
#include "mmap/mmap_trie.h"
#include "mmap/mmap_trie_node.h"
#include "mmap/sigsegv.h"
#include "jml/utils/smart_ptr_utils.h"
#include "jml/utils/string_functions.h"
#include "jml/arch/atomic_ops.h"
#include "jml/utils/guard.h"

#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/function.hpp>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>
#include <set>
#include <array>
#include <memory>
#include <iostream>

#include "mmap/mmap_trie_terminal_nodes.h"
#include "mmap/mmap_trie_sparse_nodes.h"
#include "mmap/mmap_trie_binary_nodes.h"
#include "mmap/mmap_trie_compressed_nodes.h"

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace ML;


struct MMapRegionFixture : public MMapFileFixture
{
    MMapRegionFixture(bool cleanup = true) :
        MMapFileFixture("mmap_region", cleanup)
    {}
    virtual ~MMapRegionFixture() {}
};


BOOST_FIXTURE_TEST_CASE( test_basic, MMapRegionFixture )
{
    enum {
        iterations = 10,
        pages = 64,
        totalSize = pages * page_size,

        trieId = 1,
    };

    MMapFile guardArea(RES_CREATE, filename, PERM_READ_WRITE, totalSize);
    Call_Guard unlink_guard([&] { guardArea.unlink(); });

    set<uint64_t> keys;

    // Insert a bunch of keys and snapshot.
    {
        MMapFile area(RES_OPEN, filename);
        area.trieAlloc.allocate(trieId);

        Trie trie = area.trie(trieId);
        auto current = *trie;

        for (int i = 0; i < iterations; ++i) {
            uint64_t key = random();
            keys.insert(key);

            TrieIterator it;
            bool inserted;
            tie(it, inserted) = current.insert(key, key);
            BOOST_CHECK_EQUAL(it.value(), key);
        }

        area.snapshot();
    }


    // Open the file and remove those keys but don't snapshot!
    {
        MMapFile area(RES_OPEN, filename);

        Trie trie = area.trie(trieId);
        auto current = *trie;

        BOOST_CHECK_EQUAL(current.size(), iterations);

        for(auto it = keys.begin(), end = keys.end(); it != end; ++it) {
            uint64_t key = *it;

            bool keyFound;
            uint64_t oldValue;
            tie(keyFound, oldValue) = current.compareAndRemove(key, key);
            BOOST_CHECK(keyFound);
            BOOST_CHECK_EQUAL(oldValue, key);
        }
    }

    // Open the file and remove the keys again and then snapshot.
    {
        MMapFile area(RES_OPEN, filename);

        Trie trie = area.trie(trieId);
        auto current = *trie;

        BOOST_CHECK_EQUAL(current.size(), iterations);

        for(auto it = keys.begin(), end = keys.end(); it != end; ++it) {
            uint64_t key = *it;

            bool keyFound;
            uint64_t oldValue;
            tie(keyFound, oldValue) = current.compareAndRemove(key, key);
            BOOST_CHECK(keyFound);
            BOOST_CHECK_EQUAL(oldValue, key);
        }

        area.snapshot();
    }



    // Open the file and make sure eveything is still there.
    {
        MMapFile area(RES_OPEN, filename);

        Trie trie = area.trie(trieId);

        auto current = *trie;
        BOOST_CHECK_EQUAL(current.size(), 0);
        current.reset();

        area.trieAlloc.deallocate(trieId);
        area.snapshot();
    }
}

void dumpState(ConstTrieVersion& current, ostream& stream)
{
    // stream << "Pages:" << endl;
    // area->region().dumpPages(stream);
    stream << "Trie:" << endl;
    try {
        current.dump(2, 0, stream);
    }
    // If the dump fails, just ignore it and print what we can.
    catch (...) {}
};


/** Scans the tries and makes sure everything is consistent. */
int
checkTrie(Trie& trie, Progress p, bool multiProcess, bool weakCheck = false)
{
    int errCount = 0;

    map<int, set<int> > indexMap;

    auto current = *trie;

    TrieIterator it, end;
    boost::tie(it, end) = current.beginEnd();

    // Scans the trie and gather all the values in it.
    for (; it != end;  ++it) {
        uint64_t value = it.value();
        int valueId = value >> 32;
        int valueIndex = value & 0xFFFFFFFF;

        indexMap[valueId].insert(valueIndex);
    }

    // Detect holes in a sets by summing all the indexes encountered
    for(auto it = indexMap.begin(), end = indexMap.end(); it != end; ++it) {
        int id = it->first;
        auto& indexSet = it->second;

        if (weakCheck) p.threads[id] = *indexSet.rbegin();

        int sum = accumulate(indexSet.begin(), indexSet.end(), 0);
        int max = p.threads[id];
        int expectedSum = (max * (max+1))/2;

        if (weakCheck ? expectedSum != sum : expectedSum > sum) {
            sync_cerr() << "Invalid sum: "
                << "id=" << id
                << ", sum=" << sum
                << ", expectedSum=" << expectedSum
                << ", max=" << max
                << ", weak=" << weakCheck
                << endl;
            errCount++;
        }
    }

    current.reset();

    if (weakCheck)
        sync_cerr() << "Check:   " << p.print() << endl;

    sync_cerr() << sync_dump;
    return errCount;
};


enum { TrieId = 33 };

/**
Note that the multiProcess is only a simulation of multiple processes.
   All it does is create multiple mappings within the same process.

Error output in this function should be to stdout not stderr. The crash test
  supresses everything sent to stderr because otherwise you get tons of
  expcetions from the snapshot process when you kill the child process.
*/
void snapshot_test(
        int writeCount, int readThreads, int writeThreads, bool multiProcess)
{
    cout << "Region snapshot test:"
        << " " << readThreads << " readers"
        << ", " << writeThreads << " writers"
        << ", multiProcess is " << (multiProcess ? "on" : "off")
        << ", pid is " << getpid()
        << endl;

    MMapRegionFixture fixture;
    const string& filename = fixture.filename;

    MMapFile guardArea(RES_CREATE, filename, PERM_READ_WRITE, page_size);
    Call_Guard unlink_guard([&]{ guardArea.unlink(); });

    auto writeArea = make_shared<MMapFile>(RES_OPEN, filename);
    writeArea->trieAlloc.allocate(TrieId);
    writeArea->snapshot();


    // Thread safe error dumper (won't break up the message).
    auto wrongValError = [] (
            string msg,
            uint64_t key,
            uint64_t value,
            uint64_t expectedValue)
    {
        sync_cout() << msg << " - "
            << "key=" << key
            << ", value=" << value
            << ", expectedValue=" << expectedValue
            << endl << sync_dump;
    };

    int writesFinished = 0;

    Progress writeProgress;
    Progress snapshotProgress;

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

        Trie trie = writeArea->trie(TrieId);

        for (int j = 1; j <= writeCount; ++j) {
            uint64_t key = keyDist(engine);

            // Squeeze the id of the thread and the value in a 64b word.
            uint64_t value = ((uint64_t)j) | (((uint64_t)id) << 32);

            auto current = *trie;
            auto res = current.insert(key, value);
            if (!res.second) {
                wrongValError("Collision", key, res.first.value(), value);
                errCount++;
            }

            writeProgress.threads[id] = j;
        }

        ML::atomic_inc(writesFinished);
        return errCount;
    };


    /** This is the thread in charge of snapshotting the region periodically.
        By periodically, I mean constantly.

        The reason why we need a seperate thread is that creating a snapshot is
        too expensive to be done by the writers. This is also how it would be
        implemented in a real time environment.
    */
    auto doSnapshotThread = [&] (int id) -> int {
        bool done = false;

        while (writesFinished < writeThreads || !done) {

            // Make sure we do one last snapshot after all the writers are done.
            if (writesFinished >= writeThreads)
                done = true;

            Progress p = writeProgress.snapshot();

            writeArea->snapshot();
            sync_cout() << "SNAPSHOT " << p.print() << endl << sync_dump;

            snapshotProgress.restore(p);

            // Slow down the snapshots a bit.
            // Resizing and snapshots are on the same mutex which is a problem
            // since we're constantly resizing and snapshots take a long time to
            // complete.
            this_thread::sleep_for(chrono::milliseconds(1 * writeThreads));

        }

        return 0;
    };


    // Returns a region and a trie object that can be used to read the mmap file
    auto openRegion = [&] () -> shared_ptr<MMapFile> {
        if (multiProcess)
            return make_shared<MMapFile>(RES_OPEN, filename, PERM_READ);
        else return writeArea;
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
            shared_ptr<MMapFile> readArea = openRegion();
            Trie trie = readArea->trie(TrieId);

            Progress p = snapshotProgress.snapshot();
            checkTrie(trie, p, multiProcess);
        }

        return errCount;
    };

    // Start the test.
    ThreadedTest test;
    test.start(doReadThread, readThreads, 0);
    test.start(doWriteThread, writeThreads, 1);
    test.start(doSnapshotThread, 1, 2);
    int errSum = test.joinAll(100000);


    // Make some final checks on the trie.
    {
        shared_ptr<MMapFile> readArea = openRegion();
        Trie readTrie = readArea->trie(TrieId);

        for (int id = 0; id < writeThreads; ++id) {
            BOOST_CHECK_EQUAL(
                    writeProgress.threads[id], writeCount);
        }

        errSum += checkTrie(readTrie, writeProgress, multiProcess);
    }

    writeArea->trieAlloc.deallocate(TrieId);
    writeArea->snapshot();

    BOOST_REQUIRE_MESSAGE(errSum == 0,
            "Errors were detected durring the test.");
}

#if 1

BOOST_AUTO_TEST_CASE( test_concurrent_snapshots )
{
    enum {
        writeCount = 1000,
        n = 8,
    };

    cerr << "=== CONC SNAPSHOTS ===" << endl;

    for (int i = 0; i < 3; ++i) {
        cerr << "Iteration: " << i << endl;
        // snapshot_test(writeCount, 1, 1, false);
        // snapshot_test(writeCount, n, 1, false);
        snapshot_test(writeCount, n, n, false);

        // \todo Lots of work required to make this work properly.
        // snapshot_test(writeCount, 1, 1, true);
        // snapshot_test(writeCount, n, 1, true);
        // snapshot_test(writeCount, n, n, true);
    }
}

#endif

#if 1

/**
This test requires some eye-balling to make sure things are working.
Essentially, check the "Check:" lines against the "SNAPSHOT" line that
immediately precedes it. The values should be equal or slightly higher.

The reason why this is necessary is that the the progress information in
the child process can't easily be transmitted to the parent process (would
need some kind of shared mapping).
*/
BOOST_AUTO_TEST_CASE( test_snapshot_crash )
{
    enum {
        writeCount = 10000,
        threadCount = 8,

        iterations = 10,
        stopOnUndo = 1
    };

    install_segv_handler();

    mt19937 engine;
    // Anything lower and the trie won't have to be allocated in half the runs
    // Reduce it when dev isn't in constant overload.
    uniform_int_distribution<uint64_t> waitDist(100, 1000);

    for (int it = 0; it < iterations; ++it) {

        Fork child(
                [&] { snapshot_test(writeCount, 1, threadCount, false); },
                Fork::err);

        uint64_t waitMs = waitDist(engine);
        this_thread::sleep_for(chrono::milliseconds(waitMs));

        child.kill();
        // cerr << "Kill: child pid " << child.pid << endl;

        MMapRegionFixture fixture(false);
        uint64_t undo = fixture.recoverMMap();

        // Not really an error, it might simply not have had the time.
        struct stat filestats;
        if (stat(fixture.filename.c_str(), &filestats) == -1) {
            cerr << "Check: File not created" << endl;
            continue;
        }


        MMapFile area(RES_OPEN, fixture.filename);
        Call_Guard unlink_guard([&]{ try { area.unlink(); } catch(...) {} });

        // area.trieAlloc.dumpAllocatedTries(cerr);

        // Not really an error, it might simply not have had the time.
        if (!area.trieAlloc.isAllocated(TrieId)) {
            cerr << "Check: Trie not allocated" << endl;
            continue;
        }

        Trie trie = area.trie(TrieId);
        try {
            cerr << "CRASH!" << endl;
            int errSum = checkTrie(trie, Progress(), false, true);
            BOOST_REQUIRE_EQUAL(errSum, 0);
        }
        catch(...) {
            cerr
                << "===================== STATE DUMP ====================="
                << endl;

            TriePtr root = TriePtr::fromBits(*trie.getRootPtr());

            cerr << "Trie Dump:"
                << " root=" << root.bits
                << ", type=" << root.type
                << ", offset=" << root.data
                << ", str=" << root
                << endl;


            auto current = *trie;
            try { current.dump(4); } catch (...) {}

            cerr << endl;
            throw;
        }

        uint64_t filesize = filestats.st_size;
        cerr << "Undo: " << undo << " bytes"
            << " of " << filesize << " bytes"
            << " ( " << ((double)undo / filesize * 100.0) << "% )"
            << endl;

        cerr << endl;

        if (stopOnUndo && undo > 0)
            break;
    }

}

#endif

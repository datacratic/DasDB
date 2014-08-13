/* mmap_perf_test.cc                                                 -*- C++ -*-
   Rémi Attab, 23 April 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Realistic performance test for the map container.

   \todo Comments and good coding practices would be nice... Maybe later...
*/

#include "mmap/mmap_file.h"
#include "mmap/mmap_map.h"
#include "mmap/sigsegv.h"
#include "mmap/mmap_trie_merge.h"

#include "mmap/sync_stream.h"
#include "mmap_test.h"
#include "jml/utils/guard.h"
#include "jml/utils/exc_check.h"
#include "jml/arch/timers.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <boost/thread/barrier.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <memory>
#include <array>
#include <vector>
#include <string>
#include <map>
#include <random>
#include <iostream>
#include <fstream>
#include <clocale>

using namespace std;
using namespace ML;
using namespace Datacratic;
using namespace Datacratic::MMap;


void report_args();
void report_header();
void report(const string& name, unsigned threads, unsigned ops);

static struct {
    int writeThreads;
    int readThreads;

    int64_t snapshotFreq;
    int64_t runtime;

    string filename;
    uint64_t initialSize;

    uint64_t workingSet;

    bool stringKeys;
    bool stringValues;

    bool transactional;

    string keyFile;
} Args;


/*****************************************************************************/
/* GENERATOR                                                                 */
/*****************************************************************************/

enum Source {src_file, src_random};

template<typename T, Source Src = src_random> struct Generate {};

template<Source Src>
struct Generate<uint64_t, Src>
{
    static void init() {}
    static uint64_t value() { return (random() << 32) | random(); }
    static uint64_t key()   { return value(); }
};

template<>
struct Generate<string, src_random>
{
    static void init() {}

    // String values are generally going to be much bigger then their keys.
    static string value() { return randomString(500); }
    static string key() { return randomString(36); }
};


// We're using our mmap library to store the dataset because it's thread safe
// and we'll need to generate our keys in the threads themselves.
// Also I built the damn thing some I might as well use it...
template<>
struct Generate<string, src_file>
{
    enum { trieId = 1 };

    static void init() {
        auto keyFile = [&] { return Args.keyFile + ".mmap"; };

        struct stat fs;

        nextItem = 0;

        // If the mmap dataset file doesn't exit, create it.
        if (stat(keyFile().c_str(), &fs) == -1) {
            cerr << "\rCreating " << keyFile() << "...";

            cleanupMMapFile(keyFile());
            mmapFile.reset(new MMapFile(RES_CREATE, keyFile()));
            mmapFile->trieAlloc.allocate(trieId);

            dataset.reset(new Map<uint64_t, string>(mmapFile.get(), trieId));

            uint64_t index = 0;
            ifstream is(Args.keyFile);
            while (is) {
                string line;
                getline(is, line);

                bool success = dataset->insert(index, line).second;
                ExcAssert(success);

                index++;
            }

            mmapFile->snapshot();
        }
        else {
            cerr << "\rOpening " << keyFile() << "...";

            cleanupMMapFile(keyFile());
            mmapFile.reset(new MMapFile(RES_OPEN, keyFile()));
            ExcAssert(mmapFile->trieAlloc.isAllocated(trieId));

            dataset.reset(new Map<uint64_t, string>(mmapFile.get(), trieId));
        }

        const uint64_t totalEntries =
            Args.workingSet * (Args.readThreads + Args.writeThreads);

        ExcCheckGreaterEqual(dataset->size(), totalEntries,
                "dataset is too small for the test");

    }

    static string value() { return randomString(500); }
    static string key() {
        ExcAssertLess(nextItem, dataset->size());
        uint64_t i = __sync_fetch_and_add(&nextItem, 1);

        auto it = dataset->find(i);
        ExcAssert(it != dataset->end());
        return it.value();
    }

    static uint64_t nextItem;
    static unique_ptr<MMapFile> mmapFile;
    static unique_ptr<Map<uint64_t, string> > dataset;
};

uint64_t                            Generate<string, src_file>::nextItem = 0;
unique_ptr<MMapFile>                Generate<string, src_file>::mmapFile;
unique_ptr<Map<uint64_t, string> >  Generate<string, src_file>::dataset;



typedef array<pair<unsigned, unsigned>, 2> TestResult;


/*****************************************************************************/
/* TEST BASE                                                                 */
/*****************************************************************************/

template<typename Key, typename Value, Source Src>
struct TestBase
{
    MMapFile* mmapFile;

    TestBase()
    {
        cleanupMMapTest(Args.filename, true);
        mmapFile = new MMapFile(
                RES_CREATE, Args.filename,
                PERM_READ_WRITE, Args.initialSize);

        Generate<Key, Src>::init();
        Generate<Value, Src>::init();
    }

    ~TestBase() {
        mmapFile->unlink();
        delete mmapFile;
    }


    void initWs(
            Map<Key, Value>& map,
            vector< pair<Key,Value> >& ws,
            bool insert)
    {
        ws.reserve(Args.workingSet);

        for (uint64_t i = 0; i < Args.workingSet; ++i) {
            auto kv = make_pair(
                            Generate<Key, Src>::key(),
                            Generate<Value, Src>::value());
            ws.push_back(kv);
            if (insert) map.insert(kv.first, kv.second);
        }

        cerr << '.';
    };

};


/*****************************************************************************/
/* MUTABLE MAP TEST                                                          */
/*****************************************************************************/

template<typename Key, typename Value, Source Src = src_random>
struct MutableTest : public TestBase<Key, Value, Src>
{
    TestResult run();
};

template<typename Key, typename Value, Source Src>
TestResult MutableTest<Key,Value,Src>::run()
{
    Map<Key, Value> map(this->mmapFile, 42);

    boost::barrier initBarrier(
            Args.readThreads + Args.writeThreads + (Args.snapshotFreq > 0) + 1);

    volatile bool testCompleted = false;

    auto runReadThread = [&](int id) -> int {

        int opCount = 0;

        vector<pair<Key, Value> > ws;
        this->initWs(map, ws, true);
        initBarrier.wait();

        for (uint64_t i = 0; !testCompleted; i = (i+1) % ws.size()) {
            auto version = map.current();
            auto end = version.end();

            // Batch a couple of searches so we don't needlessly spam the gcLock
            for (int j = 0; j < 10 && !testCompleted; ++j) {

                auto it = version.find(ws[i].first);
                ExcAssert(it != end);

                i = (i + 1) % ws.size();
                opCount++;
            }
        }

        return opCount;
    };

    auto runWriteThread = [&](int id) -> int {
        int opCount = 0;

        vector<pair<Key, Value> > ws;
        this->initWs(map, ws, false);
        initBarrier.wait();

        // Insert and remove are about as complicated and share alot of the same
        // code so we'll consider them equivalent for now.

        while(!testCompleted) {
            for(auto it = ws.begin(), end = ws.end();
                it != end && !testCompleted; it++)
            {
                bool success = map.insert(it->first, it->second).second;
                ExcAssert(success);

                opCount++;
            }

            for(auto it = ws.begin(), end = ws.end();
                it != end && !testCompleted; it++)
            {
                bool success = map.remove(it->first);
                ExcAssert(success);

                opCount++;
            }
        }

        return opCount;
    };

    auto runSnapshotThread = [&](int id) -> int {
        int opCount = 0;

        initBarrier.wait();

        bool done = false;
        while (!done) {

            // Make sure we do a snapshot after everything is done.
            if (testCompleted) done = true;
            else this_thread::sleep_for(chrono::milliseconds(Args.snapshotFreq));

            this->mmapFile->snapshot();
            opCount++;
        }

        return opCount;
    };

    enum { readGroup, writeGroup, snapshotGroup };

    cerr << "\rGenerating the dataset ";

    ThreadedTest test;
    if (Args.snapshotFreq != 0)
        test.start(runSnapshotThread, 1, snapshotGroup);
    test.start(runReadThread, Args.readThreads, readGroup);
    test.start(runWriteThread, Args.writeThreads, writeGroup);

    initBarrier.wait();
    cerr << " Done";

    this_thread::sleep_for(chrono::milliseconds(Args.runtime));

    testCompleted = true;

    uint64_t readOps = test.join(readGroup, 10000);
    uint64_t writeOps = test.join(writeGroup, 10000);
    uint64_t snapshotOps;
    if (Args.snapshotFreq != 0)
        snapshotOps = test.join(snapshotGroup, 10000);


    TestResult res = { {
            make_pair(Args.readThreads, readOps),
            make_pair(Args.writeThreads, writeOps)
        } };
    return res;
}

/*****************************************************************************/
/* TRANSACTIONAL MAP TEST                                                    */
/*****************************************************************************/

static int64_t opTime = 0;

template<typename Key, typename Value, Source Src = src_random>
struct TransactionalTest : public TestBase<Key, Value, Src>
{
    TestResult run();
};

template<typename Key, typename Value, Source Src>
TestResult TransactionalTest<Key,Value,Src>::run()
{
    Map<Key, Value> map(this->mmapFile, 42);
    boost::barrier initBarrier(Args.readThreads + Args.writeThreads + 1);
    volatile bool testCompleted = false;

    vector<vector<pair<Key, Value> > > writeWs;
    writeWs.resize(Args.writeThreads);

    auto runReadThread = [&](int id) -> int {
        int opCount = 0;

        vector<pair<Key, Value> > ws;
        this->initWs(map, ws, true);
        initBarrier.wait();

        for (uint64_t i = 0; !testCompleted;) {
            auto version = map.current();
            auto end = version.end();

            // Batch a couple of searches so we don't needlessly spam the gcLock
            for (int j = 0; j < 10 && !testCompleted; ++j) {

                auto it = version.find(ws[i].first);
                ExcAssert(it != end);

                i = (i + 1) % ws.size();
                opCount++;
            }
        }

        return opCount;
    };

    auto runWriteThread = [&](int id) -> int {
        int opCount = 0;

        initBarrier.wait();
        vector<pair<Key, Value> >& ws = writeWs[id];

        size_t split = ws.size() / 2;
        auto range = [&](int i) -> pair<size_t, size_t> {
            if (i == 0) return make_pair((size_t)0, (size_t)0);

            if (i % 2)
                return make_pair((size_t)0, split);
            return make_pair(split, ws.size());
        };

        for (int i = 0; !testCompleted; ++i) {
            auto tx = map.transaction();

            ML::Timer tm;

            size_t j, inEnd;
            for (tie(j, inEnd) = range(i+1); j < inEnd && !testCompleted; ++j) {
                bool success = tx.insert(ws[j].first, ws[j].second).second;
                ExcAssert(success);

                opCount++;
            }

            size_t rmEnd;
            for (tie(j, rmEnd) = range(i); j < rmEnd && !testCompleted; ++j) {
                bool success = tx.remove(ws[j].first);

                if (!success) {
                    sync_cerr() <<  endl << endl
                        << id << ": failed remove "
                        << ws[j].first << ", " << ws[j].second
                        << endl << sync_dump;

                    tx.dump(4, sync_cerr());
                    sync_cerr() << endl << sync_dump;
                }
                ExcAssert(success);

                opCount++;
            }

            opTime += tm.elapsed_wall() * 1000000000.0;

            if (!testCompleted)
                tx.commit();
        }

        return opCount;

    };

    enum { readGroup, writeGroup, snapshotGroup };

    cerr << "\rGenerating the dataset ";

    ThreadedTest test;
    test.start(runReadThread, Args.readThreads, readGroup);
    test.start(runWriteThread, Args.writeThreads, writeGroup);

    // Generate a series of sorted keys
    std::map<Key, Value> sortedWs;
    for (int i = 0; i < Args.workingSet * Args.writeThreads; ++i) {
        sortedWs.insert(make_pair(
                        Generate<Key, Src>::key(),
                        Generate<Value, Src>::value()));
    }

    // Distribute the sorted keys amongst the threads.
    auto it = sortedWs.begin();
    for (int id = 0 ; id < Args.writeThreads; ++id) {
        writeWs[id].reserve(Args.workingSet);
        for (int j = 0; j < Args.workingSet; ++j, ++it) {
            writeWs[id].push_back(make_pair(it->first, it->second));
        }
        cerr << ".";
    }

    initBarrier.wait();
    cerr << " Done";

    this_thread::sleep_for(chrono::milliseconds(Args.runtime));

    testCompleted = true;

    uint64_t readOps = test.join(readGroup, 10000);
    uint64_t writeOps = test.join(writeGroup, 10000);


    TestResult res = { {
            make_pair(Args.readThreads, readOps),
            make_pair(Args.writeThreads, writeOps)
        } };
    return res;
}


/*****************************************************************************/
/* REPORT                                                                    */
/*****************************************************************************/

void report_args()
{
    // For the thousand seperator
    setlocale(LC_ALL, "");

    auto typeName = [&] (bool isString) -> string {
        return isString ? "string" : "int";
    };

    cerr << "MMAP PERF TEST:" << endl
        // << "\tfilename:\t" << Args.filename << endl
        << "\tdata type:\t" << "< " << typeName(Args.stringKeys)
        << ", " << typeName(Args.stringValues) << " > "
        << (Args.transactional ? "TX": "") << endl
        << "\truntime:\t" << (Args.runtime /1000.0) << " sec" << endl;

    if (!Args.transactional)
        cerr << "\tsnapshotFreq:\t"
            << (Args.snapshotFreq /1000.0) << " sec" << endl;

    cerr << "\tinitialSize:\t" << (Args.initialSize /1000.0) << " kb"
        << " (" << (Args.initialSize / page_size) << " pages)" << endl
        << "\tworkingSet:\t" << Args.workingSet << " keys/thread" << endl
        << endl;

}

void report_seperator()
{
    cerr << "|";
    for (int i = 0; i < 2; ++i)
        cerr << setfill('-')
            << setw(6) << "|"
            << setw(13) << "|" << setw(13) << "|"
            << setw(15) << "|";

    if (Args.transactional)
        cerr << setw(12) << "|";

    cerr << endl;
    cerr.clear();
}

void report_header()
{
    string name[2] = { "R Th", "W Th" };

    cerr << "|";
    for (int i = 0; i < 2; ++i)
        cerr <<
            format("%5s|%12s|%12s|%14s|",
                    name[i].c_str(), "Ops/th", "Ops/sec", "nsec/Ops");

    if (Args.transactional)
        cerr << format("%11s|", "Merge Time");

    cerr << endl;
    report_seperator();

}

void report_line(
        unsigned rThreads, unsigned rOps,
        unsigned wThreads, unsigned wOps)
{
    unsigned threads[2] = { rThreads, wThreads };
    unsigned ops[2] = { rOps, wOps };

    cerr << "\r|";

    for (int i = 0; i < 2; ++i) {
        unsigned ops_th = threads[i] > 0 ? ops[i] / threads[i] : 0;

        unsigned throughput = (ops[i] * 1000ULL) / Args.runtime;
        unsigned throughput_th = threads[i] > 0 ? throughput / threads[i] : 0;

        unsigned latency = throughput_th > 0 ? 1000000000ULL / throughput_th : 0;

        cerr << format("%5d|%'12d|%'12d|%'14d|",
                threads[i], ops_th, throughput, latency);
    }

    if (Args.transactional) {
        double idleRatio =
            (double)mergeActiveTime / (mergeIdleTime + mergeActiveTime);

        if (!wThreads) idleRatio = 0.0;

        cerr << format("%'9.2f %%|", idleRatio * 100.0);

        mergeActiveTime = 0;
        mergeIdleTime = 0;
    }

    // More detailed timings for profiling and debugging.
    if (false && Args.transactional) {
        using namespace Merge;

        double total =
            dbg_mergeDiffTime + dbg_mergeInsertTime + dbg_mergeRemoveTime;

        auto pct = [&](double t) -> string {
            return format("%.2f", (t / total) * 100.0);
        };
        auto tm = [&](int64_t t) -> string {
            if (!dbg_mergeCount) return "0";
            return format("%'ld", t / dbg_mergeCount);
        };

        cerr << " cnt=" << dbg_mergeCount
            << ", dif=" << tm(dbg_mergeDiffTime)
            << ", ins=" << tm(dbg_mergeInsertTime)
            << ", rmv=" << tm(dbg_mergeRemoveTime)
            << ", opt=" << format("%'ld", wOps ? opTime / wOps : 0ULL);

        dbg_mergeCount = 0;
        dbg_mergeDiffTime = 0;
        dbg_mergeInsertTime = 0;
        dbg_mergeRemoveTime = 0;
        opTime = 0;
    }

    cerr << endl;
}


/*****************************************************************************/
/* MAIN                                                                      */
/*****************************************************************************/


template<typename Key, typename Value, Source Src>
struct TestRunnerReport {
    static void run() {
        TestResult res;

        if (Args.transactional)
            res = TransactionalTest<Key,Value,Src>().run();
        else res = MutableTest<Key, Value, Src>().run();

        report_line(res[0].first, res[0].second, res[1].first, res[1].second);
    }
};

template<typename Key, typename Value>
struct TestRunnerSrc {
    static void run() {
        if (Args.keyFile.empty())
            TestRunnerReport<Key, Value, src_random>::run();
        else TestRunnerReport<Key, Value, src_file>::run();
    }
};


template<typename Key>
struct TestRunnerValue {
    static void run() {
        if (Args.stringValues)
            TestRunnerSrc<Key, string>::run();
        else TestRunnerSrc<Key, uint64_t>::run();
    }
};

struct TestRunner {
    static void run() {
        if (Args.readThreads + Args.writeThreads <= 0) return;

        if (Args.stringKeys)
            TestRunnerValue<string>::run();
        else TestRunnerValue<uint64_t>::run();
    }
};


int main (int argc, char** argv)
{
    Args.writeThreads = 1;
    Args.readThreads = 8;
    Args.snapshotFreq = 1000;
    Args.runtime = 10000;
    Args.filename = "mmap_perf_test.mmap";
    Args.initialSize = 64 * page_size;
    Args.workingSet = 10000;
    Args.keyFile = "";

    enum TestType {
        test_single = 0,
        test_summary = 1,
        test_scaling_read = 2,
        test_scaling_write = 3,
    };
    int testType = test_single;

    using namespace boost::program_options;

    options_description options("Performance test options");
    options.add_options()
        ("string-keys,k", "Use string as keys")

        ("string-values,v", "Use string as values")

        ("write-threads,w", value<int>(&Args.writeThreads),
                "Number of write threads")

        ("read-threads,r", value<int>(&Args.readThreads),
                "Number of read threads")

        ("transactional,x", "Use transactional tries")

        ("snapshot-freq,s", value<int64_t>(&Args.snapshotFreq),
                "Frequency of the snapshots in ms (-1 = no snapshots)")

        ("runtime,t", value<int64_t>(&Args.runtime),
                "Duration of the test in ms (-1 = never ends)")

        ("filename,f", value<string>(&Args.filename),
                "Filename of the mmap file (default = mmap_perf_test)")

        ("initial-size,i", value<uint64_t>(&Args.initialSize),
                "Initial size of the mmap (affects the number of resizes")

        ("working-set,z", value<uint64_t>(&Args.workingSet),
                "Number of elements per thread in the trie")

        ("key-file", value<string>(&Args.keyFile),
                "File where to pull the keys in the dataset from.")

        ("auto-test,a", value<int>(&testType),
                "Type of test to run "
                "(1=summary, 2=scaling read, 3=scaling write)")

        ("help,h", "Print this message");

    variables_map vm;
    store(command_line_parser(argc, argv).options(options).run(), vm);
    notify(vm);

    if (vm.count("help")) {
        cerr << options << endl;
        return 1;
    }

    Args.stringKeys = vm.count("string-keys");
    Args.stringValues = vm.count("string-values");
    Args.transactional = vm.count("transactional");

    report_args();
    report_header();

    if (testType == test_single) {
        TestRunner::run();
    }
    else if (testType == test_summary) {
        unsigned maxRead = Args.readThreads;
        unsigned maxWrite = Args.writeThreads;

        enum th { th_0 = 0, th_1 = 1, th_n = 2 };
        auto setArgs = [&](th r, th w) {
            if (maxRead > 0)
                Args.readThreads = (r == th_n) ? maxRead : r;

            if (maxWrite > 0)
                Args.writeThreads = (w == th_n) ? maxWrite : w;
        };

        setArgs(th_1, th_0); TestRunner::run();
        setArgs(th_n, th_0); TestRunner::run();
        setArgs(th_0, th_1); TestRunner::run();
        setArgs(th_0, th_n); TestRunner::run();

        report_seperator();

        setArgs(th_1, th_1); TestRunner::run();
        setArgs(th_n, th_1); TestRunner::run();
        setArgs(th_1, th_n); TestRunner::run();
        setArgs(th_n, th_n); TestRunner::run();

    }
    else if (testType == test_scaling_read || testType == test_scaling_write) {
        unsigned max = testType == test_scaling_read ?
            Args.readThreads : Args.writeThreads;

        for (int i = 0; i < max; ++i) {
            if (testType == test_scaling_read)
                Args.readThreads = i;
            else Args.writeThreads = i;

            TestRunner::run();

            if (i % 5 == 4) report_seperator();
        }
    }
}

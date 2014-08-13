/** mmap_perf.cc                                 -*- C++ -*-
    Rémi Attab, 25 Jan 2013
    Copyright (c) 2013 Datacratic.  All rights reserved.

    Performance tests for DasDB.

*/

#include "mmap_perf_utils.h"
#include "mmap/mmap_file.h"
#include "mmap/mmap_map.h"
#include "soa/utils/threaded_test.h"
#include "jml/utils/guard.h"
#include "jml/utils/filter_streams.h"
#include "jml/arch/timers.h"

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <map>

using namespace std;
using namespace ML;
using namespace Datacratic;
using namespace Datacratic::MMap;


/******************************************************************************/
/* TEST FUNCTIONS                                                             */
/******************************************************************************/

template<typename Key, typename Value>
size_t findFn(
        unsigned id,
        unsigned iteration,
        size_t workingSet,
        Map<Key, Value>& map,
        Generator<Key>& keyGen,
        Generator<Value>& valueGen)
{
    const size_t ops = 1000;

    Partition<Key> keyPart(keyGen, id * workingSet, workingSet);
    Partition<Value> valuePart(valueGen, id * workingSet, workingSet);

    auto version = map.current();

    for (size_t i = 0; i < ops; ++i) {
        size_t index = iteration * ops + i;
        auto it = version.find(keyPart[index]);
        ExcAssert(it != version.end());
        ExcAssertEqual(it.value(), valuePart[index]);
    }

    version.forceUnlock();

    return ops;
}

template<typename Key, typename Value>
size_t writeFn(
        unsigned id,
        unsigned iteration,
        size_t workingSet,
        Map<Key, Value>& map,
        Generator<Key>& keyGen,
        Generator<Value>& valueGen)
{
    const size_t ops = 100;

    Partition<Key> keyPart(keyGen, id * workingSet, workingSet);
    Partition<Value> valuePart(valueGen, id * workingSet, workingSet);

    for (size_t i = 0; i < ops; ++i) {
        size_t index = iteration * ops + i;
        auto ret = map.insert(keyPart[index], valuePart[index]);
        ExcAssert(ret.second);
        ExcAssertEqual(ret.first.value(), valuePart[index]);
    }

    if (!iteration) return ops;

    for (size_t i = 0; i < ops; ++i) {
        size_t index = (iteration - 1) * ops + i;
        bool success = map.remove(keyPart[index]);
        ExcAssert(success);
    }

    map.forceUnlock();

    return ops * 2;
}

template<typename Key, typename Value>
size_t txWriteFn(
        unsigned id,
        unsigned iteration,
        size_t workingSet,
        size_t txSize,
        Map<Key, Value>& map,
        Generator<Key>& keyGen,
        Generator<Value>& valueGen,
        TransactionStats& txStats)
{
    static __thread size_t it;
    if (!iteration) it = 0;

    const size_t ops = txSize / 2;
    size_t totalOps = 0;

    Partition<Key> keyPart(keyGen, id * workingSet, workingSet);
    Partition<Value> valuePart(valueGen, id * workingSet, workingSet);

    auto tx = map.transaction();

    while (true) {
        for (size_t i = 0; i < ops; ++i) {
            size_t index = it * ops + i;
            auto ret = tx.insert(keyPart[index], valuePart[index]);
            ExcAssert(ret.second);
            ExcAssertEqual(ret.first.value(), valuePart[index]);
            totalOps++;
        }

        if (it) {
            for (size_t i = 0; i < ops; ++i) {
                size_t index = (it - 1) * ops + i;
                bool success = tx.remove(keyPart[index]);
                ExcAssert(success);
                totalOps++;
            }
        }

        it++;

        Timer tm;

        if (tx.tryCommit()) {
            txStats.success(tm.elapsed_wall());
            return totalOps;
        }

        txStats.fail(tm.elapsed_wall());
    }

    tx.forceUnlock();
}


/******************************************************************************/
/* SETUP                                                                      */
/******************************************************************************/

struct Test
{
    Test() :
        initSize(100000), workingSet(100000), sort(false),
        durationMs(1000), txSize(0), readThreads(1), writeThreads(1),
        ops("fi"), csvReport(false)
    {}

    string keyFile;
    size_t initSize;
    size_t workingSet;
    bool sort;

    unsigned durationMs;
    size_t txSize;
    size_t readThreads;
    size_t writeThreads;

    string ops; // r|f = find, w|i = insert/remove, r = replace

    bool csvReport;
    string variable;

    TransactionStats txStats;

    template<typename Key, typename Value>
    void run(bool verbose)
    {
        size_t dataset = (readThreads + writeThreads) * workingSet + initSize;

        if (verbose) cerr << "Generating values: " << fmtValue(dataset) << endl;

        Generator<Value> valueGen(dataset);
        valueGen.load();

        if (verbose) {
            cerr << "Generating keys: "
                << fmtValue(dataset) << ", " << keyFile << endl;
        }

        Generator<Key> keyGen(dataset);
        if (keyFile.empty()) keyGen.load();
        else {
            ML::filter_istream is(keyFile);
            keyGen.load(is);
        }

        if (verbose && sort) cerr << "Sorting keys" << endl;
        if (sort) keyGen.sort();


        MMapFile dasdb(RES_CREATE);
        ML::Call_Guard guard([&] { dasdb.unlink(); });

        Map<Key, Value> map(&dasdb, 1);

        ThreadedTimedTest test;
        std::map<unsigned, string> titles;
        unsigned id = 0;

        for (size_t i = 0; i < ops.size(); ++i) {
            switch(ops[i]) {

            case 'r': // read
            case 'f': // find
            {
                if (verbose) {
                    cerr << "find: "
                        << readThreads << ", " << fmtValue(workingSet)
                        << endl;
                }

                auto tx = map.transaction();
                for (size_t th = 0; th < readThreads; ++th, ++id) {
                    for (size_t j = 0; j < workingSet; ++j) {
                        int index = id * workingSet + j;
                        tx.insert(keyGen[index], valueGen[index]);
                    }
                }
                tx.commit();
                map.forceUnlock();

                auto fn = bind(
                        findFn<Key,Value>, _1, _2,  workingSet,
                        ref(map), ref(keyGen), ref(valueGen));
                int gr = test.add(fn, readThreads);
                titles[gr] = "find";
                break;
            }

            case 'w': // write
            case 'i': // insert
            {
                if (txSize) {
                    if (verbose) {
                        cerr << "tx-write: "
                            << writeThreads << ", "
                            << fmtValue(workingSet) << ", "
                            << fmtValue(txSize)
                            << endl;
                    }

                    auto fn = bind(
                            txWriteFn<Key,Value>, _1, _2, workingSet, txSize,
                            ref(map), ref(keyGen), ref(valueGen), ref(txStats));
                    int gr = test.add(fn, writeThreads);
                    titles[gr] = "tx-write";
                }
                else {
                    if (verbose) {
                        cerr << "write: "
                            << writeThreads << ", "
                            << fmtValue(workingSet) << ", "
                            << endl;
                    }

                    auto fn = bind(
                            writeFn<Key,Value>, _1, _2, workingSet,
                            ref(map), ref(keyGen), ref(valueGen));
                    int gr = test.add(fn, writeThreads);
                    titles[gr] = "write";
                }
                id += writeThreads;
                break;
            }

            default:
                cerr << "ERROR: Operand "
                    << ops[i] << " is not yet supported."
                    << endl;
            }
        }


        if (verbose) cerr << "init: " << fmtValue(initSize) << endl;
        Partition<Key> keyPart(keyGen, id * workingSet, initSize);
        Partition<Value> valuePart(valueGen, id * workingSet, initSize);

        auto tx = map.transaction();
        for (size_t i = 0; i < initSize; ++i) {
            auto ret = tx.insert(keyPart[i], valuePart[i]);
            ExcAssert(ret.second);
            ExcAssert(ret.first != tx.end());
        }
        tx.commit();
        map.forceUnlock();

        if (verbose) cerr << "running: " << fmtElapsed(durationMs / 1000.0) << endl;

        txStats.reset();

        test.run(durationMs);
        report(test, titles);
    }


    void reportHeader()
    {
        char v = variable[0];
        if ('w' != v) cerr << fmtValue(writeThreads) << " write threads" << endl;
        if ('r' != v) cerr << fmtValue(readThreads) << " read threads" << endl;
        if ('i' != v) cerr << fmtValue(initSize) << " init size" << endl;
        if ('z' != v) cerr << fmtValue(workingSet) << " working set" << endl;
        if ('x' != v && txSize)
            cerr << fmtValue(txSize) << " transaction size" << endl;

        printf("\n");

        if (!variable.empty()) {
            string title;

            switch(variable[0]) {

            case 'w': title = "writeTh"; break;
            case 'r': title = "readTh"; break;

            case 'i': title = "initSz"; break;
            case 'z': title = "WSet"; break;
            case 'x': title = "txSz"; break;

            }

            printf("%10s ", title.c_str());
        }

        for (int i = 0; i < ops.size(); ++i)
            printf("%10s %10s %10s ", "op", "sec/ops", "ops/sec");

        if (txSize) printf("%10s %10s", "cm rate", "sec/cm");

        printf("\n\n");
    }

    void report(
            ThreadedTimedTest& results,
            const std::map<unsigned, string>& titles)
    {
        if (!variable.empty()) {
            string val;

            switch(variable[0]) {

            case 'w': val = to_string(writeThreads); break;
            case 'r': val = to_string(readThreads); break;

            case 'i': val = fmtValue(initSize); break;
            case 'z': val = fmtValue(workingSet); break;
            case 'x': val = fmtValue(txSize); break;

            }

            printf("%10s ", val.c_str());
        }

        for(const auto& entry : titles) {
            unsigned id;
            string title;
            tie(id, title) = entry;
            auto dists = results.distributions(id);

            double latency = dists.first.mean();
            double throughput = accumulate(
                    dists.second.begin(), dists.second.end(), 0.0);

            printf("%10s %10s %10s ",
                    title.c_str(),
                    fmtElapsed(latency).c_str(),
                    fmtValue(throughput).c_str());
        }

        if (txSize) {
            printf("%10s %10s",
                    fmtPct(txStats.commitRate()).c_str(),
                    fmtElapsed(txStats.commitLatency()).c_str());
        }

        printf("\n");
    }

};

void runTest(Test& test, bool keyString, bool valueString, bool verbose)
{
    if (verbose) {
        cerr << "Test<"
            << (keyString ? "string" : "int") << ", "
            << (valueString ? "string" : "int") << ">"
            << endl;
    }

    if (keyString && valueString)
        test.run<string, string>(verbose);

    else if (!keyString && valueString)
        test.run<uint64_t, string>(verbose);

    else if (keyString && !valueString)
        test.run<string, uint64_t>(verbose);

    else test.run<uint64_t, uint64_t>(verbose);
}


void runAutoTest(
        Test& test, size_t start, size_t stop, size_t step,
        bool keyString, bool valueString, bool verbose)
{
    function<size_t(size_t)> stepFn;
    if (step)
        stepFn = [=] (size_t i) { return i + step; };
    else stepFn = [] (size_t i) { return i * 2; };

    for (int i = start; i <= stop; i = stepFn(i)) {
        switch(test.variable[0]) {

        case 'w': test.writeThreads = i; break;
        case 'r': test.readThreads = i; break;

        case 'i': test.initSize = i; break;
        case 'z': test.workingSet = i; break;
        case 'x': test.txSize = i; break;

        default: cerr << "ERROR: Unknown variable: " << test.variable[0] << endl;

        }

        runTest(test, keyString, valueString, verbose);
    }
}


/******************************************************************************/
/* MAIN                                                                       */
/******************************************************************************/

int main(int argc, char** argv)
{
    using namespace boost::program_options;

    Test test;

    options_description datasetOptions("Dataset");
    datasetOptions.add_options()
        ("string-keys,k", "Use string as keys")
        ("string-values,v", "Use string as values")

        ("working-set,z", value<size_t>(&test.workingSet),
                "Number of keys to modify by each thread during the test")

        ("init-size,s", value<size_t>(&test.initSize),
                "Initial size of the database")

        ("key-file,f", value<string>(&test.keyFile),
                "File containing the keys to be used in the test");


    string accessPattern = "rnd"; // || "seq"

    options_description testOptions("Manual Testing");
    testOptions.add_options()
        ("duration,d", value<unsigned>(&test.durationMs),
                "length of each tests in ms.")

        ("write-threads,w", value<size_t>(&test.writeThreads),
                "Number of write threads")

        ("read-threads,r", value<size_t>(&test.readThreads),
                "Number of read threads")

        ("tx-size,x", value<size_t>(&test.txSize),
                "Use transactions of this size during the test")

        ("ops,o", value<string>(&test.ops),
                "The ops to run for the test")

        ("access,a", value<string>(&accessPattern),
                "Data access pattern");


    // w = writeTh, r = readTh, i = initSize, w = workingSet, x = txSize
    size_t autoStart = 1;
    size_t autoStop = 32;
    size_t autoSteps = 0;

    options_description autoOptions("Automatic testing");
    autoOptions.add_options()
        ("var", value<string>(&test.variable),
                "The value to act as a variable for the test")
        ("start", value<size_t>(&autoStart), "Value to start scaling at")
        ("stop", value<size_t>(&autoStop), "Value to stop scaling at")
        ("step", value<size_t>(&autoSteps),
                "Value increments. If 0, exponential growth is used");


    options_description allOptions;
    allOptions
        .add(datasetOptions)
        .add(testOptions)
        .add(autoOptions)
        .add_options()
        ("help,h", "Print this message")
        ("verbose", "Dumps lots of debugging information");

    variables_map vm;
    store(command_line_parser(argc, argv).options(allOptions).run(), vm);
    notify(vm);

    if (vm.count("help")) {
        cerr << allOptions << endl;
        return 1;
    }

    if (accessPattern == "seq") test.sort = true;

    bool stringKeys = vm.count("string-keys");
    bool stringValues = vm.count("string-values");
    bool verbose = vm.count("verbose");

    test.reportHeader();

    Timer tm;

    if (test.variable.empty())
        runTest(test, stringKeys, stringValues, verbose);
    else
        runAutoTest(
                test, autoStart, autoStop, autoSteps,
                stringKeys, stringValues, verbose);

    printf("\n%10s elapsed\n", fmtElapsed(tm.elapsed_wall()).c_str());
}

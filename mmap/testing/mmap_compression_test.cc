/** mmap_compression_test.cc                                 -*- C++ -*-
    Rémi Attab, 19 Dec 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Tests to see how well the mmap compresses various data sets.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap_test.h"
#include "mmap/mmap_file.h"
#include "mmap/mmap_map.h"
#include "jml/utils/filter_streams.h"
#include "jml/utils/guard.h"

#include <boost/test/unit_test.hpp>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <utility>
#include <fstream>
#include <set>

using namespace std;
using namespace ML;
using namespace Datacratic;
using namespace Datacratic::MMap;

struct CompressionTestFixture : public MMapFileFixture
{
    CompressionTestFixture() : MMapFileFixture("mmap_compression_test") {}
    ~CompressionTestFixture() {}
};


enum { TrieId = 1 };


string pct(double d) { return ML::format("%6.2f%%", d * 100); }
string pct(double num, double denum) { return pct(num / denum); }

string by(double d) { return ML::format("%6.2f ", d); }
string kb(double d) { return ML::format("%6.2fk", d / 1000); }
string mb(double d) { return ML::format("%6.2fm", d / (1000  * 1000)); }
string dumpSize(double d)
{
    if (d < 1000) return by(d);
    if (d < 1000 * 1000) return kb(d);
    return mb(d);
}

void status(
        const string& test,
        const string& action,
        size_t cur, size_t target,
        size_t rate = 1000)
{
    if (cur % rate) return;

    cerr << "\r"
        << test << ": "
        << action << " "
        << dumpSize(cur) << " / " << dumpSize(target)
        << " (" << pct(static_cast<double>(cur) / target) << ")";
}

size_t fileSize(const string& file)
{
    struct stat stats;
    int res = stat(file.c_str(), &stats);
    ExcCheckErrno(!res, "can't stat file: " + file);

    return stats.st_size;
}

string compress(const string& mmapFile, const string& test)
{
    string outFile = mmapFile + ".gz";

    filter_ostream os(outFile);
    ifstream is(mmapFile, ios_base::in | ios_base::binary);

    size_t total = fileSize(mmapFile);
    size_t current = 0;

    while(is.good()) {
        status(test, "Compressing", current, total, 1000 * 1000);

        char val;
        is >> val;
        os << val;

        current += sizeof(char);
    }

    return outFile;
}

void checkCompression(
        MemoryAllocator& area, const string& mmapFile, const string& test)
{
    string gzFile = compress(mmapFile, test);
    // Call_Guard guard([&] { unlink(gzFile.c_str()); });

    double gzSize = fileSize(gzFile);
    double mmapSize = area.bytesOutstanding() - area.bytesPrivate();

    status(test, "Compressed ", mmapSize, gzSize, mmapSize);
    cerr << endl;

    // Only enable this once I can actually make it there.
    // BOOST_CHECK_LT(mmapSize, gzSize * 1.5);
}

template<typename K, typename V>
void build(
        const string& file,
        const function<pair<K, V>(unsigned)>& dataset,
        const string& test)
{
    enum { Size = 100000 };

    cerr << endl << endl << test << " "
        << "-------------------------------------------------------------------"
        << endl << endl;

    MMapFile mmap(RES_CREATE, file);
    Call_Guard guard([&] { mmap.unlink(); });

    {
        Map<K, V> map(&mmap, TrieId);
        for (size_t i = 0; i < Size; ++i) {
            status(test, "Inserting", i, Size);

            auto entry = dataset(i);
            bool ret = map.insert(entry.first, entry.second).second;
            BOOST_CHECK(ret);
        }

        cerr << "\r";
        map.stats(STATS_SAMPLING_FULL).dump();
    }

    mmap.snapshot();
    checkCompression(mmap, file, test);
}

BOOST_FIXTURE_TEST_CASE( test_seq_int_seq_int, CompressionTestFixture )
{
    auto gen = [](uint64_t i)
    {
        return make_pair(i, i);
    };
    build<uint64_t, uint64_t>(filename, gen, "<seq-int, seq-int>");
}

#if 0
BOOST_FIXTURE_TEST_CASE( test_seq_int_rnd_int, CompressionTestFixture )
{
    auto gen = [](uint64_t i)
    {
        uint64_t v = random();
        v = (v << 32) | random();
        return make_pair(i, v);
    };
    build<uint64_t, uint64_t>(filename, gen, "<seq-int, rnd-int>");
}

BOOST_FIXTURE_TEST_CASE( test_rnd_int_seq_int, CompressionTestFixture )
{
    auto gen = [](uint64_t i)
    {
        uint64_t v = random();
        v = (v << 32) | random();
        return make_pair(v, i);
    };
    build<uint64_t, uint64_t>(filename, gen, "<rnd-int, seq-int>");
}
#endif

BOOST_FIXTURE_TEST_CASE( test_rnd_int_rnd_int, CompressionTestFixture )
{
    auto gen = [](uint64_t i)
    {
        uint64_t v = random();
        v = (v << 32) | random();
        return make_pair(v, v);
    };
    build<uint64_t, uint64_t>(filename, gen, "<rnd-int, rnd-int>");
}

BOOST_FIXTURE_TEST_CASE( test_sm_str_seq_int, CompressionTestFixture )
{
    set<string> keySet;
    auto gen = [&](uint64_t i)
    {
        string str;
        do {
            str = randomString(random() % 7 + 1);
        } while(!keySet.insert(str).second);
        return make_pair(str, i);
    };
    build<string, uint64_t>(filename, gen, "<sm-str, seq-int>");
}

BOOST_FIXTURE_TEST_CASE( test_lg_str_seq_int, CompressionTestFixture )
{
    set<string> keySet;
    auto gen = [&](uint64_t i)
    {
        string str;
        do {
            str = randomString(random() % 512 + 1);
        } while(!keySet.insert(str).second);
        return make_pair(str, i);
    };
    build<string, uint64_t>(filename, gen, "<lg-string, seq-int>");
}

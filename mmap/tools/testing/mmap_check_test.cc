/** mmap_check.cc                                 -*- C++ -*-
    Mathieu Stefani, 16 May 2013
    Copyright (c) 2013 Datacratic.  All rights reserved.

    Tests for the trie checking tool
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mmap/testing/mmap_test.h"
#include "mmap_check_test_utils.h"
#include "mmap/memory_region.h"
#include "mmap/mmap_file.h"
#include "mmap/mmap_trie.h" 
#include "mmap/trie_key.h"
#include "mmap/tools/trie_check.h"
#include "mmap/mmap_trie.h"
#include "mmap/mmap_trie_node_impl.h"
#include "mmap/mmap_trie_large_key_nodes.h"
#include "mmap/mmap_trie_sparse_nodes.h"
#include "mmap/mmap_trie_terminal_nodes.h"
#include "mmap/mmap_trie_null_node.h"
#include "mmap/mmap_trie_inline_nodes.h"
#include "mmap/mmap_trie_compressed_nodes.h"
#include "mmap/mmap_trie_dense_branching_node.h"
#include "jml/utils/guard.h"
#include <boost/test/unit_test.hpp>
#include <type_traits>
#include <climits>
#include <vector>

using namespace Datacratic;
using namespace Datacratic::MMap;
using namespace std;

struct CheckTestFixture : public MMapFileFixture {
    CheckTestFixture() : MMapFileFixture("mmap_check_test") { 
    }
};


static constexpr int TrieId = 1;


template<char C, size_t N>
std::string separator() {
    return std::string(N, C);
}

template<typename Key, typename Value,
        size_t Size,
        bool Dump = false,
        typename Func>
void build(const std::string &fileName, 
           const Func &buildFn) {
    MMapFile dasdb(RES_CREATE, fileName);
    Trie trie = dasdb.trie(TrieId);
    auto current = *trie;
    for (size_t i = 0; i < Size; ++i) {
        auto kv = buildFn(i);

        bool ok;
        tie(ignore, ok) = current.insert(TrieKey(kv.first), kv.second);
    }

    if (Dump) {
        current.dump();
    }

    dasdb.snapshot();
}

template<int NodeType>
void run_test(const string &fileName,
              const string &testName,
              PositionHint hint = Begin) {

    cout << endl << 
                 separator<'-', 60>() 
              << endl
              << testName << endl <<
                 separator<'-', 60>() << endl;

    MMapFile dasdb(RES_OPEN, fileName);
    Trie trie = dasdb.trie(TrieId);
    auto current = *trie;
    auto stats = current.stats(STATS_SAMPLING_FULL);
    if (NodeType != NullTerm) {
        BOOST_CHECK(stats.nodeStats[NodeType].nodeCount > 0);
    }

    const bool corrupted = corrupt<NodeType>(current, hint);
    BOOST_CHECK(corrupted);
    /* NullTerm indicates a non-corrupted trie */
    constexpr bool expected = 
        constant_if<is_null_node<NodeType>::value, bool, true, false>::value;

    TrieChecker checker(false);
    bool ok = checker(current);
    BOOST_CHECK_EQUAL(ok, expected);
   

    if (expected == false && ok == false) {
       auto corruption = checker.corruption();

       TrieRepair repair;
       ok = repair(current, corruption);
       if (ok) {
           cout << "Trie has been repaired !\n";
       } else {
           cout << "Failed to repair the trie\n";
       }

       ok = checker(current);
       if (ok) {
           cout << "Trie checked !\n";
       }
       else {
           cout << "Trie did not check!\n";
       }
    }

    //current.dump();
}

BOOST_FIXTURE_TEST_CASE( test_check_valid, CheckTestFixture )
{
    auto fn = [](uint64_t i) {
        return make_pair(i, i * 2);
    };
    build<uint64_t, uint64_t, 10000>(filename, fn);
    run_test<NullTerm>(filename, "Valid trie");
}

/* --------------------------------------
 *           COMPRESSED NODE
 * --------------------------------------
 */

BOOST_FIXTURE_TEST_CASE( test_compressed_node_corrupt_int_int_small_beg, CheckTestFixture ) 
{
    srand(time(NULL));
    auto fn = [](uint64_t i) {
        return make_pair(i, i);
    };
    build<uint64_t, uint64_t, 100>(filename, fn);
    run_test<CompressedTerm>(filename, "<uint64_t, uint64_t> | {CompressedNode} [Begin]",
                            Begin); 
}

BOOST_FIXTURE_TEST_CASE( test_compressed_node_corrupt_int_int_beg, CheckTestFixture ) 
{
    srand(time(NULL));
    auto fn = [](uint64_t i) {
        return make_pair(i, i);
    };
    build<uint64_t, uint64_t, 100000>(filename, fn);
    run_test<CompressedTerm>(filename, "<uint64_t, uint64_t> | {CompressedNode} [Begin]",
                            Begin); 
}

BOOST_FIXTURE_TEST_CASE( test_compressed_node_corrupt_int_int_mid, CheckTestFixture ) 
{
    srand(time(NULL));
    auto fn = [](uint64_t i) {
        return make_pair(i, i);
    };
    build<uint64_t, uint64_t, 100000>(filename, fn);
    run_test<CompressedTerm>(filename, "<uint64_t, uint64_t> | {CompressedNode} [Middle]",
                            Middle); 
}

BOOST_FIXTURE_TEST_CASE( test_compressed_node_corrupt_int_int_end, CheckTestFixture ) 
{
    srand(time(NULL));
    auto fn = [](uint64_t i) {
        return make_pair(i, i);
    };
    build<uint64_t, uint64_t, 100000>(filename, fn);
    run_test<CompressedTerm>(filename, "<uint64_t, uint64_t> | {CompressedNode} [End]",
                            End); 
}

BOOST_FIXTURE_TEST_CASE( test_compressed_node_corrupt_string_int, CheckTestFixture )
{
    auto fn = [](uint64_t i) {
        const auto str = randomString(2);
        return make_pair(str, i);
    };
    build<string, uint64_t, 100000>(filename, fn);
    run_test<CompressedTerm>(filename, "<string, uint64_t> | {CompressedNode} [Begin]",
                             Begin);
}

/* --------------------------------------
 *              SPARSE NODE
 * --------------------------------------
 */

BOOST_FIXTURE_TEST_CASE( test_sparse_node_corrupt_int_int_beg, CheckTestFixture ) 
{
    auto fn = [](uint64_t i) {
        uint64_t k = random();
        k = (k << 32) | random();
        return make_pair(k, i);
    };
    build<uint64_t, uint64_t, 100000>(filename, fn);
    run_test<SparseTerm>(filename, "<uint64_t, uint64_t> | {SparseNode} [Begin]",
                         Begin);
}

BOOST_FIXTURE_TEST_CASE( test_sparse_node_corrupt_int_int_mid, CheckTestFixture ) 
{
    auto fn = [](uint64_t i) {
        uint64_t k = random();
        k = (k << 32) | random();
        return make_pair(k, i);
    };
    build<uint64_t, uint64_t, 100000>(filename, fn);
    run_test<SparseTerm>(filename, "<uint64_t, uint64_t> | {SparseNode} [Middle]",
                         Middle);
}

BOOST_FIXTURE_TEST_CASE( test_sparse_node_corrupt_int_int_end, CheckTestFixture ) 
{
    auto fn = [](uint64_t i) {
        uint64_t k = random();
        k = (k << 32) | random();
        return make_pair(k, i);
    };
    build<uint64_t, uint64_t, 100000>(filename, fn);
    run_test<SparseTerm>(filename, "<uint64_t, uint64_t> | {SparseNode} [End]",
                         End);
}

BOOST_FIXTURE_TEST_CASE( test_sparse_node_corrupt_str_int_beg, CheckTestFixture ) 
{
    auto fn = [](uint64_t i) {
        const auto str = randomString(2);
        return make_pair(str, i);
    };
    build<string, uint64_t, 100000>(filename, fn);
    run_test<SparseTerm>(filename, "<string, uint64_t> | {SparseNode} [Begin]",
                         Begin);
}

BOOST_FIXTURE_TEST_CASE( test_sparse_node_corrupt_str_int_mid, CheckTestFixture ) 
{
    auto fn = [](uint64_t i) {
        const auto str = randomString(2);
        return make_pair(str, i);
    };
    build<string, uint64_t, 100000>(filename, fn);
    run_test<SparseTerm>(filename, "<string, uint64_t> | {SparseNode} [Middle]",
                         Middle);
}

BOOST_FIXTURE_TEST_CASE( test_sparse_node_corrupt_str_int_end, CheckTestFixture ) 
{
    auto fn = [](uint64_t i) {
        const auto str = randomString(2);
        return make_pair(str, i);
    };
    build<string, uint64_t, 100000>(filename, fn);
    run_test<SparseTerm>(filename, "<string, uint64_t> | {SparseNode} [End]",
                         End);
}

/* --------------------------------------
 *          DENSEBRANCHING NODE
 * --------------------------------------
 */

BOOST_FIXTURE_TEST_CASE( test_branching_node_corrupt_int_int_beg, CheckTestFixture )
{
    auto fn = [](uint64_t i) {
        uint64_t k = random() << 32 | random() >> 8;
        return make_pair(k, random() | i << 16);
    };
    build<uint64_t, uint64_t, 100000>(filename, fn);
    run_test<DenseBranch>(filename, "<uint64_t, uint64_t> | {DenseBranch} [Begin]");
}

BOOST_FIXTURE_TEST_CASE( test_branching_node_corrupt_int_int_mid, CheckTestFixture )
{
    auto fn = [](uint64_t i) {
        uint64_t k = random() << 32 | random() >> 8;
        return make_pair(k, random() | i << 16);
    };
    build<uint64_t, uint64_t, 100000>(filename, fn);
    run_test<DenseBranch>(filename, "<uint64_t, uint64_t> | {DenseBranch} [Middle]",
            Middle);
}

BOOST_FIXTURE_TEST_CASE( test_branching_node_corrupt_int_int_end, CheckTestFixture )
{
    auto fn = [](uint64_t i) {
        uint64_t k = random() << 32 | random() >> 8;
        return make_pair(k, random() | i << 16);
    };
    build<uint64_t, uint64_t, 100000>(filename, fn);
    run_test<DenseBranch>(filename, "<uint64_t, uint64_t> | {DenseBranch} [End]",
            End);
}
